import os
from typing import Optional, Literal, List, Dict, Any
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

import httpx
from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel, Field


# ============================================================
# APP
# ============================================================
app = FastAPI(title="PREVITA API", version="1.0.99")



# ============================================================
# ENV
# ============================================================
DATABASE_URL = os.environ.get("DATABASE_URL")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")  # grupo: -100xxxxxxxxxx

ALERT_API_KEY = os.environ.get("ALERT_API_KEY")  # se setar, protege /v1/notify/telegram/run


# ============================================================
# HELPERS
# ============================================================
def _require_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")


def get_conn():
    _require_db()
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _require_telegram():
    missing = []
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


def _check_key(x_api_key: Optional[str]):
    # Se ALERT_API_KEY não estiver setada, não bloqueia (modo debug)
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def send_telegram_message(text: str):
    _require_telegram()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Telegram error: {r.status_code} {r.text}")
        return r.json()


def get_last_snapshot_ts(cur) -> Optional[datetime]:
    cur.execute("SELECT MAX(snapshot_ts) AS mx FROM public.vitals_snapshot;")
    row = cur.fetchone()
    return row["mx"] if row and row["mx"] else None


# ============================================================
# MODELOS
# ============================================================
class VitalIn(BaseModel):
    cod_atendimento: int

    id_ricadpac: Optional[int] = None

    data_lanc: str  # "2026-01-31"
    hora: int = Field(ge=0, le=23)
    minuto: int = Field(ge=0, le=59)

    # vitais
    temp: Optional[float] = None
    pas: Optional[float] = None
    pad: Optional[float] = None
    fc: Optional[float] = None
    fr: Optional[float] = None
    spo2: Optional[float] = None
    dor: Optional[float] = None

    uso_o2: Optional[str] = None
    nivel_consciencia: Optional[str] = None
    profissional: Optional[str] = None

    # ISO: "2026-01-31T17:57:00"
    event_ts: datetime


class RecUpdateIn(BaseModel):
    status: Literal["NOVO", "EM_ATENDIMENTO", "RESOLVIDO"]
    handled_by: Optional[str] = None
    handled_note: Optional[str] = None


# ============================================================
# HEALTH
# ============================================================
@app.get("/health")
def health():
    return {"status": "ok"}


# ============================================================
# INGEST VITAIS  ✅ (essa é a rota que faltava)
# ============================================================
@app.post("/v1/vitals", status_code=201)
def ingest_vital(v: VitalIn):
    """
    Recebe vitais do Power Automate e grava em public.vitals_raw.
    Após gravar, roda pipeline automaticamente para alimentar snapshot/estado/tendência/recomendações/alertas.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1) grava raw
                cur.execute(
                    """
                    INSERT INTO public.vitals_raw
                      (event_ts, cod_atendimento, id_ricadpac, data_lanc, hora_lanc, minuto_lanc,
                       temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia,
                       profissional)
                    VALUES
                      (%s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s)
                    ON CONFLICT (cod_atendimento, data_lanc, hora_lanc, minuto_lanc)
                    DO UPDATE SET
                      event_ts = EXCLUDED.event_ts,
                      id_ricadpac = EXCLUDED.id_ricadpac,
                      temp = EXCLUDED.temp,
                      pas = EXCLUDED.pas,
                      pad = EXCLUDED.pad,
                      fc = EXCLUDED.fc,
                      fr = EXCLUDED.fr,
                      spo2 = EXCLUDED.spo2,
                      dor = EXCLUDED.dor,
                      uso_o2 = EXCLUDED.uso_o2,
                      nivel_consciencia = EXCLUDED.nivel_consciencia,
                      profissional = EXCLUDED.profissional,
                      updated_at = CURRENT_TIMESTAMP;
                    """,
                    (
                        v.event_ts,
                        v.cod_atendimento,
                        v.id_ricadpac,
                        v.data_lanc,
                        v.hora,
                        v.minuto,
                        v.temp,
                        v.pas,
                        v.pad,
                        v.fc,
                        v.fr,
                        v.spo2,
                        v.dor,
                        v.uso_o2,
                        v.nivel_consciencia,
                        v.profissional,
                    ),
                )

                # 2) pipeline automática
                pipe = run_pipeline_for_patient(cur, v.cod_atendimento)

            conn.commit()

        return {
            "ok": True,
            "message": "vital registrado + pipeline executada",
            "cod_atendimento": v.cod_atendimento,
            "pipeline": pipe,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ============================================================
# NOTIFY TELEGRAM (PRODUÇÃO)
# ============================================================
@app.post("/v1/notify/telegram/run")
async def notify_telegram_run(
    minutes_back: int = 180,
    max_send: int = 10,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    """
    Envia Telegram baseado em public.clinical_recommendations.
    Regra:
      - IMEDIATO → envia sempre (mesmo se já tiver notified_at)
      - PRIORIDADE → envia somente se notified_at IS NULL
    """
    _check_key(x_api_key)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id,
                      cod_atendimento,
                      snapshot_ts,
                      recommendation_level,
                      syndrome,
                      confidence,
                      actions
                    FROM public.clinical_recommendations
                    WHERE
                      created_at >= (NOW() - (%s || ' minutes')::interval)
                      AND recommendation_level IN ('IMEDIATO', 'PRIORIDADE')
                      AND (
                        recommendation_level = 'IMEDIATO'
                        OR notified_at IS NULL
                      )
                    ORDER BY
                      CASE recommendation_level WHEN 'IMEDIATO' THEN 2 ELSE 1 END DESC,
                      snapshot_ts DESC
                    LIMIT %s;
                    """,
                    (minutes_back, max_send),
                )
                rows = cur.fetchall()

                sent = 0
                for r in rows:
                    msg = (
                        f"🚨 <b>PREVITA ALERTA {r['recommendation_level']}</b>\n"
                        f"🧾 <b>Atendimento:</b> {r['cod_atendimento']}\n"
                        f"🕒 <b>Snapshot:</b> {r['snapshot_ts']}\n"
                        f"🧠 <b>Síndrome:</b> {(r['syndrome'] or '-')}\n"
                        f"📌 <b>Confiança:</b> {(r['confidence'] or '-')}\n\n"
                        f"✅ <b>Ações:</b>\n{(r['actions'] or '').strip()[:3500]}"
                    )

                    await send_telegram_message(msg)
                    sent += 1

                    # Marca notified_at apenas para PRIORIDADE (IMEDIATO continua podendo reenviar)
                    cur.execute(
                        """
                        UPDATE public.clinical_recommendations
                        SET
                          notified_at = CASE
                            WHEN recommendation_level = 'PRIORIDADE' THEN CURRENT_TIMESTAMP
                            ELSE notified_at
                          END,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (r["id"],),
                    )

            conn.commit()

        return {"ok": True, "found": len(rows), "sent": sent}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# PIPELINE - CÁLCULOS (regra clínica simplificada + segura)
# ============================================================

def _safe_float(x):
    try:
        return float(x) if x is not None else None
    except:
        return None

def _severity_from_vitals(v: Dict[str, Any]) -> Dict[str, Any]:
    """
    Regras robustas para triagem (não substitui score oficial; serve para detecção precoce + recomendação).
    Retorna:
      - level: ESTAVEL | ATENCAO | PRIORIDADE | IMEDIATO
      - flags: lista de achados
      - score: inteiro (heurístico)
    """
    temp = _safe_float(v.get("temp"))
    pas  = _safe_float(v.get("pas"))
    pad  = _safe_float(v.get("pad"))
    fc   = _safe_float(v.get("fc"))
    fr   = _safe_float(v.get("fr"))
    spo2 = _safe_float(v.get("spo2"))
    dor  = _safe_float(v.get("dor"))

    uso_o2 = (v.get("uso_o2") or "").strip().lower()
    consc  = (v.get("nivel_consciencia") or "").strip().lower()

    flags = []
    score = 0

    # Critérios IMEDIATO (ameaça imediata)
    if spo2 is not None and spo2 < 90:
        flags.append("SpO2 < 90% (hipoxemia grave)")
        score += 6
    if pas is not None and pas < 90:
        flags.append("PAS < 90 (hipotensão)")
        score += 6
    if fr is not None and fr > 30:
        flags.append("FR > 30 (taquipneia importante)")
        score += 5
    if fc is not None and fc > 140:
        flags.append("FC > 140 (taquicardia importante)")
        score += 5
    if consc in ("rebaixado", "confuso", "inconsciente", "alerta?" , "somnolento"):
        flags.append("Alteração de consciência relatada")
        score += 5

    # Critérios PRIORIDADE (alto risco)
    if spo2 is not None and 90 <= spo2 <= 92:
        flags.append("SpO2 90-92% (hipoxemia)")
        score += 3
    if pas is not None and 90 <= pas <= 100:
        flags.append("PAS 90-100 (limítrofe)")
        score += 2
    if fr is not None and 21 <= fr <= 30:
        flags.append("FR 21-30 (taquipneia)")
        score += 2
    if fc is not None and 111 <= fc <= 140:
        flags.append("FC 111-140 (taquicardia)")
        score += 2
    if temp is not None and (temp >= 39.0 or temp <= 35.0):
        flags.append("Temperatura crítica (>=39 ou <=35)")
        score += 3

    # ATENÇÃO (monitorar)
    if temp is not None and (38.0 <= temp < 39.0):
        flags.append("Febre (38-38.9)")
        score += 1
    if dor is not None and dor >= 7:
        flags.append("Dor intensa (>=7)")
        score += 1
    if uso_o2 in ("sim", "s", "true", "1"):
        flags.append("Em uso de O2")
        score += 1

    # Decide level
    if score >= 10:
        level = "IMEDIATO"
    elif score >= 5:
        level = "PRIORIDADE"
    elif score >= 2:
        level = "ATENCAO"
    else:
        level = "ESTAVEL"

    return {"level": level, "flags": flags, "score": score}


def _trend(prev: Optional[Dict[str, Any]], curr: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compara snapshot anterior vs atual e detecta piora/melhora.
    """
    if not prev:
        return {"trend": "NOVO", "worsened": False, "improved": False, "delta": {}}

    def d(key):
        a = _safe_float(prev.get(key))
        b = _safe_float(curr.get(key))
        return None if a is None or b is None else (b - a)

    delta = {
        "temp": d("temp"),
        "pas": d("pas"),
        "pad": d("pad"),
        "fc": d("fc"),
        "fr": d("fr"),
        "spo2": d("spo2"),
        "dor": d("dor"),
    }

    worsened = False
    improved = False

    # pioras clinicamente relevantes
    if delta["spo2"] is not None and delta["spo2"] <= -3:
        worsened = True
    if delta["pas"] is not None and delta["pas"] <= -10:
        worsened = True
    if delta["fc"] is not None and delta["fc"] >= 15:
        worsened = True
    if delta["fr"] is not None and delta["fr"] >= 5:
        worsened = True
    if delta["temp"] is not None and delta["temp"] >= 0.7:
        worsened = True

    # melhoras
    if delta["spo2"] is not None and delta["spo2"] >= 3:
        improved = True
    if delta["pas"] is not None and delta["pas"] >= 10:
        improved = True
    if delta["fc"] is not None and delta["fc"] <= -15:
        improved = True
    if delta["fr"] is not None and delta["fr"] <= -5:
        improved = True
    if delta["temp"] is not None and delta["temp"] <= -0.7:
        improved = True

    if worsened and not improved:
        t = "PIOROU"
    elif improved and not worsened:
        t = "MELHOROU"
    elif improved and worsened:
        t = "OSCILANTE"
    else:
        t = "ESTAVEL"

    return {"trend": t, "worsened": worsened, "improved": improved, "delta": delta}


def _recommendations(level: str, flags: List[str], trend: str) -> str:
    """
    Texto pronto (apoio a médico/enfermagem) – objetivo, acionável, sem exagero.
    """
    base = []
    base.append(f"• Tendência: {trend}")
    if flags:
        base.append("• Achados:")
        for f in flags[:12]:
            base.append(f"  - {f}")
    else:
        base.append("• Sem achados críticos no momento.")

    if level == "IMEDIATO":
        base.append("\n✅ Conduta sugerida (apoio):")
        base.append("1) Avaliação clínica imediata à beira-leito.")
        base.append("2) Checar via aérea, oxigenação e perfusão; repetir sinais vitais.")
        base.append("3) Considerar acionar médico responsável e equipe de resposta rápida conforme protocolo.")
        base.append("4) Revisar analgesia/sedação, sangramento, sinais de sepse, broncoaspiração, eventos agudos.")
    elif level == "PRIORIDADE":
        base.append("\n✅ Conduta sugerida (apoio):")
        base.append("1) Reavaliar paciente em até 10-15 min e repetir sinais vitais.")
        base.append("2) Revisar necessidade de O2 e metas de SpO2 conforme contexto clínico.")
        base.append("3) Se mantiver tendência de piora, escalar para avaliação médica.")
    elif level == "ATENCAO":
        base.append("\n✅ Orientações (apoio):")
        base.append("1) Manter monitorização e reavaliar em 30-60 min.")
        base.append("2) Verificar dor, febre, hidratação, ansiedade; checar posicionamento do sensor SpO2.")
    else:
        base.append("\n✅ Situação estável:")
        base.append("1) Manter rotina assistencial e monitorização conforme prescrição.")
        base.append("2) Registrar evolução e orientar paciente conforme protocolo.")

    return "\n".join(base)

# ============================================================
# PIPELINE - EXECUÇÃO (raw -> snapshot -> state/trend -> recommendations -> alerts)
# ============================================================

def _fetch_latest_raw(cur, cod_atendimento: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM public.vitals_raw
        WHERE cod_atendimento = %s
        ORDER BY event_ts DESC
        LIMIT 1;
        """,
        (cod_atendimento,),
    )
    return cur.fetchone()

def _fetch_prev_snapshot(cur, cod_atendimento: int, before_ts: datetime) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM public.vitals_snapshot
        WHERE cod_atendimento = %s
          AND snapshot_ts < %s
        ORDER BY snapshot_ts DESC
        LIMIT 1;
        """,
        (cod_atendimento, before_ts),
    )
    return cur.fetchone()

def _upsert_snapshot(cur, vraw: Dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO public.vitals_snapshot
          (snapshot_ts, cod_atendimento, id_ricadpac, temp, pas, pad, fc, fr, spo2, dor,
           uso_o2, nivel_consciencia, profissional, created_at, updated_at)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
           %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          id_ricadpac = EXCLUDED.id_ricadpac,
          temp = EXCLUDED.temp,
          pas  = EXCLUDED.pas,
          pad  = EXCLUDED.pad,
          fc   = EXCLUDED.fc,
          fr   = EXCLUDED.fr,
          spo2 = EXCLUDED.spo2,
          dor  = EXCLUDED.dor,
          uso_o2 = EXCLUDED.uso_o2,
          nivel_consciencia = EXCLUDED.nivel_consciencia,
          profissional = EXCLUDED.profissional,
          updated_at = CURRENT_TIMESTAMP;
        """,
        (
            vraw["event_ts"],
            vraw["cod_atendimento"],
            vraw.get("id_ricadpac"),
            vraw.get("temp"),
            vraw.get("pas"),
            vraw.get("pad"),
            vraw.get("fc"),
            vraw.get("fr"),
            vraw.get("spo2"),
            vraw.get("dor"),
            vraw.get("uso_o2"),
            vraw.get("nivel_consciencia"),
            vraw.get("profissional"),
        ),
    )

def _insert_or_update_state(cur, cod_atendimento: int, snapshot_ts: datetime, level: str, score: int) -> None:
    # Ajuste de nomes conforme sua tabela (se tiver campos diferentes, eu te adapto em 1 min)
    cur.execute(
        """
        INSERT INTO public.clinical_state
          (cod_atendimento, snapshot_ts, state_level, score, created_at, updated_at)
        VALUES
          (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          state_level = EXCLUDED.state_level,
          score = EXCLUDED.score,
          updated_at = CURRENT_TIMESTAMP;
        """,
        (cod_atendimento, snapshot_ts, level, score),
    )

def _insert_or_update_trend(cur, cod_atendimento: int, snapshot_ts: datetime, trend: str, delta: Dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO public.clinical_trends
          (cod_atendimento, snapshot_ts, trend, delta_json, created_at, updated_at)
        VALUES
          (%s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          trend = EXCLUDED.trend,
          delta_json = EXCLUDED.delta_json,
          updated_at = CURRENT_TIMESTAMP;
        """,
        (cod_atendimento, snapshot_ts, trend, psycopg.types.json.Json(delta)),
    )

def _insert_or_update_recommendation(
    cur,
    cod_atendimento: int,
    snapshot_ts: datetime,
    level: str,
    syndrome: Optional[str],
    confidence: Optional[str],
    actions: str,
) -> None:
    cur.execute(
        """
        INSERT INTO public.clinical_recommendations
          (cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions,
           created_at, updated_at)
        VALUES
          (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          recommendation_level = EXCLUDED.recommendation_level,
          syndrome = EXCLUDED.syndrome,
          confidence = EXCLUDED.confidence,
          actions = EXCLUDED.actions,
          updated_at = CURRENT_TIMESTAMP;
        """,
        (cod_atendimento, snapshot_ts, level, syndrome, confidence, actions),
    )

def _insert_alert_notification(cur, cod_atendimento: int, snapshot_ts: datetime, level: str) -> None:
    # Só registra alerta para níveis relevantes
    if level not in ("PRIORIDADE", "IMEDIATO"):
        return
    cur.execute(
        """
        INSERT INTO public.alert_notifications
          (cod_atendimento, snapshot_ts, level, channel, status, created_at, updated_at)
        VALUES
          (%s, %s, %s, 'TELEGRAM', 'PENDENTE', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (cod_atendimento, snapshot_ts, channel)
        DO NOTHING;
        """,
        (cod_atendimento, snapshot_ts, level),
    )

def run_pipeline_for_patient(cur, cod_atendimento: int) -> Dict[str, Any]:
    """
    Pipeline completa para 1 atendimento:
      - lê último raw
      - cria snapshot
      - compara com snapshot anterior
      - escreve state/trend/recommendations
      - registra alert_notification
    """
    vraw = _fetch_latest_raw(cur, cod_atendimento)
    if not vraw:
        return {"ok": False, "reason": "no raw vitals for cod_atendimento"}

    snapshot_ts = vraw["event_ts"]
    prev = _fetch_prev_snapshot(cur, cod_atendimento, snapshot_ts)

    # 1) snapshot
    _upsert_snapshot(cur, vraw)

    # 2) severidade / tendência
    sev = _severity_from_vitals(vraw)
    tr = _trend(prev, vraw)

    # 3) recomendação (texto pronto)
    actions = _recommendations(sev["level"], sev["flags"], tr["trend"])

    # síndrome/confidence (deixe neutro por enquanto; você pode plugar sua IA depois)
    syndrome = "Deterioração clínica (triagem assistencial)" if sev["level"] in ("PRIORIDADE", "IMEDIATO") else None
    confidence = "ALTA" if sev["score"] >= 8 else ("MEDIA" if sev["score"] >= 4 else "BAIXA")

    # 4) grava tabelas
    _insert_or_update_state(cur, cod_atendimento, snapshot_ts, sev["level"], sev["score"])
    _insert_or_update_trend(cur, cod_atendimento, snapshot_ts, tr["trend"], tr["delta"])
    _insert_or_update_recommendation(cur, cod_atendimento, snapshot_ts, sev["level"], syndrome, confidence, actions)
    _insert_alert_notification(cur, cod_atendimento, snapshot_ts, sev["level"])

    return {
        "ok": True,
        "cod_atendimento": cod_atendimento,
        "snapshot_ts": str(snapshot_ts),
        "level": sev["level"],
        "trend": tr["trend"],
        "score": sev["score"],
        "flags": sev["flags"],
    }





