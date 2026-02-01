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


from datetime import timezone

def _to_aware_utc(dt: datetime) -> datetime:
    # garante timezone UTC para alert_notifications (timestamptz)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _compute_state_and_score(v: Dict[str, Any]) -> tuple[str, int, str, List[str]]:
    """
    Retorna: (state, score, reason, flags)
    Score 0-100. Flags listam os gatilhos críticos.
    """
    flags = []
    score = 0
    reasons = []

    temp = v.get("temp")
    pas = v.get("pas")
    pad = v.get("pad")
    fc  = v.get("fc")
    fr  = v.get("fr")
    spo2 = v.get("spo2")
    dor = v.get("dor")
    uso_o2 = (v.get("uso_o2") or "").strip().lower()
    consc = (v.get("nivel_consciencia") or "").strip().lower()

    # Oxigenação
    if spo2 is not None:
        if spo2 < 90:
            score += 40; flags.append("SpO2<90"); reasons.append("Hipoxemia importante (SpO2 < 90).")
        elif spo2 < 92:
            score += 25; flags.append("SpO2<92"); reasons.append("Hipoxemia (SpO2 < 92).")
        elif spo2 < 95:
            score += 10; reasons.append("SpO2 limítrofe (<95).")

    if uso_o2 in ("sim", "s", "yes", "y", "true", "1"):
        score += 8
        reasons.append("Em uso de O2 suplementar.")

    # Pressão
    if pas is not None:
        if pas < 90:
            score += 35; flags.append("PAS<90"); reasons.append("Hipotensão (PAS < 90).")
        elif pas < 100:
            score += 15; reasons.append("PAS limítrofe (<100).")
        elif pas >= 180:
            score += 20; flags.append("PAS>=180"); reasons.append("Crise hipertensiva (PAS >= 180).")

    # FC
    if fc is not None:
        if fc >= 130:
            score += 25; flags.append("FC>=130"); reasons.append("Taquicardia importante (FC >= 130).")
        elif fc >= 110:
            score += 12; reasons.append("Taquicardia (FC >= 110).")
        elif fc < 50:
            score += 20; flags.append("FC<50"); reasons.append("Bradicardia (FC < 50).")

    # FR
    if fr is not None:
        if fr >= 30:
            score += 25; flags.append("FR>=30"); reasons.append("Taquipneia importante (FR >= 30).")
        elif fr >= 22:
            score += 12; reasons.append("Taquipneia (FR >= 22).")
        elif fr < 10:
            score += 20; flags.append("FR<10"); reasons.append("Bradipneia (FR < 10).")

    # Temperatura
    if temp is not None:
        if temp >= 39:
            score += 15; reasons.append("Febre alta (>=39).")
        elif temp >= 38:
            score += 10; reasons.append("Febre (>=38).")
        elif temp < 35:
            score += 20; flags.append("T<35"); reasons.append("Hipotermia (<35).")

    # Dor
    if dor is not None:
        if dor >= 7:
            score += 8; reasons.append("Dor intensa (>=7).")
        elif dor >= 4:
            score += 4; reasons.append("Dor moderada (>=4).")

    # Consciência (se você padronizar valores depois, melhora)
    if consc:
        if "alerta" not in consc and consc not in ("a", "alerta"):
            score += 15
            flags.append("Consciência alterada")
            reasons.append(f"Nível de consciência alterado ({consc}).")

    # Normaliza score e estado
    score = max(0, min(100, score))
    if score >= 55:
        state = "CRITICO"
    elif score >= 25:
        state = "ATENCAO"
    else:
        state = "ESTAVEL"

    reason = " ".join(reasons) if reasons else "Sem alterações relevantes nos vitais."
    return state, score, reason, flags


def _compute_trend(prev_score: Optional[int], curr_score: int) -> tuple[str, int, str]:
    if prev_score is None:
        return "SEM_BASE", 0, "Primeiro registro do paciente (sem comparação)."
    delta = curr_score - prev_score
    if delta >= 15:
        return "PIORA", delta, f"Piora significativa (+{delta} pontos)."
    if delta >= 5:
        return "PIORA_LEVE", delta, f"Piora leve (+{delta} pontos)."
    if delta <= -15:
        return "MELHORA", delta, f"Melhora significativa ({delta} pontos)."
    if delta <= -5:
        return "MELHORA_LEVE", delta, f"Melhora leve ({delta} pontos)."
    return "ESTAVEL", delta, f"Sem mudança relevante ({delta} pontos)."


def _recommendation_from(state: str, trend_state: str, flags: List[str]) -> tuple[str, str, str, int, str]:
    """
    Retorna:
      (recommendation_level, recommendation_text, syndrome, confidence_int, actions_text)
    """
    syndrome = "Deterioração clínica (rule-based)"
    confidence = 75

    # IMEDIATO quando crítico OU piora significativa com gatilho importante
    if state == "CRITICO" or ("PIORA" in trend_state and any(f in flags for f in ("SpO2<90","PAS<90","FR>=30","FC>=130","Consciência alterada"))):
        level = "IMEDIATO"
        recommendation = "Risco alto de deterioração clínica. Requer avaliação imediata."
        actions = (
            "1) Avaliar paciente imediatamente (ABCDE).\n"
            "2) Confirmar sinais vitais e qualidade do sensor.\n"
            "3) Checar via aérea/respiração: oferta de O2 conforme protocolo, ausculta, trabalho respiratório.\n"
            "4) Circulação: PA, perfusão, acesso venoso, considerar fluidos conforme cenário clínico.\n"
            "5) Reavaliar em 5–10 min ou antes se piora.\n"
            "6) Acionar médico responsável e considerar time de resposta rápida."
        )
        confidence = 85
        return level, recommendation, syndrome, confidence, actions

    # PRIORIDADE quando atenção + piora leve/moderada
    if state == "ATENCAO" and trend_state in ("PIORA", "PIORA_LEVE"):
        level = "PRIORIDADE"
        recommendation = "Sinais de alerta com tendência de piora. Monitorar de perto e intervir precocemente."
        actions = (
            "1) Repetir sinais vitais em 15–30 min.\n"
            "2) Revisar dor, sedação/consciência e necessidade de O2.\n"
            "3) Garantir hidratação/analgesia conforme prescrição.\n"
            "4) Se persistir tendência de piora, escalar para avaliação médica."
        )
        confidence = 78
        return level, recommendation, syndrome, confidence, actions

    # Estável: não precisa gerar recomendação “alertável”
    level = "PRIORIDADE" if state == "ATENCAO" else "PRIORIDADE"
    recommendation = "Paciente estável no último snapshot. Manter monitorização conforme rotina."
    actions = "1) Seguir plano assistencial.\n2) Reavaliar conforme periodicidade.\n3) Registrar achados relevantes."
    confidence = 65
    return level, recommendation, syndrome, confidence, actions


def run_pipeline_for_patient(cur, cod_atendimento: int) -> Dict[str, Any]:
    """
    1) cria/atualiza snapshot baseado no último registro de vitals_raw
    2) calcula clinical_state
    3) calcula clinical_trends vs snapshot anterior
    4) cria/atualiza clinical_recommendations
    Retorna um dict para debug.
    """
    # pega o último vital do paciente
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
    last = cur.fetchone()
    if not last:
        return {"ok": False, "reason": "sem vitals_raw"}

    snapshot_ts = last["event_ts"]  # timestamp sem tz no snapshot/trends/state
    # snapshot (upsert)
    cur.execute(
        """
        INSERT INTO public.vitals_snapshot
          (cod_atendimento, snapshot_ts, temp, pas, pad, fc, fr, spo2, dor,
           uso_o2, nivel_consciencia, profissional, id_ricadpac, created_at)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          temp=EXCLUDED.temp, pas=EXCLUDED.pas, pad=EXCLUDED.pad,
          fc=EXCLUDED.fc, fr=EXCLUDED.fr, spo2=EXCLUDED.spo2, dor=EXCLUDED.dor,
          uso_o2=EXCLUDED.uso_o2, nivel_consciencia=EXCLUDED.nivel_consciencia,
          profissional=EXCLUDED.profissional, id_ricadpac=EXCLUDED.id_ricadpac;
        """,
        (
            cod_atendimento,
            snapshot_ts,
            last.get("temp"),
            last.get("pas"),
            last.get("pad"),
            last.get("fc"),
            last.get("fr"),
            last.get("spo2"),
            last.get("dor"),
            last.get("uso_o2"),
            last.get("nivel_consciencia"),
            last.get("profissional"),
            last.get("id_ricadpac"),
        ),
    )

    # buscar score anterior para trend
    cur.execute(
        """
        SELECT state_score
        FROM public.clinical_state
        WHERE cod_atendimento=%s AND snapshot_ts < %s
        ORDER BY snapshot_ts DESC
        LIMIT 1;
        """,
        (cod_atendimento, snapshot_ts),
    )
    prev = cur.fetchone()
    prev_score = prev["state_score"] if prev else None

    state, state_score, state_reason, flags = _compute_state_and_score(last)
    trend_state, trend_score, trend_reason = _compute_trend(prev_score, state_score)

    # upsert clinical_state
    cur.execute(
        """
        INSERT INTO public.clinical_state
          (cod_atendimento, id_ricadpac, snapshot_ts, state, state_score, state_reason, created_at)
        VALUES
          (%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          id_ricadpac=EXCLUDED.id_ricadpac,
          state=EXCLUDED.state,
          state_score=EXCLUDED.state_score,
          state_reason=EXCLUDED.state_reason;
        """,
        (cod_atendimento, last.get("id_ricadpac"), snapshot_ts, state, state_score, state_reason),
    )

    # upsert clinical_trends
    cur.execute(
        """
        INSERT INTO public.clinical_trends
          (cod_atendimento, snapshot_ts, trend_state, trend_score, trend_reason, created_at)
        VALUES
          (%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          trend_state=EXCLUDED.trend_state,
          trend_score=EXCLUDED.trend_score,
          trend_reason=EXCLUDED.trend_reason;
        """,
        (cod_atendimento, snapshot_ts, trend_state, trend_score, trend_reason),
    )

    # recommendation
    level, reco_text, syndrome, confidence, actions = _recommendation_from(state, trend_state, flags)

    # regra: só cria recomendação alertável quando há risco (ATENCAO+PIORA ou CRITICO)
    should_create = (state == "CRITICO") or (state == "ATENCAO" and trend_state in ("PIORA","PIORA_LEVE"))

    reco_id = None
    if should_create:
        cur.execute(
            """
            INSERT INTO public.clinical_recommendations
              (cod_atendimento, snapshot_ts, state, trend_state, recommendation_level,
               recommendation, syndrome, actions, confidence, created_at, status, updated_at)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),'NOVO',NOW())
            ON CONFLICT (cod_atendimento, snapshot_ts)
            DO UPDATE SET
              state=EXCLUDED.state,
              trend_state=EXCLUDED.trend_state,
              recommendation_level=EXCLUDED.recommendation_level,
              recommendation=EXCLUDED.recommendation,
              syndrome=EXCLUDED.syndrome,
              actions=EXCLUDED.actions,
              confidence=EXCLUDED.confidence,
              updated_at=NOW();
            """,
            (
                cod_atendimento,
                snapshot_ts,
                state,
                trend_state,
                level,
                reco_text,
                syndrome,
                actions,
                confidence,
            ),
        )
        cur.execute(
            "SELECT id FROM public.clinical_recommendations WHERE cod_atendimento=%s AND snapshot_ts=%s;",
            (cod_atendimento, snapshot_ts),
        )
        rid = cur.fetchone()
        reco_id = rid["id"] if rid else None

    return {
        "ok": True,
        "cod_atendimento": cod_atendimento,
        "snapshot_ts": str(snapshot_ts),
        "state": state,
        "state_score": state_score,
        "trend_state": trend_state,
        "trend_score": trend_score,
        "flags": flags,
        "created_recommendation": bool(should_create),
        "recommendation_level": level if should_create else None,
        "recommendation_id": reco_id,
    }



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
  "pipeline": pipe
}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

with conn.cursor() as cur:
    # ... seu INSERT vitals_raw ...
    pipe = run_pipeline_for_patient(cur, v.cod_atendimento)
conn.commit()

return {
  "ok": True,
  "message": "vital registrado + pipeline executada",
  "cod_atendimento": v.cod_atendimento,
  "pipeline": pipe
}


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

@app.post("/v1/pipeline/run")
def pipeline_run(cod_atendimento: int):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                out = run_pipeline_for_patient(cur, cod_atendimento)
            conn.commit()
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))







