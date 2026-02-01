import os
from typing import Optional, Literal, List, Dict, Any, Tuple
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

import httpx
from fastapi import FastAPI, HTTPException, Header
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
def _require_db() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")


def get_conn():
    _require_db()
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _require_telegram() -> None:
    missing = []
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


def _check_key(x_api_key: Optional[str]) -> None:
    # Se ALERT_API_KEY não estiver setada, não bloqueia (modo debug)
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _to_aware_utc(dt: datetime) -> datetime:
    # garante timezone UTC (timestamptz)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def send_telegram_message(text: str) -> Dict[str, Any]:
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
# PIPELINE (rules-based, alinhado às suas tabelas)
# ============================================================
def _compute_state_and_score(v: Dict[str, Any]) -> Tuple[str, int, str, List[str]]:
    """
    Retorna: (state, score, reason, flags)
    Score 0-100. Flags listam gatilhos críticos.
    """
    flags: List[str] = []
    reasons: List[str] = []
    score = 0

    temp = v.get("temp")
    pas = v.get("pas")
    fc = v.get("fc")
    fr = v.get("fr")
    spo2 = v.get("spo2")
    dor = v.get("dor")
    uso_o2 = (v.get("uso_o2") or "").strip().lower()
    consc = (v.get("nivel_consciencia") or "").strip().lower()

    # Oxigenação
    if spo2 is not None:
        if spo2 < 90:
            score += 40
            flags.append("SpO2<90")
            reasons.append("Hipoxemia importante (SpO2 < 90).")
        elif spo2 < 92:
            score += 25
            flags.append("SpO2<92")
            reasons.append("Hipoxemia (SpO2 < 92).")
        elif spo2 < 95:
            score += 10
            reasons.append("SpO2 limítrofe (<95).")

    if uso_o2 in ("sim", "s", "yes", "y", "true", "1"):
        score += 8
        reasons.append("Em uso de O2 suplementar.")

    # Pressão sistólica
    if pas is not None:
        if pas < 90:
            score += 35
            flags.append("PAS<90")
            reasons.append("Hipotensão (PAS < 90).")
        elif pas < 100:
            score += 15
            reasons.append("PAS limítrofe (<100).")
        elif pas >= 180:
            score += 20
            flags.append("PAS>=180")
            reasons.append("Crise hipertensiva (PAS >= 180).")

    # Frequência cardíaca
    if fc is not None:
        if fc >= 130:
            score += 25
            flags.append("FC>=130")
            reasons.append("Taquicardia importante (FC >= 130).")
        elif fc >= 110:
            score += 12
            reasons.append("Taquicardia (FC >= 110).")
        elif fc < 50:
            score += 20
            flags.append("FC<50")
            reasons.append("Bradicardia (FC < 50).")

    # Frequência respiratória
    if fr is not None:
        if fr >= 30:
            score += 25
            flags.append("FR>=30")
            reasons.append("Taquipneia importante (FR >= 30).")
        elif fr >= 22:
            score += 12
            reasons.append("Taquipneia (FR >= 22).")
        elif fr < 10:
            score += 20
            flags.append("FR<10")
            reasons.append("Bradipneia (FR < 10).")

    # Temperatura
    if temp is not None:
        if temp >= 39:
            score += 15
            reasons.append("Febre alta (>=39).")
        elif temp >= 38:
            score += 10
            reasons.append("Febre (>=38).")
        elif temp < 35:
            score += 20
            flags.append("T<35")
            reasons.append("Hipotermia (<35).")

    # Dor
    if dor is not None:
        if dor >= 7:
            score += 8
            reasons.append("Dor intensa (>=7).")
        elif dor >= 4:
            score += 4
            reasons.append("Dor moderada (>=4).")

    # Consciência
    if consc:
        if "alerta" not in consc and consc not in ("a", "alerta"):
            score += 15
            flags.append("Consciência alterada")
            reasons.append(f"Nível de consciência alterado ({consc}).")

    score = max(0, min(100, score))

    if score >= 55:
        state = "CRITICO"
    elif score >= 25:
        state = "ATENCAO"
    else:
        state = "ESTAVEL"

    reason = " ".join(reasons) if reasons else "Sem alterações relevantes nos vitais."
    return state, score, reason, flags


def _compute_trend(prev_score: Optional[int], curr_score: int) -> Tuple[str, int, str]:
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


def _recommendation_from(state: str, trend_state: str, flags: List[str]) -> Tuple[str, str, str, int, str]:
    """
    Retorna:
      (recommendation_level, recommendation_text, syndrome, confidence_int, actions_text)
    """
    syndrome = "Deterioração clínica (rule-based)"
    confidence = 75

    # IMEDIATO quando crítico OU piora com gatilho importante
    critical_flags = {"SpO2<90", "PAS<90", "FR>=30", "FC>=130", "Consciência alterada"}
    if state == "CRITICO" or ("PIORA" in trend_state and any(f in critical_flags for f in flags)):
        level = "IMEDIATO"
        recommendation = "Risco alto de deterioração clínica. Requer avaliação imediata."
        actions = (
            "1) Avaliar paciente imediatamente (ABCDE).\n"
            "2) Confirmar sinais vitais e qualidade do sensor.\n"
            "3) Respiração: oferta de O2 conforme protocolo; avaliar esforço respiratório.\n"
            "4) Circulação: PA/perfusão; acesso venoso; considerar fluidos conforme cenário clínico.\n"
            "5) Reavaliar em 5–10 min ou antes se piora.\n"
            "6) Acionar médico responsável e considerar time de resposta rápida."
        )
        confidence = 85
        return level, recommendation, syndrome, confidence, actions

    # PRIORIDADE quando atenção + piora
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

    # Estável: não cria alerta
    level = "PRIORIDADE"
    recommendation = "Paciente estável no último snapshot. Manter monitorização conforme rotina."
    actions = "1) Seguir plano assistencial.\n2) Reavaliar conforme periodicidade.\n3) Registrar achados relevantes."
    confidence = 65
    return level, recommendation, syndrome, confidence, actions


def run_pipeline_for_patient(cur, cod_atendimento: int) -> Dict[str, Any]:
    """
    1) cria/atualiza snapshot baseado no último registro de vitals_raw
    2) calcula clinical_state
    3) calcula clinical_trends vs score anterior
    4) cria/atualiza clinical_recommendations quando necessário
    """
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

    snapshot_ts = last["event_ts"]  # usado como snapshot_ts

    # snapshot (upsert)
    cur.execute(
        """
        INSERT INTO public.vitals_snapshot
          (cod_atendimento, snapshot_ts, id_ricadpac, temp, pas, pad, fc, fr, spo2, dor,
           uso_o2, nivel_consciencia, profissional, updated_at)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        ON CONFLICT (cod_atendimento, snapshot_ts)
        DO UPDATE SET
          id_ricadpac=EXCLUDED.id_ricadpac,
          temp=EXCLUDED.temp,
          pas=EXCLUDED.pas,
          pad=EXCLUDED.pad,
          fc=EXCLUDED.fc,
          fr=EXCLUDED.fr,
          spo2=EXCLUDED.spo2,
          dor=EXCLUDED.dor,
          uso_o2=EXCLUDED.uso_o2,
          nivel_consciencia=EXCLUDED.nivel_consciencia,
          profissional=EXCLUDED.profissional,
          updated_at=CURRENT_TIMESTAMP;
        """,
        (
            cod_atendimento,
            snapshot_ts,
            last.get("id_ricadpac"),
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
        ),
    )

    # score anterior para trend
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

    # clinical_state (upsert)
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

    # clinical_trends (upsert)
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

    # só cria recomendação "alertável" quando:
    should_create = (state == "CRITICO") or (state == "ATENCAO" and trend_state in ("PIORA", "PIORA_LEVE"))

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
            "SELECT id FROM public.clinical_recommendations WHERE cod_atendimento=%s AND snapshot_ts=%s LIMIT 1;",
            (cod_atendimento, snapshot_ts),
        )
        row = cur.fetchone()
        reco_id = row["id"] if row else None

    return {
        "ok": True,
        "cod_atendimento": cod_atendimento,
        "snapshot_ts": snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else str(snapshot_ts),
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
# HEALTH
# ============================================================
@app.get("/health")
def health():
    return {"status": "ok"}


# ============================================================
# INGEST VITAIS (Power Automate) + pipeline automática
# ============================================================
@app.post("/v1/vitals", status_code=201)
async def ingest_vital(v: VitalIn):
    """
    Recebe vitais do Power Automate e grava em public.vitals_raw.
    Após gravar, roda pipeline automaticamente.
    Se gerar recomendação IMEDIATO, envia Telegram na hora + registra auditoria em alert_notifications.
    """
    try:
        # 1) grava RAW + pipeline (mesma transação)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.vitals_raw
                      (event_ts, cod_atendimento, id_ricadpac, data_lanc, hora_lanc, minuto_lanc,
                       temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia, profissional)
                    VALUES
                      (%s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

                pipe = run_pipeline_for_patient(cur, v.cod_atendimento)

            conn.commit()

        # 2) Se IMEDIATO, envia Telegram imediatamente
        telegram_sent = False
        telegram_response: Optional[Dict[str, Any]] = None

        if pipe.get("created_recommendation") and pipe.get("recommendation_level") == "IMEDIATO":
            with get_conn() as conn2:
                with conn2.cursor() as cur2:
                    cur2.execute(
                        """
                        SELECT id, cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions
                        FROM public.clinical_recommendations
                        WHERE cod_atendimento=%s AND snapshot_ts=%s
                        LIMIT 1;
                        """,
                        (v.cod_atendimento, datetime.fromisoformat(pipe["snapshot_ts"])),
                    )
                    r = cur2.fetchone()

            if r:
                msg = (
                    f"🚨 <b>PREVITA ALERTA {r['recommendation_level']}</b>\n"
                    f"🧾 <b>Atendimento:</b> {r['cod_atendimento']}\n"
                    f"🕒 <b>Snapshot:</b> {r['snapshot_ts']}\n"
                    f"🧠 <b>Síndrome:</b> {(r['syndrome'] or '-')}\n"
                    f"📌 <b>Confiança:</b> {(r['confidence'] or '-')}\n\n"
                    f"✅ <b>Ações:</b>\n{(r['actions'] or '').strip()[:3500]}"
                )

                telegram_response = await send_telegram_message(msg)
                telegram_sent = True

                # auditoria do envio
                with get_conn() as conn3:
                    with conn3.cursor() as cur3:
                        cur3.execute(
                            """
                            INSERT INTO public.alert_notifications
                              (cod_atendimento, snapshot_ts, channel, sent_at, status, response)
                            VALUES
                              (%s, %s, %s, NOW(), %s, %s)
                            ON CONFLICT (cod_atendimento, snapshot_ts, channel)
                            DO UPDATE SET sent_at=EXCLUDED.sent_at, status=EXCLUDED.status, response=EXCLUDED.response;
                            """,
                            (
                                r["cod_atendimento"],
                                _to_aware_utc(r["snapshot_ts"]),
                                "telegram",
                                "SENT",
                                str(telegram_response)[:4000],
                            ),
                        )
                    conn3.commit()

        return {
            "ok": True,
            "message": "vital registrado + pipeline executada",
            "cod_atendimento": v.cod_atendimento,
            "pipeline": pipe,
            "telegram_sent": telegram_sent,
            "telegram_response": telegram_response,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# NOTIFY TELEGRAM (scheduler GitHub Actions)
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

                    tg = await send_telegram_message(msg)
                    sent += 1

                    # auditoria do envio
                    cur.execute(
                        """
                        INSERT INTO public.alert_notifications
                          (cod_atendimento, snapshot_ts, channel, sent_at, status, response)
                        VALUES
                          (%s, %s, %s, NOW(), %s, %s)
                        ON CONFLICT (cod_atendimento, snapshot_ts, channel)
                        DO UPDATE SET sent_at=EXCLUDED.sent_at, status=EXCLUDED.status, response=EXCLUDED.response;
                        """,
                        (
                            r["cod_atendimento"],
                            _to_aware_utc(r["snapshot_ts"]),
                            "telegram",
                            "SENT",
                            str(tg)[:4000],
                        ),
                    )

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
# DEBUG: rodar pipeline manualmente para 1 atendimento
# ============================================================
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
