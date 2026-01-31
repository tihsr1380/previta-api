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
app = FastAPI(title="PREVITA API", version="1.0.1")


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
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
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
            conn.commit()

        return {"ok": True, "message": "vital registrado", "cod_atendimento": v.cod_atendimento}

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
