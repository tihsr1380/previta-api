# =========================
# PREVITA API — MAIN (v2.1 - FIXED)
# =========================
import os
import logging
from typing import Optional, List, Any, Dict
from datetime import datetime, time

import requests
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field

# =========================
# LOGGING (Render-friendly)
# =========================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("previta-api")

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="2.1.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    # Se ALERT_API_KEY não estiver setada, libera (modo dev)
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# TELEGRAM
# =========================
def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return  # opcional: não quebra produção se não configurar

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            logger.warning("Telegram sendMessage failed: %s - %s", r.status_code, r.text[:300])
    except Exception as e:
        # não derruba ingestão por falha de notificação
        logger.warning("Telegram sendMessage exception: %s", str(e))

# =========================
# MODELS
# =========================
class VitalRow(BaseModel):
    cod_atendimento: int
    id_ricadpac: Optional[int] = None

    # timestamp do evento (vem do PowerBI)
    event_ts: datetime

    # campos numéricos (aceitam null)
    hora: Optional[int] = None
    minuto: Optional[int] = None
    temp: Optional[float] = None
    pas: Optional[int] = None
    pad: Optional[int] = None
    fc: Optional[int] = None
    fr: Optional[int] = None
    spo2: Optional[int] = None

    # campos texto
    dor: Optional[str] = None
    uso_o2: Optional[str] = None
    nivel_consciencia: Optional[str] = None
    profissional: Optional[str] = None


class VitalBatch(BaseModel):
    rows: List[VitalRow] = Field(default_factory=list)

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# INIT TABLES (helper)
# =========================
def ensure_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vitals_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_key TEXT UNIQUE,
                    cod_atendimento INT NOT NULL,
                    id_ricadpac INT NULL,
                    event_ts TIMESTAMP NOT NULL,
                    hora INT NULL,
                    minuto INT NULL,
                    temp DOUBLE PRECISION NULL,
                    pas INT NULL,
                    pad INT NULL,
                    fc INT NULL,
                    fr INT NULL,
                    spo2 INT NULL,
                    dor TEXT NULL,
                    uso_o2 TEXT NULL,
                    nivel_consciencia TEXT NULL,
                    profissional TEXT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_vitals_events_ts
                    ON public.vitals_events (event_ts DESC);

                CREATE TABLE IF NOT EXISTS public.clinical_recommendations (
                    id BIGSERIAL PRIMARY KEY,
                    cod_atendimento INT NOT NULL,
                    snapshot_ts TIMESTAMP NOT NULL,
                    recommendation_level TEXT NOT NULL, -- IMEDIATO | PRIORIDADE | OK
                    syndrome TEXT NULL,
                    confidence DOUBLE PRECISION NULL,
                    actions TEXT NULL,
                    notified_at TIMESTAMP NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_clinrec_att_ts
                    ON public.clinical_recommendations (cod_atendimento, snapshot_ts DESC);
            """)
        conn.commit()

@app.on_event("startup")
def _startup():
    ensure_tables()
    logger.info("Startup OK - tables ensured.")

# =========================
# LAST EVENT TS
# =========================
@app.get("/v1/vitals/last_event_ts")
def last_event_ts(x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(event_ts) AS last_ts FROM public.vitals_events;")
            row = cur.fetchone()
    last_ts = row["last_ts"]
    return {"last_event_ts": last_ts.isoformat() if last_ts else "1970-01-01T00:00:00"}

# =========================
# PIPELINE (auto)
# =========================
def compute_recommendations_for_batch(rows: List[VitalRow]) -> List[Dict[str, Any]]:
    """
    Pipeline simples e extensível:
    - regra de triagem básica + tendência (placeholder)
    - você pode evoluir aqui para modelos e lógicas avançadas depois
    """
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = "OK"
        syndrome = None
        confidence = None
        actions = None

        # Exemplo de gatilhos (ajuste conforme protocolo do hospital)
        # (isso não substitui avaliação clínica; é suporte)
        if r.spo2 is not None and r.spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.7
            actions = (
                "Reavaliar oximetria, verificar O2, checar sinais de desconforto respiratório "
                "e acionar enfermagem/médico conforme protocolo."
            )

        if r.pas is not None and r.pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.8
            actions = (
                "Checar PA manual, perfusão, sangramento/dor, acionar médico imediatamente conforme protocolo."
            )

        if level != "OK":
            recs.append(
                {
                    "cod_atendimento": r.cod_atendimento,
                    "snapshot_ts": r.event_ts,
                    "recommendation_level": level,
                    "syndrome": syndrome,
                    "confidence": confidence,
                    "actions": actions,
                }
            )
    return recs

def persist_recommendations_and_notify(recs: List[Dict[str, Any]]):
    if not recs:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for rec in recs:
                cur.execute(
                    """
                    INSERT INTO public.clinical_recommendations
                        (cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP);
                    """,
                    (
                        rec["cod_atendimento"],
                        rec["snapshot_ts"],
                        rec["recommendation_level"],
                        rec.get("syndrome"),
                        rec.get("confidence"),
                        rec.get("actions"),
                    ),
                )

                # Notifica IMEDIATO sempre
                if rec["recommendation_level"] == "IMEDIATO":
                    msg = (
                        f"🚨 <b>PREVITA – ALERTA IMEDIATO</b>\n\n"
                        f"🧾 <b>Atendimento:</b> {rec['cod_atendimento']}\n"
                        f"🕒 <b>Snapshot:</b> {rec['snapshot_ts']}\n"
                        f"🧠 <b>Síndrome:</b> {rec.get('syndrome') or '-'}\n"
                        f"✅ <b>Ações:</b>\n{(rec.get('actions') or '-').strip()[:3500]}"
                    )
                    send_telegram_message_sync(msg)

        conn.commit()

# =========================
# NORMALIZAÇÃO SÊNIOR DO TIMESTAMP
# =========================
def normalize_event_ts(r: VitalRow) -> datetime:
    """
    Corrige o principal problema real do PowerBI:
    - Muitas vezes event_ts vem como 00:00:00 (somente data)
    - Mas hora/minuto vêm separados
    Então reconstruímos data+hora+minuto para ter granularidade.
    """
    event_ts = r.event_ts

    # Se veio só a data (00:00:00) e hora/minuto existem -> reconstruir
    if event_ts is not None and event_ts.time() == time(0, 0):
        if r.hora is not None or r.minuto is not None:
            hh = int(r.hora or 0)
            mm = int(r.minuto or 0)
            event_ts = datetime(event_ts.year, event_ts.month, event_ts.day, hh, mm, 0)

    return event_ts

def build_event_key(cod_atendimento: int, event_ts: datetime) -> str:
    """
    event_key forte e estável:
    - cod_atendimento + timestamp por minuto
    - evita duplicidade de múltiplas linhas do mesmo dia
    - impede crescimento infinito por milissegundos
    """
    return f"{cod_atendimento}|{event_ts.strftime('%Y-%m-%d %H:%M')}"

# =========================
# INGEST — BATCH
# =========================
@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: VitalBatch,
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    if not payload.rows:
        return {"ok": True, "inserted": 0, "conflicts": 0, "message": "Sem linhas"}

    inserted = 0
    conflicts = 0

    # Preparar dados
    rows_prepared = []
    for r in payload.rows:
        event_ts = normalize_event_ts(r)
        event_key = build_event_key(r.cod_atendimento, event_ts)

        rows_prepared.append(
            (
                event_key,
                r.cod_atendimento,
                r.id_ricadpac,
                event_ts,
                r.hora,
                r.minuto,
                r.temp,
                r.pas,
                r.pad,
                r.fc,
                r.fr,
                r.spo2,
                r.dor,
                r.uso_o2,
                r.nivel_consciencia,
                r.profissional,
            )
        )

    # Insert em transação única
    insert_sql = """
        INSERT INTO public.vitals_events (
            event_key, cod_atendimento, id_ricadpac, event_ts,
            hora, minuto, temp, pas, pad, fc, fr, spo2,
            dor, uso_o2, nivel_consciencia, profissional
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_key) DO NOTHING;
    """

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for params in rows_prepared:
                    cur.execute(insert_sql, params)
                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        conflicts += 1
            conn.commit()

    except Exception as e:
        logger.exception("DB insert failed")
        raise HTTPException(status_code=500, detail=f"DB insert failed: {str(e)}")

    # roda pipeline automática em background (não trava o Power Automate)
    recs = compute_recommendations_for_batch(payload.rows)
    background.add_task(persist_recommendations_and_notify, recs)

    logger.info(
        "Batch received=%d inserted=%d conflicts=%d recs=%d",
        len(payload.rows), inserted, conflicts, len(recs)
    )

    return {
        "ok": True,
        "received": len(payload.rows),
        "inserted": inserted,
        "conflicts": conflicts,
        "recs_generated": len(recs),
    }

# =========================
# NOTIFY TELEGRAM — PRODUÇÃO (mantido)
# =========================
@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    minutes_back: int = 180,
    max_send: int = 10,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
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
                        recommendation_level IN ('IMEDIATO', 'PRIORIDADE')
                        AND (
                            recommendation_level = 'IMEDIATO'
                            OR notified_at IS NULL
                        )
                    ORDER BY
                        CASE recommendation_level
                            WHEN 'IMEDIATO' THEN 2
                            ELSE 1
                        END DESC,
                        snapshot_ts DESC
                    LIMIT %s;
                    """,
                    (max_send,),
                )

                rows = cur.fetchall()
                sent = 0

                for r in rows:
                    msg = (
                        f"🚨 <b>PREVITA – ALERTA {r['recommendation_level']}</b>\n\n"
                        f"🧾 <b>Atendimento:</b> {r['cod_atendimento']}\n"
                        f"🕒 <b>Snapshot:</b> {r['snapshot_ts']}\n"
                        f"🧠 <b>Síndrome:</b> {r['syndrome'] or '-'}\n"
                        f"📊 <b>Confiança:</b> {r['confidence'] or '-'}\n\n"
                        f"✅ <b>Ações:</b>\n{(r['actions'] or '').strip()[:3500]}"
                    )

                    send_telegram_message_sync(msg)

                    cur.execute(
                        """
                        UPDATE public.clinical_recommendations
                        SET
                            notified_at = CASE
                                WHEN recommendation_level = 'PRIORIDADE'
                                THEN CURRENT_TIMESTAMP
                                ELSE notified_at
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (r["id"],),
                    )

                    sent += 1

            conn.commit()

        return {"ok": True, "found": len(rows), "sent": sent}

    except Exception as e:
        logger.exception("notify_telegram_run failed")
        raise HTTPException(status_code=500, detail=str(e))
