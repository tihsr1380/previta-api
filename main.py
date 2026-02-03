# =========================
# PREVITA API — MAIN (v3.1) - FAST + SAFE
# - responde rápido (202) e grava em background
# - pool + timeouts (não trava no Neon/DB)
# - upsert com merge (não perde campos)
# =========================

import os
from typing import Optional, List, Any, Dict
from datetime import datetime, date, time

import requests
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field, ConfigDict

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.1.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

# =========================
# DB POOL
# =========================
POOL: Optional[ConnectionPool] = None

def _raise_if_no_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")

def get_conn():
    """
    Retorna conexão do pool.
    connect_timeout e statement_timeout garantem que não trava.
    """
    global POOL
    _raise_if_no_db()
    if POOL is None:
        raise RuntimeError("DB pool não inicializado")
    return POOL.connection()

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# TELEGRAM
# =========================
def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass

# =========================
# MODELS
# =========================
class VitalRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cod_atendimento: int
    id_ricadpac: Optional[int] = None

    # timestamp do evento (vem do PowerBI)
    event_ts: datetime

    hora: Optional[int] = None
    minuto: Optional[int] = None
    temp: Optional[float] = None
    pas: Optional[int] = None
    pad: Optional[int] = None
    fc: Optional[int] = None
    fr: Optional[int] = None
    spo2: Optional[int] = None

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
# INIT TABLES
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
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    global POOL
    _raise_if_no_db()

    # Pool com timeouts para não travar:
    # - connect_timeout: não fica pendurado tentando conectar
    # - statement_timeout: query não fica pendurada
    # (Neon pode dar "cold start", isso evita travas longas)
    POOL = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=5,
        kwargs={
            "row_factory": dict_row,
            "connect_timeout": 8,  # segundos
            "options": "-c statement_timeout=12000"  # 12s por statement
        }
    )

    ensure_tables()

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
    last_ts = row["last_ts"] if row else None
    return {"last_event_ts": last_ts.isoformat() if last_ts else "1970-01-01T00:00:00"}

# =========================
# RECOMMENDATIONS
# =========================
def compute_recommendations_for_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = "OK"
        syndrome = None
        confidence = None
        actions = None

        spo2 = r.get("spo2")
        pas = r.get("pas")

        if spo2 is not None and spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.7
            actions = "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar protocolo."

        if pas is not None and pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.8
            actions = "Checar PA manual, perfusão, sangramento/dor e acionar médico conforme protocolo."

        if level != "OK":
            recs.append({
                "cod_atendimento": r["cod_atendimento"],
                "snapshot_ts": r["event_ts"],
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": confidence,
                "actions": actions
            })
    return recs

def persist_recommendations_and_notify(recs: List[Dict[str, Any]]):
    if not recs:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for rec in recs:
                cur.execute("""
                    INSERT INTO public.clinical_recommendations
                        (cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP);
                """, (
                    rec["cod_atendimento"],
                    rec["snapshot_ts"],
                    rec["recommendation_level"],
                    rec.get("syndrome"),
                    rec.get("confidence"),
                    rec.get("actions"),
                ))

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
# BACKGROUND INSERT (FAST)
# =========================
def _persist_vitals_batch(rows: List[VitalRow]) -> Dict[str, int]:
    inserted = 0
    updated = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                event_key = f"{r.cod_atendimento}|{r.event_ts.isoformat()}"

                # Upsert com MERGE:
                # se chegar depois com campos que antes eram NULL, ele preenche.
                cur.execute("""
                    INSERT INTO public.vitals_events (
                        event_key, cod_atendimento, id_ricadpac, event_ts,
                        hora, minuto, temp, pas, pad, fc, fr, spo2,
                        dor, uso_o2, nivel_consciencia, profissional,
                        updated_at
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                    ON CONFLICT (event_key) DO UPDATE SET
                        id_ricadpac        = COALESCE(EXCLUDED.id_ricadpac, vitals_events.id_ricadpac),
                        hora              = COALESCE(EXCLUDED.hora, vitals_events.hora),
                        minuto            = COALESCE(EXCLUDED.minuto, vitals_events.minuto),
                        temp              = COALESCE(EXCLUDED.temp, vitals_events.temp),
                        pas               = COALESCE(EXCLUDED.pas, vitals_events.pas),
                        pad               = COALESCE(EXCLUDED.pad, vitals_events.pad),
                        fc                = COALESCE(EXCLUDED.fc, vitals_events.fc),
                        fr                = COALESCE(EXCLUDED.fr, vitals_events.fr),
                        spo2              = COALESCE(EXCLUDED.spo2, vitals_events.spo2),
                        dor               = COALESCE(EXCLUDED.dor, vitals_events.dor),
                        uso_o2            = COALESCE(EXCLUDED.uso_o2, vitals_events.uso_o2),
                        nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, vitals_events.nivel_consciencia),
                        profissional      = COALESCE(EXCLUDED.profissional, vitals_events.profissional),
                        updated_at        = CURRENT_TIMESTAMP
                    WHERE
                        EXCLUDED.id_ricadpac IS NOT NULL OR
                        EXCLUDED.hora IS NOT NULL OR
                        EXCLUDED.minuto IS NOT NULL OR
                        EXCLUDED.temp IS NOT NULL OR
                        EXCLUDED.pas IS NOT NULL OR
                        EXCLUDED.pad IS NOT NULL OR
                        EXCLUDED.fc IS NOT NULL OR
                        EXCLUDED.fr IS NOT NULL OR
                        EXCLUDED.spo2 IS NOT NULL OR
                        EXCLUDED.dor IS NOT NULL OR
                        EXCLUDED.uso_o2 IS NOT NULL OR
                        EXCLUDED.nivel_consciencia IS NOT NULL OR
                        EXCLUDED.profissional IS NOT NULL;
                """, (
                    event_key, r.cod_atendimento, r.id_ricadpac, r.event_ts,
                    r.hora, r.minuto, r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                    r.dor, r.uso_o2, r.nivel_consciencia, r.profissional
                ))

                # rowcount pode ser 1 em insert, e 1 em update (depende do driver)
                # vamos inferir pelo retorno do status message:
                status = (cur.statusmessage or "").upper()
                if status.startswith("INSERT"):
                    inserted += 1
                elif status.startswith("UPDATE"):
                    updated += 1

        conn.commit()

    return {"inserted": inserted, "updated": updated}

# =========================
# INGEST — BATCH (RESPONDE RÁPIDO)
# =========================
@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: VitalBatch,
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    if not payload.rows:
        return {"ok": True, "queued": 0, "message": "Sem linhas"}

    # RESPONDE IMEDIATO pro Power Automate não estourar tempo
    # e processa DB em background
    rows = payload.rows

    def _job():
        try:
            stats = _persist_vitals_batch(rows)
            # recomendações só depois de persistir
            rec_rows = [
                {
                    "cod_atendimento": r.cod_atendimento,
                    "event_ts": r.event_ts,
                    "pas": r.pas,
                    "spo2": r.spo2
                }
                for r in rows
            ]
            recs = compute_recommendations_for_rows(rec_rows)
            persist_recommendations_and_notify(recs)
            print(f"[INGEST] ok stats={stats} recs={len(recs)}")
        except Exception as e:
            print(f"[INGEST] ERROR: {e}")

    background.add_task(_job)

    return {"ok": True, "queued": len(rows), "message": "Processando em background"}

# =========================
# NOTIFY TELEGRAM — PRODUÇÃO
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
                cur.execute("""
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
                """, (max_send,))
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

                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET
                            notified_at = CASE
                                WHEN recommendation_level = 'PRIORIDADE'
                                THEN CURRENT_TIMESTAMP
                                ELSE notified_at
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                    """, (r["id"],))
                    sent += 1

            conn.commit()

        return {"ok": True, "found": len(rows), "sent": sent}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
