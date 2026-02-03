import os
from typing import Optional, List, Dict, Any
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field, ConfigDict

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.1.1")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=8,
        options="-c statement_timeout=12000"
    )

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# MODELS
# =========================
class VitalRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cod_atendimento: int
    id_ricadpac: Optional[int]
    event_ts: datetime

    hora: Optional[int]
    minuto: Optional[int]
    temp: Optional[float]
    pas: Optional[int]
    pad: Optional[int]
    fc: Optional[int]
    fr: Optional[int]
    spo2: Optional[int]

    dor: Optional[str]
    uso_o2: Optional[str]
    nivel_consciencia: Optional[str]
    profissional: Optional[str]

class VitalBatch(BaseModel):
    rows: List[VitalRow] = Field(default_factory=list)

# =========================
# STARTUP
# =========================
@app.on_event("startup")
def startup():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vitals_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_key TEXT UNIQUE,
                    cod_atendimento INT,
                    id_ricadpac INT,
                    event_ts TIMESTAMP,
                    hora INT,
                    minuto INT,
                    temp DOUBLE PRECISION,
                    pas INT,
                    pad INT,
                    fc INT,
                    fr INT,
                    spo2 INT,
                    dor TEXT,
                    uso_o2 TEXT,
                    nivel_consciencia TEXT,
                    profissional TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.commit()

# =========================
# INGEST (RÁPIDO)
# =========================
def persist_rows(rows: List[VitalRow]):
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                event_key = f"{r.cod_atendimento}|{r.event_ts.isoformat()}"
                cur.execute("""
                    INSERT INTO public.vitals_events (
                        event_key, cod_atendimento, id_ricadpac, event_ts,
                        hora, minuto, temp, pas, pad, fc, fr, spo2,
                        dor, uso_o2, nivel_consciencia, profissional,
                        updated_at
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                    ON CONFLICT (event_key) DO UPDATE SET
                        hora = COALESCE(EXCLUDED.hora, vitals_events.hora),
                        minuto = COALESCE(EXCLUDED.minuto, vitals_events.minuto),
                        temp = COALESCE(EXCLUDED.temp, vitals_events.temp),
                        pas = COALESCE(EXCLUDED.pas, vitals_events.pas),
                        pad = COALESCE(EXCLUDED.pad, vitals_events.pad),
                        fc = COALESCE(EXCLUDED.fc, vitals_events.fc),
                        fr = COALESCE(EXCLUDED.fr, vitals_events.fr),
                        spo2 = COALESCE(EXCLUDED.spo2, vitals_events.spo2),
                        dor = COALESCE(EXCLUDED.dor, vitals_events.dor),
                        uso_o2 = COALESCE(EXCLUDED.uso_o2, vitals_events.uso_o2),
                        nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, vitals_events.nivel_consciencia),
                        profissional = COALESCE(EXCLUDED.profissional, vitals_events.profissional),
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    event_key, r.cod_atendimento, r.id_ricadpac, r.event_ts,
                    r.hora, r.minuto, r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                    r.dor, r.uso_o2, r.nivel_consciencia, r.profissional
                ))
        conn.commit()

@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: VitalBatch,
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY")
):
    _check_key(x_api_key)

    if not payload.rows:
        return {"ok": True, "queued": 0}

    background.add_task(persist_rows, payload.rows)

    # RESPONDE IMEDIATO → Power Automate não estoura timeout
    return {"ok": True, "queued": len(payload.rows)}
