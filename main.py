import os
import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="PREVITA API", version="1.0.0")

DATABASE_URL = os.environ.get("DATABASE_URL")

class VitalIn(BaseModel):
    cod_atendimento: int
    id_ricadpac: int | None = None
    data_lanc: str          # "2026-01-28"
    hora: int               # 0-23
    minuto: int             # 0-59
    temp: float | None = None
    profissional: str | None = None
    event_ts: str           # "2026-01-28T04:15:00"

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")
    return psycopg.connect(DATABASE_URL)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/v1/vitals")
def ingest_vital(v: VitalIn):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO public.vitals_raw
              (event_ts, cod_atendimento, id_ricadpac, data_lanc, hora_lanc, minuto_lanc, temp, profissional)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_atendimento, data_lanc, hora_lanc, minuto_lanc)
            DO UPDATE SET
              event_ts = EXCLUDED.event_ts,
              temp = EXCLUDED.temp,
              profissional = EXCLUDED.profissional,
              updated_at = CURRENT_TIMESTAMP;
        """, (
            v.event_ts,
            v.cod_atendimento,
            v.id_ricadpac,
            v.data_lanc,
            v.hora,
            v.minuto,
            v.temp,
            v.profissional
        ))

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

