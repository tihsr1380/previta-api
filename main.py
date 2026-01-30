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

    # vitais
    temp: float | None = None
    pas: float | None = None
    pad: float | None = None
    fc: float | None = None
    fr: float | None = None
    spo2: float | None = None
    dor: float | None = None
    uso_o2: str | None = None
    nivel_consciencia: str | None = None

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
        """, (
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

            v.profissional
        ))

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from datetime import datetime, timedelta

def calc_risk(row: dict):
    # score base
    score = 0
    reasons = []

    temp = row.get("temp")
    pas  = row.get("pas")
    pad  = row.get("pad")
    fc   = row.get("fc")
    fr   = row.get("fr")
    spo2 = row.get("spo2")
    nivel = (row.get("nivel_consciencia") or "").strip().lower()
    uso_o2 = (row.get("uso_o2") or "").strip().lower()

    # Regras (versão v1 - simples e segura)
    if spo2 is not None and spo2 < 92:
        score += 40
        reasons.append(f"SpO2 baixa ({spo2})")

    if fc is not None and fc > 110:
        score += 20
        reasons.append(f"FC alta ({fc})")

    if fr is not None and fr > 22:
        score += 15
        reasons.append(f"FR alta ({fr})")

    if temp is not None and temp >= 38:
        score += 15
        reasons.append(f"Febre ({temp})")

    if pas is not None and pas < 90:
        score += 30
        reasons.append(f"PAS baixa ({pas})")

    # Alerta neurológico
    if nivel in ["sonolenta", "confuso", "rebaixado", "inconsciente"]:
        score += 30
        reasons.append(f"Nível consciência: {nivel}")

    # Uso de O2 e SpO2 baixa piora
    if (uso_o2 in ["aa", "sim", "cateter", "mascara"]) and (spo2 is not None and spo2 < 94):
        score += 10
        reasons.append("Uso O2 + SpO2 <94")

    # Define nível
    if score >= 60:
        level = "ALTO"
    elif score >= 30:
        level = "MODERADO"
    else:
        level = "BAIXO"

    if not reasons:
        reasons = ["Sem sinais críticos pelas regras v1"]

    return level, min(score, 100), " | ".join(reasons)


@app.post("/v1/risk/run")
def run_risk(minutes_back: int = 60, limit: int = 500):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT event_ts, cod_atendimento, id_ricadpac,
                   temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia
            FROM public.vitals_raw
            WHERE event_ts >= (NOW() - (%s || ' minutes')::interval)
            ORDER BY event_ts DESC
            LIMIT %s;
        """, (minutes_back, limit))

        rows = cur.fetchall()

        inserted = 0
        for r in rows:
            row = {
                "event_ts": r[0],
                "cod_atendimento": r[1],
                "id_ricadpac": r[2],
                "temp": r[3],
                "pas": r[4],
                "pad": r[5],
                "fc": r[6],
                "fr": r[7],
                "spo2": r[8],
                "dor": r[9],
                "uso_o2": r[10],
                "nivel_consciencia": r[11],
            }

            level, score, reason = calc_risk(row)

            cur.execute("""
                INSERT INTO public.risk_events
                  (event_ts, cod_atendimento, id_ricadpac,
                   temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia,
                   risk_level, risk_score, risk_reason)
                VALUES
                  (%s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s)
                ON CONFLICT (cod_atendimento, event_ts)
                DO UPDATE SET
                  risk_level = EXCLUDED.risk_level,
                  risk_score = EXCLUDED.risk_score,
                  risk_reason = EXCLUDED.risk_reason,
                  created_at = CURRENT_TIMESTAMP;
            """, (
                row["event_ts"], row["cod_atendimento"], row["id_ricadpac"],
                row["temp"], row["pas"], row["pad"], row["fc"], row["fr"], row["spo2"],
                row["dor"], row["uso_o2"], row["nivel_consciencia"],
                level, score, reason
            ))
            inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "processed": len(rows), "upserted": inserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


from typing import Optional

class AlertsIn(BaseModel):
    minutes_back: int = 60
    min_level: str = "MODERADO"  # MODERADO ou ALTO

@app.post("/v1/alerts/check")
def alerts_check(p: AlertsIn):
    """
    Varre risk_events recentes e grava alertas em public.alerts
    (sem duplicar: UNIQUE(cod_atendimento, risk_level, event_ts)).
    """
    level_rank = {"BAIXO": 1, "MODERADO": 2, "ALTO": 3}
    min_rank = level_rank.get(p.min_level.upper(), 2)

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT event_ts, cod_atendimento, risk_level, risk_score, risk_reason
            FROM public.risk_events
            WHERE event_ts >= (NOW() - (%s || ' minutes')::interval)
            ORDER BY event_ts DESC
        """, (p.minutes_back,))
        rows = cur.fetchall()

        inserted = 0
        for (event_ts, cod_atendimento, risk_level, risk_score, risk_reason) in rows:
            if level_rank.get((risk_level or "").upper(), 0) < min_rank:
                continue

            cur.execute("""
                INSERT INTO public.alerts (event_ts, cod_atendimento, risk_level, risk_score, risk_reason)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cod_atendimento, risk_level, event_ts) DO NOTHING
            """, (event_ts, cod_atendimento, risk_level, risk_score, risk_reason))
            if cur.rowcount == 1:
                inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "inserted": inserted, "scanned": len(rows)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

