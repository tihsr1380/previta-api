# main.py
import os, re, json
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

APP_VERSION = "4.2.0"

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

app = FastAPI(title="PREVITA API", version=APP_VERSION)

# ---------------- UTIL ----------------

def now():
    return datetime.utcnow()

def normalize_key(k: Any) -> str:
    if k is None:
        return ""
    s = str(k).strip()
    m = re.findall(r"\[([^\]]+)\]", s)
    if m:
        s = m[-1]
    s = s.replace("[", "").replace("]", "")
    s = s.lower().strip()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    return s

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {normalize_key(k): v for k, v in row.items()}

def to_int(v):
    if v is None: return None
    try:
        return int(float(str(v).replace(".", "").replace(",", ".")))
    except:
        return None

def to_float(v):
    if v is None: return None
    try:
        return float(str(v).replace(",", "."))
    except:
        return None

def parse_date(v):
    if isinstance(v, date): return v
    try:
        return datetime.fromisoformat(str(v)).date()
    except:
        return None

def parse_dt(v):
    try:
        return datetime.fromisoformat(str(v))
    except:
        return None

def build_event_ts(d: date, h: int, m: int):
    return datetime.combine(d, time(h, m))

# ---------------- POWER BI EXTRACT ----------------

def extract_rows(payload: Any) -> List[Dict]:
    if payload is None:
        return []

    if isinstance(payload, str):
        payload = json.loads(payload)

    if "body" in payload:
        payload = payload["body"]

    if "powerbi" in payload:
        payload = payload["powerbi"]

    rows = []
    for r in payload.get("results", []):
        for t in r.get("tables", []):
            rows.extend(t.get("rows", []))
    return rows

# ---------------- DB ----------------

def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

# ---------------- API ----------------

@app.post("/v1/vitals/batch")
async def vitals_batch(req: Request):
    payload = await req.json()
    rows = extract_rows(payload)

    normalized = []
    skipped = 0

    for r in rows:
        r = normalize_row(r)

        cod = to_int(r.get("cod_atendimento"))
        if not cod:
            skipped += 1
            continue

        d = parse_date(r.get("data_lanc"))
        h = to_int(r.get("hora_lanc"))
        m = to_int(r.get("minuto_lanc"))

        if not (d and h is not None and m is not None):
            skipped += 1
            continue

        event_ts = build_event_ts(d, h, m)

        normalized.append({
            "event_ts": event_ts,
            "cod_atendimento": cod,
            "id_ricadpac": to_int(r.get("id_ricadpac")),
            "data_lanc": d,
            "hora_lanc": h,
            "minuto_lanc": m,
            "temp": to_float(r.get("temp")),
            "dor": to_float(r.get("dor")),
            "fr": to_float(r.get("fr")),
            "fc": to_float(r.get("fc")),
            "pad": to_float(r.get("pad")),
            "pas": to_float(r.get("pas")),
            "spo2": to_float(r.get("spo2")),
            "uso_o2": r.get("uso_o2"),
            "nivel_consciencia": r.get("nivel_consciencia"),
            "profissional": r.get("profissional"),
            "received_at": now(),
            "source": "power_automate",
            "payload": payload
        })

    if not normalized:
        return {"ok": True, "received": len(rows), "normalized": 0, "skipped": skipped}

    with db() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO vitals_raw (
            event_ts, cod_atendimento, id_ricadpac,
            data_lanc, hora_lanc, minuto_lanc,
            temp, dor, fr, fc, pad, pas, spo2,
            uso_o2, nivel_consciencia, profissional,
            received_at, source, payload
        )
        VALUES (
            %(event_ts)s, %(cod_atendimento)s, %(id_ricadpac)s,
            %(data_lanc)s, %(hora_lanc)s, %(minuto_lanc)s,
            %(temp)s, %(dor)s, %(fr)s, %(fc)s, %(pad)s, %(pas)s, %(spo2)s,
            %(uso_o2)s, %(nivel_consciencia)s, %(profissional)s,
            %(received_at)s, %(source)s, %(payload)s
        )
        ON CONFLICT (cod_atendimento, event_ts)
        DO UPDATE SET
            temp=EXCLUDED.temp,
            dor=EXCLUDED.dor,
            fr=EXCLUDED.fr,
            fc=EXCLUDED.fc,
            pad=EXCLUDED.pad,
            pas=EXCLUDED.pas,
            spo2=EXCLUDED.spo2,
            uso_o2=EXCLUDED.uso_o2,
            nivel_consciencia=EXCLUDED.nivel_consciencia,
            profissional=EXCLUDED.profissional,
            updated_at=NOW();
        """, normalized)
        conn.commit()

    return {
        "ok": True,
        "received": len(rows),
        "normalized": len(normalized),
        "skipped": skipped,
        "version": APP_VERSION
    }
