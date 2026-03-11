import os
import re
import json
from datetime import datetime, date, time
from typing import Any, Dict, List

import psycopg
from psycopg.rows import dict_row
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


APP_VERSION = "5.0.0"

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
    if v is None:
        return None
    try:
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s.replace(",", ".")))
    except Exception:
        return None


def to_float(v):
    if v is None:
        return None
    try:
        s = str(v).strip()
        if s == "":
            return None
        return float(s.replace(",", "."))
    except Exception:
        return None


def parse_date(v):
    if isinstance(v, date):
        return v
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None

    # tenta ISO completo
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        pass

    # tenta só data
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    return None


def parse_dt(v):
    if v is None:
        return None
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
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


# ---------------- TELEGRAM ----------------

def telegram_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID não configurado")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text
        },
        timeout=30
    )
    resp.raise_for_status()


def format_alert(row: Dict[str, Any]) -> str:
    return (
        "🚨 ALERTA CLÍNICO PREVITA\n\n"
        f"Atendimento: {row.get('cod_atendimento')}\n"
        f"Data/Hora: {row.get('snapshot_ts')}\n"
        f"Alerta: {row.get('alerta')}\n\n"
        f"Temp: {row.get('temp')}\n"
        f"PA: {row.get('pas')}/{row.get('pad')}\n"
        f"FC: {row.get('fc')}\n"
        f"FR: {row.get('fr')}\n"
        f"SpO2: {row.get('spo2')}\n"
        f"Dor: {row.get('dor')}\n"
        f"Uso O2: {row.get('uso_o2')}\n"
        f"Consciência: {row.get('nivel_consciencia')}\n"
        f"Profissional: {row.get('profissional')}"
    )


def fetch_pending_telegram_alerts(max_send: int = 3):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM public.telegram_alert_queue
            ORDER BY snapshot_ts ASC
            LIMIT %s
            """,
            (max_send,)
        )
        return cur.fetchall()


def mark_alert_sent(cod_atendimento: int, snapshot_ts: datetime, alerta: str):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.telegram_alert_log
            (cod_atendimento, snapshot_ts, alerta)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (cod_atendimento, snapshot_ts, alerta)
        )
        conn.commit()


# ---------------- API BASICA ----------------

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "previta-api",
        "version": APP_VERSION
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "version": APP_VERSION
    }


# ---------------- INGESTAO VITAIS ----------------

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

        dt_min = parse_dt(r.get("data_hora_lanc_minuto"))
        if dt_min is None:
            dt_min = build_event_ts(d, h, m)

        normalized.append({
            "event_ts": build_event_ts(d, h, m),
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
            "payload": json.dumps(payload, default=str)
        })

    if not normalized:
        return {
            "ok": True,
            "received": len(rows),
            "normalized": 0,
            "skipped": skipped,
            "version": APP_VERSION
        }

    with db() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.vitals_raw (
                event_ts,
                cod_atendimento,
                id_ricadpac,
                data_lanc,
                hora_lanc,
                minuto_lanc,
                temp,
                dor,
                fr,
                fc,
                pad,
                pas,
                spo2,
                uso_o2,
                nivel_consciencia,
                profissional,
                received_at,
                source,
                payload
            )
            VALUES (
                %(event_ts)s,
                %(cod_atendimento)s,
                %(id_ricadpac)s,
                %(data_lanc)s,
                %(hora_lanc)s,
                %(minuto_lanc)s,
                %(temp)s,
                %(dor)s,
                %(fr)s,
                %(fc)s,
                %(pad)s,
                %(pas)s,
                %(spo2)s,
                %(uso_o2)s,
                %(nivel_consciencia)s,
                %(profissional)s,
                %(received_at)s,
                %(source)s,
                %(payload)s
            )
            ON CONFLICT (cod_atendimento, data_lanc, hora_lanc, minuto_lanc)
            DO UPDATE SET
                event_ts = COALESCE(EXCLUDED.event_ts, vitals_raw.event_ts),
                id_ricadpac = COALESCE(EXCLUDED.id_ricadpac, vitals_raw.id_ricadpac),
                temp = COALESCE(EXCLUDED.temp, vitals_raw.temp),
                dor = COALESCE(EXCLUDED.dor, vitals_raw.dor),
                fr = COALESCE(EXCLUDED.fr, vitals_raw.fr),
                fc = COALESCE(EXCLUDED.fc, vitals_raw.fc),
                pad = COALESCE(EXCLUDED.pad, vitals_raw.pad),
                pas = COALESCE(EXCLUDED.pas, vitals_raw.pas),
                spo2 = COALESCE(EXCLUDED.spo2, vitals_raw.spo2),
                uso_o2 = COALESCE(EXCLUDED.uso_o2, vitals_raw.uso_o2),
                nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, vitals_raw.nivel_consciencia),
                profissional = COALESCE(EXCLUDED.profissional, vitals_raw.profissional),
                received_at = COALESCE(EXCLUDED.received_at, vitals_raw.received_at),
                source = COALESCE(EXCLUDED.source, vitals_raw.source),
                payload = COALESCE(EXCLUDED.payload, vitals_raw.payload),
                updated_at = NOW()
            """,
            normalized
        )

        # atualiza snapshot consolidado após ingestão
        cur.execute("SELECT public.fn_upsert_vitals_snapshot();")
        conn.commit()

    return {
        "ok": True,
        "received": len(rows),
        "normalized": len(normalized),
        "skipped": skipped,
        "version": APP_VERSION
    }


# ---------------- PROCESSOS PREVITA ----------------

@app.post("/v1/state/run")
def run_state():
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT public.fn_upsert_vitals_snapshot();")
        conn.commit()

    return {
        "ok": True,
        "step": "state",
        "version": APP_VERSION
    }


@app.post("/v1/trends/run")
def run_trends():
    # placeholder para manter compatibilidade com o workflow já existente
    return {
        "ok": True,
        "step": "trends",
        "message": "Sem processamento adicional no momento.",
        "version": APP_VERSION
    }


@app.post("/v1/assist/recommendations/run")
def run_recommendations():
    # placeholder para manter compatibilidade com o workflow já existente
    return {
        "ok": True,
        "step": "assist_recommendations",
        "message": "Sem processamento adicional no momento.",
        "version": APP_VERSION
    }


# ---------------- TELEGRAM ----------------

@app.post("/v1/notify/telegram/run")
def run_telegram(max_send: int = 3):
    sent = 0
    errors = []

    rows = fetch_pending_telegram_alerts(max_send=max_send)

    for row in rows:
        try:
            msg = format_alert(row)
            telegram_send(msg)
            mark_alert_sent(row["cod_atendimento"], row["snapshot_ts"], row["alerta"])
            sent += 1
        except Exception as e:
            errors.append(str(e))

    return {
        "ok": True,
        "sent": sent,
        "pending_read": len(rows),
        "errors": errors,
        "version": APP_VERSION
    }


# ---------------- ERROS ----------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": str(exc),
            "version": APP_VERSION
        }
    )

