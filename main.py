import os
import re
import json
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row
import requests
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse


APP_VERSION = "5.2.0"

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

    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        pass

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


def nfloat(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def abnormal_consciousness(v: Optional[str]) -> bool:
    if not v:
        return False
    s = str(v).strip().lower()
    return s in {
        "sonolenta",
        "confusa",
        "torporosa",
        "rebaixado",
        "agitada",
        "desorientada",
        "coma"
    }


def pick_previous_non_null(history: List[Dict[str, Any]], field: str, current_snapshot_ts=None):
    """
    Busca valor anterior não nulo ignorando o snapshot atual se ele vier no histórico.
    """
    for row in history:
        if current_snapshot_ts is not None and str(row.get("event_ts")) == str(current_snapshot_ts):
            continue
        val = row.get(field)
        if val is not None:
            return val
    return None


def fmt_delta(current: Optional[float], previous: Optional[float], label: str) -> Optional[str]:
    if current is None or previous is None:
        return None
    delta = current - previous
    sign = "+" if delta > 0 else ""
    return f"{label} {sign}{delta:.0f}"


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
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ---------------- TELEGRAM ----------------

def telegram_send(text: str):
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não configurado")
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID não configurado")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text
        },
        timeout=30
    )

    if not resp.ok:
        raise RuntimeError(f"Erro Telegram: status={resp.status_code}, body={resp.text}")

    return resp.json()


def fetch_pending_telegram_alerts(max_send: int = 3):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cod_atendimento,
                    snapshot_ts,
                    temp,
                    pas,
                    pad,
                    fc,
                    fr,
                    spo2,
                    dor,
                    uso_o2,
                    nivel_consciencia,
                    profissional,
                    alerta
                FROM public.telegram_alert_queue
                ORDER BY snapshot_ts ASC
                LIMIT %s
                """,
                (max_send,)
            )
            rows = cur.fetchall()
            result = []
            for r in rows:
                if isinstance(r, dict):
                    result.append(r)
                else:
                    cols = [desc[0] for desc in cur.description]
                    result.append(dict(zip(cols, r)))
            return result


def mark_alert_sent(cod_atendimento: int, snapshot_ts: datetime, alerta: str):
    with db() as conn:
        with conn.cursor() as cur:
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


def fetch_patient_history(cod_atendimento: int, limit: int = 6) -> List[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    event_ts,
                    temp,
                    pas,
                    pad,
                    fc,
                    fr,
                    spo2,
                    dor,
                    uso_o2,
                    nivel_consciencia,
                    profissional
                FROM public.vitals_raw
                WHERE cod_atendimento = %s
                ORDER BY event_ts DESC
                LIMIT %s
                """,
                (cod_atendimento, limit)
            )
            rows = cur.fetchall()
            result = []
            for r in rows:
                if isinstance(r, dict):
                    result.append(r)
                else:
                    cols = [desc[0] for desc in cur.description]
                    result.append(dict(zip(cols, r)))
            return result


def analyze_patient(row: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    temp = nfloat(row.get("temp"))
    pas = nfloat(row.get("pas"))
    pad = nfloat(row.get("pad"))
    fc = nfloat(row.get("fc"))
    fr = nfloat(row.get("fr"))
    spo2 = nfloat(row.get("spo2"))
    consc = row.get("nivel_consciencia")
    uso_o2 = row.get("uso_o2")

    prev_temp = nfloat(pick_previous_non_null(history, "temp", row.get("snapshot_ts")))
    prev_pas = nfloat(pick_previous_non_null(history, "pas", row.get("snapshot_ts")))
    prev_fc = nfloat(pick_previous_non_null(history, "fc", row.get("snapshot_ts")))
    prev_fr = nfloat(pick_previous_non_null(history, "fr", row.get("snapshot_ts")))
    prev_spo2 = nfloat(pick_previous_non_null(history, "spo2", row.get("snapshot_ts")))

    d_temp = temp - prev_temp if temp is not None and prev_temp is not None else None
    d_pas = pas - prev_pas if pas is not None and prev_pas is not None else None
    d_fc = fc - prev_fc if fc is not None and prev_fc is not None else None
    d_fr = fr - prev_fr if fr is not None and prev_fr is not None else None
    d_spo2 = spo2 - prev_spo2 if spo2 is not None and prev_spo2 is not None else None

    respiratory = 0
    hemo = 0
    infectious = 0
    neuro = 0
    reasons = []

    # Respiratório
    if spo2 is not None and spo2 <= 92:
        respiratory += 35
        reasons.append(f"SpO2 baixa ({spo2:.0f})")
    elif spo2 is not None and spo2 <= 94:
        respiratory += 20
        reasons.append(f"SpO2 limítrofe ({spo2:.0f})")

    if fr is not None and fr >= 30:
        respiratory += 35
        reasons.append(f"FR muito elevada ({fr:.0f})")
    elif fr is not None and fr >= 24:
        respiratory += 20
        reasons.append(f"FR elevada ({fr:.0f})")

    if d_spo2 is not None and d_spo2 <= -3:
        respiratory += 15
        reasons.append(f"queda recente de SpO2 ({d_spo2:.0f})")

    if d_fr is not None and d_fr >= 5:
        respiratory += 15
        reasons.append(f"aumento recente de FR (+{d_fr:.0f})")

    # Hemodinâmico
    if pas is not None and pas < 90:
        hemo += 35
        reasons.append(f"PAS baixa ({pas:.0f})")
    elif pas is not None and pas < 100:
        hemo += 15
        reasons.append(f"PAS limítrofe ({pas:.0f})")

    if fc is not None and fc >= 130:
        hemo += 35
        reasons.append(f"FC muito elevada ({fc:.0f})")
    elif fc is not None and fc >= 110:
        hemo += 20
        reasons.append(f"FC elevada ({fc:.0f})")

    if d_pas is not None and d_pas <= -20:
        hemo += 15
        reasons.append(f"queda recente de PAS ({d_pas:.0f})")

    if d_fc is not None and d_fc >= 15:
        hemo += 15
        reasons.append(f"aumento recente de FC (+{d_fc:.0f})")

    # Infeccioso / séptico
    if temp is not None and temp >= 39:
        infectious += 35
        reasons.append(f"hipertermia importante ({temp:.1f})")
    elif temp is not None and temp >= 38.3:
        infectious += 20
        reasons.append(f"febre ({temp:.1f})")

    if temp is not None and temp >= 38 and ((fc is not None and fc >= 100) or (fr is not None and fr >= 22)):
        infectious += 25
        reasons.append("padrão compatível com resposta inflamatória/infecciosa")

    if d_temp is not None and d_temp >= 0.8:
        infectious += 10
        reasons.append(f"elevação recente de temperatura (+{d_temp:.1f})")

    # Neurológico
    if abnormal_consciousness(consc):
        neuro += 40
        reasons.append(f"consciência alterada ({consc})")

    # Síndrome principal
    scores = {
        "DETERIORACAO_RESPIRATORIA": respiratory,
        "INSTABILIDADE_HEMODINAMICA": hemo,
        "RISCO_INFECCIOSO_SEPTICO": infectious,
        "ALTERACAO_NEUROLOGICA": neuro,
    }

    syndrome = max(scores, key=scores.get)
    confidence = min(95, max(scores.values()) + 35)

    # Se tudo estiver muito baixo, vira observação
    if max(scores.values()) < 20:
        syndrome = "OBSERVACAO_CLINICA"
        confidence = 50

    if confidence >= 85:
        level = "ALERTA IMEDIATO"
    elif confidence >= 70:
        level = "ALERTA URGENTE"
    else:
        level = "ATENCAO"

    actions = []

    if syndrome == "DETERIORACAO_RESPIRATORIA":
        actions = [
            "Avaliar paciente imediatamente (ABCDE).",
            "Confirmar sinais vitais e qualidade do sensor.",
            "Respiração: ofertar O2 conforme protocolo; avaliar esforço respiratório.",
            "Elevar cabeceira e checar dispositivo/fluxo de O2.",
            "Reavaliar SpO2/FR em 5–10 min ou antes se piora.",
            "Acionar médico responsável e considerar time de resposta rápida."
        ]
    elif syndrome == "INSTABILIDADE_HEMODINAMICA":
        actions = [
            "Avaliar paciente imediatamente (ABCDE).",
            "Checar PA, perfusão periférica e frequência cardíaca novamente.",
            "Garantir acesso venoso e considerar expansão volêmica conforme cenário clínico.",
            "Monitorizar continuamente e investigar sinais de choque.",
            "Reavaliar em 5–10 min.",
            "Acionar médico responsável imediatamente."
        ]
    elif syndrome == "RISCO_INFECCIOSO_SEPTICO":
        actions = [
            "Avaliar paciente imediatamente e investigar foco infeccioso.",
            "Confirmar temperatura, FC, FR e PA.",
            "Checar perfusão, diurese e estado mental.",
            "Coletar exames conforme protocolo institucional.",
            "Reavaliar em 5–10 min se instabilidade.",
            "Acionar médico responsável com prioridade."
        ]
    elif syndrome == "ALTERACAO_NEUROLOGICA":
        actions = [
            "Avaliar estado neurológico imediatamente.",
            "Checar glicemia capilar, oxigenação e perfusão.",
            "Garantir via aérea protegida se necessário.",
            "Reavaliar nível de consciência em poucos minutos.",
            "Investigar medicações, hipoxemia e causas metabólicas.",
            "Acionar médico responsável imediatamente."
        ]
    else:
        actions = [
            "Manter observação clínica intensiva.",
            "Repetir sinais vitais em curto intervalo.",
            "Correlacionar com quadro clínico e sintomas.",
            "Reavaliar tendência nas próximas medições."
        ]

    trend_parts = [
        fmt_delta(spo2, prev_spo2, "SpO2"),
        fmt_delta(fr, prev_fr, "FR"),
        fmt_delta(fc, prev_fc, "FC"),
        fmt_delta(pas, prev_pas, "PAS"),
        fmt_delta(temp, prev_temp, "Temp"),
    ]
    trend_parts = [p for p in trend_parts if p]

    return {
        "syndrome": syndrome,
        "confidence": int(confidence),
        "level": level,
        "reasons": reasons[:5],
        "actions": actions,
        "trend_summary": trend_parts
    }


def format_intelligent_alert(row: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    level_emoji = {
        "ALERTA IMEDIATO": "🚨",
        "ALERTA URGENTE": "⚠️",
        "ATENCAO": "🟡"
    }.get(analysis["level"], "⚠️")

    trend_text = ""
    if analysis.get("trend_summary"):
        trend_text = "📈 Tendência: " + " | ".join(analysis["trend_summary"]) + "\n\n"

    reasons_text = ""
    if analysis.get("reasons"):
        reasons_text = "🔎 Achados:\n" + "\n".join([f"• {r}" for r in analysis["reasons"]]) + "\n\n"

    actions_text = "\n".join([f"{i+1}) {a}" for i, a in enumerate(analysis["actions"])])

    return (
        f"{level_emoji} PREVITA – {analysis['level']}\n\n"
        f"🧾 Atendimento: {row.get('cod_atendimento')}\n"
        f"🕒 Snapshot: {row.get('snapshot_ts')}\n"
        f"🧠 Síndrome: {analysis.get('syndrome')}\n"
        f"📊 Confiança: {analysis.get('confidence')}\n\n"
        f"{trend_text}"
        f"{reasons_text}"
        f"📌 Sinais atuais:\n"
        f"Temp: {row.get('temp')}\n"
        f"PA: {row.get('pas')}/{row.get('pad')}\n"
        f"FC: {row.get('fc')}\n"
        f"FR: {row.get('fr')}\n"
        f"SpO2: {row.get('spo2')}\n"
        f"Dor: {row.get('dor')}\n"
        f"Uso O2: {row.get('uso_o2')}\n"
        f"Consciência: {row.get('nivel_consciencia')}\n"
        f"Profissional: {row.get('profissional')}\n\n"
        f"✅ Ações sugeridas:\n{actions_text}"
    )


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
        "version": APP_VERSION,
        "database_configured": bool(DATABASE_URL),
        "telegram_token_configured": bool(TELEGRAM_BOT_TOKEN),
        "telegram_chat_configured": bool(TELEGRAM_CHAT_ID)
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

    with db() as conn:
        with conn.cursor() as cur:
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
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT public.fn_upsert_vitals_snapshot();")
            conn.commit()

    return {
        "ok": True,
        "step": "state",
        "version": APP_VERSION
    }


@app.post("/v1/trends/run")
def run_trends():
    return {
        "ok": True,
        "step": "trends",
        "message": "Tendências são avaliadas dinamicamente no envio do alerta.",
        "version": APP_VERSION
    }


@app.post("/v1/assist/recommendations/run")
def run_recommendations():
    return {
        "ok": True,
        "step": "assist_recommendations",
        "message": "As recomendações são geradas dinamicamente na análise clínica.",
        "version": APP_VERSION
    }


# ---------------- TELEGRAM ----------------

@app.get("/v1/notify/telegram/pending")
def telegram_pending(max_read: int = Query(default=10, ge=1, le=100)):
    rows = fetch_pending_telegram_alerts(max_send=max_read)
    enriched = []

    for row in rows:
        history = fetch_patient_history(row["cod_atendimento"], limit=6)
        analysis = analyze_patient(row, history)
        enriched.append({
            "row": row,
            "analysis": analysis
        })

    return {
        "ok": True,
        "count": len(enriched),
        "rows": enriched,
        "version": APP_VERSION
    }


@app.post("/v1/notify/telegram/test")
def telegram_test(message: str = Query(default="Teste PREVITA Telegram")):
    result = telegram_send(message)
    return {
        "ok": True,
        "message_sent": message,
        "telegram_result": result,
        "version": APP_VERSION
    }


@app.post("/v1/notify/telegram/run")
def run_telegram(max_send: int = Query(default=3, ge=1, le=100)):
    sent = 0
    errors = []

    try:
        rows = fetch_pending_telegram_alerts(max_send=max_send)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "stage": "fetch_pending_telegram_alerts",
                "error": str(e),
                "version": APP_VERSION
            }
        )

    for row in rows:
        try:
            history = fetch_patient_history(row["cod_atendimento"], limit=6)
            analysis = analyze_patient(row, history)
            msg = format_intelligent_alert(row, analysis)
            telegram_send(msg)
            mark_alert_sent(row["cod_atendimento"], row["snapshot_ts"], row["alerta"])
            sent += 1
        except Exception as e:
            errors.append({
                "cod_atendimento": row.get("cod_atendimento"),
                "snapshot_ts": str(row.get("snapshot_ts")),
                "alerta": row.get("alerta"),
                "error": str(e)
            })

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
