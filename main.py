import os
import re
import json
import hashlib
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row
import requests
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse


APP_VERSION = "5.4.0"

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

    if isinstance(v, datetime):
        return v

    s = str(v).strip()
    if not s:
        return None

    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

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
    if label == "Temp":
        return f"{label} {sign}{delta:.1f}"
    return f"{label} {sign}{delta:.0f}"


def compute_alert_hash(row: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    payload = {
        "cod_atendimento": row.get("cod_atendimento"),
        "syndrome": analysis.get("syndrome"),
        "priority": analysis.get("priority"),
        "confidence": analysis.get("confidence"),
        "temp": row.get("temp"),
        "pas": row.get("pas"),
        "pad": row.get("pad"),
        "fc": row.get("fc"),
        "fr": row.get("fr"),
        "spo2": row.get("spo2"),
        "uso_o2": row.get("uso_o2"),
        "nivel_consciencia": row.get("nivel_consciencia"),
        "trend_summary": analysis.get("trend_summary", []),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


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
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=30
    )

    if not resp.ok:
        raise RuntimeError(f"Erro Telegram: status={resp.status_code}, body={resp.text}")

    return resp.json()


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
            return cur.fetchall()


def fetch_risk_candidates(limit: int = 50) -> List[Dict[str, Any]]:
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
                FROM public.vitals_risk_view
                ORDER BY snapshot_ts ASC
                LIMIT %s
                """,
                (limit,)
            )
            return cur.fetchall()


def alert_already_sent(cod_atendimento: int, alert_hash: str) -> bool:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM public.telegram_alert_log
                WHERE cod_atendimento = %s
                  AND alert_hash = %s
                LIMIT 1
                """,
                (cod_atendimento, alert_hash)
            )
            return cur.fetchone() is not None


def mark_alert_sent(cod_atendimento: int, snapshot_ts: datetime, alerta: str, priority: str, alert_hash: str):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.telegram_alert_log
                (cod_atendimento, snapshot_ts, alerta, priority, alert_hash)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (cod_atendimento, snapshot_ts, alerta, priority, alert_hash)
            )
            conn.commit()


# ---------------- ANALISE INTELIGENTE ----------------

def analyze_patient(row: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    temp = nfloat(row.get("temp"))
    pas = nfloat(row.get("pas"))
    pad = nfloat(row.get("pad"))
    fc = nfloat(row.get("fc"))
    fr = nfloat(row.get("fr"))
    spo2 = nfloat(row.get("spo2"))
    consc = row.get("nivel_consciencia")

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

    if spo2 is not None and spo2 <= 90:
        respiratory += 45
        reasons.append(f"SpO2 criticamente baixa ({spo2:.0f})")
    elif spo2 is not None and spo2 <= 92:
        respiratory += 35
        reasons.append(f"SpO2 baixa ({spo2:.0f})")
    elif spo2 is not None and spo2 <= 94:
        respiratory += 20
        reasons.append(f"SpO2 limítrofe ({spo2:.0f})")

    if fr is not None and fr >= 30:
        respiratory += 40
        reasons.append(f"FR muito elevada ({fr:.0f})")
    elif fr is not None and fr >= 24:
        respiratory += 25
        reasons.append(f"FR elevada ({fr:.0f})")
    elif fr is not None and fr >= 22:
        respiratory += 15
        reasons.append(f"FR discretamente elevada ({fr:.0f})")

    if d_spo2 is not None and d_spo2 <= -3:
        respiratory += 15
        reasons.append(f"queda recente de SpO2 ({d_spo2:.0f})")

    if d_fr is not None and d_fr >= 5:
        respiratory += 15
        reasons.append(f"aumento recente de FR (+{d_fr:.0f})")

    if pas is not None and pas < 90:
        hemo += 40
        reasons.append(f"PAS baixa ({pas:.0f})")
    elif pas is not None and pas < 100:
        hemo += 20
        reasons.append(f"PAS limítrofe ({pas:.0f})")

    if fc is not None and fc >= 130:
        hemo += 40
        reasons.append(f"FC muito elevada ({fc:.0f})")
    elif fc is not None and fc >= 110:
        hemo += 25
        reasons.append(f"FC elevada ({fc:.0f})")

    if d_pas is not None and d_pas <= -20:
        hemo += 15
        reasons.append(f"queda recente de PAS ({d_pas:.0f})")

    if d_fc is not None and d_fc >= 15:
        hemo += 15
        reasons.append(f"aumento recente de FC (+{d_fc:.0f})")

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

    if abnormal_consciousness(consc):
        neuro += 45
        reasons.append(f"consciência alterada ({consc})")

    scores = {
        "DETERIORACAO_RESPIRATORIA": respiratory,
        "INSTABILIDADE_HEMODINAMICA": hemo,
        "RISCO_INFECCIOSO_SEPTICO": infectious,
        "ALTERACAO_NEUROLOGICA": neuro,
    }

    syndrome = max(scores, key=scores.get)
    top_score = max(scores.values())
    confidence = min(99, top_score + 35)

    if top_score < 20:
        syndrome = "OBSERVACAO_CLINICA"
        confidence = 50

    priority_points = 0

    if spo2 is not None and spo2 <= 90:
        priority_points += 3
    elif spo2 is not None and spo2 <= 92:
        priority_points += 2
    elif spo2 is not None and spo2 <= 94:
        priority_points += 1

    if fr is not None and fr >= 30:
        priority_points += 3
    elif fr is not None and fr >= 24:
        priority_points += 2
    elif fr is not None and fr >= 22:
        priority_points += 1

    if fc is not None and fc >= 130:
        priority_points += 3
    elif fc is not None and fc >= 110:
        priority_points += 2

    if pas is not None and pas < 90:
        priority_points += 3
    elif pas is not None and pas < 100:
        priority_points += 1

    if temp is not None and temp >= 39:
        priority_points += 2
    elif temp is not None and temp >= 38.3:
        priority_points += 1

    if abnormal_consciousness(consc):
        priority_points += 3

    if d_spo2 is not None and d_spo2 <= -3:
        priority_points += 1
    if d_fc is not None and d_fc >= 15:
        priority_points += 1
    if d_fr is not None and d_fr >= 5:
        priority_points += 1
    if d_pas is not None and d_pas <= -20:
        priority_points += 1

    if priority_points >= 7:
        priority = "CRITICA"
        level = "ALERTA IMEDIATO"
    elif priority_points >= 4:
        priority = "ALTA"
        level = "ALERTA URGENTE"
    elif priority_points >= 2:
        priority = "MODERADA"
        level = "ATENCAO"
    else:
        priority = "BAIXA"
        level = "OBSERVACAO"

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
        "priority": priority,
        "reasons": reasons[:5],
        "actions": actions,
        "trend_summary": trend_parts
    }


def format_intelligent_alert(row: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    priority_emoji = {
        "CRITICA": "🚨",
        "ALTA": "⚠️",
        "MODERADA": "🟡",
        "BAIXA": "🔵"
    }.get(analysis["priority"], "⚠️")

    trend_text = ""
    if analysis.get("trend_summary"):
        trend_text = "📈 Tendência: " + " | ".join(analysis["trend_summary"]) + "\n\n"

    reasons_text = ""
    if analysis.get("reasons"):
        reasons_text = "🔎 Achados:\n" + "\n".join([f"• {r}" for r in analysis["reasons"]]) + "\n\n"

    actions_text = "\n".join([f"{i+1}) {a}" for i, a in enumerate(analysis["actions"])])

    return (
        f"{priority_emoji} PREVITA – {analysis['level']}\n\n"
        f"🧾 Atendimento: {row.get('cod_atendimento')}\n"
        f"🕒 Snapshot: {row.get('snapshot_ts')}\n"
        f"🧠 Síndrome: {analysis.get('syndrome')}\n"
        f"🏷️ Prioridade: {analysis.get('priority')}\n"
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
    return {"ok": True, "service": "previta-api", "version": APP_VERSION}


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

        # NOVA REGRA: usa horario_lancamento como origem oficial
        event_ts = parse_dt(r.get("horario_lancamento"))
        if event_ts is None:
            skipped += 1
            continue

        normalized.append({
            "event_ts": event_ts,
            "cod_atendimento": cod,
            "id_ricadpac": to_int(r.get("id_ricadpac")),

            # derivados do horario_lancamento
            "data_lanc": event_ts.date(),
            "hora_lanc": event_ts.hour,
            "minuto_lanc": event_ts.minute,

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
                ON CONFLICT (cod_atendimento, event_ts)
                DO UPDATE SET
                    id_ricadpac = COALESCE(EXCLUDED.id_ricadpac, vitals_raw.id_ricadpac),
                    data_lanc = COALESCE(EXCLUDED.data_lanc, vitals_raw.data_lanc),
                    hora_lanc = COALESCE(EXCLUDED.hora_lanc, vitals_raw.hora_lanc),
                    minuto_lanc = COALESCE(EXCLUDED.minuto_lanc, vitals_raw.minuto_lanc),
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

    return {"ok": True, "step": "state", "version": APP_VERSION}


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
    rows = fetch_risk_candidates(limit=max_read)
    enriched = []

    for row in rows:
        history = fetch_patient_history(row["cod_atendimento"], limit=6)
        analysis = analyze_patient(row, history)

        if analysis["priority"] not in {"ALTA", "CRITICA"}:
            continue

        alert_hash = compute_alert_hash(row, analysis)
        if alert_already_sent(row["cod_atendimento"], alert_hash):
            continue

        enriched.append({
            "row": row,
            "analysis": analysis,
            "alert_hash": alert_hash
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
    scanned = 0

    try:
        rows = fetch_risk_candidates(limit=100)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "stage": "fetch_risk_candidates",
                "error": str(e),
                "version": APP_VERSION
            }
        )

    for row in rows:
        scanned += 1
        if sent >= max_send:
            break

        try:
            history = fetch_patient_history(row["cod_atendimento"], limit=6)
            analysis = analyze_patient(row, history)

            if analysis["priority"] not in {"ALTA", "CRITICA"}:
                continue

            alert_hash = compute_alert_hash(row, analysis)

            if alert_already_sent(row["cod_atendimento"], alert_hash):
                continue

            msg = format_intelligent_alert(row, analysis)
            telegram_send(msg)
            mark_alert_sent(
                row["cod_atendimento"],
                row["snapshot_ts"],
                row["alerta"],
                analysis["priority"],
                alert_hash
            )
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
        "scanned": scanned,
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
