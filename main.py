# =========================
# PREVITA API — MAIN (Completo)
# =========================
import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="1.0.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")  # opcional (recomendado)

# =========================
# DB HELPERS
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def _check_key(x_api_key: Optional[str]):
    # Se ALERT_API_KEY não estiver setada, não bloqueia.
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _ensure_tables(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.vitais (
        id BIGSERIAL PRIMARY KEY,
        event_key TEXT UNIQUE,
        event_ts TIMESTAMPTZ,
        cod_atendimento TEXT,
        id_ricadpac TEXT,
        data_lanc TEXT,
        hora INT,
        minuto INT,
        temp DOUBLE PRECISION,
        pas INT,
        pad INT,
        fc INT,
        fr INT,
        spo2 INT,
        dor INT,
        uso_o2 TEXT,
        nivel_consciencia TEXT,
        profissional TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.clinical_recommendations (
        id BIGSERIAL PRIMARY KEY,
        event_key TEXT UNIQUE,
        cod_atendimento TEXT,
        snapshot_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        recommendation_level TEXT, -- 'NORMAL' | 'PRIORIDADE' | 'IMEDIATO'
        syndrome TEXT,
        confidence DOUBLE PRECISION,
        actions TEXT,
        score_news2 INT,
        notified_at TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_vitais_atendimento_ts
    ON public.vitais (cod_atendimento, event_ts DESC);
    """)

    conn.commit()
    cur.close()

# =========================
# TELEGRAM
# =========================
def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # Telegram é opcional; se não configurar, não derruba a API
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error: {r.status_code} {r.text}")

# =========================
# MODELS
# =========================
class VitalIn(BaseModel):
    cod_atendimento: Optional[str] = None
    id_ricadpac: Optional[str] = None
    data_lanc: Optional[str] = None
    hora: Optional[int] = None
    minuto: Optional[int] = None
    temp: Optional[float] = None
    pas: Optional[int] = None
    pad: Optional[int] = None
    fc: Optional[int] = None
    fr: Optional[int] = None
    spo2: Optional[int] = None
    dor: Optional[int] = None
    uso_o2: Optional[str] = None
    nivel_consciencia: Optional[str] = None
    profissional: Optional[str] = None

    # estes 2 são CHAVE do incremental
    event_ts: Optional[str] = None  # ISO string
    event_key: str = Field(..., description="chave única do evento (ex: atendimento+timestamp+minuto)")

class BatchIn(BaseModel):
    items: List[VitalIn]

# =========================
# CLINICAL ENGINE (NEWS2 simplificado + regras)
# =========================
def _safe_int(x):
    try:
        return int(x) if x is not None else None
    except:
        return None

def _safe_float(x):
    try:
        return float(x) if x is not None else None
    except:
        return None

def calc_news2(v: VitalIn) -> int:
    """
    NEWS2 simplificado (não substitui protocolo institucional).
    Ajuste os thresholds conforme padrão clínico do hospital.
    """
    score = 0

    spo2 = _safe_int(v.spo2)
    rr = _safe_int(v.fr)
    hr = _safe_int(v.fc)
    sbp = _safe_int(v.pas)
    temp = _safe_float(v.temp)

    # SpO2 (escala 1 simplificada)
    if spo2 is not None:
        if spo2 <= 91: score += 3
        elif spo2 <= 93: score += 2
        elif spo2 <= 95: score += 1

    # FR
    if rr is not None:
        if rr <= 8: score += 3
        elif rr <= 11: score += 1
        elif rr <= 20: score += 0
        elif rr <= 24: score += 2
        else: score += 3

    # FC
    if hr is not None:
        if hr <= 40: score += 3
        elif hr <= 50: score += 1
        elif hr <= 90: score += 0
        elif hr <= 110: score += 1
        elif hr <= 130: score += 2
        else: score += 3

    # PAS (sistólica)
    if sbp is not None:
        if sbp <= 90: score += 3
        elif sbp <= 100: score += 2
        elif sbp <= 110: score += 1
        elif sbp <= 219: score += 0
        else: score += 3

    # Temperatura
    if temp is not None:
        if temp <= 35.0: score += 3
        elif temp <= 36.0: score += 1
        elif temp <= 38.0: score += 0
        elif temp <= 39.0: score += 1
        else: score += 2

    return score

def generate_recommendation(v: VitalIn) -> Dict[str, Any]:
    """
    Gera nível + ações (regras).
    """
    news2 = calc_news2(v)

    # regras “IMEDIATO”
    spo2 = _safe_int(v.spo2)
    rr = _safe_int(v.fr)
    sbp = _safe_int(v.pas)
    hr = _safe_int(v.fc)

    immediate = False
    if spo2 is not None and spo2 <= 90:
        immediate = True
    if rr is not None and rr >= 30:
        immediate = True
    if sbp is not None and sbp <= 90:
        immediate = True
    if hr is not None and hr >= 140:
        immediate = True

    if immediate or news2 >= 7:
        level = "IMEDIATO"
        syndrome = "Sinais de deterioração aguda (triagem)"
        confidence = 0.80
        actions = (
            "1) Avaliar paciente imediatamente.\n"
            "2) Checar oxigenação, PA, FR, FC e consciência.\n"
            "3) Considerar acionar médico/enfermagem conforme protocolo.\n"
            "4) Repetir sinais vitais em 5-10 min."
        )
    elif news2 >= 5:
        level = "PRIORIDADE"
        syndrome = "Risco moderado (triagem)"
        confidence = 0.70
        actions = (
            "1) Reavaliar sinais vitais em 15-30 min.\n"
            "2) Verificar dor, saturação e necessidade de O2.\n"
            "3) Considerar avaliação médica conforme protocolo.\n"
            "4) Investigar causa (dor, ansiedade, febre, sangramento, etc)."
        )
    else:
        level = "NORMAL"
        syndrome = "Sem alerta crítico (triagem)"
        confidence = 0.60
        actions = "Manter monitorização e reavaliação conforme rotina."
    return {
        "level": level,
        "syndrome": syndrome,
        "confidence": confidence,
        "actions": actions,
        "news2": news2
    }

def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # aceita ISO
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except:
        return None

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# LAST EVENT TS (para incremental no Flow)
# =========================
@app.get("/v1/vitals/last_event_ts")
def last_event_ts(x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    _check_key(x_api_key)
    try:
        conn = get_conn()
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute("SELECT MAX(event_ts) AS last_ts FROM public.vitais;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        last_ts = row["last_ts"]
        return {"last_event_ts": last_ts.isoformat() if last_ts else None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# INSERT SINGLE (para testes)
# =========================
@app.post("/v1/vitals")
def receive_vital(
    payload: VitalIn,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)
    try:
        conn = get_conn()
        _ensure_tables(conn)
        cur = conn.cursor()

        event_dt = parse_ts(payload.event_ts)

        # UPSERT vitais
        cur.execute("""
        INSERT INTO public.vitais (
            event_key, event_ts, cod_atendimento, id_ricadpac, data_lanc, hora, minuto,
            temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia, profissional
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (event_key) DO UPDATE SET
            event_ts = EXCLUDED.event_ts,
            cod_atendimento = EXCLUDED.cod_atendimento,
            id_ricadpac = EXCLUDED.id_ricadpac,
            data_lanc = EXCLUDED.data_lanc,
            hora = EXCLUDED.hora,
            minuto = EXCLUDED.minuto,
            temp = EXCLUDED.temp,
            pas = EXCLUDED.pas,
            pad = EXCLUDED.pad,
            fc = EXCLUDED.fc,
            fr = EXCLUDED.fr,
            spo2 = EXCLUDED.spo2,
            dor = EXCLUDED.dor,
            uso_o2 = EXCLUDED.uso_o2,
            nivel_consciencia = EXCLUDED.nivel_consciencia,
            profissional = EXCLUDED.profissional;
        """, (
            payload.event_key, event_dt, payload.cod_atendimento, payload.id_ricadpac,
            payload.data_lanc, payload.hora, payload.minuto,
            payload.temp, payload.pas, payload.pad, payload.fc, payload.fr,
            payload.spo2, payload.dor, payload.uso_o2, payload.nivel_consciencia,
            payload.profissional
        ))

        # PROCESSA recomendação automaticamente
        rec = generate_recommendation(payload)

        cur.execute("""
        INSERT INTO public.clinical_recommendations (
            event_key, cod_atendimento, recommendation_level, syndrome, confidence,
            actions, score_news2, snapshot_ts, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (event_key) DO UPDATE SET
            cod_atendimento = EXCLUDED.cod_atendimento,
            recommendation_level = EXCLUDED.recommendation_level,
            syndrome = EXCLUDED.syndrome,
            confidence = EXCLUDED.confidence,
            actions = EXCLUDED.actions,
            score_news2 = EXCLUDED.score_news2,
            updated_at = CURRENT_TIMESTAMP;
        """, (
            payload.event_key, payload.cod_atendimento, rec["level"], rec["syndrome"],
            rec["confidence"], rec["actions"], rec["news2"]
        ))

        conn.commit()

        # ALERTA TELEGRAM se necessário
        if rec["level"] in ("IMEDIATO", "PRIORIDADE"):
            msg = (
                f"🚨 <b>PREVITA – ALERTA {rec['level']}</b>\n\n"
                f"🧾 <b>Atendimento:</b> {payload.cod_atendimento or '-'}\n"
                f"🕒 <b>Event TS:</b> {payload.event_ts or '-'}\n"
                f"📊 <b>NEWS2:</b> {rec['news2']}\n"
                f"🧠 <b>Síndrome:</b> {rec['syndrome']}\n\n"
                f"✅ <b>Ações:</b>\n{rec['actions']}"
            )
            send_telegram_message_sync(msg)

        cur.close()
        conn.close()

        return {"ok": True, "saved": True, "event_key": payload.event_key, "level": rec["level"], "news2": rec["news2"]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# INSERT BATCH (ESSENCIAL p/ não travar Flow)
# =========================
@app.post("/v1/vitals/batch")
def receive_vitals_batch(
    batch: BatchIn,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    if not batch.items:
        return {"ok": True, "received": 0, "saved": 0, "alerts": 0}

    try:
        conn = get_conn()
        _ensure_tables(conn)
        cur = conn.cursor()

        saved = 0
        alerts = 0

        for payload in batch.items:
            event_dt = parse_ts(payload.event_ts)

            cur.execute("""
            INSERT INTO public.vitais (
                event_key, event_ts, cod_atendimento, id_ricadpac, data_lanc, hora, minuto,
                temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia, profissional
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (event_key) DO UPDATE SET
                event_ts = EXCLUDED.event_ts,
                cod_atendimento = EXCLUDED.cod_atendimento,
                id_ricadpac = EXCLUDED.id_ricadpac,
                data_lanc = EXCLUDED.data_lanc,
                hora = EXCLUDED.hora,
                minuto = EXCLUDED.minuto,
                temp = EXCLUDED.temp,
                pas = EXCLUDED.pas,
                pad = EXCLUDED.pad,
                fc = EXCLUDED.fc,
                fr = EXCLUDED.fr,
                spo2 = EXCLUDED.spo2,
                dor = EXCLUDED.dor,
                uso_o2 = EXCLUDED.uso_o2,
                nivel_consciencia = EXCLUDED.nivel_consciencia,
                profissional = EXCLUDED.profissional;
            """, (
                payload.event_key, event_dt, payload.cod_atendimento, payload.id_ricadpac,
                payload.data_lanc, payload.hora, payload.minuto,
                payload.temp, payload.pas, payload.pad, payload.fc, payload.fr,
                payload.spo2, payload.dor, payload.uso_o2, payload.nivel_consciencia,
                payload.profissional
            ))

            rec = generate_recommendation(payload)

            cur.execute("""
            INSERT INTO public.clinical_recommendations (
                event_key, cod_atendimento, recommendation_level, syndrome, confidence,
                actions, score_news2, snapshot_ts, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (event_key) DO UPDATE SET
                cod_atendimento = EXCLUDED.cod_atendimento,
                recommendation_level = EXCLUDED.recommendation_level,
                syndrome = EXCLUDED.syndrome,
                confidence = EXCLUDED.confidence,
                actions = EXCLUDED.actions,
                score_news2 = EXCLUDED.score_news2,
                updated_at = CURRENT_TIMESTAMP;
            """, (
                payload.event_key, payload.cod_atendimento, rec["level"], rec["syndrome"],
                rec["confidence"], rec["actions"], rec["news2"]
            ))

            # alerta (opcional) — evita spam: só “IMEDIATO”
            if rec["level"] == "IMEDIATO":
                msg = (
                    f"🚨 <b>PREVITA – ALERTA IMEDIATO</b>\n\n"
                    f"🧾 <b>Atendimento:</b> {payload.cod_atendimento or '-'}\n"
                    f"🕒 <b>Event TS:</b> {payload.event_ts or '-'}\n"
                    f"📊 <b>NEWS2:</b> {rec['news2']}\n\n"
                    f"✅ <b>Ações:</b>\n{rec['actions']}"
                )
                send_telegram_message_sync(msg)
                alerts += 1

            saved += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "received": len(batch.items), "saved": saved, "alerts": alerts}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# NOTIFY TELEGRAM (reuso)
# =========================
@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    max_send: int = 10,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)
    try:
        conn = get_conn()
        _ensure_tables(conn)
        cur = conn.cursor()

        # Notifica PRIORIDADE 1x (IMEDIATO já pode ter sido notificado no batch)
        cur.execute("""
            SELECT id, cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions
            FROM public.clinical_recommendations
            WHERE recommendation_level IN ('IMEDIATO', 'PRIORIDADE')
              AND (recommendation_level = 'IMEDIATO' OR notified_at IS NULL)
            ORDER BY
              CASE recommendation_level WHEN 'IMEDIATO' THEN 2 ELSE 1 END DESC,
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

            # marca notified_at somente para PRIORIDADE
            cur.execute("""
                UPDATE public.clinical_recommendations
                SET
                  notified_at = CASE WHEN recommendation_level = 'PRIORIDADE' THEN CURRENT_TIMESTAMP ELSE notified_at END,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = %s;
            """, (r["id"],))
            sent += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "found": len(rows), "sent": sent}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
