import os
from typing import Optional, Literal, List, Dict, Any

import psycopg
from psycopg.rows import dict_row

import requests
import httpx

from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel


# ============================================================
# APP
# ============================================================
app = FastAPI(title="PREVITA API", version="1.0.0")


# ============================================================
# ENV
# ============================================================
DATABASE_URL = os.environ.get("DATABASE_URL")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")  # grupo: -100xxxxxxxxxx

ALERT_API_KEY = os.environ.get("ALERT_API_KEY")  # chave interna p/ proteger rotas


# ============================================================
# HELPERS
# ============================================================
def _require_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")


def get_conn():
    _require_db()
    # dict_row -> fetch retorna dicts (mais seguro e mais legível)
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _require_telegram():
    missing = []
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


def _check_key(x_api_key: Optional[str]):
    # Se ALERT_API_KEY não estiver setada, não bloqueia (modo debug)
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def send_telegram_message_sync(text: str):
    _require_telegram()
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


async def send_telegram_message_async(text: str):
    _require_telegram()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Telegram error: {r.status_code} {r.text}")
        return r.json()


def get_last_snapshot_ts(cur) -> Optional[str]:
    """
    Retorna o MAX(snapshot_ts) de vitals_snapshot.
    Usamos isso como "agora clínico" para evitar bugs de timezone (NOW() em UTC).
    """
    cur.execute("SELECT MAX(snapshot_ts) AS mx FROM public.vitals_snapshot;")
    row = cur.fetchone()
    return row["mx"] if row else None


# ============================================================
# MODELOS
# ============================================================
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


class AlertsIn(BaseModel):
    minutes_back: int = 60
    min_level: str = "MODERADO"  # MODERADO ou ALTO


class AlertStatusIn(BaseModel):
    status: Literal["NOVO", "EM_ATENDIMENTO", "RESOLVIDO"]
    note: Optional[str] = None


class StateRunIn(BaseModel):
    minutes_back: int = 240
    history_minutes: int = 360
    limit: int = 500


class TrendsRunIn(BaseModel):
    minutes_back: int = 1440
    history_minutes: int = 360
    limit: int = 500


class EarlyAlertsIn(BaseModel):
    minutes_back: int = 1440
    min_trend_state: str = "ATENCAO"
    exclude_state: str | None = None


class AssistAlertsIn(BaseModel):
    minutes_back: int = 360
    min_score: int = 60
    include_states: list[str] = ["PIORA", "CRITICO"]


class RecUpdateIn(BaseModel):
    status: Literal["NOVO", "EM_ATENDIMENTO", "RESOLVIDO"]
    handled_by: Optional[str] = None
    handled_note: Optional[str] = None


class DispatchPullIn(BaseModel):
    minutes_back: int = 720
    limit: int = 50
    channel: str = "TELEGRAM"  # TELEGRAM | WHATSAPP | SMS | TEST


class DispatchMarkIn(BaseModel):
    cod_atendimento: int
    snapshot_ts: str  # iso
    target: Literal["MEDICO", "ENFERMAGEM"]
    status: Literal["SENT", "FAILED", "ACK"] = "SENT"
    error: Optional[str] = None


# ============================================================
# HEALTH
# ============================================================
@app.get("/health")
def health():
    return {"status": "ok"}


# ============================================================
# INGEST VITAIS
# ============================================================
@app.post("/v1/vitals")
def ingest_vital(v: VitalIn):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
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

        return {"ok": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# RISCO (risk_events)
# ============================================================
def calc_risk(row: dict, history: list[dict]):
    score = 0
    reasons = []

    def delta(field):
        values = [h.get(field) for h in history if h.get(field) is not None]
        if len(values) < 2:
            return 0
        return values[-1] - values[0]

    temp = row.get("temp")
    pas  = row.get("pas")
    fc   = row.get("fc")
    fr   = row.get("fr")
    spo2 = row.get("spo2")
    nivel = (row.get("nivel_consciencia") or "").lower()
    uso_o2 = (row.get("uso_o2") or "").lower()

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

    if nivel in ["sonolenta", "confuso", "rebaixado"]:
        score += 30
        reasons.append(f"Consciência alterada ({nivel})")

    if delta("spo2") <= -3:
        score += 25
        reasons.append("Queda progressiva de SpO2")

    if delta("fc") >= 15:
        score += 15
        reasons.append("Aumento progressivo de FC")

    if delta("fr") >= 5:
        score += 15
        reasons.append("Aumento progressivo de FR")

    if delta("pas") <= -20:
        score += 20
        reasons.append("Queda progressiva de PAS")

    if delta("temp") >= 1:
        score += 10
        reasons.append("Elevação progressiva de temperatura")

    if uso_o2 in ["aa", "sim"] and delta("spo2") < 0:
        score += 10
        reasons.append("Uso de O2 + queda de SpO2")

    if score >= 70:
        level = "ALTO"
    elif score >= 35:
        level = "MODERADO"
    else:
        level = "BAIXO"

    if not reasons:
        reasons = ["Sem sinais críticos ou tendências relevantes"]

    return level, min(score, 100), " | ".join(reasons)


@app.post("/v1/risk/run")
def run_risk(minutes_back: int = 60, limit: int = 500):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
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
                        "event_ts": r["event_ts"],
                        "cod_atendimento": r["cod_atendimento"],
                        "id_ricadpac": r["id_ricadpac"],
                        "temp": r["temp"],
                        "pas": r["pas"],
                        "pad": r["pad"],
                        "fc": r["fc"],
                        "fr": r["fr"],
                        "spo2": r["spo2"],
                        "dor": r["dor"],
                        "uso_o2": r["uso_o2"],
                        "nivel_consciencia": r["nivel_consciencia"],
                    }

                    history: list[dict] = []
                    level, score, reason = calc_risk(row, history)

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

        return {"ok": True, "processed": len(rows), "upserted": inserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ALERTS (risk_events -> alerts)
# ============================================================
@app.post("/v1/alerts/check")
def alerts_check(p: AlertsIn):
    level_rank = {"BAIXO": 1, "MODERADO": 2, "ALTO": 3}
    min_rank = level_rank.get(p.min_level.upper(), 2)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT event_ts, cod_atendimento, risk_level, risk_score, risk_reason
                    FROM public.risk_events
                    WHERE event_ts >= (NOW() - (%s || ' minutes')::interval)
                    ORDER BY event_ts DESC
                """, (p.minutes_back,))
                rows = cur.fetchall()

                inserted = 0
                for r in rows:
                    risk_level = (r["risk_level"] or "").upper()
                    if level_rank.get(risk_level, 0) < min_rank:
                        continue

                    cur.execute("""
                        INSERT INTO public.alerts
                          (event_ts, cod_atendimento, risk_level, risk_score, risk_reason, status)
                        VALUES
                          (%s, %s, %s, %s, %s, 'NOVO')
                        ON CONFLICT (cod_atendimento, risk_level, event_ts) DO NOTHING
                    """, (
                        r["event_ts"], r["cod_atendimento"], r["risk_level"],
                        r["risk_score"], r["risk_reason"]
                    ))
                    if cur.rowcount == 1:
                        inserted += 1

            conn.commit()

        return {"ok": True, "inserted": inserted, "scanned": len(rows)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/alerts")
def list_alerts(
    status: str = Query(default="NOVO", description="NOVO | EM_ATENDIMENTO | RESOLVIDO"),
    limit: int = Query(default=50, ge=1, le=500),
):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, event_ts, cod_atendimento, risk_level, risk_score, risk_reason,
                           status, created_at, updated_at
                    FROM public.alerts
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s;
                """, (status.upper().strip(), limit))
                rows = cur.fetchall()

        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "event_ts": r["event_ts"].isoformat() if r["event_ts"] else None,
                "cod_atendimento": r["cod_atendimento"],
                "risk_level": r["risk_level"],
                "risk_score": r["risk_score"],
                "risk_reason": r["risk_reason"],
                "status": r["status"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            })

        return {"ok": True, "count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/v1/alerts/{alert_id}")
def update_alert_status(alert_id: int, payload: AlertStatusIn):
    st = (payload.status or "").upper().strip()
    if st not in ("NOVO", "EM_ATENDIMENTO", "RESOLVIDO"):
        raise HTTPException(status_code=400, detail="status inválido.")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if st == "RESOLVIDO":
                    cur.execute("""
                        UPDATE public.alerts
                        SET status = %s,
                            handled_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, cod_atendimento, status, updated_at;
                    """, (st, alert_id))
                else:
                    cur.execute("""
                        UPDATE public.alerts
                        SET status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, cod_atendimento, status, updated_at;
                    """, (st, alert_id))

                row = cur.fetchone()
            conn.commit()

        if not row:
            raise HTTPException(status_code=404, detail="alerta não encontrado")

        return {
            "ok": True,
            "id": row["id"],
            "cod_atendimento": row["cod_atendimento"],
            "status": row["status"],
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# STATE (ancorado no MAX(snapshot_ts))
# ============================================================
def _safe_num(x):
    return None if x is None else float(x)


def calc_state(snapshot: dict, history: list[dict]):
    reasons = []
    score = 0

    temp = _safe_num(snapshot.get("temp"))
    pas  = _safe_num(snapshot.get("pas"))
    fc   = _safe_num(snapshot.get("fc"))
    fr   = _safe_num(snapshot.get("fr"))
    spo2 = _safe_num(snapshot.get("spo2"))
    uso_o2 = (snapshot.get("uso_o2") or "").strip().lower()
    nivel = (snapshot.get("nivel_consciencia") or "").strip().lower()

    def series(field):
        return [h.get(field) for h in history if h.get(field) is not None]

    def delta(field):
        vals = series(field)
        if len(vals) < 2:
            return 0
        return vals[-1] - vals[0]

    def persist_low_spo2(th=94):
        vals = series("spo2")
        tail = vals[-3:] if len(vals) >= 3 else vals
        return sum(1 for v in tail if v < th)

    def persist_high_fr(th=22):
        vals = series("fr")
        tail = vals[-3:] if len(vals) >= 3 else vals
        return sum(1 for v in tail if v > th)

    def persist_high_fc(th=110):
        vals = series("fc")
        tail = vals[-3:] if len(vals) >= 3 else vals
        return sum(1 for v in tail if v > th)

    critical_hits = 0
    if spo2 is not None and spo2 < 90:
        critical_hits += 1
        reasons.append(f"SpO2 muito baixa ({spo2})")
    if pas is not None and pas < 85:
        critical_hits += 1
        reasons.append(f"PAS muito baixa ({pas})")
    if fc is not None and fc >= 140:
        critical_hits += 1
        reasons.append(f"FC muito alta ({fc})")
    if fr is not None and fr >= 30:
        critical_hits += 1
        reasons.append(f"FR muito alta ({fr})")
    if temp is not None and (temp < 35 or temp >= 39):
        critical_hits += 1
        reasons.append(f"Temperatura crítica ({temp})")
    if nivel in ["inconsciente", "rebaixado importante", "coma"]:
        critical_hits += 1
        reasons.append(f"Consciência crítica ({nivel})")

    if critical_hits >= 2:
        score = 90 + min(10, critical_hits * 2)
        return "CRITICO", min(score, 100), " | ".join(reasons)

    if delta("spo2") <= -3 and (spo2 is not None and spo2 < 94):
        score += 25
        reasons.append("Queda progressiva de SpO2 + valor baixo")
    if delta("fc") >= 15 and (fc is not None and fc > 100):
        score += 15
        reasons.append("FC em subida + taquicardia")
    if delta("fr") >= 5 and (fr is not None and fr > 20):
        score += 15
        reasons.append("FR em subida + taquipneia")
    if delta("pas") <= -20 and (pas is not None and pas < 95):
        score += 20
        reasons.append("Queda progressiva de PAS + PAS baixa")

    if persist_low_spo2(94) >= 2:
        score += 15
        reasons.append("SpO2 baixa persistente (últimas medições)")
    if persist_high_fr(22) >= 2:
        score += 10
        reasons.append("FR alta persistente (últimas medições)")
    if persist_high_fc(110) >= 2:
        score += 10
        reasons.append("FC alta persistente (últimas medições)")

    if uso_o2 in ["sim", "cateter", "mascara", "venturi", "o2"] and delta("spo2") < 0:
        score += 10
        reasons.append("Uso de O2 com piora de SpO2")

    if score >= 45:
        return "INSTABILIZANDO", min(score + 35, 100), " | ".join(reasons)

    if spo2 is not None and spo2 < 94:
        score += 20
        reasons.append(f"SpO2 baixa ({spo2})")
    if fc is not None and fc > 110:
        score += 15
        reasons.append(f"Taquicardia ({fc})")
    if fr is not None and fr > 22:
        score += 10
        reasons.append(f"Taquipneia ({fr})")
    if pas is not None and pas < 90:
        score += 20
        reasons.append(f"Hipotensão (PAS {pas})")
    if temp is not None and temp >= 38:
        score += 10
        reasons.append(f"Febre ({temp})")

    if score >= 30:
        return "EM_RISCO", min(score + 15, 100), " | ".join(reasons)

    comp = 0
    if delta("fc") >= 10:
        comp += 1
        reasons.append("FC subindo (compensação)")
    if delta("fr") >= 4:
        comp += 1
        reasons.append("FR subindo (compensação)")
    if delta("spo2") < 0:
        comp += 1
        reasons.append("SpO2 caindo (compensação)")
    if pas is not None and pas < 100 and (delta("pas") < 0):
        comp += 1
        reasons.append("PAS em queda (compensação)")

    if comp >= 2:
        return "COMPENSANDO", 35, " | ".join(reasons)

    if (spo2 is not None and 94 <= spo2 < 96) or (fc is not None and 95 <= fc <= 110) or (fr is not None and 19 <= fr <= 22):
        reasons.append("Pequenas alterações: manter observação e reavaliar")
        return "EM_OBSERVACAO", 20, " | ".join(reasons)

    return "ESTAVEL", 10, "Sinais dentro do esperado sem tendência de piora"


@app.post("/v1/state/run")
def state_run(p: StateRunIn):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                last_snapshot = get_last_snapshot_ts(cur)
                if not last_snapshot:
                    return {"ok": True, "processed": 0, "upserted": 0, "reason": "Sem snapshots em vitals_snapshot"}

                cur.execute("""
                    SELECT cod_atendimento, snapshot_ts, id_ricadpac,
                           temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia, profissional
                    FROM public.vitals_snapshot
                    WHERE snapshot_ts >= (%s - (%s || ' minutes')::interval)
                    ORDER BY snapshot_ts DESC
                    LIMIT %s;
                """, (last_snapshot, p.minutes_back, p.limit))
                snaps = cur.fetchall()

                upserted = 0
                for s in snaps:
                    snapshot = dict(s)

                    cur.execute("""
                        SELECT event_ts, temp, pas, fc, fr, spo2
                        FROM public.vitals_raw
                        WHERE cod_atendimento = %s
                          AND event_ts >= (%s - (%s || ' minutes')::interval)
                          AND event_ts <= %s
                        ORDER BY event_ts ASC;
                    """, (snapshot["cod_atendimento"], snapshot["snapshot_ts"], p.history_minutes, snapshot["snapshot_ts"]))

                    hrows = cur.fetchall()
                    history = []
                    for hr in hrows:
                        history.append({
                            "event_ts": hr["event_ts"],
                            "temp": _safe_num(hr["temp"]),
                            "pas": _safe_num(hr["pas"]),
                            "fc": _safe_num(hr["fc"]),
                            "fr": _safe_num(hr["fr"]),
                            "spo2": _safe_num(hr["spo2"]),
                        })

                    state, state_score, state_reason = calc_state(snapshot, history)

                    cur.execute("""
                        INSERT INTO public.clinical_state
                          (cod_atendimento, id_ricadpac, snapshot_ts,
                           state, state_score, state_reason)
                        VALUES
                          (%s, %s, %s,
                           %s, %s, %s)
                        ON CONFLICT (cod_atendimento, snapshot_ts)
                        DO UPDATE SET
                          state = EXCLUDED.state,
                          state_score = EXCLUDED.state_score,
                          state_reason = EXCLUDED.state_reason,
                          created_at = CURRENT_TIMESTAMP;
                    """, (
                        snapshot["cod_atendimento"],
                        snapshot["id_ricadpac"],
                        snapshot["snapshot_ts"],
                        state,
                        state_score,
                        state_reason
                    ))

                    upserted += 1

            conn.commit()

        return {"ok": True, "processed": len(snaps), "upserted": upserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# TRENDS (ancorado no MAX(snapshot_ts))
# ============================================================
def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def calc_trends(snapshot: dict, history: list[dict]):
    score = 0
    reasons = []

    def series(field):
        vals = []
        for h in history:
            v = h.get(field)
            if v is None:
                continue
            vals.append(float(v))
        return vals

    def slope(field):
        vals = series(field)
        if len(vals) < 2:
            return 0.0
        return vals[-1] - vals[0]

    d_spo2 = slope("spo2")
    d_fc   = slope("fc")
    d_fr   = slope("fr")
    d_pas  = slope("pas")
    d_temp = slope("temp")

    spo2 = _safe_float(snapshot.get("spo2"))
    fc   = _safe_float(snapshot.get("fc"))
    fr   = _safe_float(snapshot.get("fr"))
    pas  = _safe_float(snapshot.get("pas"))
    temp = _safe_float(snapshot.get("temp"))

    uso_o2 = (snapshot.get("uso_o2") or "").lower()
    nivel  = (snapshot.get("nivel_consciencia") or "").lower()

    if d_spo2 <= -3:
        score += 25
        reasons.append("SpO2 em queda (tendência)")

    if d_fr >= 5:
        score += 15
        reasons.append("FR em subida (tendência)")

    if d_spo2 <= -2 and d_fr >= 3:
        score += 20
        reasons.append("Piora respiratória combinada (SpO2↓ + FR↑)")

    if uso_o2 in ["aa", "sim", "s", "uso"] and d_spo2 < 0:
        score += 10
        reasons.append("Uso de O2 + SpO2 piorando")

    if d_pas <= -20:
        score += 20
        reasons.append("PAS em queda progressiva")

    if d_temp >= 1.0:
        score += 10
        reasons.append("Temperatura em elevação progressiva")

    if d_fc >= 15:
        score += 15
        reasons.append("FC em aumento progressivo")

    if nivel in ["sonolenta", "confuso", "rebaixado"]:
        score += 15
        reasons.append("Consciência alterada (sinal precoce)")

    if spo2 is not None and 92 <= spo2 <= 94:
        score += 10
        reasons.append(f"SpO2 limítrofe ({spo2})")

    if fr is not None and 20 <= fr <= 22:
        score += 5
        reasons.append(f"FR limítrofe ({fr})")

    if fc is not None and 100 <= fc <= 110:
        score += 5
        reasons.append(f"FC limítrofe ({fc})")

    if score >= 45:
        state = "PIORA"
    elif score >= 20:
        state = "ATENCAO"
    else:
        state = "ESTAVEL"

    if not reasons:
        reasons = ["Sem tendência relevante de piora na janela analisada"]

    return state, min(score, 100), " | ".join(reasons)


@app.post("/v1/trends/run")
def run_trends(p: TrendsRunIn):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                last_snapshot = get_last_snapshot_ts(cur)
                if not last_snapshot:
                    return {"ok": True, "processed": 0, "upserted": 0, "reason": "Sem snapshots em vitals_snapshot"}

                cur.execute("""
                    SELECT cod_atendimento, snapshot_ts, id_ricadpac,
                           temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia
                    FROM public.vitals_snapshot
                    WHERE snapshot_ts >= (%s - (%s || ' minutes')::interval)
                    ORDER BY snapshot_ts DESC
                    LIMIT %s;
                """, (last_snapshot, p.minutes_back, p.limit))
                snaps = cur.fetchall()

                upserted = 0
                for s in snaps:
                    snapshot = dict(s)

                    cur.execute("""
                        SELECT event_ts, temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia
                        FROM public.vitals_raw
                        WHERE cod_atendimento = %s
                          AND event_ts >= (%s - (%s || ' minutes')::interval)
                          AND event_ts <= %s
                        ORDER BY event_ts ASC;
                    """, (snapshot["cod_atendimento"], snapshot["snapshot_ts"], p.history_minutes, snapshot["snapshot_ts"]))

                    hist_rows = cur.fetchall()
                    history = []
                    for h in hist_rows:
                        history.append({
                            "event_ts": h["event_ts"],
                            "temp": h["temp"],
                            "pas": h["pas"],
                            "pad": h["pad"],
                            "fc": h["fc"],
                            "fr": h["fr"],
                            "spo2": h["spo2"],
                            "dor": h["dor"],
                            "uso_o2": h["uso_o2"],
                            "nivel_consciencia": h["nivel_consciencia"],
                        })

                    trend_state, trend_score, trend_reason = calc_trends(snapshot, history)

                    cur.execute("""
                        INSERT INTO public.clinical_trends
                          (cod_atendimento, snapshot_ts, trend_state, trend_score, trend_reason)
                        VALUES
                          (%s, %s, %s, %s, %s)
                        ON CONFLICT (cod_atendimento, snapshot_ts)
                        DO UPDATE SET
                          trend_state = EXCLUDED.trend_state,
                          trend_score = EXCLUDED.trend_score,
                          trend_reason = EXCLUDED.trend_reason,
                          created_at = CURRENT_TIMESTAMP;
                    """, (
                        snapshot["cod_atendimento"],
                        snapshot["snapshot_ts"],
                        trend_state,
                        trend_score,
                        trend_reason
                    ))
                    upserted += 1

            conn.commit()

        return {"ok": True, "processed": len(snaps), "upserted": upserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# EARLY ALERTS
# ============================================================
@app.post("/v1/early-alerts/check")
def early_alerts_check(p: EarlyAlertsIn):
    rank = {"ESTAVEL": 1, "ATENCAO": 2, "PIORA": 3}
    min_rank = rank.get(p.min_trend_state.upper(), 2)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                last_snapshot = get_last_snapshot_ts(cur)
                if not last_snapshot:
                    return {"ok": True, "scanned": 0, "inserted": 0, "reason": "Sem snapshots"}

                cur.execute("""
                    SELECT snapshot_ts, cod_atendimento, trend_state, trend_score, trend_reason
                    FROM public.clinical_trends
                    WHERE snapshot_ts >= (%s - (%s || ' minutes')::interval)
                    ORDER BY snapshot_ts DESC;
                """, (last_snapshot, p.minutes_back))
                trends = cur.fetchall()

                inserted = 0
                scanned = 0

                for t in trends:
                    scanned += 1
                    ts = (t["trend_state"] or "").upper()
                    if rank.get(ts, 0) < min_rank:
                        continue

                    if p.exclude_state:
                        cur.execute("""
                            SELECT state
                            FROM public.clinical_state
                            WHERE cod_atendimento = %s AND snapshot_ts = %s
                            LIMIT 1;
                        """, (t["cod_atendimento"], t["snapshot_ts"]))
                        r = cur.fetchone()
                        if r and (r["state"] or "").upper() == p.exclude_state.upper():
                            continue

                    alert_level = ts

                    cur.execute("""
                        INSERT INTO public.early_alerts
                          (snapshot_ts, cod_atendimento, alert_level, alert_score, alert_reason)
                        VALUES
                          (%s, %s, %s, %s, %s)
                        ON CONFLICT (cod_atendimento, snapshot_ts, alert_level)
                        DO NOTHING;
                    """, (
                        t["snapshot_ts"],
                        t["cod_atendimento"],
                        alert_level,
                        int(t["trend_score"] or 0),
                        t["trend_reason"] or ""
                    ))

                    if cur.rowcount == 1:
                        inserted += 1

            conn.commit()

        return {"ok": True, "scanned": scanned, "inserted": inserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ASSIST ALERTS
# ============================================================
@app.post("/v1/assist/alerts/run")
def assist_alerts_run(p: AssistAlertsIn):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                last_snapshot = get_last_snapshot_ts(cur)
                if not last_snapshot:
                    return {"ok": True, "scanned": 0, "inserted": 0, "skipped_active": 0, "reason": "Sem snapshots"}

                include_states = [(s or "").upper() for s in (p.include_states or [])]
                if not include_states:
                    include_states = ["PIORA", "CRITICO"]

                cur.execute("""
                    SELECT cod_atendimento, snapshot_ts, trend_state, trend_score, trend_reason
                    FROM public.clinical_trends
                    WHERE snapshot_ts >= (%s - (%s || ' minutes')::interval)
                      AND UPPER(trend_state) = ANY(%s)
                      AND trend_score >= %s
                    ORDER BY snapshot_ts DESC;
                """, (last_snapshot, p.minutes_back, include_states, p.min_score))

                rows = cur.fetchall()

                inserted = 0
                skipped = 0

                for r in rows:
                    trend_state = (r["trend_state"] or "").upper()
                    alert_level = "CRITICO" if trend_state == "CRITICO" else "ATENCAO"

                    cur.execute("""
                        INSERT INTO public.assistential_alerts
                          (cod_atendimento, snapshot_ts, alert_level, alert_score, alert_reason, status)
                        VALUES
                          (%s, %s, %s, %s, %s, 'NOVO')
                        ON CONFLICT ON CONSTRAINT uq_assist_alert_active DO NOTHING;
                    """, (
                        r["cod_atendimento"],
                        r["snapshot_ts"],
                        alert_level,
                        int(r["trend_score"] or 0),
                        r["trend_reason"] or ""
                    ))

                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        skipped += 1

            conn.commit()

        return {"ok": True, "scanned": len(rows), "inserted": inserted, "skipped_active": skipped}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# RECOMMENDATIONS (run)
# ============================================================
@app.post("/v1/assist/recommendations/run")
def run_recommendations(minutes_back: int = 180, limit: int = 1000):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                last_snapshot = get_last_snapshot_ts(cur)
                if not last_snapshot:
                    return {"ok": True, "processed": 0, "upserted": 0, "reason": "Sem snapshots em vitals_snapshot"}

                cur.execute("""
                    SELECT
                        s.cod_atendimento,
                        s.snapshot_ts,
                        s.state,
                        t.trend_state,
                        v.spo2, v.fr, v.fc, v.pas, v.pad, v.temp, v.dor,
                        v.uso_o2, v.nivel_consciencia
                    FROM public.clinical_state s
                    LEFT JOIN public.clinical_trends t
                      ON t.cod_atendimento = s.cod_atendimento
                     AND t.snapshot_ts = s.snapshot_ts
                    LEFT JOIN public.vitals_snapshot v
                      ON v.cod_atendimento = s.cod_atendimento
                     AND v.snapshot_ts = s.snapshot_ts
                    WHERE s.snapshot_ts >= (%s - (%s || ' minutes')::interval)
                    ORDER BY s.snapshot_ts DESC
                    LIMIT %s;
                """, (last_snapshot, minutes_back, limit))
                rows = cur.fetchall()

                upserted = 0

                for r in rows:
                    cod_atendimento = r["cod_atendimento"]
                    snapshot_ts = r["snapshot_ts"]
                    state = r["state"]
                    trend_state = r["trend_state"]

                    spo2 = r["spo2"]
                    fr = r["fr"]
                    fc = r["fc"]
                    pas = r["pas"]
                    pad = r["pad"]
                    temp = r["temp"]
                    dor = r["dor"]
                    uso_o2 = r["uso_o2"]
                    nivel_consciencia = r["nivel_consciencia"]

                    st = (state or "").upper()
                    tr = (trend_state or "").upper()
                    uso = (uso_o2 or "").strip().lower()
                    niv = (nivel_consciencia or "").strip().lower()

                    # defaults
                    level = "ATENCAO"
                    syndrome = "ROTINA"
                    confidence = 30
                    recommendation = "Manter rotina assistencial e reavaliar conforme protocolo."
                    actions = "Reavaliar sinais vitais conforme rotina do setor. Documentar evolução."

                    # motor de síndromes
                    resp_hits = 0
                    if spo2 is not None and spo2 < 92: resp_hits += 1
                    if fr is not None and fr >= 24: resp_hits += 1
                    if uso in ["sim", "s", "cateter", "mascara", "venturi", "o2", "uso"]: resp_hits += 1
                    if tr == "PIORA" and spo2 is not None and spo2 < 94: resp_hits += 1

                    shock_hits = 0
                    if pas is not None and pas <= 90: shock_hits += 1
                    if fc is not None and fc >= 120: shock_hits += 1
                    if tr == "PIORA" and pas is not None and pas < 95: shock_hits += 1

                    sepsis_hits = 0
                    if temp is not None and temp >= 38: sepsis_hits += 1
                    if fc is not None and fc >= 110: sepsis_hits += 1
                    if tr == "PIORA": sepsis_hits += 1

                    pain_hits = 0
                    if dor is not None and dor >= 7: pain_hits += 1
                    if fc is not None and fc >= 110: pain_hits += 1
                    if pas is not None and pas >= 160: pain_hits += 1

                    neuro_hits = 0
                    if niv in ["sonolenta", "confuso", "rebaixado", "rebaixado importante", "inconsciente"]: neuro_hits += 1
                    if spo2 is not None and spo2 < 94: neuro_hits += 1
                    if fr is not None and fr <= 10: neuro_hits += 1

                    # IMEDIATO
                    if (
                        st == "CRITICO"
                        or (resp_hits >= 3 and (spo2 is not None and spo2 < 90))
                        or (shock_hits >= 2 and (pas is not None and pas <= 85))
                        or (fc is not None and fc >= 180)
                        or (fr is not None and fr >= 30)
                    ):
                        level = "IMEDIATO"
                        confidence = 90

                        if shock_hits >= resp_hits and shock_hits >= sepsis_hits and shock_hits >= neuro_hits:
                            syndrome = "CHOQUE_HIPOPERFUSAO"
                            recommendation = "Suspeita de hipoperfusão/choque. Acionar médico imediatamente."
                            actions = (
                                "1) Repetir PA/FC/SpO2 agora e em 5 min.\n"
                                "2) Avaliar sangramento ativo, drenos, curativos, débito urinário (se houver).\n"
                                "3) Garantir acesso venoso pérvio; preparar cristalóide conforme prescrição/protocolo.\n"
                                "4) Acionar médico/plantonista imediatamente e registrar achados."
                            )
                        elif resp_hits >= shock_hits and resp_hits >= sepsis_hits and resp_hits >= neuro_hits:
                            syndrome = "DETERIORACAO_RESPIRATORIA"
                            recommendation = "Suspeita de deterioração respiratória. Acionar médico imediatamente."
                            actions = (
                                "1) Checar oximetria e padrão respiratório agora.\n"
                                "2) Elevar cabeceira, verificar via aérea e posicionamento.\n"
                                "3) Conferir O2 (fluxo/dispositivo), considerar ajuste conforme protocolo.\n"
                                "4) Reavaliar SpO2/FR em 5–10 min e acionar médico imediatamente.\n"
                                "5) Registrar e manter monitorização contínua."
                            )
                        elif neuro_hits >= 2:
                            syndrome = "ALTERACAO_NEURO_SEDACAO"
                            recommendation = "Alteração neurológica/sedação com risco. Acionar médico imediatamente."
                            actions = (
                                "1) Avaliar consciência (AVPU/Glasgow se aplicável) e sinais vitais.\n"
                                "2) Checar FR e SpO2 continuamente.\n"
                                "3) Revisar medicações sedativas/analgésicas recentes (registrar horário/dose).\n"
                                "4) Acionar médico imediatamente."
                            )
                        else:
                            syndrome = "DETERIORACAO_AGUDA"
                            recommendation = "Sinais de deterioração aguda. Acionar médico imediatamente."
                            actions = (
                                "1) Repetir sinais vitais imediatamente.\n"
                                "2) Garantir monitorização contínua.\n"
                                "3) Acionar médico/plantonista e registrar achados."
                            )

                    # PRIORIDADE
                    elif (
                        resp_hits >= 2
                        or shock_hits >= 2
                        or sepsis_hits >= 2
                        or (tr == "PIORA")
                        or (st in ["EM_RISCO", "INSTABILIZANDO", "EM_OBSERVACAO"])
                    ):
                        level = "PRIORIDADE"
                        confidence = 70

                        if resp_hits >= 2:
                            syndrome = "RISCO_RESPIRATORIO"
                            recommendation = "Risco respiratório em evolução. Reavaliar em 15–30 min e considerar avaliação médica."
                            actions = (
                                "1) Reavaliar SpO2/FR em 15 min.\n"
                                "2) Verificar necessidade/aderência ao O2 (se em uso).\n"
                                "3) Orientar posicionamento e monitorização contínua.\n"
                                "4) Se SpO2 cair ou FR subir, escalar para IMEDIATO."
                            )
                        elif shock_hits >= 2:
                            syndrome = "RISCO_HIPOPERFUSAO"
                            recommendation = "Risco hemodinâmico. Reavaliar em 15–30 min e considerar avaliação médica."
                            actions = (
                                "1) Repetir PA/FC em 15 min.\n"
                                "2) Verificar sangramento, diurese e perfusão periférica.\n"
                                "3) Se PAS cair/FC subir, escalar para IMEDIATO."
                            )
                        elif sepsis_hits >= 2:
                            syndrome = "RISCO_INFECCIOSO"
                            recommendation = "Sinais compatíveis com resposta inflamatória/infecciosa. Reavaliar em 30 min e considerar avaliação médica."
                            actions = (
                                "1) Repetir temperatura e FC em 30 min.\n"
                                "2) Avaliar foco (ferida, drenos, queixas, calafrios).\n"
                                "3) Se piora progressiva, escalar para avaliação médica."
                            )
                        elif pain_hits >= 2:
                            syndrome = "DOR_DESCONTROLADA"
                            recommendation = "Dor descontrolada com repercussão. Reavaliar analgesia e sinais vitais."
                            actions = (
                                "1) Reavaliar dor e sinais vitais em 30 min.\n"
                                "2) Checar analgesia prescrita e adesão.\n"
                                "3) Se PA/FC persistirem elevadas, comunicar médico."
                            )
                        else:
                            syndrome = "ATENCAO_TENDENCIA"
                            recommendation = "Tendência de piora detectada. Reavaliar e considerar avaliação médica."
                            actions = (
                                "1) Reavaliar sinais vitais em 30 min.\n"
                                "2) Manter monitorização e documentar evolução.\n"
                                "3) Se tendência persistir, escalar."
                            )

                    else:
                        syndrome = "ROTINA"
                        level = "ATENCAO"
                        confidence = 40
                        recommendation = "Sem sinais de deterioração iminente no recorte atual. Manter rotina e reavaliar."
                        actions = "Reavaliar sinais vitais conforme rotina. Documentar evolução. Se houver queixa, reavaliar antes."

                    cur.execute("""
                        INSERT INTO public.clinical_recommendations
                          (cod_atendimento, snapshot_ts, state, trend_state,
                           recommendation_level, recommendation,
                           syndrome, actions, confidence)
                        VALUES
                          (%s, %s, %s, %s,
                           %s, %s,
                           %s, %s, %s)
                        ON CONFLICT (cod_atendimento, snapshot_ts)
                        DO UPDATE SET
                          recommendation_level = EXCLUDED.recommendation_level,
                          recommendation = EXCLUDED.recommendation,
                          syndrome = EXCLUDED.syndrome,
                          actions = EXCLUDED.actions,
                          confidence = EXCLUDED.confidence,
                          updated_at = CURRENT_TIMESTAMP;
                    """, (
                        cod_atendimento, snapshot_ts, state, trend_state,
                        level, recommendation,
                        syndrome, actions, int(confidence or 0)
                    ))

                    upserted += 1

            conn.commit()

        return {"ok": True, "processed": len(rows), "upserted": upserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# RECOMMENDATIONS (list/patch) — ÚNICA rota /v1/assist/recommendations
# ============================================================
@app.get("/v1/assist/recommendations")
def list_recommendations(
    status: str = Query(default="NOVO", description="NOVO | EM_ATENDIMENTO | RESOLVIDO"),
    limit: int = Query(default=50, ge=1, le=500),
):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      id,
                      cod_atendimento,
                      snapshot_ts,
                      state,
                      trend_state,
                      recommendation_level,
                      recommendation,
                      syndrome,
                      actions,
                      confidence,
                      status,
                      created_at,
                      updated_at,
                      handled_at,
                      handled_by,
                      handled_note,
                      notified_at
                    FROM public.clinical_recommendations
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s;
                """, (status.upper().strip(), limit))
                rows = cur.fetchall()

        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "cod_atendimento": r["cod_atendimento"],
                "snapshot_ts": r["snapshot_ts"].isoformat() if r["snapshot_ts"] else None,
                "state": r["state"],
                "trend_state": r["trend_state"],
                "recommendation_level": r["recommendation_level"],
                "recommendation": r["recommendation"],
                "syndrome": r["syndrome"],
                "actions": r["actions"],
                "confidence": r["confidence"],
                "status": r["status"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                "handled_at": r["handled_at"].isoformat() if r["handled_at"] else None,
                "handled_by": r["handled_by"],
                "handled_note": r["handled_note"],
                "notified_at": r["notified_at"].isoformat() if r["notified_at"] else None,
            })

        return {"ok": True, "count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/v1/assist/recommendations/{rec_id}")
def update_recommendation(rec_id: int, payload: RecUpdateIn):
    st = payload.status.upper().strip()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if st == "RESOLVIDO":
                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET status = %s,
                            handled_at = CURRENT_TIMESTAMP,
                            handled_by = %s,
                            handled_note = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, cod_atendimento, status, updated_at, handled_at;
                    """, (st, payload.handled_by, payload.handled_note, rec_id))
                    row = cur.fetchone()
                else:
                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, cod_atendimento, status, updated_at;
                    """, (st, rec_id))
                    row = cur.fetchone()

            conn.commit()

        if not row:
            raise HTTPException(status_code=404, detail="recomendação não encontrada")

        resp = {
            "ok": True,
            "id": row["id"],
            "cod_atendimento": row["cod_atendimento"],
            "status": row["status"],
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }
        if st == "RESOLVIDO":
            resp["handled_at"] = row["handled_at"].isoformat() if row["handled_at"] else None
        return resp

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# DISPATCH (pull/mark) — opcional (pronto p/ orquestrador)
# ============================================================
def _mk_message_medico(item: dict) -> str:
    return (
        "⚠️ <b>ALERTA PREVITA – {level}</b>\n\n"
        "Atendimento: <b>{cod}</b>\n"
        "Síndrome: <b>{syn}</b>\n"
        "Confiança: <b>{conf}%</b>\n\n"
        "<b>Ações:</b>\n{actions}\n\n"
        "Registrar evolução e reavaliar imediatamente."
    ).format(
        level=item["recommendation_level"],
        cod=item["cod_atendimento"],
        syn=item.get("syndrome") or "NA",
        conf=item.get("confidence") or 0,
        actions=item.get("actions") or "-"
    )


def _mk_message_enfermagem(item: dict) -> str:
    return (
        "⚠️ <b>PRIORIDADE ASSISTENCIAL – PREVITA</b>\n\n"
        "Atendimento: <b>{cod}</b>\n"
        "Nível: <b>{level}</b>\n"
        "Confiança: <b>{conf}%</b>\n\n"
        "<b>Condutas:</b>\n{actions}\n\n"
        "Se piora, escalar para médico."
    ).format(
        cod=item["cod_atendimento"],
        level=item["recommendation_level"],
        conf=item.get("confidence") or 0,
        actions=item.get("actions") or "-"
    )


@app.post("/v1/assist/dispatch/pull")
def dispatch_pull(p: DispatchPullIn):
    """
    Retorna itens que precisam ser enviados AGORA (IMEDIATO/PRIORIDADE),
    e cria log PENDING (evita duplicar) — se você tiver dispatch_log criado.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      cod_atendimento,
                      snapshot_ts,
                      recommendation_level,
                      syndrome,
                      confidence,
                      actions
                    FROM public.clinical_recommendations
                    WHERE created_at >= (NOW() - (%s || ' minutes')::interval)
                      AND status = 'NOVO'
                      AND (
                        (recommendation_level = 'IMEDIATO' AND confidence >= 85)
                        OR
                        (recommendation_level = 'PRIORIDADE' AND confidence >= 70)
                      )
                    ORDER BY
                      CASE recommendation_level WHEN 'IMEDIATO' THEN 2 ELSE 1 END DESC,
                      confidence DESC NULLS LAST,
                      created_at ASC
                    LIMIT %s;
                """, (p.minutes_back, p.limit))

                rows = cur.fetchall()

                items: List[Dict[str, Any]] = []
                for r in rows:
                    base = {
                        "cod_atendimento": r["cod_atendimento"],
                        "snapshot_ts": r["snapshot_ts"],
                        "recommendation_level": r["recommendation_level"],
                        "syndrome": r["syndrome"],
                        "confidence": int(r["confidence"] or 0),
                        "actions": r["actions"] or ""
                    }

                    target = "MEDICO" if r["recommendation_level"] == "IMEDIATO" else "ENFERMAGEM"
                    msg = _mk_message_medico(base) if target == "MEDICO" else _mk_message_enfermagem(base)

                    items.append({
                        **base,
                        "target": target,
                        "channel": p.channel.upper(),
                        "message": msg
                    })

        return {"ok": True, "count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/v1/assist/dispatch/mark")
def dispatch_mark(p: DispatchMarkIn):
    """
    Endpoint “stub” (se você já tiver dispatch_log, dá pra implementar update aqui).
    Mantive por compatibilidade.
    """
    return {"ok": True, "status": p.status}


# ============================================================
# NOTIFY TELEGRAM (PRODUÇÃO) — ÚNICO endpoint
# - gatilho por created_at (evita retroativo explodir)
# - status NOVO
# - recommendation_level IMEDIATO/PRIORIDADE
# - anti-duplicação por tabela alert_notifications
# ============================================================
@app.post("/v1/notify/telegram/run")
async def notify_telegram_run(
    minutes_back: int = 720,  # janela do "novo" pelo created_at
    max_send: int = 10,
    x_api_key: str | None = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)
    _require_telegram()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # anti-duplicação
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.alert_notifications (
                      id bigserial PRIMARY KEY,
                      cod_atendimento int NOT NULL,
                      snapshot_ts timestamptz NOT NULL,
                      channel text NOT NULL DEFAULT 'TELEGRAM',
                      sent_at timestamptz NOT NULL DEFAULT now(),
                      status text NOT NULL DEFAULT 'SENT',
                      response text NULL
                    );
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_notifications
                    ON public.alert_notifications (cod_atendimento, snapshot_ts, channel);
                """)

                # gatilho por created_at => evita “lançamento retroativo” reabrir alerta velho
                cur.execute("""
                    SELECT
  cr.id,
  cr.cod_atendimento,
  cr.snapshot_ts,
  cr.state,
  cr.trend_state,
  cr.recommendation_level,
  cr.recommendation,
  cr.syndrome,
  cr.actions,
  cr.confidence
FROM clinical_recommendations cr
WHERE
(
  -- REGRA 1: SEMPRE ENVIAR CRÍTICO
  cr.state = 'CRITICO'

  OR

  -- REGRA 2: PRIORIDADE / IMEDIATO (controle normal)
  (
    cr.recommendation_level IN ('IMEDIATO', 'PRIORIDADE')
    AND cr.notified_at IS NULL
  )
)
ORDER BY
  CASE
    WHEN cr.state = 'CRITICO' THEN 2
    ELSE 1
  END DESC,
  cr.confidence DESC,
  cr.snapshot_ts DESC
LIMIT :max_send;

                """, (minutes_back, max_send))

                rows = cur.fetchall()
                found = len(rows)

                sent = 0
                skipped = 0

                for r in rows:
                    # reserva no log anti-duplicação
                    cur.execute("""
                        INSERT INTO public.alert_notifications (cod_atendimento, snapshot_ts, channel)
                        VALUES (%s, %s, 'TELEGRAM')
                        ON CONFLICT (cod_atendimento, snapshot_ts, channel) DO NOTHING;
                    """, (r["cod_atendimento"], r["snapshot_ts"]))

                    if cur.rowcount == 0:
                        skipped += 1
                        continue

                    msg = (
                        f"🚨 <b>PREVITA – ALERTA {r['recommendation_level']}</b>\n\n"
                        f"🆔 <b>Atendimento:</b> {r['cod_atendimento']}\n"
                        f"📌 <b>Estado:</b> {r.get('state') or '-'} / {r.get('trend_state') or '-'}\n"
                        f"🧩 <b>Síndrome:</b> {r.get('syndrome') or '-'}\n"
                        f"🧠 <b>Confiança:</b> {r.get('confidence') or '-'}\n"
                        f"⏱️ <b>Snapshot:</b> {r['snapshot_ts']}\n\n"
                        f"📣 <b>Recomendação:</b>\n{(r.get('recommendation') or '').strip()[:1200]}\n\n"
                        f"✅ <b>Ações:</b>\n{(r.get('actions') or '').strip()[:2500]}"
                    )

                    tg_resp = await send_telegram_message_async(msg)

                    # salva resp do telegram
                    cur.execute("""
                        UPDATE public.alert_notifications
                        SET response = %s
                        WHERE cod_atendimento=%s AND snapshot_ts=%s AND channel='TELEGRAM';
                    """, (str(tg_resp)[:2000], r["cod_atendimento"], r["snapshot_ts"]))

                    # marca como notificado (para consumo interno)
                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET notified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                    """, (r["id"],))

                    sent += 1

            conn.commit()

        return {"ok": True, "found": found, "sent": sent, "skipped_already_sent": skipped}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

