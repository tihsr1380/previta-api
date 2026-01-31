import os
import psycopg
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Literal


app = FastAPI(title="PREVITA API", version="1.0.0")
import requests

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não configurados")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error: {r.status_code} {r.text}")


DATABASE_URL = os.environ.get("DATABASE_URL")


# =========================
# MODELOS
# =========================
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
    note: Optional[str] = None  # opcional (observação futura)


# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")
    return psycopg.connect(DATABASE_URL)


def get_last_snapshot_ts(cur):
    """
    Retorna o MAX(snapshot_ts) de vitals_snapshot.
    Usamos isso como "agora clínico" para evitar bugs de timezone (NOW() em UTC).
    """
    cur.execute("SELECT MAX(snapshot_ts) FROM public.vitals_snapshot;")
    return cur.fetchone()[0]


# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}


# =========================
# INGEST VITAIS
# =========================
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


# =========================
# RISCO
# =========================
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
        cur.close()
        conn.close()

        return {"ok": True, "processed": len(rows), "upserted": inserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# ALERTAS (risk_events -> alerts)
# =========================
@app.post("/v1/alerts/check")
def alerts_check(p: AlertsIn):
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
                INSERT INTO public.alerts
                  (event_ts, cod_atendimento, risk_level, risk_score, risk_reason, status)
                VALUES
                  (%s, %s, %s, %s, %s, 'NOVO')
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


@app.get("/v1/alerts")
def list_alerts(
    status: str = Query(default="NOVO", description="NOVO | EM_ATENDIMENTO | RESOLVIDO"),
    limit: int = Query(default=50, ge=1, le=500),
):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, event_ts, cod_atendimento, risk_level, risk_score, risk_reason, status, created_at, updated_at
            FROM public.alerts
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s;
        """, (status.upper(), limit))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        items = []
        for r in rows:
            items.append({
                "id": r[0],
                "event_ts": r[1].isoformat() if r[1] else None,
                "cod_atendimento": r[2],
                "risk_level": r[3],
                "risk_score": r[4],
                "risk_reason": r[5],
                "status": r[6],
                "created_at": r[7].isoformat() if r[7] else None,
                "updated_at": r[8].isoformat() if r[8] else None,
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
        conn = get_conn()
        cur = conn.cursor()

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
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="alerta não encontrado")

        return {
            "ok": True,
            "id": row[0],
            "cod_atendimento": row[1],
            "status": row[2],
            "updated_at": row[3].isoformat() if row[3] else None
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# STATE (já estava correto: ancorado no MAX(snapshot_ts))
# =========================
class StateRunIn(BaseModel):
    minutes_back: int = 240
    history_minutes: int = 360
    limit: int = 500


def _safe_num(x):
    return None if x is None else float(x)


def calc_state(snapshot: dict, history: list[dict]):
    # (mantive exatamente como você mandou)
    reasons = []
    score = 0

    temp = _safe_num(snapshot.get("temp"))
    pas  = _safe_num(snapshot.get("pas"))
    pad  = _safe_num(snapshot.get("pad"))
    fc   = _safe_num(snapshot.get("fc"))
    fr   = _safe_num(snapshot.get("fr"))
    spo2 = _safe_num(snapshot.get("spo2"))
    uso_o2 = (snapshot.get("uso_o2") or "").strip().lower()
    nivel = (snapshot.get("nivel_consciencia") or "").strip().lower()

    def series(field):
        vals = [h.get(field) for h in history if h.get(field) is not None]
        return vals

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
        conn = get_conn()
        cur = conn.cursor()

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
            snapshot = {
                "cod_atendimento": s[0],
                "snapshot_ts": s[1],
                "id_ricadpac": s[2],
                "temp": s[3],
                "pas": s[4],
                "pad": s[5],
                "fc": s[6],
                "fr": s[7],
                "spo2": s[8],
                "dor": s[9],
                "uso_o2": s[10],
                "nivel_consciencia": s[11],
                "profissional": s[12],
            }

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
                    "event_ts": hr[0],
                    "temp": _safe_num(hr[1]),
                    "pas": _safe_num(hr[2]),
                    "fc": _safe_num(hr[3]),
                    "fr": _safe_num(hr[4]),
                    "spo2": _safe_num(hr[5]),
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
        cur.close()
        conn.close()

        return {"ok": True, "processed": len(snaps), "upserted": upserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# TRENDS (corrigido: ancorado no MAX(snapshot_ts))
# =========================
class TrendsRunIn(BaseModel):
    minutes_back: int = 1440
    history_minutes: int = 360
    limit: int = 500


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
        conn = get_conn()
        cur = conn.cursor()

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
            cod_atendimento = s[0]
            snapshot_ts     = s[1]

            snapshot = {
                "cod_atendimento": cod_atendimento,
                "snapshot_ts": snapshot_ts,
                "id_ricadpac": s[2],
                "temp": s[3],
                "pas": s[4],
                "pad": s[5],
                "fc": s[6],
                "fr": s[7],
                "spo2": s[8],
                "dor": s[9],
                "uso_o2": s[10],
                "nivel_consciencia": s[11],
            }

            cur.execute("""
                SELECT event_ts, temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia
                FROM public.vitals_raw
                WHERE cod_atendimento = %s
                  AND event_ts >= (%s - (%s || ' minutes')::interval)
                  AND event_ts <= %s
                ORDER BY event_ts ASC;
            """, (cod_atendimento, snapshot_ts, p.history_minutes, snapshot_ts))
            hist_rows = cur.fetchall()

            history = []
            for h in hist_rows:
                history.append({
                    "event_ts": h[0],
                    "temp": h[1],
                    "pas": h[2],
                    "pad": h[3],
                    "fc": h[4],
                    "fr": h[5],
                    "spo2": h[6],
                    "dor": h[7],
                    "uso_o2": h[8],
                    "nivel_consciencia": h[9],
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
            """, (cod_atendimento, snapshot_ts, trend_state, trend_score, trend_reason))

            upserted += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "processed": len(snaps), "upserted": upserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# EARLY ALERTS
# =========================
class EarlyAlertsIn(BaseModel):
    minutes_back: int = 1440
    min_trend_state: str = "ATENCAO"
    exclude_state: str | None = None


@app.post("/v1/early-alerts/check")
def early_alerts_check(p: EarlyAlertsIn):
    rank = {"ESTAVEL": 1, "ATENCAO": 2, "PIORA": 3}
    min_rank = rank.get(p.min_trend_state.upper(), 2)

    try:
        conn = get_conn()
        cur = conn.cursor()

        # Aqui também vale o mesmo cuidado. Ancoramos no last_snapshot.
        last_snapshot = get_last_snapshot_ts(cur)
        if not last_snapshot:
            return {"ok": True, "scanned": 0, "inserted": 0, "reason": "Sem snapshots"}

        cur.execute("""
            SELECT t.snapshot_ts, t.cod_atendimento, t.trend_state, t.trend_score, t.trend_reason
            FROM public.clinical_trends t
            WHERE t.snapshot_ts >= (%s - (%s || ' minutes')::interval)
            ORDER BY t.snapshot_ts DESC;
        """, (last_snapshot, p.minutes_back))
        trends = cur.fetchall()

        inserted = 0
        scanned = 0

        for snapshot_ts, cod_atendimento, trend_state, trend_score, trend_reason in trends:
            scanned += 1
            ts = (trend_state or "").upper()

            if rank.get(ts, 0) < min_rank:
                continue

            if p.exclude_state:
                cur.execute("""
                    SELECT state
                    FROM public.clinical_state
                    WHERE cod_atendimento = %s AND snapshot_ts = %s
                    LIMIT 1;
                """, (cod_atendimento, snapshot_ts))
                r = cur.fetchone()
                if r and (r[0] or "").upper() == p.exclude_state.upper():
                    continue

            alert_level = ts

            cur.execute("""
                INSERT INTO public.early_alerts
                  (snapshot_ts, cod_atendimento, alert_level, alert_score, alert_reason)
                VALUES
                  (%s, %s, %s, %s, %s)
                ON CONFLICT (cod_atendimento, snapshot_ts, alert_level)
                DO NOTHING;
            """, (snapshot_ts, cod_atendimento, alert_level, int(trend_score or 0), trend_reason or ""))

            if cur.rowcount == 1:
                inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "scanned": scanned, "inserted": inserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# ASSIST ALERTS (corrigido: ancorado no MAX(snapshot_ts))
# =========================
class AssistAlertsIn(BaseModel):
    minutes_back: int = 360
    min_score: int = 60
    include_states: list[str] = ["PIORA", "CRITICO"]


@app.post("/v1/assist/alerts/run")
def assist_alerts_run(p: AssistAlertsIn):
    try:
        conn = get_conn()
        cur = conn.cursor()

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

        for (cod_atendimento, snapshot_ts, trend_state, trend_score, trend_reason) in rows:
            if (trend_state or "").upper() == "CRITICO":
                alert_level = "CRITICO"
            else:
                alert_level = "ATENCAO"

            cur.execute("""
                INSERT INTO public.assistential_alerts
                  (cod_atendimento, snapshot_ts, alert_level, alert_score, alert_reason, status)
                VALUES
                  (%s, %s, %s, %s, %s, 'NOVO')
                ON CONFLICT ON CONSTRAINT uq_assist_alert_active DO NOTHING;
            """, (cod_atendimento, snapshot_ts, alert_level, int(trend_score or 0), trend_reason or ""))

            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "scanned": len(rows), "inserted": inserted, "skipped_active": skipped}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# RECOMMENDATIONS (✅ CORRIGIDO: ancorado no MAX(snapshot_ts))
# =========================
@app.post("/v1/assist/recommendations/run")
def run_recommendations(minutes_back: int = 180, limit: int = 1000):
    try:
        conn = get_conn()
        cur = conn.cursor()

        # tempo clínico (não NOW())
        cur.execute("SELECT MAX(snapshot_ts) FROM public.vitals_snapshot;")
        last_snapshot = cur.fetchone()[0]
        if not last_snapshot:
            return {"ok": True, "processed": 0, "upserted": 0, "reason": "Sem snapshots em vitals_snapshot"}

        cur.execute("""
            SELECT
                s.cod_atendimento,
                s.snapshot_ts,
                s.state,
                t.trend_state,
                COALESCE(v.spo2, NULL) AS spo2,
                COALESCE(v.fr, NULL)   AS fr,
                COALESCE(v.fc, NULL)   AS fc,
                COALESCE(v.pas, NULL)  AS pas,
                COALESCE(v.pad, NULL)  AS pad,
                COALESCE(v.temp, NULL) AS temp,
                COALESCE(v.dor, NULL)  AS dor,
                v.uso_o2,
                v.nivel_consciencia
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
            (
                cod_atendimento, snapshot_ts, state, trend_state,
                spo2, fr, fc, pas, pad, temp, dor,
                uso_o2, nivel_consciencia
            ) = r

            # normalização
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

            # =========================
            # Motor de Síndromes (E5.2)
            # =========================

            # 1) Falência respiratória precoce / deterioração respiratória
            resp_hits = 0
            if spo2 is not None and spo2 < 92: resp_hits += 1
            if fr is not None and fr >= 24: resp_hits += 1
            if uso in ["sim", "s", "cateter", "mascara", "venturi", "o2", "uso"]: resp_hits += 1
            if tr == "PIORA" and spo2 is not None and spo2 < 94: resp_hits += 1

            # 2) Choque/hipoperfusão (sangramento/desidratação/vasodilatação)
            shock_hits = 0
            if pas is not None and pas <= 90: shock_hits += 1
            if fc is not None and fc >= 120: shock_hits += 1
            if tr == "PIORA" and pas is not None and pas < 95: shock_hits += 1

            # 3) Sepse / inflamação sistêmica (screening inicial)
            sepsis_hits = 0
            if temp is not None and temp >= 38: sepsis_hits += 1
            if fc is not None and fc >= 110: sepsis_hits += 1
            if tr == "PIORA": sepsis_hits += 1

            # 4) Dor descontrolada (impacto hemodinâmico e risco de sangramento)
            pain_hits = 0
            if dor is not None and dor >= 7: pain_hits += 1
            if fc is not None and fc >= 110: pain_hits += 1
            if pas is not None and pas >= 160: pain_hits += 1  # se você usa PAS alta como risco

            # 5) Neurológico/sedação (consciência alterada)
            neuro_hits = 0
            if niv in ["sonolenta", "confuso", "rebaixado", "rebaixado importante", "inconsciente"]: neuro_hits += 1
            if spo2 is not None and spo2 < 94: neuro_hits += 1
            if fr is not None and fr <= 10: neuro_hits += 1

            # =========================
            # Priorização (ordem importa)
            # =========================

            # IMEDIATO - risco direto/ameaça
            if (
                st == "CRITICO"
                or (resp_hits >= 3 and (spo2 is not None and spo2 < 90))
                or (shock_hits >= 2 and (pas is not None and pas <= 85))
                or (fc is not None and fc >= 180)
                or (fr is not None and fr >= 30)
            ):
                level = "IMEDIATO"
                confidence = 90

                # decide síndrome principal
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

            # PRIORIDADE - precisa agir em minutos e reavaliar
            elif (
                resp_hits >= 2
                or shock_hits >= 2
                or sepsis_hits >= 2
                or (tr == "PIORA")
                or (st in ["EM_RISCO", "INSTABILIZANDO", "EM_OBSERVACAO"])
            ):
                level = "PRIORIDADE"
                confidence = 70

                # síndrome mais provável
                if resp_hits >= 2:
                    syndrome = "RISCO_RESPIRATORIO"
                    recommendation = "Risco respiratório em evolução. Reavaliar em até 15–30 min e considerar avaliação médica."
                    actions = (
                        "1) Reavaliar SpO2/FR em 15 min.\n"
                        "2) Verificar necessidade/aderência ao O2 (se em uso).\n"
                        "3) Orientar posicionamento e monitorização contínua.\n"
                        "4) Se SpO2 cair ou FR subir, escalar para IMEDIATO."
                    )
                elif shock_hits >= 2:
                    syndrome = "RISCO_HIPOPERFUSAO"
                    recommendation = "Risco hemodinâmico. Reavaliar em até 15–30 min e considerar avaliação médica."
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

            # ATENÇÃO / ROTINA
            else:
                syndrome = "ROTINA"
                level = "ATENCAO"
                confidence = 40
                recommendation = "Sem sinais de deterioração iminente no recorte atual. Manter rotina e reavaliar."
                actions = "Reavaliar sinais vitais conforme rotina. Documentar evolução. Se houver queixa clínica, reavaliar antes."

            # UPSERT com novos campos (syndrome/actions/confidence)
            cur.execute("""
                INSERT INTO public.clinical_recommendations
                  (cod_atendimento, snapshot_ts, state, trend_state,
                   recommendation_level, recommendation,
                   syndrome, actions, confidence)
                VALUES (%s, %s, %s, %s,
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
        cur.close()
        conn.close()

        return {"ok": True, "processed": len(rows), "upserted": upserted}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/assist/recommendations")
def list_recommendations(
    minutes_back: int = Query(default=720, ge=1, le=10080),
    level: str | None = Query(default=None, description="IMEDIATO | PRIORIDADE | ATENCAO"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Lista recomendações recentes para consumo no Power Automate/painel.
    Filtros:
      - minutes_back: janela de tempo
      - level: opcional (IMEDIATO, PRIORIDADE, ATENCAO)
      - limit: paginação simples
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        if level:
            cur.execute("""
                SELECT
                    cod_atendimento,
                    snapshot_ts,
                    state,
                    trend_state,
                    recommendation_level,
                    recommendation,
                    created_at
                FROM public.clinical_recommendations
                WHERE snapshot_ts >= (NOW() - (%s || ' minutes')::interval)
                  AND UPPER(recommendation_level) = UPPER(%s)
                ORDER BY snapshot_ts DESC
                LIMIT %s;
            """, (minutes_back, level, limit))
        else:
            cur.execute("""
                SELECT
                    cod_atendimento,
                    snapshot_ts,
                    state,
                    trend_state,
                    recommendation_level,
                    recommendation,
                    created_at
                FROM public.clinical_recommendations
                WHERE snapshot_ts >= (NOW() - (%s || ' minutes')::interval)
                ORDER BY snapshot_ts DESC
                LIMIT %s;
            """, (minutes_back, limit))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        items = []
        for r in rows:
            items.append({
                "cod_atendimento": r[0],
                "snapshot_ts": r[1].isoformat() if r[1] else None,
                "state": r[2],
                "trend_state": r[3],
                "recommendation_level": r[4],
                "recommendation": r[5],
                "created_at": r[6].isoformat() if r[6] else None,
            })

        return {"ok": True, "count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
from fastapi import Body

class RecUpdateIn(BaseModel):
    status: Literal["NOVO", "EM_ATENDIMENTO", "RESOLVIDO"]
    handled_by: Optional[str] = None
    handled_note: Optional[str] = None


@app.get("/v1/assist/recommendations")
def list_recommendations(
    status: str = Query(default="NOVO", description="NOVO | EM_ATENDIMENTO | RESOLVIDO"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Lista recomendações assistenciais por status (padrão NOVO).
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT
              id,
              cod_atendimento,
              snapshot_ts,
              state,
              trend_state,
              recommendation_level,
              recommendation,
              status,
              created_at,
              updated_at,
              handled_at,
              handled_by,
              handled_note
            FROM public.clinical_recommendations
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s;
        """, (status.upper().strip(), limit))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        items = []
        for r in rows:
            items.append({
                "id": r[0],
                "cod_atendimento": r[1],
                "snapshot_ts": r[2].isoformat() if r[2] else None,
                "state": r[3],
                "trend_state": r[4],
                "recommendation_level": r[5],
                "recommendation": r[6],
                "status": r[7],
                "created_at": r[8].isoformat() if r[8] else None,
                "updated_at": r[9].isoformat() if r[9] else None,
                "handled_at": r[10].isoformat() if r[10] else None,
                "handled_by": r[11],
                "handled_note": r[12],
            })

        return {"ok": True, "count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/v1/assist/recommendations/{rec_id}")
def update_recommendation(rec_id: int, payload: RecUpdateIn):
    """
    Atualiza status da recomendação assistencial:
      - EM_ATENDIMENTO: marca updated_at
      - RESOLVIDO: marca handled_at + handled_by + handled_note + updated_at
    """
    st = payload.status.upper().strip()

    try:
        conn = get_conn()
        cur = conn.cursor()

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
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="recomendação não encontrada")

        # resposta
        resp = {
            "ok": True,
            "id": row[0],
            "cod_atendimento": row[1],
            "status": row[2],
            "updated_at": row[3].isoformat() if row[3] else None,
        }
        if st == "RESOLVIDO":
            resp["handled_at"] = row[4].isoformat() if row[4] else None

        return resp

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from typing import List, Dict, Any
from fastapi import Body

class DispatchPullIn(BaseModel):
    minutes_back: int = 720          # janela de busca (12h)
    limit: int = 50                  # quantos itens retornar
    channel: str = "WHATSAPP"        # WHATSAPP | SMS | TEST

def _mk_message_medico(item: dict) -> str:
    return (
        "⚠️ ALERTA PREVITA – {level}\n\n"
        "Atendimento: {cod}\n"
        "Síndrome: {syn}\n"
        "Confiança: {conf}%\n\n"
        "Ações (resumo):\n{actions}\n\n"
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
        "⚠️ PRIORIDADE ASSISTENCIAL – PREVITA\n\n"
        "Atendimento: {cod}\n"
        "Nível: {level}\n"
        "Confiança: {conf}%\n\n"
        "Condutas:\n{actions}\n\n"
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
    e cria log PENDING (evita duplicar).
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        # regra de corte (pode ajustar depois)
        cur.execute("""
            SELECT
              cod_atendimento,
              snapshot_ts,
              recommendation_level,
              syndrome,
              confidence,
              actions
            FROM public.clinical_recommendations
            WHERE snapshot_ts >= (
              (SELECT max(snapshot_ts) FROM public.clinical_recommendations)
              - (%s || ' minutes')::interval
            )
              AND (
                (recommendation_level = 'IMEDIATO' AND confidence >= 85)
                OR
                (recommendation_level = 'PRIORIDADE' AND confidence >= 70)
              )
            ORDER BY snapshot_ts DESC
            LIMIT %s;
        """, (p.minutes_back, p.limit))

        rows = cur.fetchall()

        items: List[Dict[str, Any]] = []
        for (cod, snapshot_ts, level, syndrome, confidence, actions) in rows:
            base = {
                "cod_atendimento": cod,
                "snapshot_ts": snapshot_ts,
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": int(confidence or 0),
                "actions": actions or ""
            }

            # target por severidade
            target = "MEDICO" if level == "IMEDIATO" else "ENFERMAGEM"

            # tenta “reservar” no log para não duplicar
            cur.execute("""
                INSERT INTO public.dispatch_log
                  (cod_atendimento, snapshot_ts, target, channel, status)
                VALUES
                  (%s, %s, %s, %s, 'PENDING')
                ON CONFLICT (cod_atendimento, snapshot_ts, target)
                DO NOTHING;
            """, (cod, snapshot_ts, target, p.channel.upper()))

            if cur.rowcount != 1:
                # já estava reservado/mandado antes
                continue

            # monta mensagem pronta
            if target == "MEDICO":
                msg = _mk_message_medico(base)
            else:
                msg = _mk_message_enfermagem(base)

            items.append({
                **base,
                "target": target,
                "channel": p.channel.upper(),
                "message": msg
            })

        conn.commit()
        cur.close()
        conn.close()

        return {"ok": True, "count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class DispatchMarkIn(BaseModel):
    cod_atendimento: int
    snapshot_ts: str  # iso
    target: Literal["MEDICO", "ENFERMAGEM"]
    status: Literal["SENT", "FAILED", "ACK"] = "SENT"
    error: Optional[str] = None

@app.post("/v1/assist/dispatch/mark")
def dispatch_mark(p: DispatchMarkIn):
    """
    Robô chama isso depois que tentou enviar (SENT/FAILED) ou quando alguém confirmou (ACK).
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            UPDATE public.dispatch_log
            SET status = %s,
                attempts = attempts + 1,
                last_error = %s,
                sent_at = CASE WHEN %s='SENT' THEN CURRENT_TIMESTAMP ELSE sent_at END
            WHERE cod_atendimento = %s
              AND snapshot_ts = %s::timestamp
              AND target = %s
            RETURNING id;
        """, (p.status, p.error, p.status, p.cod_atendimento, p.snapshot_ts, p.target))

        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="dispatch_log não encontrado")

        return {"ok": True, "status": p.status}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))








