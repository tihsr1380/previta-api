import os
import psycopg
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional, Literal

app = FastAPI(title="PREVITA API", version="1.0.0")

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


# PATCH para atualizar status do alerta
class AlertUpdateIn(BaseModel):
    status: Literal["NOVO", "EM_ATENDIMENTO", "RESOLVIDO"]


# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")
    return psycopg.connect(DATABASE_URL)


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

    # ===== Estado atual =====
    temp = row.get("temp")
    pas  = row.get("pas")
    fc   = row.get("fc")
    fr   = row.get("fr")
    spo2 = row.get("spo2")
    nivel = (row.get("nivel_consciencia") or "").lower()
    uso_o2 = (row.get("uso_o2") or "").lower()

    # Regras pontuais
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

    # ===== Tendências (requer histórico preenchido) =====
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

    # Uso de O2 + tendência respiratória
    if uso_o2 in ["aa", "sim"] and delta("spo2") < 0:
        score += 10
        reasons.append("Uso de O2 + queda de SpO2")

    # Classificação final
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
    """
    Calcula risco e faz UPSERT em risk_events.
    Observação: tendências só funcionam bem quando o 'history' é construído por paciente
    (vamos melhorar isso em uma etapa posterior).
    """
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

            # Hoje está passando history vazio (tendências ficam neutras).
            # Vamos corrigir isso numa próxima etapa (histórico por paciente).
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
# ALERTAS
# =========================
@app.post("/v1/alerts/check")
def alerts_check(p: AlertsIn):
    """
    Varre risk_events recentes e grava alertas em public.alerts
    (sem duplicar via UNIQUE(cod_atendimento, risk_level, event_ts)).

    Agora também grava status='NOVO' no insert.
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
    """
    Lista alertas por status (padrão: NOVO).
    Ideal para Power Automate buscar o que precisa notificar.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, event_ts, cod_atendimento, risk_level, risk_score, risk_reason, status, created_at
            FROM public.alerts
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (status.upper(), limit))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        data = []
        for r in rows:
            data.append({
                "id": r[0],
                "event_ts": r[1].isoformat() if r[1] else None,
                "cod_atendimento": r[2],
                "risk_level": r[3],
                "risk_score": r[4],
                "risk_reason": r[5],
                "status": r[6],
                "created_at": r[7].isoformat() if r[7] else None,
            })

        return {"ok": True, "count": len(data), "items": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/v1/alerts/{alert_id}")
def update_alert(alert_id: int, payload: AlertUpdateIn):
    """
    Atualiza status do alerta.
    - EM_ATENDIMENTO ou RESOLVIDO
    - Preenche handled_at quando RESOLVIDO
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        new_status = payload.status.upper()

        if new_status == "RESOLVIDO":
            cur.execute("""
                UPDATE public.alerts
                SET status = %s,
                    handled_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_status, alert_id))
        else:
            cur.execute("""
                UPDATE public.alerts
                SET status = %s
                WHERE id = %s
            """, (new_status, alert_id))

        conn.commit()

        if cur.rowcount == 0:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail="alert_id não encontrado")

        cur.close()
        conn.close()

        return {"ok": True, "id": alert_id, "status": new_status}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from typing import Optional
from pydantic import BaseModel

class AlertStatusIn(BaseModel):
    status: str  # "EM_ATENDIMENTO" ou "RESOLVIDO"
    note: Optional[str] = None  # opcional (observação)

@app.get("/v1/alerts")
def list_alerts(status: str = "NOVO", limit: int = 50):
    """
    Lista alertas por status. Ex:
      /v1/alerts?status=NOVO&limit=50
      /v1/alerts?status=EM_ATENDIMENTO
    """
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
    """
    Atualiza status de um alerta.
    status permitido: EM_ATENDIMENTO, RESOLVIDO
    """
    st = (payload.status or "").upper().strip()
    if st not in ("EM_ATENDIMENTO", "RESOLVIDO"):
        raise HTTPException(status_code=400, detail="status inválido. Use EM_ATENDIMENTO ou RESOLVIDO.")

    try:
        conn = get_conn()
        cur = conn.cursor()

        # opcional: se quiser salvar note no futuro, criamos coluna depois.
        # por enquanto só status + updated_at
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

class StateRunIn(BaseModel):
    minutes_back: int = 240   # janela para considerar snapshots recentes
    history_minutes: int = 360  # histórico para tendência/persistência (6h)
    limit: int = 500

def _safe_num(x):
    return None if x is None else float(x)

def calc_state(snapshot: dict, history: list[dict]):
    """
    Retorna: (state, state_score, reason)
    Estados:
      - ESTAVEL
      - EM_OBSERVACAO
      - COMPENSANDO
      - EM_RISCO
      - INSTABILIZANDO
      - CRITICO
    """
    reasons = []
    score = 0

    # ===== dados atuais (snapshot) =====
    temp = _safe_num(snapshot.get("temp"))
    pas  = _safe_num(snapshot.get("pas"))
    pad  = _safe_num(snapshot.get("pad"))
    fc   = _safe_num(snapshot.get("fc"))
    fr   = _safe_num(snapshot.get("fr"))
    spo2 = _safe_num(snapshot.get("spo2"))
    uso_o2 = (snapshot.get("uso_o2") or "").strip().lower()
    nivel = (snapshot.get("nivel_consciencia") or "").strip().lower()

    # ===== helpers de tendência/persistência =====
    def series(field):
        vals = [h.get(field) for h in history if h.get(field) is not None]
        return vals

    def delta(field):
        vals = series(field)
        if len(vals) < 2:
            return 0
        return vals[-1] - vals[0]

    def last(field):
        vals = series(field)
        return vals[-1] if vals else None

    def minv(field):
        vals = series(field)
        return min(vals) if vals else None

    def maxv(field):
        vals = series(field)
        return max(vals) if vals else None

    # Persistência simples: quantas leituras anormais em sequência (últimas 3)
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

    # ===== Regras de estado (pensamento clínico) =====
    # 1) CRÍTICO (ameaça imediata)
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
        # estado crítico com múltiplos sistemas envolvidos
        score = 90 + min(10, critical_hits * 2)
        return "CRITICO", min(score, 100), " | ".join(reasons)

    # 2) INSTABILIZANDO (já fora do normal + tendência/persistência)
    # foco: sinais compensatórios e piora progressiva
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

    # uso de O2 + piora sugere risco de deterioração respiratória
    if uso_o2 in ["sim", "cateter", "mascara", "venturi", "o2"] and delta("spo2") < 0:
        score += 10
        reasons.append("Uso de O2 com piora de SpO2")

    # Se score já subiu bastante, classifica como INSTABILIZANDO
    if score >= 45:
        return "INSTABILIZANDO", min(score + 35, 100), " | ".join(reasons)

    # 3) EM_RISCO (alteração relevante sem franca instabilidade)
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

    # 4) COMPENSANDO (valores quase normais, mas tendência/persistência sugere compensação)
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

    # 5) EM_OBSERVACAO (pequenas alterações, sem tendência forte)
    if (spo2 is not None and 94 <= spo2 < 96) or (fc is not None and 95 <= fc <= 110) or (fr is not None and 19 <= fr <= 22):
        reasons.append("Pequenas alterações: manter observação e reavaliar")
        return "EM_OBSERVACAO", 20, " | ".join(reasons)

    # 6) ESTÁVEL
    return "ESTAVEL", 10, "Sinais dentro do esperado sem tendência de piora"

@app.post("/v1/state/run")
def state_run(p: StateRunIn):
    """
    Calcula clinical_state com base em vitals_snapshot (estado atual)
    + histórico recente em vitals_raw (tendência/persistência).
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        # snapshots recentes (ancorado no max(snapshot_ts) para evitar problema de timezone)
        cur.execute("""
            SELECT cod_atendimento, snapshot_ts, id_ricadpac,
                   temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia, profissional
            FROM public.vitals_snapshot
            WHERE snapshot_ts >= (
                (SELECT max(snapshot_ts) FROM public.vitals_snapshot)
                - (%s || ' minutes')::interval
            )
            ORDER BY snapshot_ts DESC
            LIMIT %s;
        """, (p.minutes_back, p.limit))
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

            # histórico do mesmo atendimento (ancorado no snapshot_ts)
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

            # upsert por atendimento + snapshot_ts
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






