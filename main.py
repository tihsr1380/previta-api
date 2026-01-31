import os
import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional

app = FastAPI(title="PREVITA API", version="1.0.0")

DATABASE_URL = os.environ.get("DATABASE_URL")


# =========================
# MODELS
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
# INGEST VITALS
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
# RISK ENGINE (STATE + TRENDS)
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

    # ===== Tendências (últimas 6h) =====
    if delta("spo2") <= -3:
        score += 25
        reasons.append("Queda progressiva de SpO2 (6h)")

    if delta("fc") >= 15:
        score += 15
        reasons.append("Aumento progressivo de FC (6h)")

    if delta("fr") >= 5:
        score += 15
        reasons.append("Aumento progressivo de FR (6h)")

    if delta("pas") <= -20:
        score += 20
        reasons.append("Queda progressiva de PAS (6h)")

    if delta("temp") >= 1:
        score += 10
        reasons.append("Elevação progressiva de temperatura (6h)")

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


# =========================
# RISK RUN (APLICA ETAPA 4.3)
# =========================
@app.post("/v1/risk/run")
def run_risk(minutes_back: int = 60, limit: int = 500):
    """
    Calcula risco usando a tabela public.vitals_snapshot (último valor não-nulo por vital),
    e grava em public.risk_events (upsert por cod_atendimento + event_ts).
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Busca snapshots recentes (mais confiável do que vitals_raw linha-a-linha)
        cur.execute("""
            SELECT snapshot_ts, cod_atendimento, id_ricadpac,
                   temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia
            FROM public.vitals_snapshot
            WHERE snapshot_ts >= (
                (SELECT max(snapshot_ts) FROM public.vitals_snapshot)
                - (%s || ' minutes')::interval
            )
            ORDER BY snapshot_ts DESC
            LIMIT %s;
        """, (minutes_back, limit))

        rows = cur.fetchall()

        inserted = 0
        for r in rows:
            row = {
                "event_ts": r[0],  # <-- agora event_ts = snapshot_ts
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

            # Para snapshot, a parte de "tendência" ainda pode usar history se você quiser evoluir depois.
            # Por enquanto, calc_risk(row, history) vai funcionar bem com o estado atual.
            level, score, reason = calc_risk(row, history=[])

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
                  id_ricadpac = EXCLUDED.id_ricadpac,
                  temp = EXCLUDED.temp,
                  pas = EXCLUDED.pas,
                  pad = EXCLUDED.pad,
                  fc = EXCLUDED.fc,
                  fr = EXCLUDED.fr,
                  spo2 = EXCLUDED.spo2,
                  dor = EXCLUDED.dor,
                  uso_o2 = EXCLUDED.uso_o2,
                  nivel_consciencia = EXCLUDED.nivel_consciencia,
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
# ALERTS CHECK
# =========================
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

from pydantic import BaseModel

class SnapshotRunIn(BaseModel):
    minutes_back: int = 180   # janela de captura (últimos X minutos)
    limit_atend: int = 500    # limite de atendimentos processados por chamada

@app.post("/v1/snapshot/run")
def snapshot_run(p: SnapshotRunIn):
    """
    Consolida vitais_raw em 1 linha por atendimento (vitals_snapshot),
    pegando o último valor NÃO NULO de cada sinal vital dentro da janela.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        sql = """
        WITH recent AS (
          SELECT *
          FROM public.vitals_raw
          WHERE event_ts >= (
  (SELECT max(event_ts) FROM public.vitals_raw)
  - (%s || ' minutes')::interval
),

        atend AS (
          SELECT DISTINCT cod_atendimento
          FROM recent
          ORDER BY cod_atendimento
          LIMIT %s
        ),

        last_event AS (
          SELECT DISTINCT ON (r.cod_atendimento)
            r.cod_atendimento,
            r.event_ts AS snapshot_ts,
            r.id_ricadpac,
            r.profissional
          FROM recent r
          JOIN atend a ON a.cod_atendimento = r.cod_atendimento
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),

        last_temp AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.temp
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.temp IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_pas AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.pas
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.pas IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_pad AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.pad
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.pad IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_fc AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.fc
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.fc IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_fr AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.fr
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.fr IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_spo2 AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.spo2
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.spo2 IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_dor AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.dor
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.dor IS NOT NULL
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_uso_o2 AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.uso_o2
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.uso_o2 IS NOT NULL AND r.uso_o2 <> ''
          ORDER BY r.cod_atendimento, r.event_ts DESC
        ),
        last_nc AS (
          SELECT DISTINCT ON (r.cod_atendimento) r.cod_atendimento, r.nivel_consciencia
          FROM recent r JOIN atend a ON a.cod_atendimento=r.cod_atendimento
          WHERE r.nivel_consciencia IS NOT NULL AND r.nivel_consciencia <> ''
          ORDER BY r.cod_atendimento, r.event_ts DESC
        )

        INSERT INTO public.vitals_snapshot
          (cod_atendimento, snapshot_ts, id_ricadpac,
           temp, pas, pad, fc, fr, spo2, dor, uso_o2, nivel_consciencia,
           profissional, updated_at)
        SELECT
          e.cod_atendimento,
          e.snapshot_ts,
          e.id_ricadpac,
          t.temp,
          ps.pas,
          pd.pad,
          f.fc,
          fr.fr,
          s.spo2,
          d.dor,
          u.uso_o2,
          nc.nivel_consciencia,
          e.profissional,
          CURRENT_TIMESTAMP
        FROM last_event e
        LEFT JOIN last_temp t    ON t.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_pas  ps   ON ps.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_pad  pd   ON pd.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_fc   f    ON f.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_fr   fr   ON fr.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_spo2 s    ON s.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_dor  d    ON d.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_uso_o2 u  ON u.cod_atendimento = e.cod_atendimento
        LEFT JOIN last_nc   nc   ON nc.cod_atendimento = e.cod_atendimento

        ON CONFLICT (cod_atendimento)
        DO UPDATE SET
          snapshot_ts = EXCLUDED.snapshot_ts,
          id_ricadpac = COALESCE(EXCLUDED.id_ricadpac, public.vitals_snapshot.id_ricadpac),

          temp = COALESCE(EXCLUDED.temp, public.vitals_snapshot.temp),
          pas  = COALESCE(EXCLUDED.pas,  public.vitals_snapshot.pas),
          pad  = COALESCE(EXCLUDED.pad,  public.vitals_snapshot.pad),
          fc   = COALESCE(EXCLUDED.fc,   public.vitals_snapshot.fc),
          fr   = COALESCE(EXCLUDED.fr,   public.vitals_snapshot.fr),
          spo2 = COALESCE(EXCLUDED.spo2, public.vitals_snapshot.spo2),
          dor  = COALESCE(EXCLUDED.dor,  public.vitals_snapshot.dor),
          uso_o2 = COALESCE(EXCLUDED.uso_o2, public.vitals_snapshot.uso_o2),
          nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, public.vitals_snapshot.nivel_consciencia),

          profissional = COALESCE(EXCLUDED.profissional, public.vitals_snapshot.profissional),
          updated_at = CURRENT_TIMESTAMP;
        """

        cur.execute(sql, (p.minutes_back, p.limit_atend))
        conn.commit()

        cur.close()
        conn.close()

        return {"ok": True, "minutes_back": p.minutes_back, "limit_atend": p.limit_atend}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




