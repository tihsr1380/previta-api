# =========================
# PREVITA API — MAIN (v3) — SÊNIOR
# Corrige:
# 1) event_ts vindo como 00:00 (cria timestamp real com hora/minuto)
# 2) colisão de event_key (inclui minuto no timestamp real)
# 3) ON CONFLICT agora faz MERGE (preenche campos faltantes)
# 4) aceita dor numérica (0) sem quebrar (converte para string)
# 5) UPSERT em lote com retorno de inserted/updated
# =========================

import os
from typing import Optional, List, Any, Dict, Union
from datetime import datetime, date, time

import requests
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field, model_validator

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.0.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    # autocommit False por padrão; usamos commit explícito
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    # Se ALERT_API_KEY não estiver setada, libera (modo dev)
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# TELEGRAM
# =========================
def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=15)
    except Exception:
        # não derruba ingestão por falha de notificação
        return

# =========================
# MODELS
# =========================
class VitalRow(BaseModel):
    cod_atendimento: int
    id_ricadpac: Optional[int] = None

    # Pode vir do PowerBI como datetime (às vezes 00:00:00).
    # Pode vir também como "data_lanc" separada (alguns fluxos montam assim).
    event_ts: Optional[datetime] = None
    data_lanc: Optional[Union[date, datetime, str]] = None  # aceita string "2026-02-02"

    hora: Optional[int] = None
    minuto: Optional[int] = None

    temp: Optional[float] = None
    pas: Optional[int] = None
    pad: Optional[int] = None
    fc: Optional[int] = None
    fr: Optional[int] = None
    spo2: Optional[int] = None

    # dor no PowerBI costuma vir 0 / 0,00 (numérico)
    dor: Optional[Union[str, int, float]] = None

    uso_o2: Optional[str] = None
    nivel_consciencia: Optional[str] = None
    profissional: Optional[str] = None

    @model_validator(mode="after")
    def normalize_fields(self):
        """
        1) Normaliza dor numérica para string (para não quebrar o insert).
        2) Garante event_ts REAL (data + hora + minuto) quando:
           - event_ts é None, ou
           - event_ts veio com 00:00:00 mas há hora/minuto, ou
           - event_ts veio como data apenas.
        """
        # dor: converte para string se vier numérico
        if self.dor is not None and not isinstance(self.dor, str):
            # mantém "0" ao invés de "0.0" quando possível
            if isinstance(self.dor, float) and self.dor.is_integer():
                self.dor = str(int(self.dor))
            else:
                self.dor = str(self.dor)

        # Determina base_date
        base_dt: Optional[datetime] = None

        # 1) se event_ts existe
        if self.event_ts is not None:
            base_dt = self.event_ts

        # 2) senão tenta data_lanc
        if base_dt is None and self.data_lanc is not None:
            if isinstance(self.data_lanc, datetime):
                base_dt = self.data_lanc
            elif isinstance(self.data_lanc, date):
                base_dt = datetime.combine(self.data_lanc, time(0, 0, 0))
            elif isinstance(self.data_lanc, str):
                # tenta "YYYY-MM-DD" ou ISO
                try:
                    if "T" in self.data_lanc:
                        base_dt = datetime.fromisoformat(self.data_lanc)
                    else:
                        base_dt = datetime.combine(date.fromisoformat(self.data_lanc), time(0, 0, 0))
                except Exception:
                    # se não parsear, deixa base_dt None (vai falhar abaixo)
                    base_dt = None

        if base_dt is None:
            raise ValueError("event_ts ou data_lanc precisam estar presentes (ao menos a data)")

        # Se hora/minuto vierem, monta timestamp real
        if self.hora is not None and self.minuto is not None:
            # Se veio 00:00:00 ou veio uma data que não representa hora/minuto, substitui
            if base_dt.hour == 0 and base_dt.minute == 0 and base_dt.second == 0:
                self.event_ts = datetime.combine(base_dt.date(), time(self.hora, self.minuto, 0))
            else:
                # Mesmo se tiver hora no event_ts, preferimos a hora/minuto do registro
                self.event_ts = datetime.combine(base_dt.date(), time(self.hora, self.minuto, 0))
        else:
            # sem hora/minuto, mantém o event_ts como está
            self.event_ts = base_dt

        return self

class VitalBatch(BaseModel):
    rows: List[VitalRow] = Field(default_factory=list)

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok", "version": app.version}

# =========================
# INIT TABLES
# =========================
def ensure_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vitals_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_key TEXT UNIQUE,
                    cod_atendimento INT NOT NULL,
                    id_ricadpac INT NULL,
                    event_ts TIMESTAMP NOT NULL,
                    hora INT NULL,
                    minuto INT NULL,
                    temp DOUBLE PRECISION NULL,
                    pas INT NULL,
                    pad INT NULL,
                    fc INT NULL,
                    fr INT NULL,
                    spo2 INT NULL,
                    dor TEXT NULL,
                    uso_o2 TEXT NULL,
                    nivel_consciencia TEXT NULL,
                    profissional TEXT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_vitals_events_ts
                    ON public.vitals_events (event_ts DESC);

                CREATE TABLE IF NOT EXISTS public.clinical_recommendations (
                    id BIGSERIAL PRIMARY KEY,
                    cod_atendimento INT NOT NULL,
                    snapshot_ts TIMESTAMP NOT NULL,
                    recommendation_level TEXT NOT NULL, -- IMEDIATO | PRIORIDADE | OK
                    syndrome TEXT NULL,
                    confidence DOUBLE PRECISION NULL,
                    actions TEXT NULL,
                    notified_at TIMESTAMP NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_clinrec_att_ts
                    ON public.clinical_recommendations (cod_atendimento, snapshot_ts DESC);
            """)
        conn.commit()

@app.on_event("startup")
def _startup():
    ensure_tables()

# =========================
# LAST EVENT TS
# =========================
@app.get("/v1/vitals/last_event_ts")
def last_event_ts(x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(event_ts) AS last_ts FROM public.vitals_events;")
            row = cur.fetchone()
    last_ts = row["last_ts"]
    return {"last_event_ts": last_ts.isoformat() if last_ts else "1970-01-01T00:00:00"}

# =========================
# PIPELINE (auto)
# =========================
def compute_recommendations_for_batch(rows: List[VitalRow]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = "OK"
        syndrome = None
        confidence = None
        actions = None

        if r.spo2 is not None and r.spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.7
            actions = "Reavaliar oximetria, verificar O2, checar sinais de desconforto respiratório e acionar enfermagem/médico conforme protocolo."

        if r.pas is not None and r.pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.8
            actions = "Checar PA manual, perfusão, sangramento/dor, acionar médico imediatamente conforme protocolo."

        if level != "OK":
            recs.append({
                "cod_atendimento": r.cod_atendimento,
                "snapshot_ts": r.event_ts,
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": confidence,
                "actions": actions
            })
    return recs

def persist_recommendations_and_notify(recs: List[Dict[str, Any]]):
    if not recs:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for rec in recs:
                cur.execute("""
                    INSERT INTO public.clinical_recommendations
                        (cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP);
                """, (
                    rec["cod_atendimento"],
                    rec["snapshot_ts"],
                    rec["recommendation_level"],
                    rec.get("syndrome"),
                    rec.get("confidence"),
                    rec.get("actions"),
                ))

                if rec["recommendation_level"] == "IMEDIATO":
                    msg = (
                        f"🚨 <b>PREVITA – ALERTA IMEDIATO</b>\n\n"
                        f"🧾 <b>Atendimento:</b> {rec['cod_atendimento']}\n"
                        f"🕒 <b>Snapshot:</b> {rec['snapshot_ts']}\n"
                        f"🧠 <b>Síndrome:</b> {rec.get('syndrome') or '-'}\n"
                        f"✅ <b>Ações:</b>\n{(rec.get('actions') or '-').strip()[:3500]}"
                    )
                    send_telegram_message_sync(msg)

        conn.commit()

# =========================
# INGEST — BATCH (UPSERT MERGE)
# =========================
@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: VitalBatch,
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    if not payload.rows:
        return {"ok": True, "inserted": 0, "updated": 0, "message": "Sem linhas"}

    # UPSERT que:
    # - insere se não existe
    # - se existe, atualiza SOMENTE quando o valor novo não for NULL
    #   (perfeito para "linhas parciais" vindas do PowerBI)
    upsert_sql = """
        INSERT INTO public.vitals_events (
            event_key, cod_atendimento, id_ricadpac, event_ts,
            hora, minuto, temp, pas, pad, fc, fr, spo2,
            dor, uso_o2, nivel_consciencia, profissional, updated_at
        )
        VALUES (
            %s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (event_key) DO UPDATE
        SET
            -- mantemos o que já existe, mas completamos com o que chegar
            id_ricadpac = COALESCE(EXCLUDED.id_ricadpac, public.vitals_events.id_ricadpac),
            event_ts    = COALESCE(EXCLUDED.event_ts, public.vitals_events.event_ts),

            hora   = CASE WHEN EXCLUDED.hora   IS NOT NULL THEN EXCLUDED.hora   ELSE public.vitals_events.hora   END,
            minuto = CASE WHEN EXCLUDED.minuto IS NOT NULL THEN EXCLUDED.minuto ELSE public.vitals_events.minuto END,

            temp = CASE WHEN EXCLUDED.temp IS NOT NULL THEN EXCLUDED.temp ELSE public.vitals_events.temp END,
            pas  = CASE WHEN EXCLUDED.pas  IS NOT NULL THEN EXCLUDED.pas  ELSE public.vitals_events.pas  END,
            pad  = CASE WHEN EXCLUDED.pad  IS NOT NULL THEN EXCLUDED.pad  ELSE public.vitals_events.pad  END,
            fc   = CASE WHEN EXCLUDED.fc   IS NOT NULL THEN EXCLUDED.fc   ELSE public.vitals_events.fc   END,
            fr   = CASE WHEN EXCLUDED.fr   IS NOT NULL THEN EXCLUDED.fr   ELSE public.vitals_events.fr   END,
            spo2 = CASE WHEN EXCLUDED.spo2 IS NOT NULL THEN EXCLUDED.spo2 ELSE public.vitals_events.spo2 END,

            dor = CASE WHEN EXCLUDED.dor IS NOT NULL THEN EXCLUDED.dor ELSE public.vitals_events.dor END,
            uso_o2 = CASE WHEN EXCLUDED.uso_o2 IS NOT NULL THEN EXCLUDED.uso_o2 ELSE public.vitals_events.uso_o2 END,
            nivel_consciencia = CASE WHEN EXCLUDED.nivel_consciencia IS NOT NULL THEN EXCLUDED.nivel_consciencia ELSE public.vitals_events.nivel_consciencia END,
            profissional = CASE WHEN EXCLUDED.profissional IS NOT NULL THEN EXCLUDED.profissional ELSE public.vitals_events.profissional END,

            updated_at = CURRENT_TIMESTAMP
        RETURNING (xmax = 0) AS inserted;
    """

    inserted = 0
    updated = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in payload.rows:
                # EVENT KEY AGORA É SEGURO:
                # - event_ts já vem "real" (data + hora + minuto)
                # - inclui id_ricadpac para reduzir colisão caso o mesmo atendimento gere múltiplas séries
                rk = r.id_ricadpac or 0
                event_key = f"{r.cod_atendimento}|{rk}|{r.event_ts.isoformat()}"

                cur.execute(
                    upsert_sql,
                    (
                        event_key, r.cod_atendimento, r.id_ricadpac, r.event_ts,
                        r.hora, r.minuto, r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                        r.dor, r.uso_o2, r.nivel_consciencia, r.profissional
                    )
                )
                row = cur.fetchone()
                if row and row["inserted"]:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    # pipeline em background
    recs = compute_recommendations_for_batch(payload.rows)
    background.add_task(persist_recommendations_and_notify, recs)

    return {
        "ok": True,
        "received": len(payload.rows),
        "inserted": inserted,
        "updated": updated,
        "recs_generated": len(recs),
    }

# =========================
# NOTIFY TELEGRAM — PRODUÇÃO
# =========================
@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    minutes_back: int = 180,
    max_send: int = 10,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        id,
                        cod_atendimento,
                        snapshot_ts,
                        recommendation_level,
                        syndrome,
                        confidence,
                        actions
                    FROM public.clinical_recommendations
                    WHERE
                        recommendation_level IN ('IMEDIATO', 'PRIORIDADE')
                        AND (
                            recommendation_level = 'IMEDIATO'
                            OR notified_at IS NULL
                        )
                    ORDER BY
                        CASE recommendation_level
                            WHEN 'IMEDIATO' THEN 2
                            ELSE 1
                        END DESC,
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

                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET
                            notified_at = CASE
                                WHEN recommendation_level = 'PRIORIDADE'
                                THEN CURRENT_TIMESTAMP
                                ELSE notified_at
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                    """, (r["id"],))

                    sent += 1

            conn.commit()

        return {"ok": True, "found": len(rows), "sent": sent}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
