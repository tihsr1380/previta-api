# =========================
# PREVITA API — MAIN (v3.9)
# =========================
# Objetivo (robusto para Render + Neon pooler + Power Automate):
# 1) Receber payload do Power Automate (várias formas: powerbi/body/results/tables/rows, rows, lista direta)
# 2) Gravar SEMPRE bruto em public.vitals_raw (para auditoria / replay)
# 3) Normalizar e gravar em public.vitals_events (upsert com merge – não perde campos)
# 4) Gerar recomendações (críticos) e gravar em public.clinical_recommendations (anti-duplicação)
# 5) Notificar Telegram para IMEDIATO (e opcional PRIORIDADE)
#
# Importante:
# - NÃO usa psycopg_pool (te dava ModuleNotFoundError)
# - NÃO usa "options=-c statement_timeout=..." (Neon pooler pode rejeitar). Em vez disso usa SET LOCAL statement_timeout.
# - Faz "migração" no startup: se a tabela existir sem colunas novas (ex: received_at), adiciona.
# - Responde rápido (202-like) e processa em background.

import os
import json
import time
from typing import Optional, List, Dict, Any, Iterable
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Request
from pydantic import BaseModel, Field, ConfigDict

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.9.0")

# =========================
# ENV
# =========================
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Timeouts (ajustáveis)
DB_CONNECT_TIMEOUT = int(os.environ.get("DB_CONNECT_TIMEOUT", "10"))
DB_STATEMENT_TIMEOUT_MS = int(os.environ.get("DB_STATEMENT_TIMEOUT_MS", "15000"))  # 15s

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# TELEGRAM (opcional)
# =========================
def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        requests.post(url, json=payload, timeout=12)
    except Exception:
        # não derruba pipeline por falha de notificação
        pass

# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")
    # NÃO passar statement_timeout via options (Neon pooler pode rejeitar).
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=DB_CONNECT_TIMEOUT,
    )

def _set_local_timeouts(cur):
    # aplica timeout por transação, compatível com pooler
    try:
        cur.execute("SET LOCAL statement_timeout = %s;", (DB_STATEMENT_TIMEOUT_MS,))
    except Exception:
        # se o pooler bloquear SET LOCAL por algum motivo, não mata o fluxo
        pass

def ensure_tables_once():
    """
    Cria tabelas se não existirem e faz MIGRAÇÃO segura (ADD COLUMN IF NOT EXISTS)
    para não quebrar quando a tabela já existe com estrutura antiga.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            _set_local_timeouts(cur)

            # -------------------------
            # VITALS_RAW (audit trail)
            # -------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vitals_raw (
                    id BIGSERIAL PRIMARY KEY
                );
            """)
            cur.execute("ALTER TABLE public.vitals_raw ADD COLUMN IF NOT EXISTS received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;")
            cur.execute("ALTER TABLE public.vitals_raw ADD COLUMN IF NOT EXISTS source TEXT NULL;")
            cur.execute("ALTER TABLE public.vitals_raw ADD COLUMN IF NOT EXISTS payload JSONB NULL;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_vitals_raw_received ON public.vitals_raw (received_at DESC);")

            # -------------------------
            # VITALS_EVENTS (normalizado)
            # -------------------------
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
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_vitals_events_ts ON public.vitals_events (event_ts DESC);")

            # -------------------------
            # CLINICAL_RECOMMENDATIONS
            # -------------------------
            cur.execute("""
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
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_clinrec_att_ts ON public.clinical_recommendations (cod_atendimento, snapshot_ts DESC);")

            # anti-duplicação (não cria duplicado no mesmo snapshot/síndrome)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'uq_clinrec_att_snapshot_synd'
                    ) THEN
                        ALTER TABLE public.clinical_recommendations
                        ADD CONSTRAINT uq_clinrec_att_snapshot_synd
                        UNIQUE (cod_atendimento, snapshot_ts, syndrome);
                    END IF;
                END $$;
            """)

        conn.commit()

def ensure_tables_with_retry(max_seconds: int = 60):
    start = time.time()
    wait = 1.0
    last_err = None
    while time.time() - start < max_seconds:
        try:
            ensure_tables_once()
            print("[OK] DB OK / tabelas garantidas")
            return True
        except Exception as e:
            last_err = e
            print(f"[WARN] DB ainda não pronto: {e}")
            time.sleep(wait)
            wait = min(wait * 1.6, 6.0)
    print(f"[WARN] DB não ficou pronto no startup. Último erro: {last_err}")
    return False

@app.on_event("startup")
def _startup():
    # Não derruba o app se DB oscilar.
    ensure_tables_with_retry()

# =========================
# MODELS
# =========================
class VitalRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cod_atendimento: int
    id_ricadpac: Optional[int] = None
    event_ts: datetime

    hora: Optional[int] = None
    minuto: Optional[int] = None
    temp: Optional[float] = None
    pas: Optional[int] = None
    pad: Optional[int] = None
    fc: Optional[int] = None
    fr: Optional[int] = None
    spo2: Optional[int] = None

    dor: Optional[str] = None
    uso_o2: Optional[str] = None
    nivel_consciencia: Optional[str] = None
    profissional: Optional[str] = None

class VitalBatch(BaseModel):
    rows: List[VitalRow] = Field(default_factory=list)

# =========================
# HELPERS — parsing PowerBI
# =========================
def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        return int(float(str(x).replace(",", ".")))
    except Exception:
        return None

def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def _parse_iso_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # PowerBI às vezes manda Z
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # fallback simples
        try:
            # "2026-02-02T00:00:00"
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None

def _parse_event_ts(d: Dict[str, Any]) -> datetime:
    """
    Prioridade:
    1) event_ts
    2) DATA_HORA_LANC_MINUTO (se confiável)
    3) DATA_LANC + HORA_LANC + MINUTO_LANC
    """
    ev = _parse_iso_dt(_pick(d, "event_ts", "EVENT_TS"))
    if ev:
        return ev

    dh = _parse_iso_dt(_pick(d, "DATA_HORA_LANC_MINUTO", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_HORA_LANC_MINUTO]"))
    dl = _parse_iso_dt(_pick(d, "DATA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]"))
    h = _to_int(_pick(d, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]"))
    m = _to_int(_pick(d, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]"))

    if dh:
        # se vier zerado (00:00:00) mas tem hora/minuto, reconstrói em cima do DATA_LANC
        if (dh.hour == 0 and dh.minute == 0) and dl and (h is not None or m is not None):
            return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
        return dh

    if not dl:
        raise ValueError("Sem DATA_LANC para construir event_ts")
    return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)

def normalize_powerbi_rows(rows: List[Dict[str, Any]]) -> List[VitalRow]:
    """
    Aceita:
    - chaves curtas: COD_ATENDIMENTO, TEMP...
    - chaves PowerBI: VW_PREVITA_VITAIS_AGRUPADOS[...]
    - ou body já “flat”
    """
    out: List[VitalRow] = []
    for r in rows:
        try:
            cod = _to_int(_pick(r, "COD_ATENDIMENTO", "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]"))
            if cod is None:
                raise ValueError("cod_atendimento ausente")

            payload = {
                "cod_atendimento": cod,
                "id_ricadpac": _to_int(_pick(r, "ID_RICADPAC", "VW_PREVITA_VITAIS_AGRUPADOS[ID_RICADPAC]")),
                "event_ts": _parse_event_ts(r),

                "hora": _to_int(_pick(r, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]")),
                "minuto": _to_int(_pick(r, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]")),

                "temp": _to_float(_pick(r, "TEMP", "VW_PREVITA_VITAIS_AGRUPADOS[TEMP]")),
                "dor": _pick(r, "DOR", "VW_PREVITA_VITAIS_AGRUPADOS[DOR]"),
                "fr": _to_int(_pick(r, "FR", "VW_PREVITA_VITAIS_AGRUPADOS[FR]")),
                "fc": _to_int(_pick(r, "FC", "VW_PREVITA_VITAIS_AGRUPADOS[FC]")),
                "pad": _to_int(_pick(r, "PAD", "VW_PREVITA_VITAIS_AGRUPADOS[PAD]")),
                "pas": _to_int(_pick(r, "PAS", "VW_PREVITA_VITAIS_AGRUPADOS[PAS]")),
                "spo2": _to_int(_pick(r, "SPO2", "VW_PREVITA_VITAIS_AGRUPADOS[SPO2]")),
                "uso_o2": _pick(r, "USO_O2", "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]"),
                "nivel_consciencia": _pick(r, "NIVEL_CONSCIENCIA", "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]"),
                "profissional": _pick(r, "PROFISSIONAL", "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]"),
            }
            out.append(VitalRow(**payload))
        except Exception as e:
            print(f"[WARN] Linha inválida ignorada: {e}. Row={r}")
    return out

def extract_rows_any_shape(payload: Any) -> List[Dict[str, Any]]:
    """
    O Power Automate / PowerBI pode mandar:
    1) {"rows":[...]}
    2) [...] (lista direta)
    3) {"powerbi": <body do action>}
       onde body.results[0].tables[0].rows é a lista
    4) {"body": {"results":...}} etc.
    """
    if payload is None:
        return []

    # lista crua
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        return []

    # caso 1
    if isinstance(payload.get("rows"), list):
        return [x for x in payload["rows"] if isinstance(x, dict)]

    # caso 3/4
    root = payload.get("powerbi") or payload.get("body") or payload
    if not isinstance(root, dict):
        return []

    results = root.get("results")
    if isinstance(results, list) and results:
        tables = results[0].get("tables") if isinstance(results[0], dict) else None
        if isinstance(tables, list) and tables:
            rows = tables[0].get("rows") if isinstance(tables[0], dict) else None
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]

    # fallback: às vezes vem direto com tables/rows
    tables = root.get("tables")
    if isinstance(tables, list) and tables:
        rows = tables[0].get("rows") if isinstance(tables[0], dict) else None
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]

    return []

# =========================
# RECOMMENDATIONS (simples, evolutiva)
# =========================
def compute_recommendations(rows: List[VitalRow]) -> List[Dict[str, Any]]:
    """
    Gera SOMENTE críticos (PRIORIDADE / IMEDIATO).
    Você pode evoluir depois com tendência, score, etc.
    """
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = "OK"
        syndrome = None
        confidence = None
        actions = None

        # gatilhos base
        if r.spo2 is not None and r.spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.70
            actions = "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar protocolo."

        if r.pas is not None and r.pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.80
            actions = "Checar PA manual, perfusão, sangramento/dor e acionar médico conforme protocolo."

        if level != "OK":
            recs.append({
                "cod_atendimento": r.cod_atendimento,
                "snapshot_ts": r.event_ts,
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": confidence,
                "actions": actions,
            })
    return recs

# =========================
# PERSIST (RAW + EVENTS + RECS + TELEGRAM)
# =========================
def persist_raw(payload_obj: Any, source: str = "power_automate") -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _set_local_timeouts(cur)
            cur.execute(
                "INSERT INTO public.vitals_raw (source, payload) VALUES (%s, %s::jsonb);",
                (source, json.dumps(payload_obj, ensure_ascii=False)),
            )
        conn.commit()

def upsert_events_merge(rows: List[VitalRow]) -> Dict[str, int]:
    inserted = 0
    updated = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            _set_local_timeouts(cur)

            for r in rows:
                event_key = f"{r.cod_atendimento}|{r.event_ts.isoformat()}"

                cur.execute("""
                    INSERT INTO public.vitals_events (
                        event_key, cod_atendimento, id_ricadpac, event_ts,
                        hora, minuto, temp, pas, pad, fc, fr, spo2,
                        dor, uso_o2, nivel_consciencia, profissional,
                        updated_at
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                    ON CONFLICT (event_key) DO UPDATE SET
                        id_ricadpac        = COALESCE(EXCLUDED.id_ricadpac, vitals_events.id_ricadpac),
                        hora              = COALESCE(EXCLUDED.hora, vitals_events.hora),
                        minuto            = COALESCE(EXCLUDED.minuto, vitals_events.minuto),
                        temp              = COALESCE(EXCLUDED.temp, vitals_events.temp),
                        pas               = COALESCE(EXCLUDED.pas, vitals_events.pas),
                        pad               = COALESCE(EXCLUDED.pad, vitals_events.pad),
                        fc                = COALESCE(EXCLUDED.fc, vitals_events.fc),
                        fr                = COALESCE(EXCLUDED.fr, vitals_events.fr),
                        spo2              = COALESCE(EXCLUDED.spo2, vitals_events.spo2),
                        dor               = COALESCE(EXCLUDED.dor, vitals_events.dor),
                        uso_o2            = COALESCE(EXCLUDED.uso_o2, vitals_events.uso_o2),
                        nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, vitals_events.nivel_consciencia),
                        profissional      = COALESCE(EXCLUDED.profissional, vitals_events.profissional),
                        updated_at        = CURRENT_TIMESTAMP
                    WHERE
                        EXCLUDED.id_ricadpac IS NOT NULL OR
                        EXCLUDED.hora IS NOT NULL OR
                        EXCLUDED.minuto IS NOT NULL OR
                        EXCLUDED.temp IS NOT NULL OR
                        EXCLUDED.pas IS NOT NULL OR
                        EXCLUDED.pad IS NOT NULL OR
                        EXCLUDED.fc IS NOT NULL OR
                        EXCLUDED.fr IS NOT NULL OR
                        EXCLUDED.spo2 IS NOT NULL OR
                        EXCLUDED.dor IS NOT NULL OR
                        EXCLUDED.uso_o2 IS NOT NULL OR
                        EXCLUDED.nivel_consciencia IS NOT NULL OR
                        EXCLUDED.profissional IS NOT NULL;
                """, (
                    event_key, r.cod_atendimento, r.id_ricadpac, r.event_ts,
                    r.hora, r.minuto, r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                    r.dor, r.uso_o2, r.nivel_consciencia, r.profissional
                ))

                status = (cur.statusmessage or "").upper()
                if status.startswith("INSERT"):
                    inserted += 1
                elif status.startswith("UPDATE"):
                    updated += 1

        conn.commit()

    return {"inserted": inserted, "updated": updated}

def persist_recommendations_and_notify(recs: List[Dict[str, Any]], notify_priority: bool = False) -> int:
    if not recs:
        return 0

    inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            _set_local_timeouts(cur)

            for rec in recs:
                # anti-duplicação via unique constraint (cod_atendimento, snapshot_ts, syndrome)
                try:
                    cur.execute("""
                        INSERT INTO public.clinical_recommendations
                            (cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                        ON CONFLICT ON CONSTRAINT uq_clinrec_att_snapshot_synd DO NOTHING;
                    """, (
                        rec["cod_atendimento"],
                        rec["snapshot_ts"],
                        rec["recommendation_level"],
                        rec.get("syndrome"),
                        rec.get("confidence"),
                        rec.get("actions"),
                    ))
                    if cur.rowcount == 1:
                        inserted += 1
                except Exception as e:
                    print(f"[WARN] Falha inserindo recomendação: {e}")

                # Telegram: IMEDIATO sempre. PRIORIDADE se notify_priority=True
                if rec["recommendation_level"] == "IMEDIATO" or (notify_priority and rec["recommendation_level"] == "PRIORIDADE"):
                    msg = (
                        f"🚨 <b>PREVITA – ALERTA {rec['recommendation_level']}</b>\n\n"
                        f"🧾 <b>Atendimento:</b> {rec['cod_atendimento']}\n"
                        f"🕒 <b>Snapshot:</b> {rec['snapshot_ts']}\n"
                        f"🧠 <b>Síndrome:</b> {rec.get('syndrome') or '-'}\n"
                        f"📊 <b>Confiança:</b> {rec.get('confidence') or '-'}\n\n"
                        f"✅ <b>Ações:</b>\n{(rec.get('actions') or '').strip()[:3500]}"
                    )
                    send_telegram_message_sync(msg)

        conn.commit()

    return inserted

def process_ingest_job(payload_obj: Any, source: str = "power_automate") -> Dict[str, Any]:
    """
    Pipeline completo:
    - grava raw
    - extrai rows
    - normaliza
    - upsert merge events
    - recomendações (críticos) + telegram
    """
    persist_raw(payload_obj, source=source)

    raw_rows = extract_rows_any_shape(payload_obj)
    normalized = normalize_powerbi_rows(raw_rows)

    if not normalized:
        return {"ok": True, "received": len(raw_rows), "normalized": 0, "message": "Nenhuma linha passou na normalização (chaves/valores inesperados)"}

    stats = upsert_events_merge(normalized)

    recs = compute_recommendations(normalized)
    recs_inserted = persist_recommendations_and_notify(recs, notify_priority=False)

    return {
        "ok": True,
        "received": len(raw_rows),
        "normalized": len(normalized),
        "events": stats,
        "recs_generated": len(recs),
        "recs_inserted": recs_inserted,
    }

# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    return {"status": "ok", "version": app.version}

@app.get("/v1/db/ping")
def db_ping(x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _set_local_timeouts(cur)
            cur.execute("SELECT 1 AS ok;")
            r = cur.fetchone()
    return {"ok": True, "db": r["ok"]}

@app.post("/v1/vitals/batch")
async def vitals_batch(
    request: Request,
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    """
    Aceita QUALQUER um destes bodies:
    - {"rows":[...]}
    - [...] (lista)
    - {"powerbi": @{outputs('PBI_Query_Vitais')?['body']}}
    - {"body": ...} (qualquer variação)
    """
    _check_key(x_api_key)

    try:
        payload_obj = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body inválido (JSON esperado)")

    # Responde rápido para não estourar timeout do Power Automate
    def _job():
        try:
            result = process_ingest_job(payload_obj, source="power_automate")
            print(f"[INGEST] {result}")
        except Exception as e:
            print(f"[INGEST][ERROR] {e}")

    background.add_task(_job)

    # devolve um resumo rápido (sem travar)
    raw_rows = extract_rows_any_shape(payload_obj)
    return {
        "ok": True,
        "queued": True,
        "received_preview": len(raw_rows),
        "message": "Processando em background (raw + events + recomendações + telegram).",
    }

# Endpoint opcional para disparar manualmente alertas não notificados (se quiser usar)
@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    max_send: int = 10,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    with get_conn() as conn:
        with conn.cursor() as cur:
            _set_local_timeouts(cur)

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

                # marca PRIORIDADE como notificado; IMEDIATO mantém histórico (pode ajustar)
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
