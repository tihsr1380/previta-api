# =========================
# PREVITA API — MAIN (v3.3.0)
# =========================
# Objetivos (nível sênior):
# - Corrigir definitivamente erro IPv6 no Render (Neon) forçando IPv4 via hostaddr
# - Não derrubar o app no startup se o DB oscilar (retry)
# - Aceitar payload do Power Automate em múltiplos formatos:
#     1) {"rows":[{...},{...}]}
#     2) [{...},{...}]
#     3) {"rows": { ...obj... }}  -> converte para lista [obj]
# - Normalizar chaves do PowerBI (VW_PREVITA_VITAIS_AGRUPADOS[...]) e chaves curtas
# - Construir event_ts robusto (DATA_HORA_LANC_MINUTO pode vir zerado)
# - Upsert MERGE (não perde campos: se antes veio NULL e depois vem valor, atualiza)
# - Responder rápido para Power Automate (processa DB em background)

import os
import time
import socket
from typing import Optional, List, Dict, Any
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field, ConfigDict

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.3.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass

# =========================
# DB CONNECT (Render + Neon) — FORÇA IPv4
# =========================
def _resolve_ipv4_or_fail(hostname: str) -> str:
    """
    Resolve apenas A record (IPv4). Render frequentemente não tem rota IPv6.
    """
    infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
    if not infos:
        raise RuntimeError(f"Não consegui resolver IPv4 para {hostname}")
    return infos[0][4][0]

def _extract_hostname_from_url(db_url: str) -> str:
    """
    Extrai hostname do DATABASE_URL no formato:
    postgresql://user:pass@HOST:PORT/db?params
    """
    if "@" not in db_url:
        raise RuntimeError("DATABASE_URL inválida: falta '@'")

    after_at = db_url.split("@", 1)[1]
    host_port = after_at.split("/", 1)[0]

    # suporte a [ipv6]:port, mas aqui queremos detectar e evitar
    if host_port.startswith("[") and "]" in host_port:
        host = host_port.split("]")[0].lstrip("[")
        return host

    # host:port
    if ":" in host_port:
        return host_port.split(":")[0]

    return host_port

def _strip_bad_params(db_url: str) -> str:
    """
    Remove parâmetros que costumam quebrar ou não serem necessários em pooler/Render.
    Principal: channel_binding=require (pode causar dor de cabeça)
    """
    # se não tem querystring, nada a fazer
    if "?" not in db_url:
        return db_url

    base, qs = db_url.split("?", 1)
    parts = [p for p in qs.split("&") if p.strip()]

    # remove channel_binding=require
    parts = [p for p in parts if not p.lower().startswith("channel_binding=")]

    return base + "?" + "&".join(parts) if parts else base

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")

    url = _strip_bad_params(DATABASE_URL)
    host = _extract_hostname_from_url(url)

    # Força IPv4
    ipv4 = _resolve_ipv4_or_fail(host)

    # hostaddr força libpq a usar IPv4, mantendo host para TLS/SNI
    if "?" in url:
        conninfo = f"{url}&hostaddr={ipv4}"
    else:
        conninfo = f"{url}?hostaddr={ipv4}"

    return psycopg.connect(
        conninfo,
        row_factory=dict_row,
        connect_timeout=10,
        options="-c statement_timeout=15000",
    )

# =========================
# INIT TABLES (com retry)
# =========================
def ensure_tables_once():
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
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_vitals_events_ts
                ON public.vitals_events (event_ts DESC);
            """)
        conn.commit()

def ensure_tables_with_retry(max_seconds: int = 45) -> bool:
    """
    Não derruba a API se o banco estiver em cold start / indisponível momentâneo.
    """
    start = time.time()
    wait = 1.0
    last_err: Optional[Exception] = None

    while time.time() - start < max_seconds:
        try:
            ensure_tables_once()
            print("[OK] Tabelas garantidas / DB OK")
            return True
        except Exception as e:
            last_err = e
            print(f"[WARN] DB ainda não pronto: {e}")
            time.sleep(wait)
            wait = min(wait * 1.7, 6.0)

    print(f"[WARN] DB não ficou pronto no startup (seguindo mesmo assim). Último erro: {last_err}")
    return False

@app.on_event("startup")
def _startup():
    ensure_tables_with_retry()

# =========================
# MODELS / NORMALIZAÇÃO
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

def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _parse_iso_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _parse_event_ts(d: Dict[str, Any]) -> datetime:
    """
    DATA_HORA_LANC_MINUTO às vezes vem zerado (00:00:00).
    Prioridade:
      1) event_ts
      2) DATA_HORA_LANC_MINUTO (se não zerado)
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
        # se vier 00:00:00 mas hora/minuto existem, reconstruímos pelo DATA_LANC
        if (dh.hour == 0 and dh.minute == 0) and dl and (h is not None or m is not None):
            return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
        return dh

    if not dl:
        raise ValueError("Sem DATA_LANC para construir event_ts")
    return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)

def normalize_powerbi_rows(raw_rows: List[Dict[str, Any]]) -> List[VitalRow]:
    """
    Aceita chaves curtas e chaves PowerBI VW_PREVITA_VITAIS_AGRUPADOS[...]
    """
    out: List[VitalRow] = []
    for r in raw_rows:
        try:
            cod = _pick(r, "COD_ATENDIMENTO", "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]")
            if cod is None:
                raise ValueError("Sem COD_ATENDIMENTO")

            payload = {
                "cod_atendimento": int(cod),
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

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# DB PING (testar rapidamente no navegador)
# =========================
@app.get("/v1/db/ping")
def db_ping(x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            r = cur.fetchone()
    return {"ok": True, "db": r["ok"]}

# =========================
# PERSISTÊNCIA (UPSERT MERGE)
# =========================
def persist_rows_merge(rows: List[VitalRow]) -> int:
    """
    Upsert com merge:
    - não perde valores já gravados
    - se chegar depois com campos antes NULL, preenche
    """
    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
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
                        updated_at        = CURRENT_TIMESTAMP;
                """, (
                    event_key, r.cod_atendimento, r.id_ricadpac, r.event_ts,
                    r.hora, r.minuto, r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                    r.dor, r.uso_o2, r.nivel_consciencia, r.profissional
                ))
                count += 1
        conn.commit()
    return count

# =========================
# INGEST — BATCH (aceita vários formatos e responde rápido)
# =========================
@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: Any,  # aceita dict/list
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    # Formatos aceitos:
    # 1) {"rows":[...]}
    # 2) [...]
    # 3) {"rows": {...obj...}} (vira lista com 1 item)
    raw_rows: List[Dict[str, Any]]

    if isinstance(payload, dict) and "rows" in payload:
        r = payload.get("rows")
        if r is None:
            raw_rows = []
        elif isinstance(r, list):
            raw_rows = r
        elif isinstance(r, dict):
            raw_rows = [r]
        else:
            raise HTTPException(status_code=422, detail="Campo 'rows' inválido. Use lista ou objeto.")
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        raise HTTPException(status_code=422, detail="Payload inválido. Envie {'rows':[...]} ou uma lista [...]")

    if not raw_rows:
        return {"ok": True, "queued": 0, "message": "Sem linhas"}

    # Normaliza
    rows = normalize_powerbi_rows(raw_rows)
    if not rows:
        return {"ok": True, "queued": 0, "message": "Linhas inválidas / sem campos mínimos"}

    # Background para não estourar timeout no Power Automate
    background.add_task(persist_rows_merge, rows)

    # Resposta imediata
    return {"ok": True, "queued": len(rows), "message": "Processando em background"}
