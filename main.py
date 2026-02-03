import os
import time
import socket
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field, ConfigDict, ValidationError

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.2.0")

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
        requests.post(url, json=payload, timeout=15)
    except Exception:
        pass

# =========================
# DB CONNECT (robusto)
# =========================
def _is_ipv6_literal(host: str) -> bool:
    # bem simples: se tem ":" e não tem letras (hostname), provavelmente é IPv6
    # exemplos: "2600:..." ou "[2600:...]"
    h = host.strip()
    if h.startswith("[") and h.endswith("]"):
        h = h[1:-1]
    return ":" in h and all(c in "0123456789abcdefABCDEF:." for c in h)

def _resolve_ipv4(hostname: str) -> Optional[str]:
    """
    Força IPv4 quando o DNS retorna AAAA primeiro e o ambiente não tem IPv6.
    Se não conseguir, retorna None.
    """
    try:
        infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except Exception:
        return None
    return None

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")

    # Se alguém colocou IPv6 literal no DATABASE_URL, isso vai quebrar no Render.
    # Nesse caso, é obrigatório trocar o DATABASE_URL para hostname do Neon.
    if "@" in DATABASE_URL:
        # tentativa leve de detectar IPv6 literal após "@"
        after_at = DATABASE_URL.split("@", 1)[1]
        host_port = after_at.split("/", 1)[0]
        host = host_port
        if ":" in host_port:
            # pode ser host:port, mas em IPv6 literal vem como 2600:...:5432 ou [2600:...]:5432
            # se for [IPv6]:port, pega dentro do []
            if host_port.startswith("["):
                host = host_port.split("]")[0].lstrip("[")
            else:
                host = host_port  # pode incluir porta; ainda detectaremos pelo literal
        if _is_ipv6_literal(host):
            raise RuntimeError(
                "Seu DATABASE_URL está usando IPv6 literal (2600:...). "
                "O Render está sem rota IPv6. Troque o DATABASE_URL para o hostname do Neon (endpoint) "
                "ou use o pooler do Neon."
            )

    # Conecta com timeouts seguros
    # statement_timeout via options pode falhar em alguns ambientes; mantemos leve.
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=10,
        options="-c statement_timeout=15000",
    )

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

def ensure_tables_with_retry(max_seconds: int = 45):
    """
    Não derruba a API se o banco demorar.
    Tenta por até max_seconds e depois segue sem matar o processo.
    """
    start = time.time()
    wait = 1.0
    last_err = None

    while time.time() - start < max_seconds:
        try:
            ensure_tables_once()
            return True
        except Exception as e:
            last_err = e
            time.sleep(wait)
            wait = min(wait * 1.7, 6.0)

    # loga no console (Render) e segue
    print(f"[WARN] DB não ficou pronto a tempo no startup: {last_err}")
    return False

@app.on_event("startup")
def _startup():
    # importante: NÃO mata o app se DB oscilar
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

class VitalBatch(BaseModel):
    rows: List[VitalRow] = Field(default_factory=list)

def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _parse_event_ts(d: Dict[str, Any]) -> datetime:
    """
    Corrige o problema do seu print:
    DATA_HORA_LANC_MINUTO às vezes vem zerado (00:00:00).
    Prioridade:
      1) event_ts já pronto
      2) DATA_HORA_LANC_MINUTO se confiável
      3) DATA_LANC + HORA_LANC + MINUTO_LANC
    """
    ev = _pick(d, "event_ts", "EVENT_TS")
    if ev:
        return datetime.fromisoformat(str(ev).replace("Z", "+00:00"))

    dh = _pick(d, "DATA_HORA_LANC_MINUTO", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_HORA_LANC_MINUTO]")
    dl = _pick(d, "DATA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]")
    h  = _pick(d, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]")
    m  = _pick(d, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]")

    # tenta DATA_HORA_LANC_MINUTO primeiro
    if dh:
        try:
            dt = datetime.fromisoformat(str(dh).replace("Z", "+00:00"))
            # se vier 00:00:00 mas hora/minuto existem, reconstruímos
            if (dt.hour == 0 and dt.minute == 0) and (h is not None or m is not None) and dl:
                base = datetime.fromisoformat(str(dl).replace("Z", "+00:00"))
                return base.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
            return dt
        except Exception:
            pass

    # fallback: DATA_LANC + HORA_LANC + MINUTO_LANC
    if not dl:
        raise ValueError("Sem DATA_LANC/DATE para construir event_ts")
    base = datetime.fromisoformat(str(dl).replace("Z", "+00:00"))
    return base.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)

def normalize_powerbi_rows(rows: List[Dict[str, Any]]) -> List[VitalRow]:
    """
    Aceita tanto:
      - chaves curtas (COD_ATENDIMENTO, TEMP...)
      - quanto as chaves do PowerBI (VW_PREVITA_VITAIS_AGRUPADOS[...])
    """
    out: List[VitalRow] = []
    for r in rows:
        try:
            payload = {
                "cod_atendimento": int(_pick(r, "COD_ATENDIMENTO", "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]")),
                "id_ricadpac": _pick(r, "ID_RICADPAC", "VW_PREVITA_VITAIS_AGRUPADOS[ID_RICADPAC]"),
                "event_ts": _parse_event_ts(r),

                "hora": _pick(r, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]"),
                "minuto": _pick(r, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]"),

                "temp": _pick(r, "TEMP", "VW_PREVITA_VITAIS_AGRUPADOS[TEMP]"),
                "dor": _pick(r, "DOR", "VW_PREVITA_VITAIS_AGRUPADOS[DOR]"),
                "fr": _pick(r, "FR", "VW_PREVITA_VITAIS_AGRUPADOS[FR]"),
                "fc": _pick(r, "FC", "VW_PREVITA_VITAIS_AGRUPADOS[FC]"),
                "pad": _pick(r, "PAD", "VW_PREVITA_VITAIS_AGRUPADOS[PAD]"),
                "pas": _pick(r, "PAS", "VW_PREVITA_VITAIS_AGRUPADOS[PAS]"),
                "spo2": _pick(r, "SPO2", "VW_PREVITA_VITAIS_AGRUPADOS[SPO2]"),
                "uso_o2": _pick(r, "USO_O2", "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]"),
                "nivel_consciencia": _pick(r, "NIVEL_CONSCIENCIA", "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]"),
                "profissional": _pick(r, "PROFISSIONAL", "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]"),
            }
            out.append(VitalRow(**payload))
        except Exception as e:
            # pula linha inválida, mas não quebra o batch todo
            print(f"[WARN] Linha inválida ignorada: {e}. Row={r}")
    return out

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# INGEST (idempotente + merge)
# =========================
def persist_rows_merge(rows: List[VitalRow]) -> int:
    inserted_or_updated = 0
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
                        hora = COALESCE(EXCLUDED.hora, vitals_events.hora),
                        minuto = COALESCE(EXCLUDED.minuto, vitals_events.minuto),
                        temp = COALESCE(EXCLUDED.temp, vitals_events.temp),
                        pas = COALESCE(EXCLUDED.pas, vitals_events.pas),
                        pad = COALESCE(EXCLUDED.pad, vitals_events.pad),
                        fc = COALESCE(EXCLUDED.fc, vitals_events.fc),
                        fr = COALESCE(EXCLUDED.fr, vitals_events.fr),
                        spo2 = COALESCE(EXCLUDED.spo2, vitals_events.spo2),
                        dor = COALESCE(EXCLUDED.dor, vitals_events.dor),
                        uso_o2 = COALESCE(EXCLUDED.uso_o2, vitals_events.uso_o2),
                        nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, vitals_events.nivel_consciencia),
                        profissional = COALESCE(EXCLUDED.profissional, vitals_events.profissional),
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    event_key, r.cod_atendimento, r.id_ricadpac, r.event_ts,
                    r.hora, r.minuto, r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                    r.dor, r.uso_o2, r.nivel_consciencia, r.profissional
                ))
                inserted_or_updated += 1
        conn.commit()
    return inserted_or_updated

@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: Dict[str, Any],  # <- aceita tanto {"rows":[...]} quanto lista direta
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    # aceita:
    # 1) {"rows":[...]}
    # 2) [...] (lista crua)
    raw_rows = None
    if isinstance(payload, dict) and "rows" in payload:
        raw_rows = payload.get("rows") or []
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        raise HTTPException(status_code=422, detail="Payload inválido. Envie {'rows':[...]} ou uma lista [...]")

    if not raw_rows:
        return {"ok": True, "queued": 0, "message": "Sem linhas"}

    # normaliza do PowerBI para modelo interno
    rows = normalize_powerbi_rows(raw_rows)
    if not rows:
        return {"ok": True, "queued": 0, "message": "Linhas inválidas / sem campos mínimos"}

    # roda em background para o Power Automate não estourar tempo
    background.add_task(persist_rows_merge, rows)

    return {"ok": True, "queued": len(rows)}

# (opcional) endpoint útil pra testar o DB sem mexer no fluxo
@app.get("/v1/db/ping")
def db_ping(x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            r = cur.fetchone()
    return {"ok": True, "db": r["ok"]}
