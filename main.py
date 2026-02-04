import os
import time
import socket
from typing import Optional, List, Dict, Any
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, Body
from pydantic import BaseModel, ConfigDict

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="4.0.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Enviar PRIORIDADE também? (default: não)
TELEGRAM_SEND_PRIORITY = os.environ.get("TELEGRAM_SEND_PRIORITY", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# TELEGRAM (robusto)
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
        # não derruba pipeline por falha no Telegram
        pass

# =========================
# DB CONNECT (Render + Neon Pooler)
# - remove channel_binding
# - força IPv4 com hostaddr (Render às vezes sem IPv6)
# - timeout via SET statement_timeout (pooler-friendly)
# =========================
def _strip_bad_params(db_url: str) -> str:
    if "?" not in db_url:
        return db_url
    base, qs = db_url.split("?", 1)
    parts = [p for p in qs.split("&") if p.strip()]
    parts = [p for p in parts if not p.lower().startswith("channel_binding=")]
    return base + "?" + "&".join(parts) if parts else base

def _extract_hostname_from_url(db_url: str) -> str:
    if "@" not in db_url:
        raise RuntimeError("DATABASE_URL inválida: falta '@'")
    after_at = db_url.split("@", 1)[1]
    host_port = after_at.split("/", 1)[0]
    if host_port.startswith("[") and "]" in host_port:
        return host_port.split("]")[0].lstrip("[")
    if ":" in host_port:
        return host_port.split(":")[0]
    return host_port

def _resolve_ipv4(hostname: str) -> str:
    infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
    if not infos:
        raise RuntimeError(f"Não consegui resolver IPv4 para {hostname}")
    return infos[0][4][0]

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")

    url = _strip_bad_params(DATABASE_URL)
    host = _extract_hostname_from_url(url)
    ipv4 = _resolve_ipv4(host)

    conninfo = f"{url}&hostaddr={ipv4}" if "?" in url else f"{url}?hostaddr={ipv4}"

    conn = psycopg.connect(
        conninfo,
        row_factory=dict_row,
        connect_timeout=10,
    )

    # pooler-friendly: timeout por sessão
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 15000;")  # 15s/statement
    return conn

# =========================
# TABLES (RAW + EVENTS + RECOMMENDATIONS)
# =========================
def ensure_tables_once():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # RAW
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vitals_raw (
                    id BIGSERIAL PRIMARY KEY,
                    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source TEXT NULL,
                    payload JSONB NOT NULL
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_vitals_raw_received
                ON public.vitals_raw (received_at DESC);
            """)

            # EVENTS (normalizado)
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

            # RECOMMENDATIONS (críticos)
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
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_clinrec_att_ts
                ON public.clinical_recommendations (cod_atendimento, snapshot_ts DESC);
            """)

            # evita duplicar a mesma recomendação do mesmo evento
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'uq_clinrec_att_snapshot_synd'
                    ) THEN
                        ALTER TABLE public.clinical_recommendations
                        ADD CONSTRAINT uq_clinrec_att_snapshot_synd
                        UNIQUE (cod_atendimento, snapshot_ts, syndrome);
                    END IF;
                END $$;
            """)
        conn.commit()

def ensure_tables_with_retry(max_seconds: int = 45) -> bool:
    start = time.time()
    wait = 1.0
    last_err: Optional[Exception] = None
    while time.time() - start < max_seconds:
        try:
            ensure_tables_once()
            print("[OK] DB OK / tabelas garantidas")
            return True
        except Exception as e:
            last_err = e
            print(f"[WARN] DB ainda não pronto: {e}")
            time.sleep(wait)
            wait = min(wait * 1.7, 6.0)
    print(f"[WARN] DB não ficou pronto no startup. Último erro: {last_err}")
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
    # prioridade: event_ts -> DATA_HORA_LANC_MINUTO -> DATA_LANC + HORA/MINUTO
    ev = _parse_iso_dt(_pick(d, "event_ts", "EVENT_TS"))
    if ev:
        return ev

    dh = _parse_iso_dt(_pick(d, "DATA_HORA_LANC_MINUTO", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_HORA_LANC_MINUTO]"))
    dl = _parse_iso_dt(_pick(d, "DATA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]"))
    h = _to_int(_pick(d, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]"))
    m = _to_int(_pick(d, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]"))

    if dh:
        # se vier zerado (00:00) mas temos hora/minuto corretos e DATA_LANC, reconstruímos
        if (dh.hour == 0 and dh.minute == 0) and dl and (h is not None or m is not None):
            return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
        return dh

    if not dl:
        raise ValueError("Sem DATA_LANC para construir event_ts")
    return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)

def normalize_powerbi_rows(raw_rows: List[Dict[str, Any]]) -> List[VitalRow]:
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
# PIPELINE — DETERIORAÇÃO (regra sênior inicial)
# Retorna apenas críticos (PRIORIDADE/IMEDIATO)
# =========================
def compute_deterioration(row: VitalRow) -> List[Dict[str, Any]]:
    """
    Estratégia: gerar 0..N recomendações por evento.
    Se nada crítico, retorna [].
    """
    recs: List[Dict[str, Any]] = []

    def add(level: str, syndrome: str, confidence: float, actions: str):
        recs.append({
            "cod_atendimento": row.cod_atendimento,
            "snapshot_ts": row.event_ts,
            "recommendation_level": level,
            "syndrome": syndrome,
            "confidence": confidence,
            "actions": actions
        })

    # 1) Hipoxemia
    if row.spo2 is not None and row.spo2 < 90:
        add("IMEDIATO", "Hipoxemia grave", 0.85,
            "Checar oximetria, avaliar via aérea, iniciar/ajustar O2 conforme protocolo e acionar médico imediatamente.")
    elif row.spo2 is not None and row.spo2 < 92:
        add("PRIORIDADE", "Hipoxemia", 0.70,
            "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar enfermeiro/médico conforme protocolo.")

    # 2) Hipotensão
    if row.pas is not None and row.pas < 90:
        add("IMEDIATO", "Hipotensão", 0.80,
            "Repetir PA manual, avaliar perfusão, sangramento/dor, checar volume e acionar médico imediatamente conforme protocolo.")

    # 3) Taquicardia / Bradicardia
    if row.fc is not None and row.fc >= 130:
        add("PRIORIDADE", "Taquicardia importante", 0.65,
            "Confirmar FC, avaliar dor/ansiedade/hipovolemia/febre, checar PA e sinais associados. Considerar acionar médico.")
    if row.fc is not None and row.fc <= 40:
        add("IMEDIATO", "Bradicardia importante", 0.75,
            "Confirmar FC, avaliar sintomas, checar PA e nível de consciência. Acionar médico imediatamente conforme protocolo.")

    # 4) FR alta
    if row.fr is not None and row.fr >= 30:
        add("PRIORIDADE", "Taquipneia", 0.65,
            "Avaliar desconforto respiratório, checar O2, ausculta e sinais associados. Considerar acionar médico.")

    # 5) Temperatura alta (quando existe)
    if row.temp is not None and row.temp >= 39.0:
        add("PRIORIDADE", "Febre alta", 0.60,
            "Confirmar temperatura, avaliar foco infeccioso, checar sinais vitais, orientar conduta conforme protocolo.")

    # 6) Nível de consciência (ALERTA)
    if row.nivel_consciencia is not None:
        nv = str(row.nivel_consciencia).strip().upper()
        if nv in ("ALERTA", "ALTERADO", "CONFUSO"):
            # atenção: no seu dataset aparece 'ALERTA' como texto; aqui tratamos como marcador clínico
            add("PRIORIDADE", "Alteração de consciência", 0.60,
                "Reavaliar paciente, glicemia capilar se indicado, checar sinais vitais e acionar equipe conforme protocolo.")

    return recs

# =========================
# PERSIST — RAW + EVENTS + RECOMMENDATIONS + TELEGRAM
# =========================
def persist_all(source: str, raw_rows: List[Dict[str, Any]], events: List[VitalRow]) -> Dict[str, int]:
    inserted_raw = 0
    inserted_or_updated_events = 0
    inserted_recs = 0
    telegram_sent = 0

    # calcula recomendações só para críticos
    all_recs: List[Dict[str, Any]] = []
    for ev in events:
        all_recs.extend(compute_deterioration(ev))

    with get_conn() as conn:
        with conn.cursor() as cur:
            # RAW
            cur.execute(
                "INSERT INTO public.vitals_raw (source, payload) VALUES (%s, %s::jsonb);",
                (source, psycopg.types.json.Jsonb(raw_rows)),
            )
            inserted_raw = 1

            # EVENTS (merge)
            for r in events:
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
                inserted_or_updated_events += 1

            # RECOMMENDATIONS (somente críticos)
            for rec in all_recs:
                cur.execute("""
                    INSERT INTO public.clinical_recommendations
                        (cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, updated_at)
                    VALUES
                        (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                    ON CONFLICT (cod_atendimento, snapshot_ts, syndrome)
                    DO UPDATE SET
                        recommendation_level = EXCLUDED.recommendation_level,
                        confidence = COALESCE(EXCLUDED.confidence, public.clinical_recommendations.confidence),
                        actions = COALESCE(EXCLUDED.actions, public.clinical_recommendations.actions),
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    rec["cod_atendimento"],
                    rec["snapshot_ts"],
                    rec["recommendation_level"],
                    rec.get("syndrome"),
                    rec.get("confidence"),
                    rec.get("actions"),
                ))
                inserted_recs += 1

            conn.commit()

    # Telegram (fora da transação)
    for rec in all_recs:
        level = rec["recommendation_level"]
        if level == "IMEDIATO" or (TELEGRAM_SEND_PRIORITY and level == "PRIORIDADE"):
            msg = (
                f"🚨 <b>PREVITA – ALERTA {level}</b>\n\n"
                f"🧾 <b>Atendimento:</b> {rec['cod_atendimento']}\n"
                f"🕒 <b>Snapshot:</b> {rec['snapshot_ts']}\n"
                f"🧠 <b>Síndrome:</b> {rec.get('syndrome') or '-'}\n"
                f"📊 <b>Confiança:</b> {rec.get('confidence') or '-'}\n\n"
                f"✅ <b>Ações:</b>\n{(rec.get('actions') or '-').strip()[:3500]}"
            )
            send_telegram_message_sync(msg)
            telegram_sent += 1

    return {
        "inserted_raw": inserted_raw,
        "inserted_or_updated_events": inserted_or_updated_events,
        "critical_recommendations_written": inserted_recs,
        "telegram_sent": telegram_sent
    }

# =========================
# ENDPOINTS
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/v1/db/ping")
def db_ping(x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            r = cur.fetchone()
    return {"ok": True, "db": r["ok"]}

@app.post("/v1/vitals/batch")
def vitals_batch(
    payload: Any = Body(...),
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    # aceita:
    # 1) {"rows":[...]}
    # 2) [...]
    # 3) {"rows": {...}} -> vira [obj]
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
        return {"ok": True, "received": 0, "normalized": 0, "message": "Sem linhas"}

    events = normalize_powerbi_rows(raw_rows)
    if not events:
        return {
            "ok": True,
            "received": len(raw_rows),
            "normalized": 0,
            "message": "Nenhuma linha passou na normalização. Verifique as chaves do payload enviado pelo Power Automate."
        }

    # grava tudo e gera recomendações críticas + telegram
    try:
        stats = persist_all(source="power_automate", raw_rows=raw_rows, events=events)
        return {
            "ok": True,
            "received": len(raw_rows),
            "normalized": len(events),
            **stats
        }
    except Exception as e:
        # NÃO ESCONDE ERRO: Power Automate precisa ver isso
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {e}")
