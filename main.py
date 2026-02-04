import os
import time
import socket
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, Body

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="4.1.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Enviar PRIORIDADE também? (default: não)
TELEGRAM_SEND_PRIORITY = os.environ.get("TELEGRAM_SEND_PRIORITY", "0").strip().lower() in ("1", "true", "yes", "y")

# =========================
# AUTH
# =========================
def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# =========================
# TELEGRAM
# =========================
def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        r = requests.post(url, json=payload, timeout=12)
        return r.status_code == 200
    except Exception:
        return False

# =========================
# DB CONNECT (Neon Pooler + Render sem IPv6)
# - remove channel_binding
# - força IPv4 via hostaddr
# - statement_timeout via SET (pooler-friendly)
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
        raise RuntimeError("DATABASE_URL inválida (falta '@')")
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
        raise RuntimeError("DATABASE_URL não configurada")

    url = _strip_bad_params(DATABASE_URL)
    host = _extract_hostname_from_url(url)
    ipv4 = _resolve_ipv4(host)

    conninfo = f"{url}&hostaddr={ipv4}" if "?" in url else f"{url}?hostaddr={ipv4}"

    conn = psycopg.connect(
        conninfo,
        row_factory=dict_row,
        connect_timeout=10,
    )
    # pooler-friendly timeout por sessão
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 15000;")
    return conn

# =========================
# TABLES
# =========================
def ensure_tables_once():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # RAW (auditoria)
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

            # EVENTS normalizado
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

            # recommendations
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.clinical_recommendations (
                    id BIGSERIAL PRIMARY KEY,
                    cod_atendimento INT NOT NULL,
                    snapshot_ts TIMESTAMP NOT NULL,
                    recommendation_level TEXT NOT NULL,
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

            # unique anti-duplicação
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
# NORMALIZAÇÃO (SÊNIOR)
# - aceita dict rows OU array rows + columns (PowerBI padrão)
# - aceita payload inteiro do PowerBI (results/tables)
# =========================
DEFAULT_ORDER = [
    "COD_ATENDIMENTO",
    "ID_RICADPAC",
    "DATA_LANC",
    "HORA_LANC",
    "MINUTO_LANC",
    "DATA_HORA_LANC_MINUTO",
    "TEMP",
    "DOR",
    "FR",
    "FC",
    "PAD",
    "PAS",
    "SPO2",
    "USO_O2",
    "NIVEL_CONSCIENCIA",
    "PROFISSIONAL",
]

def _norm_key(k: str) -> str:
    return str(k).strip().upper()

def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None

def _to_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    s = str(v).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _build_event_ts(row: Dict[str, Any]) -> Optional[datetime]:
    # prioridade: DATA_HORA_LANC_MINUTO (se não vier zerado)
    dh = _parse_dt(row.get("DATA_HORA_LANC_MINUTO"))
    dl = _parse_dt(row.get("DATA_LANC"))
    h = _to_int(row.get("HORA_LANC"))
    m = _to_int(row.get("MINUTO_LANC"))

    if dh:
        # se vier 00:00 e tiver H/M e DATA_LANC, reconstruir
        if dh.hour == 0 and dh.minute == 0 and dl and (h is not None or m is not None):
            return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
        return dh

    if dl:
        return dl.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)

    return None

def _try_extract_powerbi(payload: Any) -> Tuple[List[str], List[Any]]:
    """
    Extrai do formato PowerBI:
    body.results[0].tables[0].columns (name) + rows (array)
    Retorna (columns, rows)
    """
    if not isinstance(payload, dict):
        return ([], [])
    # pode vir dentro de "powerbi"
    if "powerbi" in payload and isinstance(payload["powerbi"], dict):
        payload = payload["powerbi"]

    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return ([], [])
    tables = results[0].get("tables")
    if not isinstance(tables, list) or not tables:
        return ([], [])
    t0 = tables[0]
    cols = t0.get("columns")
    rows = t0.get("rows")
    col_names: List[str] = []
    if isinstance(cols, list):
        for c in cols:
            if isinstance(c, dict) and "name" in c:
                col_names.append(str(c["name"]))
            elif isinstance(c, str):
                col_names.append(c)
    if isinstance(rows, list):
        return (col_names, rows)
    return (col_names, [])

def _rows_to_dicts(columns: List[str], rows: List[Any]) -> List[Dict[str, Any]]:
    """
    Converte rows:
    - se row já é dict: normaliza chaves
    - se row é list: mapeia por columns; se columns vazio usa DEFAULT_ORDER
    """
    out: List[Dict[str, Any]] = []
    cols_norm = [ _norm_key(c) for c in (columns or DEFAULT_ORDER) ]

    for r in rows:
        if isinstance(r, dict):
            d = {}
            for k, v in r.items():
                kk = _norm_key(k)
                # remove prefixos do PowerBI tipo VW_PREVITA...[X]
                if "[" in kk and kk.endswith("]"):
                    kk = kk.split("[", 1)[1].rstrip("]")
                d[kk] = v
            out.append(d)
        elif isinstance(r, list):
            d = {}
            for i, v in enumerate(r):
                if i < len(cols_norm):
                    d[cols_norm[i]] = v
            out.append(d)
        else:
            # linha inválida
            continue
    return out

def normalize_any_payload_to_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    Aceita:
    - {"rows":[...], "columns":[...]}
    - {"rows":[[...]], "columns":[...]}  (PowerBI)
    - {"powerbi":{...}} ou body inteiro do PBI (results/tables)
    - lista crua
    """
    # 1) formato powerbi full
    cols, rows = _try_extract_powerbi(payload)
    if rows:
        return _rows_to_dicts(cols, rows)

    # 2) formato direto
    if isinstance(payload, dict) and "rows" in payload:
        rows0 = payload.get("rows")
        cols0 = payload.get("columns") or []
        if isinstance(rows0, list):
            return _rows_to_dicts(cols0 if isinstance(cols0, list) else [], rows0)
        if isinstance(rows0, dict):
            return _rows_to_dicts(cols0 if isinstance(cols0, list) else [], [rows0])
        return []

    # 3) lista crua
    if isinstance(payload, list):
        return _rows_to_dicts([], payload)

    return []

def dictrow_to_event(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # precisa ter COD_ATENDIMENTO
    cod = _to_int(d.get("COD_ATENDIMENTO"))
    if cod is None:
        return None

    event_ts = _build_event_ts(d)
    if not event_ts:
        return None

    return {
        "cod_atendimento": cod,
        "id_ricadpac": _to_int(d.get("ID_RICADPAC")),
        "event_ts": event_ts,
        "hora": _to_int(d.get("HORA_LANC")),
        "minuto": _to_int(d.get("MINUTO_LANC")),
        "temp": _to_float(d.get("TEMP")),
        "dor": _to_str(d.get("DOR")),
        "fr": _to_int(d.get("FR")),
        "fc": _to_int(d.get("FC")),
        "pad": _to_int(d.get("PAD")),
        "pas": _to_int(d.get("PAS")),
        "spo2": _to_int(d.get("SPO2")),
        "uso_o2": _to_str(d.get("USO_O2")),
        "nivel_consciencia": _to_str(d.get("NIVEL_CONSCIENCIA")),
        "profissional": _to_str(d.get("PROFISSIONAL")),
    }

# =========================
# DETERIORAÇÃO (CRÍTICOS)
# =========================
def compute_deterioration(ev: Dict[str, Any]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []

    def add(level: str, syndrome: str, confidence: float, actions: str):
        recs.append({
            "cod_atendimento": ev["cod_atendimento"],
            "snapshot_ts": ev["event_ts"],
            "recommendation_level": level,
            "syndrome": syndrome,
            "confidence": confidence,
            "actions": actions
        })

    spo2 = ev.get("spo2")
    pas = ev.get("pas")
    fc = ev.get("fc")
    fr = ev.get("fr")
    temp = ev.get("temp")

    # Hipoxemia
    if spo2 is not None and spo2 < 90:
        add("IMEDIATO", "Hipoxemia grave", 0.85,
            "Checar oximetria, avaliar via aérea, ajustar O2 conforme protocolo e acionar médico imediatamente.")
    elif spo2 is not None and spo2 < 92:
        add("PRIORIDADE", "Hipoxemia", 0.70,
            "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar protocolo.")

    # Hipotensão
    if pas is not None and pas < 90:
        add("IMEDIATO", "Hipotensão", 0.80,
            "Repetir PA manual, avaliar perfusão/sangramento e acionar médico imediatamente conforme protocolo.")

    # FC
    if fc is not None and fc >= 130:
        add("PRIORIDADE", "Taquicardia importante", 0.65,
            "Confirmar FC, avaliar dor/ansiedade/hipovolemia/febre e checar PA.")
    if fc is not None and fc <= 40:
        add("IMEDIATO", "Bradicardia importante", 0.75,
            "Confirmar FC, avaliar sintomas, checar PA e acionar médico imediatamente.")

    # FR
    if fr is not None and fr >= 30:
        add("PRIORIDADE", "Taquipneia", 0.65,
            "Avaliar desconforto respiratório, checar O2, ausculta e acionar equipe conforme protocolo.")

    # Temp
    if temp is not None and temp >= 39.0:
        add("PRIORIDADE", "Febre alta", 0.60,
            "Confirmar temperatura, avaliar foco infeccioso e seguir protocolo institucional.")

    return recs

# =========================
# PERSIST (RAW + EVENTS + RECS + TELEGRAM)
# =========================
def persist_pipeline(source: str, raw_payload: Any, rows_dict: List[Dict[str, Any]]) -> Dict[str, int]:
    inserted_raw = 0
    events_written = 0
    recs_written = 0
    telegram_sent = 0

    # normaliza para eventos
    events: List[Dict[str, Any]] = []
    for d in rows_dict:
        ev = dictrow_to_event(d)
        if ev:
            events.append(ev)

    # calcula recomendações só para críticos
    all_recs: List[Dict[str, Any]] = []
    for ev in events:
        all_recs.extend(compute_deterioration(ev))

    with get_conn() as conn:
        with conn.cursor() as cur:
            # RAW sempre
            cur.execute(
                "INSERT INTO public.vitals_raw (source, payload) VALUES (%s, %s::jsonb);",
                (source, psycopg.types.json.Jsonb(raw_payload)),
            )
            inserted_raw = 1

            # EVENTS upsert merge
            for ev in events:
                event_key = f"{ev['cod_atendimento']}|{ev['event_ts'].isoformat()}"
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
                    event_key, ev["cod_atendimento"], ev["id_ricadpac"], ev["event_ts"],
                    ev["hora"], ev["minuto"], ev["temp"], ev["pas"], ev["pad"], ev["fc"], ev["fr"], ev["spo2"],
                    ev["dor"], ev["uso_o2"], ev["nivel_consciencia"], ev["profissional"]
                ))
                events_written += 1

            # RECS (só críticos) + marca notified_at quando enviaremos
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
                recs_written += 1

        conn.commit()

    # Telegram (fora do commit)
    for rec in all_recs:
        lvl = rec["recommendation_level"]
        if lvl == "IMEDIATO" or (TELEGRAM_SEND_PRIORITY and lvl == "PRIORIDADE"):
            msg = (
                f"🚨 <b>PREVITA – ALERTA {lvl}</b>\n\n"
                f"🧾 <b>Atendimento:</b> {rec['cod_atendimento']}\n"
                f"🕒 <b>Snapshot:</b> {rec['snapshot_ts']}\n"
                f"🧠 <b>Síndrome:</b> {rec.get('syndrome') or '-'}\n"
                f"📊 <b>Confiança:</b> {rec.get('confidence') or '-'}\n\n"
                f"✅ <b>Ações:</b>\n{(rec.get('actions') or '-').strip()[:3500]}"
            )
            if send_telegram_message_sync(msg):
                telegram_sent += 1

    return {
        "inserted_raw": inserted_raw,
        "events_written": events_written,
        "critical_recs_written": recs_written,
        "telegram_sent": telegram_sent
    }

# =========================
# ENDPOINTS
# =========================
@app.get("/health")
def health():
    return {"status": "ok", "version": app.version}

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

    # converte qualquer formato para lista de dicts padronizados
    rows_dict = normalize_any_payload_to_rows(payload)

    received = 0
    # tenta estimar received de forma honesta
    if isinstance(payload, dict) and "rows" in payload and isinstance(payload.get("rows"), list):
        received = len(payload["rows"])
    else:
        # se veio full powerbi, rows_dict é o received real
        received = len(rows_dict)

    if not rows_dict:
        return {
            "ok": True,
            "received": received,
            "normalized": 0,
            "message": "Nenhuma linha passou. Seu payload está vindo como arrays sem columns OU sem COD_ATENDIMENTO/DATA_LANC/HORA/MINUTO."
        }

    # executa pipeline completo (e se falhar, retorna 500)
    try:
        stats = persist_pipeline(source="power_automate", raw_payload=payload, rows_dict=rows_dict)
        return {
            "ok": True,
            "received": received,
            "normalized": len(rows_dict),
            **stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {e}")

@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    minutes_back: int = 180,
    max_send: int = 50,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    """
    Reenvia Telegram (útil se quiser rodar por agendamento):
    - busca recs IMEDIATO sempre
    - busca recs PRIORIDADE apenas se TELEGRAM_SEND_PRIORITY=1 e notified_at is null
    """
    _check_key(x_api_key)

    cutoff = datetime.utcnow().timestamp() - (minutes_back * 60)

    with get_conn() as conn:
        with conn.cursor() as cur:
            if TELEGRAM_SEND_PRIORITY:
                cur.execute("""
                    SELECT id, cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, notified_at
                    FROM public.clinical_recommendations
                    WHERE
                        snapshot_ts >= to_timestamp(%s)
                        AND (
                            recommendation_level = 'IMEDIATO'
                            OR (recommendation_level = 'PRIORIDADE' AND notified_at IS NULL)
                        )
                    ORDER BY
                        CASE recommendation_level WHEN 'IMEDIATO' THEN 2 ELSE 1 END DESC,
                        snapshot_ts DESC
                    LIMIT %s;
                """, (cutoff, max_send))
            else:
                cur.execute("""
                    SELECT id, cod_atendimento, snapshot_ts, recommendation_level, syndrome, confidence, actions, notified_at
                    FROM public.clinical_recommendations
                    WHERE
                        snapshot_ts >= to_timestamp(%s)
                        AND recommendation_level = 'IMEDIATO'
                    ORDER BY snapshot_ts DESC
                    LIMIT %s;
                """, (cutoff, max_send))

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
                if send_telegram_message_sync(msg):
                    sent += 1
                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET notified_at = COALESCE(notified_at, CURRENT_TIMESTAMP),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                    """, (r["id"],))

        conn.commit()

    return {"ok": True, "found": len(rows), "sent": sent}
