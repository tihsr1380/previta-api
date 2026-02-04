import os
import re
import json
import time
from typing import Optional, List, Dict, Any
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, Body
from pydantic import BaseModel, ConfigDict

# ======================================================
# APP
# ======================================================
app = FastAPI(title="PREVITA API", version="4.0.2")

# ======================================================
# ENV
# ======================================================
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# ======================================================
# AUTH
# ======================================================
def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ======================================================
# TELEGRAM
# ======================================================
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
    except Exception as e:
        print(f"[TELEGRAM] WARN: {e}", flush=True)

# ======================================================
# DB
# ======================================================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=12,
    )

def ensure_schema_once():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.vitals_raw (
                    id BIGSERIAL PRIMARY KEY,
                    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source TEXT NULL,
                    payload JSONB NOT NULL
                );
            """)
            cur.execute("ALTER TABLE public.vitals_raw ADD COLUMN IF NOT EXISTS received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;")
            cur.execute("ALTER TABLE public.vitals_raw ADD COLUMN IF NOT EXISTS source TEXT NULL;")
            cur.execute("ALTER TABLE public.vitals_raw ADD COLUMN IF NOT EXISTS payload JSONB;")

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
        conn.commit()

def ensure_schema_with_retry(max_seconds: int = 45) -> bool:
    start = time.time()
    wait = 1.0
    last_err = None
    while time.time() - start < max_seconds:
        try:
            ensure_schema_once()
            return True
        except Exception as e:
            last_err = e
            print(f"[WARN] DB ainda não pronto: {e}", flush=True)
            time.sleep(wait)
            wait = min(wait * 1.7, 6.0)
    print(f"[WARN] DB não ficou pronto no startup. Último erro: {last_err}", flush=True)
    return False

@app.on_event("startup")
def _startup():
    ensure_schema_with_retry()

# ======================================================
# NORMALIZAÇÃO
# ======================================================
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

def _pick(d: Dict[str, Any], *keys: str):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None

def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None

def _parse_dt_iso(v) -> Optional[datetime]:
    if not v:
        return None
    try:
        s = str(v).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _parse_event_ts(r: Dict[str, Any]) -> datetime:
    ev = _pick(r, "event_ts", "EVENT_TS")
    dt = _parse_dt_iso(ev)
    if dt:
        return dt.replace(second=0, microsecond=0)

    dh = _pick(r, "DATA_HORA_LANC_MINUTO", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_HORA_LANC_MINUTO]")
    dl = _pick(r, "DATA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]")
    h  = _pick(r, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]")
    m  = _pick(r, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]")

    dt_dh = _parse_dt_iso(dh)
    if dt_dh:
        if dt_dh.hour == 0 and dt_dh.minute == 0 and dl is not None:
            base = _parse_dt_iso(dl)
            if base:
                return base.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
        return dt_dh.replace(second=0, microsecond=0)

    base = _parse_dt_iso(dl)
    if not base:
        raise ValueError("Sem DATA_LANC/DATE para construir event_ts")
    return base.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)

def normalize_powerbi_rows(rows: List[Dict[str, Any]]) -> List[VitalRow]:
    out: List[VitalRow] = []
    for r in rows:
        try:
            cod = _pick(r, "COD_ATENDIMENTO", "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]")
            if cod is None:
                continue

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
            print(f"[WARN] Linha inválida ignorada: {e}", flush=True)
    return out

# ======================================================
# EXTRATOR (inclui payload achatado do Power Automate)
# ======================================================
_FLAT_RE = re.compile(
    r"^(?:powerbi\.)?results\.(\d+)\.tables\.(\d+)\.rows\.(\d+)\.\[(.+?)\]$"
)
_FLAT_RE2 = re.compile(
    r"^(?:powerbi\.)?results\.(\d+)\.tables\.(\d+)\.rows\.(\d+)\.(.+?)$"
)

def _try_json_load(v: Any) -> Any:
    if isinstance(v, str):
        s = v.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
    return v

def rebuild_rows_from_flat(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Constrói rows[] quando o Power Automate manda chaves achatadas:
      powerbi.results.0.tables.0.rows.0.[COD_ATENDIMENTO]
    """
    rows_map: Dict[int, Dict[str, Any]] = {}

    for k, v in payload.items():
        if not isinstance(k, str):
            continue

        m = _FLAT_RE.match(k)
        if m:
            row_i = int(m.group(3))
            field = m.group(4)
            rows_map.setdefault(row_i, {})[field] = _try_json_load(v)
            continue

        m2 = _FLAT_RE2.match(k)
        if m2:
            row_i = int(m2.group(3))
            field = m2.group(4)
            rows_map.setdefault(row_i, {})[field] = _try_json_load(v)
            continue

    if not rows_map:
        return []

    return [rows_map[i] for i in sorted(rows_map.keys())]

def extract_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    payload = _try_json_load(payload)

    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        return []

    # 1) formato direto {"rows":[...]}
    if isinstance(payload.get("rows"), list):
        return payload["rows"]

    # 2) formato { "powerbi": {body do PBI} }
    pb = payload.get("powerbi")
    pb = _try_json_load(pb)
    if isinstance(pb, dict):
        try:
            rows = pb["results"][0]["tables"][0]["rows"]
            if isinstance(rows, list):
                return rows
        except Exception:
            pass

    # 3) formato body do PBI direto {results:[{tables:[{rows:[]}]}]}
    try:
        rows = payload["results"][0]["tables"][0]["rows"]
        if isinstance(rows, list):
            return rows
    except Exception:
        pass

    # 4) formato ACHATADO do Power Automate (principal causa do seu problema)
    flat_rows = rebuild_rows_from_flat(payload)
    if flat_rows:
        return flat_rows

    # 5) às vezes vem dentro de um campo único que é string JSON
    for v in payload.values():
        vv = _try_json_load(v)
        if isinstance(vv, dict) or isinstance(vv, list):
            rr = extract_rows_from_payload(vv)
            if rr:
                return rr

    return []

# ======================================================
# CRITICIDADE
# ======================================================
def compute_recommendations(rows: List[VitalRow]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = None
        syndrome = None
        confidence = None
        actions = None

        if r.pas is not None and r.pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.85
            actions = "Checar PA manual, perfusão, sangramento/dor e acionar médico conforme protocolo."
        elif r.spo2 is not None and r.spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.70
            actions = "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar protocolo."
        elif r.fc is not None and (r.fc >= 130 or r.fc <= 40):
            level = "PRIORIDADE"
            syndrome = "FC crítica"
            confidence = 0.65
            actions = "Repetir sinais vitais, checar sintomas, ECG se disponível e acionar médico."

        if level:
            recs.append({
                "cod_atendimento": r.cod_atendimento,
                "snapshot_ts": r.event_ts,
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": confidence,
                "actions": actions
            })
    return recs

# ======================================================
# PERSIST
# ======================================================
def persist_all(raw_payload: Any, rows: List[VitalRow]) -> Dict[str, Any]:
    ensure_schema_once()

    raw_inserted = 0
    events_upserted = 0
    critical_recs = 0

    raw_obj = raw_payload if isinstance(raw_payload, dict) else {"payload": raw_payload}

    with get_conn() as conn:
        with conn.cursor() as cur:
            # RAW
            cur.execute(
                "INSERT INTO public.vitals_raw (source, payload) VALUES (%s, %s::jsonb);",
                ("power_automate", psycopg.types.json.Json(raw_obj)),
            )
            raw_inserted += 1

            # EVENTS
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
                events_upserted += 1

            # RECS + TELEGRAM
            recs = compute_recommendations(rows)
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
                critical_recs += 1

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

    return {
        "raw_inserted": raw_inserted,
        "events_upserted": events_upserted,
        "critical_recommendations": critical_recs,
    }

# ======================================================
# ENDPOINTS
# ======================================================
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

    # extrai rows mesmo se vier achatado
    rows_raw = extract_rows_from_payload(payload)
    received = len(rows_raw)

    vitals = normalize_powerbi_rows(rows_raw)
    normalized = len(vitals)

    print(f"[INGEST] received_rows={received} normalized={normalized}", flush=True)

    if normalized <= 0:
        # devolve diagnóstico para o Power Automate
        return {
            "ok": True,
            "queued": False,
            "received": received,
            "normalized": 0,
            "message": "Nenhuma linha passou na normalização. Provável payload achatado/sem COD_ATENDIMENTO/DATA_LANC. (Agora a API tenta reconstruir rows, mas verifique campos.)",
        }

    try:
        stats = persist_all(raw_payload=payload, rows=vitals)
        print(f"[INGEST] OK stats={stats}", flush=True)
        return {
            "ok": True,
            "queued": False,
            "received": received,
            "normalized": normalized,
            "stats": stats,
            "message": "Inserido no Neon (raw + events + recommendations) e telegram enviado quando crítico.",
        }
    except Exception as e:
        print(f"[INGEST] ERROR: {e}", flush=True)
        raise HTTPException(status_code=500, detail=str(e))
