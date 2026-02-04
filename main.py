import os
import re
import json
import time
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, Body
from pydantic import BaseModel, ConfigDict

app = FastAPI(title="PREVITA API", version="4.0.4")

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


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


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=12)


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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_vitals_events_ts ON public.vitals_events (event_ts DESC);")

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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_clinrec_att_ts ON public.clinical_recommendations (cod_atendimento, snapshot_ts DESC);")
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


def _try_json_load(v: Any) -> Any:
    if isinstance(v, str):
        s = v.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
    return v


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


def _clean_key(k: str) -> str:
    """Remove colchetes e espaços: '[COD_ATENDIMENTO]' -> 'COD_ATENDIMENTO'."""
    s = str(k).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    return s


def _alias_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cria aliases de chaves:
    - remove colchetes
    - mantém original e versão limpa
    - adiciona versão UPPER
    """
    out = dict(row)
    for k, v in list(row.items()):
        if not isinstance(k, str):
            continue
        ck = _clean_key(k)
        if ck != k:
            out.setdefault(ck, v)
        out.setdefault(ck.upper(), v)
    return out


def _pick(d: Dict[str, Any], *keys: str):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _parse_event_ts(r: Dict[str, Any]) -> datetime:
    # tenta achar DATETIME pronto
    ev = _pick(r, "event_ts", "EVENT_TS")
    dt = _parse_dt_iso(ev)
    if dt:
        return dt.replace(second=0, microsecond=0)

    # tenta DATA_HORA_LANC_MINUTO
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
    for rr in rows:
        r = _alias_row_keys(rr)  # ✅ limpa colchetes e cria aliases
        try:
            cod = _pick(
                r,
                "COD_ATENDIMENTO",
                "cod_atendimento",
                "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]",
                "[COD_ATENDIMENTO]",
            )
            if cod is None:
                continue

            payload = {
                "cod_atendimento": int(cod),
                "id_ricadpac": _to_int(_pick(r, "ID_RICADPAC", "id_ricadpac", "VW_PREVITA_VITAIS_AGRUPADOS[ID_RICADPAC]", "[ID_RICADPAC]")),
                "event_ts": _parse_event_ts(r),

                "hora": _to_int(_pick(r, "HORA_LANC", "hora_lanc", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]", "[HORA_LANC]")),
                "minuto": _to_int(_pick(r, "MINUTO_LANC", "minuto_lanc", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]", "[MINUTO_LANC]")),

                "temp": _to_float(_pick(r, "TEMP", "temp", "VW_PREVITA_VITAIS_AGRUPADOS[TEMP]", "[TEMP]")),
                "dor": _pick(r, "DOR", "dor", "VW_PREVITA_VITAIS_AGRUPADOS[DOR]", "[DOR]"),
                "fr": _to_int(_pick(r, "FR", "fr", "VW_PREVITA_VITAIS_AGRUPADOS[FR]", "[FR]")),
                "fc": _to_int(_pick(r, "FC", "fc", "VW_PREVITA_VITAIS_AGRUPADOS[FC]", "[FC]")),
                "pad": _to_int(_pick(r, "PAD", "pad", "VW_PREVITA_VITAIS_AGRUPADOS[PAD]", "[PAD]")),
                "pas": _to_int(_pick(r, "PAS", "pas", "VW_PREVITA_VITAIS_AGRUPADOS[PAS]", "[PAS]")),
                "spo2": _to_int(_pick(r, "SPO2", "spo2", "VW_PREVITA_VITAIS_AGRUPADOS[SPO2]", "[SPO2]")),
                "uso_o2": _pick(r, "USO_O2", "uso_o2", "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]", "[USO_O2]"),
                "nivel_consciencia": _pick(r, "NIVEL_CONSCIENCIA", "nivel_consciencia", "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]", "[NIVEL_CONSCIENCIA]"),
                "profissional": _pick(r, "PROFISSIONAL", "profissional", "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]", "[PROFISSIONAL]"),
            }
            out.append(VitalRow(**payload))
        except Exception as e:
            print(f"[WARN] Linha inválida ignorada: {e}", flush=True)
    return out


def _extract_tables_struct(pb: Dict[str, Any]) -> Optional[Tuple[List[Dict[str, Any]], List[Any]]]:
    """
    Retorna (columns, rows) se encontrar em results[0].tables[0].
    columns: lista de dicts com "name"
    rows: lista (pode ser lista de dict ou lista de list)
    """
    try:
        tables = pb["results"][0]["tables"]
        if not tables:
            return None
        t0 = tables[0]
        cols = t0.get("columns") or []
        rows = t0.get("rows") or []
        if isinstance(rows, list):
            return cols, rows
    except Exception:
        return None
    return None


def _rows_list_to_dicts(columns: List[Dict[str, Any]], rows: List[Any]) -> List[Dict[str, Any]]:
    """
    Se rows vier como lista de listas, converte usando columns[].name.
    """
    if not rows:
        return []

    # se já é lista de dict, devolve direto
    if isinstance(rows[0], dict):
        return rows  # type: ignore

    # se não tem columns, não dá pra mapear com segurança
    if not columns or not isinstance(rows[0], list):
        return []

    names = []
    for c in columns:
        nm = c.get("name")
        if nm is None:
            nm = ""
        names.append(str(nm))

    out: List[Dict[str, Any]] = []
    for arr in rows:
        if not isinstance(arr, list):
            continue
        d = {}
        for i, nm in enumerate(names):
            if i < len(arr):
                d[nm] = arr[i]
        out.append(d)
    return out


def extract_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    payload = _try_json_load(payload)
    if not isinstance(payload, dict):
        return []

    pb = _try_json_load(payload.get("powerbi"))
    if isinstance(pb, dict):
        ts = _extract_tables_struct(pb)
        if ts:
            cols, rows = ts
            mapped = _rows_list_to_dicts(cols, rows)
            if mapped:
                return mapped

        # fallback clássico: rows já como dicts (sem columns)
        try:
            rows = pb["results"][0]["tables"][0]["rows"]
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                return rows
        except Exception:
            pass

    # fallback: se vier direto sem "powerbi"
    if isinstance(payload.get("rows"), list):
        rr = payload["rows"]
        if rr and isinstance(rr[0], dict):
            return rr

    return []


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


def persist_all(raw_payload: Any, rows: List[VitalRow]) -> Dict[str, Any]:
    ensure_schema_once()

    raw_inserted = 0
    events_upserted = 0
    critical_recs = 0

    raw_obj = raw_payload if isinstance(raw_payload, dict) else {"payload": raw_payload}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.vitals_raw (source, payload) VALUES (%s, %s::jsonb);",
                ("power_automate", psycopg.types.json.Json(raw_obj)),
            )
            raw_inserted += 1

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

    return {"raw_inserted": raw_inserted, "events_upserted": events_upserted, "critical_recommendations": critical_recs}


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
def vitals_batch(payload: Any = Body(...), x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)

    rows_raw = extract_rows_from_payload(payload)
    received = len(rows_raw)

    vitals = normalize_powerbi_rows(rows_raw)
    normalized = len(vitals)

    print(f"[INGEST] received_rows={received} normalized={normalized}", flush=True)

    if normalized <= 0:
        # DEBUG: tipo e chaves do 1º row
        row0 = rows_raw[0] if rows_raw else None
        row0_type = type(row0).__name__ if row0 is not None else None
        row0_keys = None
        if isinstance(row0, dict):
            row0_keys = list(row0.keys())[:80]

        pb = None
        pb_keys = None
        if isinstance(payload, dict):
            pb = _try_json_load(payload.get("powerbi"))
            if isinstance(pb, dict):
                pb_keys = list(pb.keys())[:60]

        return {
            "ok": True,
            "queued": False,
            "received": received,
            "normalized": 0,
            "message": "Nenhuma linha passou na normalização. COD_ATENDIMENTO não foi encontrado no formato retornado.",
            "debug": {
                "top_keys": list(payload.keys())[:50] if isinstance(payload, dict) else None,
                "powerbi_keys": pb_keys,
                "row0_type": row0_type,
                "row0_keys_sample": row0_keys,
            }
        }

    stats = persist_all(raw_payload=payload, rows=vitals)
    print(f"[INGEST] OK stats={stats}", flush=True)

    return {
        "ok": True,
        "queued": False,
        "received": received,
        "normalized": normalized,
        "stats": stats,
        "message": "Inserido no Neon (raw + events + recommendations). Telegram enviado quando crítico.",
    }
