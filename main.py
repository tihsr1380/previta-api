import os
import json
import time
import traceback
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, Body
from pydantic import BaseModel, ConfigDict

app = FastAPI(title="PREVITA API", version="4.0.8")

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


def _sanitize_db_url(url: str) -> str:
    if not url:
        return url
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    # remove parâmetros problemáticos para pooler/Neon
    for bad in ["options", "statement_timeout", "idle_in_transaction_session_timeout"]:
        q.pop(bad, None)
    new_query = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")
    url = _sanitize_db_url(DATABASE_URL)
    return psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=12,
        options="",  # anula PGOPTIONS do ambiente
    )


def ensure_schema_with_retry(max_seconds: int = 30) -> bool:
    start = time.time()
    wait = 1.0
    last_err = None
    while time.time() - start < max_seconds:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
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


def get_table_columns(table: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, is_nullable, data_type, column_default
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position;
            """, (table,))
            return cur.fetchall() or []


def build_insert(table: str, row_values: Dict[str, Any]) -> Tuple[str, List[Any]]:
    cols_meta = get_table_columns(table)
    existing = {c["column_name"] for c in cols_meta}
    cols = []
    vals = []
    for k, v in row_values.items():
        if k in existing:
            cols.append(k)
            vals.append(v)
    if not cols:
        raise RuntimeError(f"Nenhuma coluna compatível encontrada para inserir em {table}")
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT INTO public.{table} ({col_list}) VALUES ({placeholders});"
    return sql, vals


class VitalRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    cod_atendimento: int
    id_ricadpac: Optional[int] = None
    event_ts: datetime
    data_lanc: date
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
        return int(float(v))
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
    s = str(k).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    return s


def _alias_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
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


def _parse_data_lanc(r: Dict[str, Any], event_ts: datetime) -> date:
    dl = _pick(r, "DATA_LANC", "data_lanc", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]", "[DATA_LANC]")
    dt = _parse_dt_iso(dl)
    if dt:
        return dt.date()
    return event_ts.date()


def normalize_powerbi_rows(rows: List[Dict[str, Any]]) -> List[VitalRow]:
    out: List[VitalRow] = []
    for rr in rows:
        r = _alias_row_keys(rr)
        try:
            cod = _pick(r, "COD_ATENDIMENTO", "cod_atendimento", "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]", "[COD_ATENDIMENTO]")
            if cod is None:
                continue

            event_ts = _parse_event_ts(r)
            data_lanc = _parse_data_lanc(r, event_ts)

            payload = {
                "cod_atendimento": int(float(cod)),
                "id_ricadpac": _to_int(_pick(r, "ID_RICADPAC", "id_ricadpac", "VW_PREVITA_VITAIS_AGRUPADOS[ID_RICADPAC]", "[ID_RICADPAC]")),
                "event_ts": event_ts,
                "data_lanc": data_lanc,

                "hora": _to_int(_pick(r, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]", "[HORA_LANC]")),
                "minuto": _to_int(_pick(r, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]", "[MINUTO_LANC]")),

                "temp": _to_float(_pick(r, "TEMP", "VW_PREVITA_VITAIS_AGRUPADOS[TEMP]", "[TEMP]")),
                "dor": _pick(r, "DOR", "VW_PREVITA_VITAIS_AGRUPADOS[DOR]", "[DOR]"),
                "fr": _to_int(_pick(r, "FR", "VW_PREVITA_VITAIS_AGRUPADOS[FR]", "[FR]")),
                "fc": _to_int(_pick(r, "FC", "VW_PREVITA_VITAIS_AGRUPADOS[FC]", "[FC]")),
                "pad": _to_int(_pick(r, "PAD", "VW_PREVITA_VITAIS_AGRUPADOS[PAD]", "[PAD]")),
                "pas": _to_int(_pick(r, "PAS", "VW_PREVITA_VITAIS_AGRUPADOS[PAS]", "[PAS]")),
                "spo2": _to_int(_pick(r, "SPO2", "VW_PREVITA_VITAIS_AGRUPADOS[SPO2]", "[SPO2]")),
                "uso_o2": _pick(r, "USO_O2", "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]", "[USO_O2]"),
                "nivel_consciencia": _pick(r, "NIVEL_CONSCIENCIA", "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]", "[NIVEL_CONSCIENCIA]"),
                "profissional": _pick(r, "PROFISSIONAL", "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]", "[PROFISSIONAL]"),
            }
            out.append(VitalRow(**payload))
        except Exception as e:
            print(f"[WARN] Linha inválida ignorada: {e}", flush=True)
    return out


def _extract_tables_struct(pb: Dict[str, Any]) -> Optional[Tuple[List[Dict[str, Any]], List[Any]]]:
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
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return rows  # type: ignore
    if not columns or not isinstance(rows[0], list):
        return []
    names = [str(c.get("name") or "") for c in columns]
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
        try:
            rows = pb["results"][0]["tables"][0]["rows"]
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                return rows
        except Exception:
            pass
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


def upsert_vitals_events(cur, cols_events: set, r: VitalRow):
    # obrigatórios para o nosso event_key
    if "event_key" not in cols_events:
        print("[WARN] vitals_events não tem event_key; pulando UPSERT.", flush=True)
        return False

    event_key = f"{r.cod_atendimento}|{r.event_ts.isoformat()}"

    # valores possíveis
    candidate = {
        "event_key": event_key,
        "cod_atendimento": r.cod_atendimento,
        "id_ricadpac": r.id_ricadpac,
        "event_ts": r.event_ts,

        # só entra se existir na tabela:
        "data_lanc": r.data_lanc,
        "temp": r.temp,
        "pas": r.pas,
        "pad": r.pad,
        "fc": r.fc,
        "fr": r.fr,
        "spo2": r.spo2,
        "dor": r.dor,
        "uso_o2": r.uso_o2,
        "nivel_consciencia": r.nivel_consciencia,
        "profissional": r.profissional,
    }

    # filtra apenas colunas existentes
    insert_cols = [k for k in candidate.keys() if k in cols_events]
    insert_vals = [candidate[k] for k in insert_cols]

    # updated_at (se existir) sempre atualiza
    if "updated_at" in cols_events:
        insert_cols.append("updated_at")
        insert_vals.append(datetime.utcnow())

    # monta INSERT
    cols_sql = ", ".join(insert_cols)
    ph_sql = ", ".join(["%s"] * len(insert_cols))

    # monta UPDATE SET dinamicamente (não atualiza chaves)
    updatable = [c for c in insert_cols if c not in ("event_key", "cod_atendimento")]
    set_parts = []
    for c in updatable:
        if c == "updated_at":
            set_parts.append("updated_at = CURRENT_TIMESTAMP")
        else:
            set_parts.append(f"{c} = COALESCE(EXCLUDED.{c}, vitals_events.{c})")

    set_sql = ", ".join(set_parts) if set_parts else "updated_at = CURRENT_TIMESTAMP"

    sql = f"""
        INSERT INTO public.vitals_events ({cols_sql})
        VALUES ({ph_sql})
        ON CONFLICT (event_key) DO UPDATE SET
            {set_sql};
    """

    cur.execute(sql, insert_vals)
    return True


def persist_all(raw_payload: Any, rows: List[VitalRow]) -> Dict[str, Any]:
    raw_inserted = 0
    events_upserted = 0
    critical_recs = 0

    now = datetime.utcnow()
    raw_obj = raw_payload if isinstance(raw_payload, dict) else {"payload": raw_payload}

    cols_events = {c["column_name"] for c in get_table_columns("vitals_events")}

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) vitals_raw (schema-driven + data_lanc obrigatório)
            for r in rows:
                raw_row_values = {
                    "event_ts": r.event_ts,
                    "data_lanc": r.data_lanc,

                    "cod_atendimento": r.cod_atendimento,
                    "id_ricadpac": r.id_ricadpac,

                    "hora_lanc": r.hora,
                    "minuto_lanc": r.minuto,

                    "hora": r.hora,
                    "minuto": r.minuto,

                    "temp": r.temp,
                    "pas": r.pas,
                    "pad": r.pad,
                    "fc": r.fc,
                    "fr": r.fr,
                    "spo2": r.spo2,
                    "dor": r.dor,
                    "uso_o2": r.uso_o2,
                    "nivel_consciencia": r.nivel_consciencia,
                    "profissional": r.profissional,

                    "source": "power_automate",
                    "received_at": now,
                    "created_at": now,
                    "updated_at": now,
                    "payload": json.dumps(raw_obj),
                }

                sql, vals = build_insert("vitals_raw", raw_row_values)
                cur.execute(sql, vals)
                raw_inserted += 1

            # 2) vitals_events (UPSERT dinâmico SEM assumir data_lanc)
            for r in rows:
                if upsert_vitals_events(cur, cols_events, r):
                    events_upserted += 1

            # 3) recomendações + Telegram
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
        "critical_recommendations": critical_recs
    }


@app.get("/health")
def health():
    return {"status": "ok", "version": "4.0.8"}


@app.get("/v1/db/ping")
def db_ping(x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            r = cur.fetchone()
    return {"ok": True, "db": r["ok"], "version": "4.0.8"}


@app.post("/v1/vitals/batch")
def vitals_batch(payload: Any = Body(...), x_api_key: Optional[str] = Header(None, alias="X-API-KEY")):
    _check_key(x_api_key)
    try:
        rows_raw = extract_rows_from_payload(payload)
        received = len(rows_raw)
        vitals = normalize_powerbi_rows(rows_raw)
        normalized = len(vitals)

        print(f"[INGEST] received_rows={received} normalized={normalized}", flush=True)

        if normalized <= 0:
            row0 = rows_raw[0] if rows_raw else None
            return {
                "ok": True,
                "queued": False,
                "received": received,
                "normalized": 0,
                "message": "Nenhuma linha passou na normalização. COD_ATENDIMENTO não foi encontrado no formato retornado.",
                "debug": {
                    "top_keys": list(payload.keys())[:50] if isinstance(payload, dict) else None,
                    "row0_type": type(row0).__name__ if row0 is not None else None,
                    "row0_keys_sample": list(row0.keys())[:80] if isinstance(row0, dict) else None,
                    "version": "4.0.8",
                }
            }

        stats = persist_all(raw_payload=payload, rows=vitals)

        return {
            "ok": True,
            "queued": False,
            "received": received,
            "normalized": normalized,
            "stats": stats,
            "message": "Gravou vitals_raw (com data_lanc) e fez UPSERT em vitals_events conforme schema real. Recommendations + Telegram para críticos.",
            "version": "4.0.8"
        }

    except Exception as e:
        err = str(e)
        tb = traceback.format_exc()
        print("[ERROR] /v1/vitals/batch crashed:", err, flush=True)
        print(tb, flush=True)
        return {"ok": False, "error": err, "where": "/v1/vitals/batch", "version": "4.0.8"}
