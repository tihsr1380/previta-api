# main.py
import os
import re
import json
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

APP_VERSION = "4.1.1"  # bump para confirmar deploy

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

app = FastAPI(title="PREVITA API", version=APP_VERSION)

# -----------------------------
# Helpers: parsing & normalize
# -----------------------------
def _now_ts() -> datetime:
    return datetime.utcnow()


def _normalize_key(k: Any) -> str:
    """
    Normaliza chaves vindas do PowerBI/PowerAutomate.

    Exemplos que resolve:
    - "COD_ATENDIMENTO" -> "cod_atendimento"
    - "[PAD]" -> "pad"
    - "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]" -> "cod_atendimento"
    - " Tabela [ COD_ATENDIMENTO ] " -> "cod_atendimento"
    """
    if k is None:
        return ""
    s = str(k).strip()

    # pega o último bloco [ ... ] se existir (caso Tabela[COLUNA])
    m = re.findall(r"\[([^\]]+)\]", s)
    if m:
        s = m[-1]

    # remove colchetes restantes e limpa
    s = s.replace("[", "").replace("]", "").strip()

    # normaliza separadores
    s = s.lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    s = re.sub(r"__+", "_", s)
    return s


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return None
    # remove thousand separators like "21.101,00" -> "21101"
    s2 = s.replace(".", "").replace(",", ".")
    try:
        f = float(s2)
        return int(f)
    except Exception:
        digits = re.sub(r"\D+", "", s)
        return int(digits) if digits else None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return None
    # pt-BR: "36,5" -> 36.5 ; also handle "87,00"
    if re.search(r"\d+,\d+", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _parse_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("T", " ")
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        pass
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            return date(int(yyyy), int(mm), int(dd))
        except Exception:
            return None
    return None


def _parse_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("T", " ")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    Accepts many shapes:
    - {"powerbi": {...}}
    - {"results":[{"tables":[{"rows":[...] }]}]}
    - {"tables":[{"rows":[...]}]}
    - {"rows":[...]}
    - [...]
    """
    if payload is None:
        return []

    if isinstance(payload, (bytes, str)):
        try:
            payload = json.loads(payload)
        except Exception:
            return []

    if isinstance(payload, dict) and "powerbi" in payload:
        return _extract_rows(payload["powerbi"])

    if isinstance(payload, dict) and "rows" in payload and isinstance(payload["rows"], list):
        return [r for r in payload["rows"] if isinstance(r, dict)]

    if isinstance(payload, dict) and "tables" in payload and isinstance(payload["tables"], list):
        all_rows: List[Dict[str, Any]] = []
        for t in payload["tables"]:
            if isinstance(t, dict) and isinstance(t.get("rows"), list):
                all_rows.extend([r for r in t["rows"] if isinstance(r, dict)])
        return all_rows

    if isinstance(payload, dict) and "results" in payload and isinstance(payload["results"], list):
        all_rows: List[Dict[str, Any]] = []
        for res in payload["results"]:
            if isinstance(res, dict) and isinstance(res.get("tables"), list):
                for t in res["tables"]:
                    if isinstance(t, dict) and isinstance(t.get("rows"), list):
                        all_rows.extend([r for r in t["rows"] if isinstance(r, dict)])
        return all_rows

    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]

    return []


def _normalize_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (row or {}).items():
        out[_normalize_key(k)] = v
    return out


def _pick_first(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _build_event_ts(data_lanc: date, hora_lanc: int, minuto_lanc: int) -> datetime:
    return datetime.combine(data_lanc, time(hour=hora_lanc, minute=minuto_lanc, second=0))


# -----------------------------
# DB helpers
# -----------------------------
def _db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _ensure_indexes(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS vitals_raw_cod_event_uq
        ON vitals_raw (cod_atendimento, event_ts);
        """)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS risk_events_cod_event_uq
        ON risk_events (cod_atendimento, event_ts);
        """)
        # vitals_events (se existir)
        cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='vitals_events'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS vitals_events_cod_event_uq ON vitals_events (cod_atendimento, event_ts);';
            END IF;
        END$$;
        """)
    conn.commit()


def _get_table_columns(conn, table: str, schema: str = "public") -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        rows = cur.fetchall()
    return [r["column_name"] for r in rows]


def _table_exists(conn, table: str, schema: str = "public") -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


# -----------------------------
# Risk logic (baseline)
# -----------------------------
def _compute_risk(v: Dict[str, Any]) -> Tuple[str, int, str]:
    score = 0
    reasons = []

    spo2 = v.get("spo2")
    fc = v.get("fc")
    fr = v.get("fr")
    pas = v.get("pas")
    pad = v.get("pad")
    temp = v.get("temp")
    nivel = (v.get("nivel_consciencia") or "").strip().upper()
    uso_o2 = (v.get("uso_o2") or "").strip().upper()

    if spo2 is not None:
        if spo2 < 90:
            score += 6
            reasons.append("SpO2 < 90")
        elif spo2 < 92:
            score += 4
            reasons.append("SpO2 < 92")

    if fc is not None:
        if fc >= 130 or fc <= 40:
            score += 4
            reasons.append("FC crítica")
        elif fc >= 110 or fc <= 50:
            score += 2
            reasons.append("FC alterada")

    if fr is not None:
        if fr >= 30 or fr <= 8:
            score += 4
            reasons.append("FR crítica")
        elif fr >= 22 or fr <= 10:
            score += 2
            reasons.append("FR alterada")

    if pas is not None:
        if pas < 90:
            score += 4
            reasons.append("PAS < 90")
        elif pas < 100:
            score += 2
            reasons.append("PAS < 100")

    if pad is not None and pad < 60:
        score += 2
        reasons.append("PAD < 60")

    if temp is not None:
        if temp >= 39.0 or temp < 35.0:
            score += 3
            reasons.append("Temperatura crítica")
        elif temp >= 38.0:
            score += 2
            reasons.append("Febre")

    if "SONOL" in nivel or "CONF" in nivel or "REBAIX" in nivel:
        score += 3
        reasons.append("Consciência alterada")

    if uso_o2 not in ("", "NA", "N/A", "AR AMBIENTE", "AA"):
        score += 1
        reasons.append("Em O2 suplementar")

    if score >= 8:
        return "CRITICO", score, "; ".join(reasons) if reasons else "Score alto"
    if score >= 4:
        return "ATENCAO", score, "; ".join(reasons) if reasons else "Score moderado"
    return "OK", score, "; ".join(reasons) if reasons else "Sem critérios"


def _send_telegram_message(text: str) -> Tuple[bool, str]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False, "Telegram não configurado (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)."

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        if 200 <= resp.status_code < 300:
            return True, "ok"
        return False, f"HTTP {resp.status_code}: {resp.text[:300]}"
    except Exception as e:
        return False, str(e)


# -----------------------------
# API endpoints
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION}


@app.get("/v1/db/ping")
def db_ping():
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as ok;")
                row = cur.fetchone()
        return {"ok": True, "db": row["ok"], "version": APP_VERSION}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "version": APP_VERSION})


@app.post("/v1/vitals/batch")
async def vitals_batch(req: Request):
    received_at = _now_ts()

    try:
        payload = await req.json()
    except Exception:
        payload = None

    rows = _extract_rows(payload)

    # Debug das chaves do payload (nível raiz)
    debug_keys_sample = []
    if isinstance(payload, dict):
        debug_keys_sample = list(payload.keys())[:10]

    # Debug das chaves do primeiro row
    debug_first_row_original_keys: List[str] = []
    debug_first_row_normalized_keys: List[str] = []
    if rows and isinstance(rows[0], dict):
        debug_first_row_original_keys = list(rows[0].keys())[:80]
        debug_first_row_normalized_keys = list(_normalize_row_keys(rows[0]).keys())[:80]

    # Normalize all rows
    normalized_rows_raw: List[Dict[str, Any]] = []
    for r in rows:
        normalized_rows_raw.append(_normalize_row_keys(r))

    vitals: List[Dict[str, Any]] = []
    skipped = 0
    skip_reasons: Dict[str, int] = {}

    for r in normalized_rows_raw:
        # Agora reconhece também "tabela[cod_atendimento]" etc
        cod_at = _to_int(_pick_first(r, "cod_atendimento", "id", "cod_atend", "codatendimento"))
        if cod_at is None:
            skipped += 1
            skip_reasons["missing_cod_atendimento"] = skip_reasons.get("missing_cod_atendimento", 0) + 1
            continue

        id_ricadpac = _to_int(_pick_first(r, "id_ricadpac", "ricadpac", "id_paciente", "idpaciente"))

        data_lanc = _parse_date(_pick_first(r, "data_lanc", "data"))
        hora_lanc = _to_int(_pick_first(r, "hora_lanc", "hora"))
        minuto_lanc = _to_int(_pick_first(r, "minuto_lanc", "minuto", "min"))

        # Se vier datetime completo, usa como fallback
        dt_full = _parse_datetime(_pick_first(r, "data_hora_lanc_minuto", "datahora", "event_ts", "timestamp"))
        if dt_full and (data_lanc is None or hora_lanc is None or minuto_lanc is None):
            data_lanc = data_lanc or dt_full.date()
            hora_lanc = hora_lanc if hora_lanc is not None else dt_full.hour
            minuto_lanc = minuto_lanc if minuto_lanc is not None else dt_full.minute

        if data_lanc is None:
            skipped += 1
            skip_reasons["missing_data_lanc"] = skip_reasons.get("missing_data_lanc", 0) + 1
            continue
        if hora_lanc is None:
            skipped += 1
            skip_reasons["missing_hora_lanc"] = skip_reasons.get("missing_hora_lanc", 0) + 1
            continue
        if minuto_lanc is None:
            skipped += 1
            skip_reasons["missing_minuto_lanc"] = skip_reasons.get("missing_minuto_lanc", 0) + 1
            continue

        event_ts = _build_event_ts(data_lanc, hora_lanc, minuto_lanc)

        v = {
            "event_ts": event_ts,
            "cod_atendimento": cod_at,
            "id_ricadpac": id_ricadpac,
            "data_lanc": data_lanc,
            "hora_lanc": hora_lanc,
            "minuto_lanc": minuto_lanc,
            "temp": _to_float(_pick_first(r, "temp", "temperatura")),
            "dor": _to_float(_pick_first(r, "dor")),
            "fr": _to_float(_pick_first(r, "fr", "freq_respiratoria")),
            "fc": _to_float(_pick_first(r, "fc", "freq_cardiaca")),
            "pad": _to_float(_pick_first(r, "pad")),
            "pas": _to_float(_pick_first(r, "pas")),
            "spo2": _to_float(_pick_first(r, "spo2", "sao2", "sat")),
            "uso_o2": _pick_first(r, "uso_o2", "uso2", "o2"),
            "nivel_consciencia": _pick_first(r, "nivel_consciencia", "nivel", "nivel_consciencia_"),
            "profissional": _pick_first(r, "profissional"),
            "received_at": received_at,
            "source": "power_automate",
            "payload": payload,
        }

        # Clean text fields
        for tf in ("uso_o2", "nivel_consciencia", "profissional"):
            if isinstance(v.get(tf), str):
                v[tf] = v[tf].strip()

        vitals.append(v)

    if not vitals:
        msg = "Nenhuma linha passou na normalização."
        if skip_reasons.get("missing_cod_atendimento"):
            msg = "Nenhuma linha passou na normalização. Payload não contém COD_ATENDIMENTO em formato reconhecido."
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "queued": False,
                "received": len(rows),
                "normalized": 0,
                "skipped": skipped,
                "skip_reasons": skip_reasons,
                "message": msg,
                "debug_keys_sample": debug_keys_sample,
                "debug_first_row_original_keys": debug_first_row_original_keys,
                "debug_first_row_normalized_keys": debug_first_row_normalized_keys,
                "version": APP_VERSION,
            },
        )

    inserted_raw = 0
    updated_raw = 0
    inserted_events = 0
    updated_events = 0
    risk_upserts = 0
    telegram_sent = 0
    telegram_errors: List[str] = []

    try:
        with _db_connect() as conn:
            _ensure_indexes(conn)

            raw_cols = _get_table_columns(conn, "vitals_raw")
            events_exists = _table_exists(conn, "vitals_events")
            events_cols = _get_table_columns(conn, "vitals_events") if events_exists else []
            risk_exists = _table_exists(conn, "risk_events")
            risk_cols = _get_table_columns(conn, "risk_events") if risk_exists else []

            raw_target_cols = [
                c for c in [
                    "event_ts", "cod_atendimento", "id_ricadpac", "data_lanc", "hora_lanc", "minuto_lanc",
                    "temp", "dor", "fr", "fc", "pad", "pas", "spo2",
                    "uso_o2", "nivel_consciencia", "profissional",
                    "received_at", "source", "payload",
                    "updated_at",
                ]
                if c in raw_cols
            ]

            for v in vitals:
                if "updated_at" in raw_target_cols:
                    v["updated_at"] = received_at

            raw_placeholders = ", ".join([f"%({c})s" for c in raw_target_cols])
            raw_columns_sql = ", ".join(raw_target_cols)

            raw_update_cols = [c for c in raw_target_cols if c not in ("cod_atendimento", "event_ts")]
            raw_update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in raw_update_cols if c != "payload"])
            if "payload" in raw_update_cols:
                raw_update_sql = (raw_update_sql + ", " if raw_update_sql else "") + "payload=EXCLUDED.payload"
            if "updated_at" in raw_cols:
                raw_update_sql = (raw_update_sql + ", " if raw_update_sql else "") + "updated_at=NOW()"

            raw_sql = f"""
                INSERT INTO vitals_raw ({raw_columns_sql})
                VALUES ({raw_placeholders})
                ON CONFLICT (cod_atendimento, event_ts)
                DO UPDATE SET {raw_update_sql}
            """

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cod_atendimento, event_ts FROM vitals_raw WHERE (cod_atendimento, event_ts) = ANY(%s)",
                    ([(v["cod_atendimento"], v["event_ts"]) for v in vitals],),
                )
                existing = {(r["cod_atendimento"], r["event_ts"]) for r in cur.fetchall()}

                cur.executemany(raw_sql, vitals)

                inserted_raw = sum(1 for v in vitals if (v["cod_atendimento"], v["event_ts"]) not in existing)
                updated_raw = len(vitals) - inserted_raw

            if events_exists:
                events_target_cols = [
                    c for c in [
                        "event_ts", "cod_atendimento", "id_ricadpac",
                        "data_lanc", "hora_lanc", "minuto_lanc",
                        "temp", "dor", "fr", "fc", "pad", "pas", "spo2",
                        "uso_o2", "nivel_consciencia", "profissional",
                        "created_at",
                    ]
                    if c in events_cols
                ]

                for v in vitals:
                    if "created_at" in events_target_cols and "created_at" not in v:
                        v["created_at"] = received_at

                events_placeholders = ", ".join([f"%({c})s" for c in events_target_cols])
                events_columns_sql = ", ".join(events_target_cols)

                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT cod_atendimento, event_ts FROM vitals_events WHERE (cod_atendimento, event_ts) = ANY(%s)",
                        ([(v["cod_atendimento"], v["event_ts"]) for v in vitals],),
                    )
                    existing_e = {(r["cod_atendimento"], r["event_ts"]) for r in cur.fetchall()}

                    update_cols = [c for c in events_target_cols if c not in ("cod_atendimento", "event_ts", "created_at")]
                    update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols]) if update_cols else "event_ts=EXCLUDED.event_ts"

                    events_sql = f"""
                        INSERT INTO vitals_events ({events_columns_sql})
                        VALUES ({events_placeholders})
                        ON CONFLICT (cod_atendimento, event_ts)
                        DO UPDATE SET {update_sql}
                    """
                    cur.executemany(events_sql, vitals)

                    inserted_events = sum(1 for v in vitals if (v["cod_atendimento"], v["event_ts"]) not in existing_e)
                    updated_events = len(vitals) - inserted_events

            critical_items: List[Dict[str, Any]] = []
            if risk_exists:
                risk_rows: List[Dict[str, Any]] = []
                for v in vitals:
                    level, score, reason = _compute_risk(v)
                    rr = {
                        "event_ts": v["event_ts"],
                        "cod_atendimento": v["cod_atendimento"],
                        "id_ricadpac": v.get("id_ricadpac"),
                        "temp": v.get("temp"),
                        "pas": v.get("pas"),
                        "pad": v.get("pad"),
                        "fc": v.get("fc"),
                        "fr": v.get("fr"),
                        "spo2": v.get("spo2"),
                        "dor": v.get("dor"),
                        "uso_o2": v.get("uso_o2"),
                        "nivel_consciencia": v.get("nivel_consciencia"),
                        "risk_level": level,
                        "risk_score": score,
                        "risk_reason": reason,
                    }
                    rr = {k: rr[k] for k in rr.keys() if k in risk_cols}
                    risk_rows.append(rr)

                    if level == "CRITICO":
                        critical_items.append({"v": v, "risk_score": score, "risk_reason": reason})

                if risk_rows:
                    risk_target_cols = list(risk_rows[0].keys())
                    risk_cols_sql = ", ".join(risk_target_cols)
                    risk_placeholders = ", ".join([f"%({c})s" for c in risk_target_cols])
                    update_cols = [c for c in risk_target_cols if c not in ("cod_atendimento", "event_ts")]
                    update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols]) if update_cols else "event_ts=EXCLUDED.event_ts"
                    risk_sql = f"""
                        INSERT INTO risk_events ({risk_cols_sql})
                        VALUES ({risk_placeholders})
                        ON CONFLICT (cod_atendimento, event_ts)
                        DO UPDATE SET {update_sql}
                    """
                    with conn.cursor() as cur:
                        cur.executemany(risk_sql, risk_rows)
                    risk_upserts = len(risk_rows)

            conn.commit()

        for item in critical_items:
            v = item["v"]
            msg = (
                "🚨 PREVITA - PACIENTE CRÍTICO\n"
                f"Atendimento: {v['cod_atendimento']}\n"
                f"Evento: {v['event_ts']}\n"
                f"Score: {item['risk_score']}\n"
                f"Motivo: {item['risk_reason']}\n\n"
                f"TEMP: {v.get('temp')}\n"
                f"FC: {v.get('fc')} | FR: {v.get('fr')}\n"
                f"PAS/PAD: {v.get('pas')}/{v.get('pad')}\n"
                f"SpO2: {v.get('spo2')} | O2: {v.get('uso_o2')}\n"
                f"Consciência: {v.get('nivel_consciencia')}\n"
                f"Profissional: {v.get('profissional')}\n"
            )
            ok, info = _send_telegram_message(msg)
            if ok:
                telegram_sent += 1
            else:
                telegram_errors.append(info)

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "queued": False,
                "received": len(rows),
                "normalized": len(vitals),
                "skipped": skipped,
                "skip_reasons": skip_reasons,
                "vitals_raw": {"inserted": inserted_raw, "updated": updated_raw},
                "vitals_events": {"exists": events_exists, "inserted": inserted_events, "updated": updated_events},
                "risk_events": {"exists": risk_exists, "upserts": risk_upserts},
                "telegram": {"sent": telegram_sent, "errors": telegram_errors[:5]},
                "message": "Processado com UPSERT por (cod_atendimento,event_ts). Alterações no mesmo minuto atualizam a linha (não duplicam).",
                "debug_keys_sample": debug_keys_sample,
                "debug_first_row_original_keys": debug_first_row_original_keys,
                "debug_first_row_normalized_keys": debug_first_row_normalized_keys,
                "version": APP_VERSION,
            },
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": str(e),
                "where": "/v1/vitals/batch",
                "version": APP_VERSION,
            },
        )
