import os
import json
import re
import logging
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg import sql
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("previta-api")

APP_VERSION = os.getenv("APP_VERSION", "4.1.0")

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def _sanitize_database_url(url: str) -> str:
    """
    Remove parâmetros problemáticos (ex: options/statement_timeout) que alguns poolers podem rejeitar.
    Mantém sslmode/channel_binding se estiverem presentes.
    """
    if not url:
        return url
    # Remove parâmetros que podem quebrar poolers/pgbouncer
    # Ex.: ...?sslmode=require&options=-c%20statement_timeout=...
    url = re.sub(r"([?&])options=[^&]+", r"\1", url, flags=re.IGNORECASE)
    url = re.sub(r"([?&])statement_timeout=[^&]+", r"\1", url, flags=re.IGNORECASE)
    # limpa ?& ou && finais
    url = url.replace("?&", "?").replace("&&", "&").rstrip("&").rstrip("?")
    return url

DATABASE_URL = _sanitize_database_url(os.getenv("DATABASE_URL", "").strip())
SOURCE_NAME = os.getenv("SOURCE_NAME", "power_automate")

if not DATABASE_URL:
    log.warning("DATABASE_URL não definido. Configure no Render (Environment Variables).")

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado.")
    # psycopg3
    return psycopg.connect(DATABASE_URL)

def get_table_columns(conn, table: str, schema: str = "public") -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    rows = conn.execute(q, (schema, table)).fetchall()
    return [r[0] for r in rows]

def has_column(cols: List[str], col: str) -> bool:
    return col in cols

def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # troca vírgula decimal
        s = s.replace(".", "").replace(",", ".") if re.search(r"\d+,\d+", s) else s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    return None

def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        s = s.replace(".", "").replace(",", ".")
        try:
            return int(float(s))
        except Exception:
            return None
    return None

def parse_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # pode vir "2026-02-03T00:00:00" ou "2026-02-03"
        try:
            if "T" in s:
                return datetime.fromisoformat(s.replace("Z", "")).date()
            return datetime.fromisoformat(s).date()
        except Exception:
            return None
    return None

def parse_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", ""))
        except Exception:
            return None
    return None

# -----------------------------------------------------------------------------
# Payload extraction / normalization
# -----------------------------------------------------------------------------
def normalize_key(k: str) -> str:
    """
    "[FC]" -> "FC"
    " NIVEL_CONSCIENCIA " -> "NIVEL_CONSCIENCIA"
    """
    if k is None:
        return ""
    k = str(k).strip()
    k = k.strip("[]")
    k = k.replace(" ", "_")
    k = k.upper()
    return k

def extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Espera receber algo como:
    { "powerbi": { "results": [ { "tables": [ { "rows": [ {...}, ... ] } ] } ] } }

    Também tolera:
    { "results": ... } ou { "tables": ... } se vier direto.
    """
    obj = payload
    if isinstance(payload, dict) and "powerbi" in payload and isinstance(payload["powerbi"], dict):
        obj = payload["powerbi"]

    # Tentativas em cascata
    if isinstance(obj, dict) and "results" in obj:
        results = obj.get("results") or []
        if results and isinstance(results, list):
            r0 = results[0] or {}
            tables = r0.get("tables") or []
            if tables and isinstance(tables, list):
                t0 = tables[0] or {}
                rows = t0.get("rows") or []
                if isinstance(rows, list):
                    return [x for x in rows if isinstance(x, dict)]
    if isinstance(obj, dict) and "tables" in obj:
        tables = obj.get("tables") or []
        if tables and isinstance(tables, list):
            t0 = tables[0] or {}
            rows = t0.get("rows") or []
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
    if isinstance(obj, dict) and "rows" in obj and isinstance(obj["rows"], list):
        return [x for x in obj["rows"] if isinstance(x, dict)]

    return []

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte um row do Power BI em campos canônicos usados no banco.
    """
    norm: Dict[str, Any] = {}
    for k, v in row.items():
        nk = normalize_key(k)
        norm[nk] = v

    # Mapas de possíveis nomes
    cod_atendimento = safe_int(norm.get("COD_ATENDIMENTO")) or safe_int(norm.get("ID"))  # seu Power BI mostra "ID"
    id_ricadpac = safe_int(norm.get("ID_RICADPAC")) or safe_int(norm.get("ID_RICADPAC_"))  # tolerância
    data_lanc = parse_date(norm.get("DATA_LANC")) or parse_date(norm.get("DATA")) or parse_date(norm.get("DATA_LANCAMENTO"))
    hora_lanc = safe_int(norm.get("HORA_LANC"))
    minuto_lanc = safe_int(norm.get("MINUTO_LANC"))

    # Alguns modelos têm DATA_HORA_LANC_MINUTO direto
    dt_full = parse_datetime(norm.get("DATA_HORA_LANC_MINUTO")) or parse_datetime(norm.get("EVENT_TS"))

    if dt_full is None:
        # monta event_ts a partir de data/hora/minuto
        if data_lanc and hora_lanc is not None and minuto_lanc is not None:
            dt_full = datetime.combine(data_lanc, time(hour=hora_lanc, minute=minuto_lanc, second=0))

    # vitais
    out = {
        "cod_atendimento": cod_atendimento,
        "id_ricadpac": id_ricadpac,
        "data_lanc": data_lanc,
        "hora_lanc": hora_lanc,
        "minuto_lanc": minuto_lanc,
        "event_ts": dt_full,
        "temp": safe_float(norm.get("TEMP")),
        "dor": safe_float(norm.get("DOR")),
        "fr": safe_float(norm.get("FR")),
        "fc": safe_float(norm.get("FC")),
        "pad": safe_float(norm.get("PAD")),
        "pas": safe_float(norm.get("PAS")),
        "spo2": safe_float(norm.get("SPO2")),
        "uso_o2": (str(norm.get("USO_O2")).strip() if norm.get("USO_O2") is not None else None),
        "nivel_consciencia": (str(norm.get("NIVEL_CONSCIENCIA")).strip() if norm.get("NIVEL_CONSCIENCIA") is not None else None),
        "profissional": (str(norm.get("PROFISSIONAL")).strip() if norm.get("PROFISSIONAL") is not None else None),
        # raw
        "source": SOURCE_NAME,
        "raw_payload": row,  # salva o row original
    }

    # event_key para deduplicar (mesmo atendimento + mesmo minuto)
    if out["cod_atendimento"] is not None and out["event_ts"] is not None:
        out["event_key"] = f'{out["cod_atendimento"]}::{out["event_ts"].isoformat()}'
    else:
        out["event_key"] = None

    return out

# -----------------------------------------------------------------------------
# UPSERT builder (schema-aware)
# -----------------------------------------------------------------------------
def upsert_rows(conn, table: str, rows: List[Dict[str, Any]], schema: str = "public") -> Tuple[int, int, int, List[str]]:
    """
    Insere/atualiza rows em uma tabela, usando somente colunas existentes.
    Tenta ON CONFLICT (event_key) se a coluna existir; senão faz INSERT simples.
    Retorna: inserted_like, updated_like, skipped, warnings[]
    """
    cols = get_table_columns(conn, table, schema=schema)
    if not cols:
        raise RuntimeError(f"Tabela {schema}.{table} não encontrada ou sem colunas.")

    warnings: List[str] = []
    inserted_like = 0
    updated_like = 0
    skipped = 0

    # Colunas mínimas para vitals_raw (NOT NULL no seu banco)
    required = []
    if table == "vitals_raw":
        required = ["event_ts", "cod_atendimento", "id_ricadpac", "data_lanc", "event_key"]
    elif table == "vitals_events":
        required = ["event_ts", "cod_atendimento", "id_ricadpac", "event_key"]

    use_conflict = has_column(cols, "event_key")

    for r in rows:
        # valida required
        missing = [k for k in required if r.get(k) is None]
        if missing:
            skipped += 1
            continue

        # Monta dict só com colunas existentes
        payload = {}
        for k, v in r.items():
            if k in cols:
                payload[k] = v

        # Campos padrão se existirem
        now = datetime.utcnow()
        if "updated_at" in cols:
            payload["updated_at"] = now
        if "created_at" in cols and "created_at" not in payload:
            payload["created_at"] = now

        # raw_payload: salva JSON se a coluna for json/jsonb
        if "raw_payload" in cols and isinstance(payload.get("raw_payload"), (dict, list)):
            payload["raw_payload"] = json.dumps(payload["raw_payload"], ensure_ascii=False)

        if not payload:
            skipped += 1
            continue

        columns = list(payload.keys())
        values = [payload[c] for c in columns]

        # INSERT
        insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )

        if use_conflict:
            # UPDATE somente do que veio (exceto created_at)
            set_cols = [c for c in columns if c not in ("created_at",)]
            if set_cols:
                update_stmt = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in set_cols
                )
                insert_stmt = insert_stmt + sql.SQL(" ON CONFLICT (event_key) DO UPDATE SET {}").format(update_stmt)
            else:
                insert_stmt = insert_stmt + sql.SQL(" ON CONFLICT (event_key) DO NOTHING")

        try:
            conn.execute(insert_stmt, values)
            # Não dá para saber com perfeição insert vs update sem RETURNING e xmax; simplificamos:
            inserted_like += 1
        except Exception as e:
            msg = str(e)
            # Se falhar por ON CONFLICT alvo inexistente, tenta INSERT simples
            if "there is no unique or exclusion constraint matching the ON CONFLICT specification" in msg:
                warnings.append(f"{table}: não há constraint única em event_key; inserindo sem upsert.")
                try:
                    conn.execute(
                        sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                            sql.Identifier(schema),
                            sql.Identifier(table),
                            sql.SQL(", ").join(map(sql.Identifier, columns)),
                            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                        ),
                        values,
                    )
                    inserted_like += 1
                except Exception as e2:
                    warnings.append(f"{table}: falha ao inserir uma linha: {e2}")
                    skipped += 1
            else:
                warnings.append(f"{table}: falha ao inserir uma linha: {e}")
                skipped += 1

    return inserted_like, updated_like, skipped, warnings

# -----------------------------------------------------------------------------
# Risk / Recommendations (gancho)
# -----------------------------------------------------------------------------
def compute_risk(row: Dict[str, Any]) -> Tuple[int, str, str]:
    """
    Regra simples (placeholder) só para não quebrar o fluxo.
    Você depois substitui pela IA.
    """
    score = 0
    reasons = []

    spo2 = row.get("spo2")
    pas = row.get("pas")
    fc = row.get("fc")
    fr = row.get("fr")
    temp = row.get("temp")

    if spo2 is not None and spo2 < 92:
        score += 3
        reasons.append(f"SpO2 baixa ({spo2})")
    if pas is not None and pas < 90:
        score += 3
        reasons.append(f"PAS baixa ({pas})")
    if fc is not None and fc > 120:
        score += 2
        reasons.append(f"FC alta ({fc})")
    if fr is not None and fr > 24:
        score += 2
        reasons.append(f"FR alta ({fr})")
    if temp is not None and temp >= 38:
        score += 1
        reasons.append(f"Febre ({temp})")

    if score >= 5:
        level = "CRITICO"
    elif score >= 3:
        level = "ATENCAO"
    else:
        level = "OK"

    text = "; ".join(reasons) if reasons else "Sem critérios de risco pela regra simples."
    return score, level, text

def upsert_recommendations_if_exists(conn, rows: List[Dict[str, Any]]):
    """
    Se existir uma tabela de recomendações, grava/atualiza.
    (Schema-aware: só escreve colunas existentes.)
    """
    # tente nomes comuns (ajuste se o seu nome for diferente)
    candidates = ["clinical_recommendations", "clinical_recommendat", "recommendations"]
    existing = None
    for t in candidates:
        try:
            cols = get_table_columns(conn, t)
            if cols:
                existing = t
                break
        except Exception:
            continue

    if not existing:
        return

    cols = get_table_columns(conn, existing)
    use_conflict = "event_key" in cols

    for r in rows:
        score, level, text = compute_risk(r)
        payload = {
            "event_key": r.get("event_key"),
            "cod_atendimento": r.get("cod_atendimento"),
            "id_ricadpac": r.get("id_ricadpac"),
            "event_ts": r.get("event_ts"),
            "risk_score": score,
            "risk_level": level,
            "recommendation": text,
            "source": SOURCE_NAME,
            "updated_at": datetime.utcnow(),
        }

        payload = {k: v for k, v in payload.items() if k in cols and v is not None}
        if "created_at" in cols and "created_at" not in payload:
            payload["created_at"] = datetime.utcnow()

        if not payload:
            continue
        if "event_key" not in payload:
            continue

        columns = list(payload.keys())
        values = [payload[c] for c in columns]

        stmt = sql.SQL("INSERT INTO public.{} ({}) VALUES ({})").format(
            sql.Identifier(existing),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        if use_conflict:
            set_cols = [c for c in columns if c not in ("created_at",)]
            if set_cols:
                stmt = stmt + sql.SQL(" ON CONFLICT (event_key) DO UPDATE SET {}").format(
                    sql.SQL(", ").join(
                        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                        for c in set_cols
                    )
                )

        try:
            conn.execute(stmt, values)
        except Exception:
            # não travar o batch por causa de recomendações
            continue

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="PREVITA API", version=APP_VERSION, openapi_url="/openapi.json")

@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION}

@app.get("/v1/db/ping")
def db_ping():
    try:
        with db_connect() as conn:
            v = conn.execute("SELECT 1").fetchone()[0]
        return {"ok": True, "db": v}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.post("/v1/vitals/batch")
async def vitals_batch(request: Request):
    """
    Recebe payload do Power Automate e grava em:
    - vitals_raw (todos os campos vitais)
    - vitals_events (somente colunas existentes)
    + (gancho) recommendations se existir tabela compatível
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body precisa ser JSON.")

    rows_raw = extract_rows(payload)
    received = len(rows_raw)

    # debug_keys_sample (para te ajudar)
    debug_keys_sample = []
    if isinstance(payload, dict):
        debug_keys_sample = list(payload.keys())[:8]

    normalized_rows: List[Dict[str, Any]] = []
    for rr in rows_raw:
        nr = normalize_row(rr)

        # filtro mínimo: precisa reconhecer atendimento + horário
        if nr.get("cod_atendimento") is None:
            continue
        if nr.get("event_ts") is None:
            continue
        if nr.get("data_lanc") is None:
            # tenta derivar do event_ts se existir
            if nr.get("event_ts"):
                nr["data_lanc"] = nr["event_ts"].date()
        if nr.get("data_lanc") is None:
            continue
        if nr.get("event_key") is None:
            nr["event_key"] = f'{nr["cod_atendimento"]}::{nr["event_ts"].isoformat()}'

        normalized_rows.append(nr)

    normalized = len(normalized_rows)
    if normalized == 0:
        return {
            "ok": True,
            "queued": False,
            "received": received,
            "normalized": 0,
            "inserted_raw": 0,
            "inserted_events": 0,
            "skipped": received,
            "message": "Nenhuma linha passou na normalização. Payload não contém campos mínimos (ID/COD_ATENDIMENTO + DATA_LANC/HORA_LANC/MINUTO_LANC ou DATA_HORA_LANC_MINUTO).",
            "debug_keys_sample": debug_keys_sample,
            "version": APP_VERSION,
        }

    warnings_all: List[str] = []

    try:
        with db_connect() as conn:
            with conn.transaction():
                # vitals_raw: grava tudo que existir na tabela
                ins_raw, _, skip_raw, warn_raw = upsert_rows(conn, "vitals_raw", normalized_rows)
                warnings_all.extend(warn_raw)

                # vitals_events: grava só o que existir (sem "data_lanc" se não existir)
                ins_evt, _, skip_evt, warn_evt = upsert_rows(conn, "vitals_events", normalized_rows)
                warnings_all.extend(warn_evt)

                # gancho: recomendações (se existir tabela compatível)
                upsert_recommendations_if_exists(conn, normalized_rows)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e), "where": "/v1/vitals/batch", "version": APP_VERSION},
        )

    # Se quiser: aqui você acopla o envio Telegram somente para CRITICO (não incluí token/chat por segurança)
    # - Sugestão: enviar só quando houve mudança real (event_key novo/alterado)

    msg = "Dados processados e gravados (raw + events)."
    if warnings_all:
        msg += " Com avisos: " + " | ".join(warnings_all[:3])  # limita

    return {
        "ok": True,
        "queued": False,
        "received": received,
        "normalized": normalized,
        "inserted_raw": ins_raw,
        "inserted_events": ins_evt,
        "skipped": max(0, received - normalized),
        "message": msg,
        "debug_keys_sample": debug_keys_sample,
        "version": APP_VERSION,
    }
