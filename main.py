# =========================
# PREVITA API — MAIN (v3)  ✅ Robust ingest + merge partial rows
# =========================
import os
from typing import Optional, List, Any, Dict, Tuple, Union
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Request

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="3.0.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

# =========================
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

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
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception:
        pass

# =========================
# TABLES
# =========================
def ensure_tables():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.vitals_events (
            id BIGSERIAL PRIMARY KEY,
            event_key TEXT UNIQUE,
            src_id BIGINT NULL,
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

        CREATE INDEX IF NOT EXISTS idx_vitals_events_ts
            ON public.vitals_events (event_ts DESC);

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

        CREATE INDEX IF NOT EXISTS idx_clinrec_att_ts
            ON public.clinical_recommendations (cod_atendimento, snapshot_ts DESC);
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def _startup():
    ensure_tables()

# =========================
# HELPERS — parsing
# =========================
PBI_MAP = {
    "VW_PREVITA_VITAIS_AGRUPADOS[ID]": "src_id",
    "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]": "cod_atendimento",
    "VW_PREVITA_VITAIS_AGRUPADOS[ID_RICADPAC]": "id_ricadpac",
    "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]": "data_lanc",
    "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]": "hora",
    "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]": "minuto",
    "VW_PREVITA_VITAIS_AGRUPADOS[DATA_HORA_LANC_MINUTO]": "data_hora_lanc_minuto",
    "VW_PREVITA_VITAIS_AGRUPADOS[TEMP]": "temp",
    "VW_PREVITA_VITAIS_AGRUPADOS[PAS]": "pas",
    "VW_PREVITA_VITAIS_AGRUPADOS[PAD]": "pad",
    "VW_PREVITA_VITAIS_AGRUPADOS[FC]": "fc",
    "VW_PREVITA_VITAIS_AGRUPADOS[FR]": "fr",
    "VW_PREVITA_VITAIS_AGRUPADOS[SPO2]": "spo2",
    "VW_PREVITA_VITAIS_AGRUPADOS[DOR]": "dor",
    "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]": "uso_o2",
    "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]": "nivel_consciencia",
    "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]": "profissional",
}

def _to_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        # PowerBI às vezes manda 21037.00 etc
        return int(float(v))
    except Exception:
        return None

def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _to_str(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None

def _parse_iso_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        # tenta pegar só a data
        try:
            return datetime.fromisoformat(s.split("T")[0])
        except Exception:
            return None

def normalize_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte chaves PowerBI (VW_PREVITA...) para chaves canônicas.
    Se já vier canônico, mantém.
    """
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        kk = PBI_MAP.get(k, k)
        out[kk] = v
    return out

def build_event_ts(n: Dict[str, Any]) -> datetime:
    """
    Regra sênior (estável):
    - event_ts = DATA_LANC (apenas data) + HORA_LANC + MINUTO_LANC
    - se DATA_LANC não existir, tenta DATA_HORA_LANC_MINUTO
    """
    base = _parse_iso_date(_to_str(n.get("data_lanc"))) or _parse_iso_date(_to_str(n.get("data_hora_lanc_minuto")))
    if not base:
        raise ValueError("Sem data válida (DATA_LANC/DATA_HORA_LANC_MINUTO)")

    hora = _to_int(n.get("hora")) or 0
    minuto = _to_int(n.get("minuto")) or 0
    return datetime(base.year, base.month, base.day, hora, minuto, 0)

def make_event_key(n: Dict[str, Any], event_ts: datetime) -> str:
    """
    Chave idempotente para MESCLAR linhas parciais do mesmo minuto.
    Inclui src_id (ID do dataset) quando disponível.
    """
    cod = _to_int(n.get("cod_atendimento"))
    if cod is None:
        raise ValueError("cod_atendimento ausente")
    ric = _to_int(n.get("id_ricadpac"))
    sid = _to_int(n.get("src_id"))
    # chave por minuto + id + atendimento/paciente
    return f"{cod}|{ric or 0}|{sid or 0}|{event_ts.strftime('%Y-%m-%d %H:%M')}"

def extract_rows_from_any_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    Aceita:
      A) {"rows":[...]}
      B) [...]
      C) resposta PowerBI: {"results":[{"tables":[{"rows":[...]}]}]}
    """
    if payload is None:
        return []

    # B) lista direta
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        return []

    # A) {"rows":[...]}
    if isinstance(payload.get("rows"), list):
        return [x for x in payload["rows"] if isinstance(x, dict)]

    # C) resposta PowerBI
    results = payload.get("results")
    if isinstance(results, list) and results:
        try:
            tables = results[0].get("tables")
            if isinstance(tables, list) and tables:
                rows = tables[0].get("rows")
                if isinstance(rows, list):
                    return [x for x in rows if isinstance(x, dict)]
        except Exception:
            pass

    return []

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok", "version": "3.0.0"}

# =========================
# LAST EVENT TS
# =========================
@app.get("/v1/vitals/last_event_ts")
def last_event_ts(x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    _check_key(x_api_key)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(event_ts) AS last_ts FROM public.vitals_events;")
    row = cur.fetchone()
    cur.close()
    conn.close()
    last_ts = row["last_ts"]
    return {"last_event_ts": last_ts.isoformat() if last_ts else "1970-01-01T00:00:00"}

# =========================
# RECOMMENDATIONS (same idea)
# =========================
def compute_recommendations_for_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = "OK"
        syndrome = None
        confidence = None
        actions = None

        spo2 = r.get("spo2")
        pas = r.get("pas")

        if isinstance(spo2, (int, float)) and spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.7
            actions = "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar protocolo."

        if isinstance(pas, (int, float)) and pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.8
            actions = "Checar PA manual, perfusão, sangramento/dor e acionar médico conforme protocolo."

        if level != "OK":
            recs.append({
                "cod_atendimento": r["cod_atendimento"],
                "snapshot_ts": r["event_ts"],
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": confidence,
                "actions": actions
            })
    return recs

def persist_recommendations_and_notify(recs: List[Dict[str, Any]]):
    if not recs:
        return

    conn = get_conn()
    cur = conn.cursor()

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

        if rec["recommendation_level"] == "IMEDIATO":
            msg = (
                f"🚨 <b>PREVITA – ALERTA IMEDIATO</b>\n\n"
                f"🧾 <b>Atendimento:</b> {rec['cod_atendimento']}\n"
                f"🕒 <b>Snapshot:</b> {rec['snapshot_ts']}\n"
                f"🧠 <b>Síndrome:</b> {rec.get('syndrome') or '-'}\n"
                f"✅ <b>Ações:</b>\n{(rec.get('actions') or '-').strip()[:3500]}"
            )
            send_telegram_message_sync(msg)

    conn.commit()
    cur.close()
    conn.close()

# =========================
# INGEST — BATCH (robust)
# =========================
@app.post("/v1/vitals/batch")
async def vitals_batch(
    request: Request,
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body inválido (JSON esperado)")

    raw_rows = extract_rows_from_any_payload(payload)
    if not raw_rows:
        return {"ok": True, "inserted": 0, "updated": 0, "message": "Sem linhas"}

    # normaliza + monta event_ts + tipagem
    normalized: List[Dict[str, Any]] = []
    errors: List[str] = []
    for rr in raw_rows:
        try:
            n = normalize_row(rr)

            cod = _to_int(n.get("cod_atendimento"))
            if cod is None:
                raise ValueError("cod_atendimento ausente")

            event_ts = build_event_ts(n)
            event_key = make_event_key(n, event_ts)

            normalized.append({
                "event_key": event_key,
                "src_id": _to_int(n.get("src_id")),
                "cod_atendimento": cod,
                "id_ricadpac": _to_int(n.get("id_ricadpac")),
                "event_ts": event_ts,
                "hora": _to_int(n.get("hora")),
                "minuto": _to_int(n.get("minuto")),
                "temp": _to_float(n.get("temp")),
                "pas": _to_int(n.get("pas")),
                "pad": _to_int(n.get("pad")),
                "fc": _to_int(n.get("fc")),
                "fr": _to_int(n.get("fr")),
                "spo2": _to_int(n.get("spo2")),
                "dor": _to_str(n.get("dor")),
                "uso_o2": _to_str(n.get("uso_o2")),
                "nivel_consciencia": _to_str(n.get("nivel_consciencia")),
                "profissional": _to_str(n.get("profissional")),
            })
        except Exception as e:
            errors.append(str(e))

    if not normalized:
        raise HTTPException(status_code=422, detail={"msg": "Nenhuma linha válida", "errors": errors[:20]})

    # UPSERT que MESCLA campos parciais (não apaga com NULL)
    conn = get_conn()
    cur = conn.cursor()

    inserted = 0
    updated = 0

    upsert_sql = """
        INSERT INTO public.vitals_events (
            event_key, src_id, cod_atendimento, id_ricadpac, event_ts,
            hora, minuto, temp, pas, pad, fc, fr, spo2,
            dor, uso_o2, nivel_consciencia, profissional, updated_at
        )
        VALUES (
            %(event_key)s, %(src_id)s, %(cod_atendimento)s, %(id_ricadpac)s, %(event_ts)s,
            %(hora)s, %(minuto)s, %(temp)s, %(pas)s, %(pad)s, %(fc)s, %(fr)s, %(spo2)s,
            %(dor)s, %(uso_o2)s, %(nivel_consciencia)s, %(profissional)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (event_key) DO UPDATE SET
            src_id = COALESCE(EXCLUDED.src_id, public.vitals_events.src_id),
            id_ricadpac = COALESCE(EXCLUDED.id_ricadpac, public.vitals_events.id_ricadpac),
            event_ts = COALESCE(EXCLUDED.event_ts, public.vitals_events.event_ts),
            hora = COALESCE(EXCLUDED.hora, public.vitals_events.hora),
            minuto = COALESCE(EXCLUDED.minuto, public.vitals_events.minuto),
            temp = COALESCE(EXCLUDED.temp, public.vitals_events.temp),
            pas = COALESCE(EXCLUDED.pas, public.vitals_events.pas),
            pad = COALESCE(EXCLUDED.pad, public.vitals_events.pad),
            fc = COALESCE(EXCLUDED.fc, public.vitals_events.fc),
            fr = COALESCE(EXCLUDED.fr, public.vitals_events.fr),
            spo2 = COALESCE(EXCLUDED.spo2, public.vitals_events.spo2),
            dor = COALESCE(EXCLUDED.dor, public.vitals_events.dor),
            uso_o2 = COALESCE(EXCLUDED.uso_o2, public.vitals_events.uso_o2),
            nivel_consciencia = COALESCE(EXCLUDED.nivel_consciencia, public.vitals_events.nivel_consciencia),
            profissional = COALESCE(EXCLUDED.profissional, public.vitals_events.profissional),
            updated_at = CURRENT_TIMESTAMP
        RETURNING (xmax = 0) AS inserted_flag;
    """

    for row in normalized:
        cur.execute(upsert_sql, row)
        ret = cur.fetchone()
        if ret and ret["inserted_flag"]:
            inserted += 1
        else:
            updated += 1

    conn.commit()
    cur.close()
    conn.close()

    # recomendações com base nas linhas já normalizadas
    rec_rows = [{"cod_atendimento": r["cod_atendimento"], "event_ts": r["event_ts"], "pas": r["pas"], "spo2": r["spo2"]} for r in normalized]
    recs = compute_recommendations_for_rows(rec_rows)
    background.add_task(persist_recommendations_and_notify, recs)

    return {
        "ok": True,
        "received": len(raw_rows),
        "parsed": len(normalized),
        "inserted": inserted,
        "updated": updated,
        "recs_generated": len(recs),
        "row_errors": errors[:10],
    }
