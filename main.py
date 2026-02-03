# =========================
# PREVITA API — MAIN (v4)
# FAST + SAFE + POWERBI READY
# - aceita payload do PowerBI/Automate em vários formatos
# - normaliza chaves curtas e VW_[...]
# - reconstrói event_ts quando DATA_HORA_LANC_MINUTO vem zerado
# - upsert com MERGE (não perde campos)
# - responde rápido (202) e grava em background
# - sem psycopg_pool (evita ModuleNotFound no Render)
# =========================

import os
import time as _time
from typing import Optional, List, Dict, Any, Union
from datetime import datetime

import requests
import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Response, status
from pydantic import BaseModel, Field, ConfigDict


# =========================
# APP / ENV
# =========================
app = FastAPI(title="PREVITA API", version="4.0.0")

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
# DB
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no Render")
    # timeouts defensivos para Render/Neon
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

                    -- chave de idempotência (única)
                    event_key TEXT UNIQUE,

                    -- opcional (se você quiser rastrear o ID do registro do PowerBI)
                    source_id BIGINT NULL,

                    cod_atendimento INT NOT NULL,
                    id_ricadpac INT NULL,

                    -- carimbo do evento (real)
                    event_ts TIMESTAMP NOT NULL,

                    -- componentes (úteis p/ debug e reconstrução)
                    data_lanc DATE NULL,
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

                    -- guarda o payload cru (pra auditoria / não perder dado)
                    raw JSONB NULL,

                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_vitals_events_ts
                ON public.vitals_events (event_ts DESC);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_vitals_events_att_ts
                ON public.vitals_events (cod_atendimento, event_ts DESC);
            """)

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
        conn.commit()


def ensure_tables_with_retry(max_seconds: int = 45) -> bool:
    start = _time.time()
    wait = 1.0
    last_err = None

    while _time.time() - start < max_seconds:
        try:
            ensure_tables_once()
            return True
        except Exception as e:
            last_err = e
            _time.sleep(wait)
            wait = min(wait * 1.7, 6.0)

    print(f"[WARN] DB não ficou pronto a tempo no startup: {last_err}")
    return False


@app.on_event("startup")
def _startup():
    ensure_tables_with_retry()


# =========================
# MODELS
# =========================
class VitalRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cod_atendimento: int
    id_ricadpac: Optional[int] = None
    event_ts: datetime

    data_lanc: Optional[str] = None  # manter string ISO (YYYY-MM-DD) e converter no SQL
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

    source_id: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None


# =========================
# HELPERS: normalização PowerBI
# =========================
def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        # PowerBI às vezes manda 21.037,00 ou "21037"
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        return int(float(x))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        return float(x)
    except Exception:
        return None


def _parse_dt_iso(x: Any) -> Optional[datetime]:
    if not x:
        return None
    try:
        s = str(x).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _parse_event_ts(row: Dict[str, Any]) -> datetime:
    """
    Estratégia robusta:
    1) se vier EVENT_TS / event_ts
    2) se vier DATA_HORA_LANC_MINUTO e NÃO estiver zerado
    3) senão: DATA_LANC + HORA_LANC + MINUTO_LANC
    """
    ev = _pick(row, "event_ts", "EVENT_TS")
    dt = _parse_dt_iso(ev)
    if dt:
        return dt

    dh = _pick(row, "DATA_HORA_LANC_MINUTO", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_HORA_LANC_MINUTO]")
    dt_dh = _parse_dt_iso(dh)

    dl = _pick(row, "DATA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]")
    h = _pick(row, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]")
    m = _pick(row, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]")

    # se DATA_HORA_LANC_MINUTO existe e está OK, usa.
    if dt_dh:
        # Se vier 00:00:00 mas temos hora/minuto e data_lanc, reconstrói.
        if (dt_dh.hour == 0 and dt_dh.minute == 0) and (h is not None or m is not None) and dl:
            base = _parse_dt_iso(dl)
            if not base:
                raise ValueError("DATA_LANC inválida para reconstruir event_ts")
            return base.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)
        return dt_dh

    # fallback: DATA_LANC + HORA + MINUTO
    base = _parse_dt_iso(dl)
    if not base:
        raise ValueError("Sem DATA_LANC válida para construir event_ts")
    return base.replace(hour=int(h or 0), minute=int(m or 0), second=0, microsecond=0)


def _extract_rows_from_any_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    Aceita:
    1) {"rows":[...]}
    2) [...]
    3) PowerBI Query bruto: {"results":[{"tables":[{"rows":[...]}]}]}
       (às vezes vem encapsulado em {"body":{...}})
    """
    if payload is None:
        return []

    # payload pode vir como {"body": {...}}
    if isinstance(payload, dict) and "body" in payload and isinstance(payload["body"], (dict, list)):
        payload = payload["body"]

    if isinstance(payload, list):
        # lista direta
        return [r for r in payload if isinstance(r, dict)]

    if isinstance(payload, dict):
        if isinstance(payload.get("rows"), list):
            return [r for r in payload["rows"] if isinstance(r, dict)]

        # formato powerbi
        results = payload.get("results")
        if isinstance(results, list) and results:
            tables = (results[0] or {}).get("tables")
            if isinstance(tables, list) and tables:
                rows = (tables[0] or {}).get("rows")
                if isinstance(rows, list):
                    return [r for r in rows if isinstance(r, dict)]

    return []


def normalize_rows(raw_rows: List[Dict[str, Any]]) -> List[VitalRow]:
    out: List[VitalRow] = []

    for r in raw_rows:
        try:
            cod = _to_int(_pick(r, "COD_ATENDIMENTO", "VW_PREVITA_VITAIS_AGRUPADOS[COD_ATENDIMENTO]"))
            if not cod:
                raise ValueError("Sem COD_ATENDIMENTO")

            id_r = _to_int(_pick(r, "ID_RICADPAC", "VW_PREVITA_VITAIS_AGRUPADOS[ID_RICADPAC]"))
            src_id = _to_int(_pick(r, "ID", "VW_PREVITA_VITAIS_AGRUPADOS[ID]"))

            dl = _pick(r, "DATA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[DATA_LANC]")
            dl_iso = None
            if dl:
                dt_dl = _parse_dt_iso(dl)
                if dt_dl:
                    dl_iso = dt_dl.date().isoformat()

            event_ts = _parse_event_ts(r)

            row = VitalRow(
                cod_atendimento=cod,
                id_ricadpac=id_r,
                source_id=src_id,
                event_ts=event_ts,

                data_lanc=dl_iso,
                hora=_to_int(_pick(r, "HORA_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[HORA_LANC]")),
                minuto=_to_int(_pick(r, "MINUTO_LANC", "VW_PREVITA_VITAIS_AGRUPADOS[MINUTO_LANC]")),

                temp=_to_float(_pick(r, "TEMP", "VW_PREVITA_VITAIS_AGRUPADOS[TEMP]")),
                dor=str(_pick(r, "DOR", "VW_PREVITA_VITAIS_AGRUPADOS[DOR]")) if _pick(r, "DOR", "VW_PREVITA_VITAIS_AGRUPADOS[DOR]") is not None else None,
                fr=_to_int(_pick(r, "FR", "VW_PREVITA_VITAIS_AGRUPADOS[FR]")),
                fc=_to_int(_pick(r, "FC", "VW_PREVITA_VITAIS_AGRUPADOS[FC]")),
                pad=_to_int(_pick(r, "PAD", "VW_PREVITA_VITAIS_AGRUPADOS[PAD]")),
                pas=_to_int(_pick(r, "PAS", "VW_PREVITA_VITAIS_AGRUPADOS[PAS]")),
                spo2=_to_int(_pick(r, "SPO2", "VW_PREVITA_VITAIS_AGRUPADOS[SPO2]")),
                uso_o2=str(_pick(r, "USO_O2", "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]")) if _pick(r, "USO_O2", "VW_PREVITA_VITAIS_AGRUPADOS[USO_O2]") is not None else None,
                nivel_consciencia=str(_pick(r, "NIVEL_CONSCIENCIA", "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]")) if _pick(r, "NIVEL_CONSCIENCIA", "VW_PREVITA_VITAIS_AGRUPADOS[NIVEL_CONSCIENCIA]") is not None else None,
                profissional=str(_pick(r, "PROFISSIONAL", "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]")) if _pick(r, "PROFISSIONAL", "VW_PREVITA_VITAIS_AGRUPADOS[PROFISSIONAL]") is not None else None,

                raw=r,
            )

            out.append(row)
        except Exception as e:
            print(f"[WARN] Linha ignorada (inválida): {e} | row={r}")

    return out


# =========================
# RECOMMENDATIONS (simples / placeholder)
# =========================
def compute_recommendations(rows: List[VitalRow]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for r in rows:
        level = "OK"
        syndrome = None
        confidence = None
        actions = None

        if r.spo2 is not None and r.spo2 < 92:
            level = "PRIORIDADE"
            syndrome = "Hipoxemia"
            confidence = 0.7
            actions = "Reavaliar oximetria, verificar O2, checar desconforto respiratório e acionar protocolo."

        if r.pas is not None and r.pas < 90:
            level = "IMEDIATO"
            syndrome = "Hipotensão"
            confidence = 0.8
            actions = "Checar PA manual, perfusão, sangramento/dor e acionar médico conforme protocolo."

        if level != "OK":
            recs.append({
                "cod_atendimento": r.cod_atendimento,
                "snapshot_ts": r.event_ts,
                "recommendation_level": level,
                "syndrome": syndrome,
                "confidence": confidence,
                "actions": actions,
            })
    return recs


def persist_recommendations_and_notify(recs: List[Dict[str, Any]]):
    if not recs:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
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


# =========================
# PERSIST (UPSERT COM MERGE)
# =========================
def persist_rows_merge(rows: List[VitalRow]) -> Dict[str, int]:
    inserted = 0
    updated = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                # 🔑 event_key robusta:
                # usa COD + DATA_LANC + HORA + MINUTO quando existir,
                # senão usa COD + event_ts iso.
                if r.data_lanc and r.hora is not None and r.minuto is not None:
                    event_key = f"{r.cod_atendimento}|{r.data_lanc}|{int(r.hora)}|{int(r.minuto)}"
                else:
                    event_key = f"{r.cod_atendimento}|{r.event_ts.isoformat()}"

                cur.execute("""
                    INSERT INTO public.vitals_events (
                        event_key, source_id,
                        cod_atendimento, id_ricadpac,
                        event_ts, data_lanc, hora, minuto,
                        temp, pas, pad, fc, fr, spo2,
                        dor, uso_o2, nivel_consciencia, profissional,
                        raw,
                        updated_at
                    )
                    VALUES (
                        %s, %s,
                        %s, %s,
                        %s, %s::date, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s::jsonb,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (event_key) DO UPDATE SET
                        source_id          = COALESCE(EXCLUDED.source_id, vitals_events.source_id),
                        id_ricadpac        = COALESCE(EXCLUDED.id_ricadpac, vitals_events.id_ricadpac),

                        event_ts           = COALESCE(EXCLUDED.event_ts, vitals_events.event_ts),
                        data_lanc          = COALESCE(EXCLUDED.data_lanc, vitals_events.data_lanc),
                        hora               = COALESCE(EXCLUDED.hora, vitals_events.hora),
                        minuto             = COALESCE(EXCLUDED.minuto, vitals_events.minuto),

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

                        raw               = COALESCE(EXCLUDED.raw, vitals_events.raw),

                        updated_at        = CURRENT_TIMESTAMP;
                """, (
                    event_key, r.source_id,
                    r.cod_atendimento, r.id_ricadpac,
                    r.event_ts, r.data_lanc, r.hora, r.minuto,
                    r.temp, r.pas, r.pad, r.fc, r.fr, r.spo2,
                    r.dor, r.uso_o2, r.nivel_consciencia, r.profissional,
                    psycopg.types.json.Json(r.raw) if r.raw is not None else None,
                ))

                status_msg = (cur.statusmessage or "").upper()
                if status_msg.startswith("INSERT"):
                    inserted += 1
                elif status_msg.startswith("UPDATE"):
                    updated += 1

        conn.commit()

    return {"inserted": inserted, "updated": updated}


def _background_job(rows: List[VitalRow]):
    try:
        stats = persist_rows_merge(rows)
        recs = compute_recommendations(rows)
        persist_recommendations_and_notify(recs)
        print(f"[INGEST] OK stats={stats} recs={len(recs)}")
    except Exception as e:
        print(f"[INGEST] ERROR: {e}")


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
    payload: Any,  # aceita qualquer formato
    background: BackgroundTasks,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    raw_rows = _extract_rows_from_any_payload(payload)

    if not raw_rows:
        return {"ok": True, "queued": 0, "message": "Sem linhas no payload (rows vazio ou formato inválido)"}

    rows = normalize_rows(raw_rows)

    if not rows:
        return {"ok": True, "queued": 0, "message": "Nenhuma linha válida após normalização"}

    # responde rápido (evita timeout do Power Automate)
    background.add_task(_background_job, rows)
    return Response(
        content='{"ok": true, "queued": %d}' % len(rows),
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
    )


@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    max_send: int = 10,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        id,
                        cod_atendimento,
                        snapshot_ts,
                        recommendation_level,
                        syndrome,
                        confidence,
                        actions
                    FROM public.clinical_recommendations
                    WHERE
                        recommendation_level IN ('IMEDIATO', 'PRIORIDADE')
                        AND (
                            recommendation_level = 'IMEDIATO'
                            OR notified_at IS NULL
                        )
                    ORDER BY
                        CASE recommendation_level
                            WHEN 'IMEDIATO' THEN 2
                            ELSE 1
                        END DESC,
                        snapshot_ts DESC
                    LIMIT %s;
                """, (max_send,))
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
                    send_telegram_message_sync(msg)

                    cur.execute("""
                        UPDATE public.clinical_recommendations
                        SET
                            notified_at = CASE
                                WHEN recommendation_level = 'PRIORIDADE'
                                THEN CURRENT_TIMESTAMP
                                ELSE notified_at
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                    """, (r["id"],))
                    sent += 1

            conn.commit()

        return {"ok": True, "found": len(rows), "sent": sent}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
