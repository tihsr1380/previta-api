# =========================
# PREVITA API — MAIN
# =========================
import os
from typing import Optional

import psycopg
from psycopg.rows import dict_row

import requests
from fastapi import FastAPI, HTTPException, Header

# =========================
# APP
# =========================
app = FastAPI(title="PREVITA API", version="1.0.0")

# =========================
# ENV
# =========================
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY")

# =========================
# HELPERS
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def _check_key(x_api_key: Optional[str]):
    if ALERT_API_KEY and x_api_key != ALERT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def send_telegram_message_sync(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram ENV não configurado")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    r = requests.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error: {r.status_code} {r.text}")

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# NOTIFY TELEGRAM — PRODUÇÃO
# =========================
@app.post("/v1/notify/telegram/run")
def notify_telegram_run(
    minutes_back: int = 180,
    max_send: int = 10,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    _check_key(x_api_key)

    try:
        conn = get_conn()
        cur = conn.cursor()

        # 🔴 REGRA CLÍNICA DEFINITIVA
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

            # ⚠️ Só marca notified_at se PRIORIDADE
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
        cur.close()
        conn.close()

        return {
            "ok": True,
            "found": len(rows),
            "sent": sent
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
