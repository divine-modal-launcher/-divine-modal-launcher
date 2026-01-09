import hashlib
import json
import logging
import os
import sqlite3
import time
from queue import Queue
from threading import Thread
from typing import Any, Dict

import requests
from flask import Flask, jsonify, request

# ================= CONFIG =================
PORT = int(os.getenv("PORT", "5000"))
DB_PATH = os.getenv("BOT_DB", "pipeline_bot.db")

WEBHOOK_PATH_TOKEN = os.getenv("WEBHOOK_PATH_TOKEN", "").strip()  # TradingView secret path token
WEBHOOK_AUTH_KEY = os.getenv("WEBHOOK_AUTH_KEY", "").strip()  # Internal API key (engine calls)
ALLOW_NO_KEY_LOCAL = os.getenv("ALLOW_NO_KEY_LOCAL", "false").lower() == "true"

PIPELINE_ALERT_THRESHOLD = float(os.getenv("PIPELINE_ALERT_THRESHOLD", "1.2"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BOT_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ================= LOGGING =================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("pipeline_service")

app = Flask(__name__)

# ================= ASYNC TELEGRAM =================
tg_queue: "Queue[Dict[str, Any]]" = Queue()


def telegram_worker() -> None:
    while True:
        payload = tg_queue.get()
        try:
            if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
                logger.debug("Telegram not configured; skipping send.")
            else:
                requests.post(BOT_URL, json=payload, timeout=6)
        except Exception:
            logger.exception("Telegram worker error")
        finally:
            tg_queue.task_done()


Thread(target=telegram_worker, daemon=True).start()


def send_telegram_async(text: str) -> None:
    if not text:
        return
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram disabled. Message:\n%s", text)
        return
    tg_queue.put({"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"})


# ================= DB =================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db() -> None:
    conn = db()
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT,
        payload_json TEXT,
        payload_hash TEXT UNIQUE,
        signal_id TEXT,
        decision TEXT,
        should_execute INTEGER,
        reason TEXT
    )"""
    )
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS open_trades (
        pair TEXT PRIMARY KEY,
        status TEXT,
        opened_at TEXT
    )"""
    )
    conn.commit()
    conn.close()


init_db()

# ================= AUTH + HELPERS =================

def is_authorized_internal() -> bool:
    if not WEBHOOK_AUTH_KEY:
        return ALLOW_NO_KEY_LOCAL
    return request.headers.get("X-Api-Key", "") == WEBHOOK_AUTH_KEY


def normalize_pair(p: Any) -> str:
    return str(p or "").upper().replace("/", "").replace("-", "").replace("_", "").strip()


def stable_hash(data: Dict[str, Any]) -> str:
    raw = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def open_trade_exists(pair: str) -> bool:
    if not pair:
        return False
    conn = db()
    try:
        row = conn.execute("SELECT status FROM open_trades WHERE pair=?", (pair,)).fetchone()
        return bool(row and row[0] == "OPEN")
    finally:
        conn.close()


def save_signal(data: Dict[str, Any], h: str, decision: str, should_execute: bool, reason: str) -> bool:
    """
    Returns True if inserted; False if duplicate.
    """
    received_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    signal_id = str(data.get("signal_id") or data.get("id") or "").strip() or None
    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO signals(received_at, payload_json, payload_hash, signal_id, decision, should_execute, reason)
            VALUES (?,?,?,?,?,?,?)
        """,
            (received_at, json.dumps(data), h, signal_id, decision, 1 if should_execute else 0, reason),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


# ================= ROUTES =================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/webhook/<token>", methods=["POST"])
def webhook(token: str):
    # TradingView secret path protection
    if WEBHOOK_PATH_TOKEN and token != WEBHOOK_PATH_TOKEN:
        logger.warning("Forbidden webhook access (bad token).")
        return jsonify({"error": "Forbidden"}), 403

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON"}), 400

    # Basic required field check
    pair = normalize_pair(data.get("pair1"))
    if not pair:
        return jsonify({"error": "Missing pair1"}), 400

    pipeline = float(data.get("pipeline1", 0) or 0)
    bias = str(data.get("bias1") or "N/A")

    # Dedupe hash
    h = stable_hash(data)

    # Decision logic
    decision = "APPROVED"
    should_execute = True
    reason = ""

    if open_trade_exists(pair):
        decision = "REJECTED"
        should_execute = False
        reason = "OPEN_TRADE_EXISTS"
        logger.info("Signal rejected for %s: open trade exists.", pair)

    # Save to DB (even rejected signals are valuable for forensics)
    saved = save_signal(data, h, decision, should_execute, reason)
    if not saved:
        decision = "IGNORED"
        should_execute = False
        reason = "DUPLICATE_SIGNAL"
        logger.info("Signal ignored: duplicate hash=%s", h)

    # Notify
    msg = (
        f"🚀 *SIGNAL*: `{pair}`\nPipeline: `{pipeline:.2f}`\nBias: `{bias}`\nDecision: *{decision}*"
    )
    if reason:
        msg += f"\nReason: `{reason}`"
    if pipeline >= PIPELINE_ALERT_THRESHOLD:
        msg += "\n💧 *HIGH PROBABILITY ALERT*"

    send_telegram_async(msg)

    return (
        jsonify(
            {
                "status": "received",
                "saved": saved,
                "hash": h,
                "pair1": pair,
                "decision": decision,
                "should_execute": should_execute,
                "reason": reason,
            }
        ),
        200,
    )


@app.route("/trade/open", methods=["POST"])
def mark_open():
    """Execution engine calls this when a position is opened."""
    if not is_authorized_internal():
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    pair = normalize_pair(data.get("pair"))
    if not pair:
        return jsonify({"error": "Missing 'pair'"}), 400

    conn = db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO open_trades(pair, status, opened_at) VALUES (?, 'OPEN', ?)",
            (pair, time.strftime("%Y-%m-%dT%H:%M:%S")),
        )
        conn.commit()
    finally:
        conn.close()

    send_telegram_async(f"🟢 *TRADE OPENED*: `{pair}`")
    return jsonify({"status": "success", "pair": pair}), 200


@app.route("/trade/close", methods=["POST"])
def mark_close():
    """Execution engine calls this when a position is closed."""
    if not is_authorized_internal():
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    pair = normalize_pair(data.get("pair"))
    reason = str(data.get("reason") or "ENGINE_EXIT").upper()

    if not pair:
        return jsonify({"error": "Missing 'pair'"}), 400

    conn = db()
    try:
        conn.execute("DELETE FROM open_trades WHERE pair=?", (pair,))
        conn.commit()
    finally:
        conn.close()

    send_telegram_async(f"🏁 *TRADE CLOSED*: `{pair}`  Reason: `{reason}`")
    return jsonify({"status": "success", "pair": pair, "reason": reason}), 200


@app.route("/query/latest", methods=["GET"])
def query_latest():
    """Internal query to see last signal stored."""
    if not is_authorized_internal():
        return jsonify({"error": "Unauthorized"}), 401

    conn = db()
    try:
        row = conn.execute(
            """
            SELECT received_at, payload_hash, decision, should_execute, reason, payload_json
            FROM signals ORDER BY id DESC LIMIT 1
        """
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"message": "No signals yet."}), 200

    return (
        jsonify(
            {
                "received_at": row[0],
                "hash": row[1],
                "decision": row[2],
                "should_execute": bool(row[3]),
                "reason": row[4],
                "payload": json.loads(row[5]),
            }
        ),
        200,
    )


# ================= RUN =================
if __name__ == "__main__":
    if WEBHOOK_PATH_TOKEN == "":
        logger.warning("WEBHOOK_PATH_TOKEN is empty. Set it before exposing publicly.")
    if WEBHOOK_AUTH_KEY == "" and not ALLOW_NO_KEY_LOCAL:
        logger.warning("WEBHOOK_AUTH_KEY is empty. Set it for /trade/* and /query/latest security.")
    app.run(host="0.0.0.0", port=PORT)
