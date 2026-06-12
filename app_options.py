import asyncio
import json
import logging
import os
import sqlite3
import smtplib
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta, date
from email.mime.text import MIMEText
from typing import Dict
from zoneinfo import ZoneInfo

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionLatestQuoteRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce, ContractType
from alpaca.trading.requests import (
    GetOptionContractsRequest,
    GetOrdersRequest,
    MarketOrderRequest,
)
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("options_engine.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("OptionsEngine")

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
SYMBOLS = {"SMCI", "MSTR", "TSLA", "COIN", "HOOD", "AMC"}
symbol_locks = {symbol: asyncio.Lock() for symbol in SYMBOLS}

MAX_PREMIUM_PER_CONTRACT  = float(os.environ.get("MAX_PREMIUM_PER_CONTRACT",  "300"))   # max to pay per contract ($)
STOP_LOSS_PCT             = float(os.environ.get("STOP_LOSS_PCT",             "0.50"))  # close if premium falls 50%
PARTIAL_TP_PCT            = float(os.environ.get("PARTIAL_TP_PCT",            "1.50"))  # take half off at 150% gain
FULL_TP_PCT               = float(os.environ.get("FULL_TP_PCT",               "2.50"))  # close all at 250% gain
DAILY_LOSS_LIMIT          = float(os.environ.get("DAILY_OPTIONS_LOSS_LIMIT",  "-500"))  # circuit breaker
MAX_ALERT_AGE_SECONDS     = int(os.environ.get("MAX_ALERT_AGE_SECONDS",       "60"))
IDEMPOTENCY_TTL_SECONDS   = int(os.environ.get("IDEMPOTENCY_TTL_SECONDS",     "300"))
HEARTBEAT_INTERVAL        = float(os.environ.get("HEARTBEAT_INTERVAL",        "10"))
ALPACA_PAPER              = os.environ.get("ALPACA_PAPER", "true").lower() == "true"
STATE_DB_PATH             = os.environ.get("OPTIONS_DB_PATH", "options_state.sqlite3")

NO_NEW_ENTRIES_AFTER_HOUR   = int(os.environ.get("NO_NEW_ENTRIES_AFTER_HOUR",   "14"))
NO_NEW_ENTRIES_AFTER_MINUTE = int(os.environ.get("NO_NEW_ENTRIES_AFTER_MINUTE", "30"))

ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
GMAIL_USER        = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD= os.environ.get("GMAIL_APP_PASSWORD")
SMS_GATEWAY       = os.environ.get("SMS_GATEWAY", "4436429291@tmomail.net")
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET")

trading_client    = None
data_client       = None
active_options: Dict[str, Dict] = {}   # keyed by underlying symbol
processed_alerts: Dict[str, float] = {}
daily_pnl: float = 0.0
daily_pnl_date: str = ""
heartbeat_task    = None

# ──────────────────────────────────────────────
# LIFESPAN
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    global heartbeat_task
    init_db()
    load_state()
    heartbeat_task = asyncio.create_task(heartbeat_loop())
    logger.info("Options heartbeat started.")
    try:
        yield
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                logger.info("Options heartbeat stopped.")

app = FastAPI(title="930 AM Options Breakout Engine v1.0", lifespan=lifespan)

# ──────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────
def init_db():
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS active_options (
                symbol TEXT PRIMARY KEY,
                data   TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_alerts (
                alert_id TEXT PRIMARY KEY,
                seen_at  REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_pnl (
                trade_date TEXT PRIMARY KEY,
                pnl        REAL NOT NULL DEFAULT 0
            )
        """)

def load_state():
    global daily_pnl, daily_pnl_date
    ny_tz     = ZoneInfo("America/New_York")
    today_str = datetime.now(ny_tz).strftime("%Y-%m-%d")
    now       = time.time()
    try:
        with sqlite3.connect(STATE_DB_PATH) as conn:
            for symbol, data in conn.execute("SELECT symbol, data FROM active_options"):
                try:
                    active_options[symbol] = json.loads(data)
                except json.JSONDecodeError:
                    pass

            expired = now - IDEMPOTENCY_TTL_SECONDS
            conn.execute("DELETE FROM processed_alerts WHERE seen_at < ?", (expired,))
            for alert_id, seen_at in conn.execute("SELECT alert_id, seen_at FROM processed_alerts"):
                processed_alerts[alert_id] = float(seen_at)

            row = conn.execute(
                "SELECT pnl FROM daily_pnl WHERE trade_date = ?", (today_str,)
            ).fetchone()
            daily_pnl      = float(row[0]) if row else 0.0
            daily_pnl_date = today_str

        logger.info(f"State loaded: active={len(active_options)}, daily_pnl=${daily_pnl:.2f}")
    except Exception as e:
        logger.error(f"State load failed: {e}")

def persist_option(symbol):
    trade = active_options.get(symbol)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        if trade is None:
            conn.execute("DELETE FROM active_options WHERE symbol = ?", (symbol,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO active_options(symbol, data) VALUES (?, ?)",
                (symbol, json.dumps(trade)),
            )

def remove_option(symbol):
    active_options.pop(symbol, None)
    persist_option(symbol)

def record_pnl(amount: float):
    global daily_pnl, daily_pnl_date
    ny_tz     = ZoneInfo("America/New_York")
    today_str = datetime.now(ny_tz).strftime("%Y-%m-%d")
    if daily_pnl_date != today_str:
        daily_pnl      = 0.0
        daily_pnl_date = today_str
    daily_pnl += amount
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO daily_pnl(trade_date, pnl) VALUES (?, ?)",
            (today_str, daily_pnl),
        )
    logger.info(f"Daily options P&L updated: ${daily_pnl:.2f}")

def persist_alert(alert_id, seen_at):
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO processed_alerts(alert_id, seen_at) VALUES (?, ?)",
            (alert_id, seen_at),
        )

def drop_alert(alert_id):
    processed_alerts.pop(alert_id, None)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute("DELETE FROM processed_alerts WHERE alert_id = ?", (alert_id,))

# ──────────────────────────────────────────────
# ALPACA CLIENTS
# ──────────────────────────────────────────────
def get_trading_client():
    global trading_client
    if trading_client is None:
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY.")
        trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
    return trading_client

def get_data_client():
    global data_client
    if data_client is None:
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY.")
        data_client = OptionHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    return data_client

async def alpaca(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def send_sms(subject: str, message: str):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        return
    try:
        msg           = MIMEText(message)
        msg["Subject"]= subject
        msg["From"]   = GMAIL_USER
        msg["To"]     = SMS_GATEWAY
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            s.send_message(msg)
        logger.info(f"SMS sent: {subject}")
    except Exception as e:
        logger.error(f"SMS failed: {e}")

def remember_alert(alert_id: str) -> bool:
    now     = time.time()
    expired = [k for k, v in processed_alerts.items() if now - v > IDEMPOTENCY_TTL_SECONDS]
    for k in expired:
        drop_alert(k)
    if alert_id in processed_alerts:
        return False
    processed_alerts[alert_id] = now
    persist_alert(alert_id, now)
    return True

def validate_freshness(payload: dict, received_at: datetime) -> datetime:
    raw = payload.get("alert_time")
    if not raw:
        return received_at
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - parsed).total_seconds()
        if age < -5:
            raise HTTPException(400, "Alert timestamp is in the future.")
        if age > MAX_ALERT_AGE_SECONDS:
            raise HTTPException(409, "Stale alert rejected.")
        return parsed.astimezone(timezone.utc)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "Invalid alert_time.")

def validate_market_hours():
    ny_tz   = ZoneInfo("America/New_York")
    now_ny  = datetime.now(ny_tz)
    if now_ny.weekday() >= 5:
        raise HTTPException(403, "Market closed — weekend.")
    market_open  = now_ny.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now_ny.replace(hour=16, minute=0,  second=0, microsecond=0)
    if not (market_open <= now_ny <= market_close):
        raise HTTPException(403, "Market closed.")

def past_options_cutoff() -> bool:
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    cutoff = NO_NEW_ENTRIES_AFTER_HOUR * 60 + NO_NEW_ENTRIES_AFTER_MINUTE
    return (now_ny.hour * 60 + now_ny.minute) >= cutoff

def resolve_expiry(dte_mode: str) -> date:
    ny_tz = ZoneInfo("America/New_York")
    today = datetime.now(ny_tz).date()
    mapping = {"0DTE": 0, "1DTE": 1, "2DTE": 2}
    if dte_mode in mapping:
        return today + timedelta(days=mapping[dte_mode])
    if dte_mode == "Weekly":
        # next Friday
        days_ahead = (4 - today.weekday()) % 7
        return today + timedelta(days=days_ahead or 7)
    return today

async def find_contract(symbol: str, option_type: str, strike: float, expiry: date):
    client       = get_trading_client()
    contract_type = ContractType.CALL if option_type == "call" else ContractType.PUT
    result = await alpaca(
        client.get_option_contracts,
        GetOptionContractsRequest(
            underlying_symbols=[symbol],
            expiration_date=expiry,
            type=contract_type,
            strike_price_gte=str(strike - 0.01),
            strike_price_lte=str(strike + 0.01),
        ),
    )
    contracts = result.option_contracts if hasattr(result, "option_contracts") else list(result)
    if not contracts:
        raise HTTPException(404, f"No {option_type} contract found for {symbol} strike={strike} expiry={expiry}.")
    return contracts[0]

async def get_option_mid_price(contract_symbol: str) -> float:
    try:
        quotes = await alpaca(
            get_data_client().get_option_latest_quote,
            OptionLatestQuoteRequest(symbol_or_symbols=contract_symbol),
        )
        quote = quotes[contract_symbol]
        bid   = float(quote.bid_price or 0)
        ask   = float(quote.ask_price or 0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2, 2)
        return float(quote.ask_price or quote.bid_price or 0)
    except Exception as e:
        logger.error(f"Quote fetch failed for {contract_symbol}: {e}")
        return 0.0

async def close_option_position(symbol: str, qty: int, reason: str):
    trade = active_options.get(symbol)
    if not trade:
        return False
    client          = get_trading_client()
    contract_symbol = trade["contract_symbol"]
    option_type     = trade["option_type"]
    exit_side       = OrderSide.SELL
    try:
        await alpaca(
            client.submit_order,
            order_data=MarketOrderRequest(
                symbol=contract_symbol,
                qty=qty,
                side=exit_side,
                time_in_force=TimeInForce.DAY,
            ),
        )
        logger.info(f"Closed {qty}x {contract_symbol} ({symbol} {option_type}). Reason: {reason}")
        if qty >= trade.get("remaining_contracts", trade["contracts"]):
            remove_option(symbol)
        else:
            trade["remaining_contracts"] = trade.get("remaining_contracts", trade["contracts"]) - qty
            persist_option(symbol)
        return True
    except Exception as e:
        logger.error(f"Option close failed for {symbol}: {e}")
        return False

# ──────────────────────────────────────────────
# HEALTH
# ──────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "active_options": list(active_options.keys()), "daily_pnl": daily_pnl}

# ──────────────────────────────────────────────
# WEBHOOK — ENTRY
# ──────────────────────────────────────────────
@app.post("/webhook/options")
async def handle_options_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "Malformed JSON payload.")

    if WEBHOOK_SECRET and payload.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized.")

    logger.info(f"Options webhook: {payload}")

    symbol = payload.get("symbol")
    action = str(payload.get("action", "")).upper()

    if symbol not in SYMBOLS:
        return JSONResponse(200, content={"status": "ignored", "reason": f"{symbol} not in watchlist."})

    if action == "CLOSE_ALL_OPTIONS":
        return await handle_eod_close(symbol, background_tasks)

    if action not in {"BUY_CALL", "BUY_PUT"}:
        raise HTTPException(400, f"Unknown action: {action}")

    if symbol_locks[symbol].locked():
        return {"status": "ignored", "reason": f"{symbol} already processing."}

    async with symbol_locks[symbol]:
        return await process_options_entry(symbol, action, payload, background_tasks)

async def handle_eod_close(symbol: str, background_tasks: BackgroundTasks):
    if symbol not in active_options:
        return {"status": "ignored", "reason": f"No active option for {symbol}."}
    trade = active_options[symbol]
    qty   = trade.get("remaining_contracts", trade["contracts"])
    ok    = await close_option_position(symbol, qty, "EOD Liquidation")
    if ok:
        background_tasks.add_task(send_sms, "OPTIONS EOD EXIT", f"Closed {symbol} {trade['option_type'].upper()} at EOD.")
    return {"status": "success" if ok else "failed", "action": "eod_close", "symbol": symbol}

async def process_options_entry(symbol: str, action: str, payload: dict, background_tasks: BackgroundTasks):
    received_at = datetime.now(timezone.utc)
    alert_id    = f"{symbol}_{action}_{received_at.strftime('%Y%m%d%H%M%S')}"

    if not remember_alert(alert_id):
        return {"status": "ignored", "reason": "Duplicate alert.", "alert_id": alert_id}

    validate_freshness(payload, received_at)
    validate_market_hours()

    # Time cutoff — no new 0DTE entries after 2:30 PM ET
    if past_options_cutoff():
        logger.warning(f"Options entry blocked for {symbol}: past {NO_NEW_ENTRIES_AFTER_HOUR}:{NO_NEW_ENTRIES_AFTER_MINUTE:02d} ET cutoff.")
        return {"status": "blocked", "reason": f"No new options entries after {NO_NEW_ENTRIES_AFTER_HOUR}:{NO_NEW_ENTRIES_AFTER_MINUTE:02d} ET.", "alert_id": alert_id}

    # Circuit breaker
    if daily_pnl <= DAILY_LOSS_LIMIT:
        return {"status": "blocked", "reason": f"Daily loss limit hit (${daily_pnl:.2f})."}

    # One position per underlying at a time
    if symbol in active_options:
        return {"status": "ignored", "reason": f"Already have an open option on {symbol}.", "alert_id": alert_id}

    option_type = "call" if action == "BUY_CALL" else "put"
    try:
        strike      = float(payload["strike"])
        contracts   = int(payload.get("contracts", 1))
        dte_mode    = str(payload.get("dte", "0DTE"))
        atr         = float(payload.get("atr", 0))
        signal_price= float(payload.get("price", 0))
        session     = str(payload.get("session", "morning"))
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"Invalid payload: {e}")

    expiry   = resolve_expiry(dte_mode)
    contract = await find_contract(symbol, option_type, strike, expiry)
    mid      = await get_option_mid_price(contract.symbol)

    if mid <= 0:
        raise HTTPException(503, f"Could not get live quote for {contract.symbol}.")

    cost_per_contract = mid * 100   # options quoted per share, 100 shares per contract
    if cost_per_contract > MAX_PREMIUM_PER_CONTRACT:
        return {
            "status": "blocked",
            "reason": f"Premium ${cost_per_contract:.2f} exceeds max ${MAX_PREMIUM_PER_CONTRACT:.2f}.",
            "contract": contract.symbol,
            "mid": mid,
        }

    try:
        client = get_trading_client()
        order  = await alpaca(
            client.submit_order,
            order_data=MarketOrderRequest(
                symbol=contract.symbol,
                qty=contracts,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
            ),
        )

        active_options[symbol] = {
            "contract_symbol":    contract.symbol,
            "option_type":        option_type,
            "strike":             strike,
            "expiry":             str(expiry),
            "contracts":          contracts,
            "remaining_contracts":contracts,
            "entry_premium":      mid,
            "entry_cost":         cost_per_contract * contracts,
            "partial_tp_taken":   False,
            "order_id":           str(order.id),
            "atr":                atr,
            "signal_price":       signal_price,
            "session":            session,
            "alert_id":           alert_id,
        }
        persist_option(symbol)

        logger.info(
            f"OPTIONS ENTRY: {symbol} {option_type.upper()} {contracts}x {contract.symbol} "
            f"@ mid={mid:.2f} (${cost_per_contract:.0f}/contract), strike={strike}, expiry={expiry}, "
            f"session={session}, atr={atr:.2f}"
        )
        background_tasks.add_task(
            send_sms,
            "OPTIONS ENTRY",
            f"{symbol} {option_type.upper()} {contracts}x @ ${mid:.2f} | strike={strike} | {dte_mode}",
        )
        return {
            "status":           "success",
            "action":           f"bought_{option_type}",
            "contract":         contract.symbol,
            "contracts":        contracts,
            "entry_premium":    mid,
            "entry_cost":       cost_per_contract * contracts,
            "strike":           strike,
            "expiry":           str(expiry),
            "order_id":         str(order.id),
            "alert_id":         alert_id,
        }

    except Exception as e:
        logger.error(f"Options order failed for {symbol}: {e}")
        drop_alert(alert_id)
        raise HTTPException(500, f"Options order error: {e}")

# ──────────────────────────────────────────────
# HEARTBEAT — MONITOR & EXIT
# ──────────────────────────────────────────────
@app.post("/heartbeat/options")
async def monitor_options():
    if not active_options:
        return {"status": "idle"}

    results = []
    for symbol in list(active_options):
        trade           = active_options[symbol]
        contract_symbol = trade["contract_symbol"]
        entry_premium   = float(trade["entry_premium"])
        remaining       = trade.get("remaining_contracts", trade["contracts"])

        current_mid = await get_option_mid_price(contract_symbol)
        if current_mid <= 0:
            logger.warning(f"No quote for {contract_symbol}, skipping heartbeat.")
            continue

        pct_change = (current_mid - entry_premium) / entry_premium
        dollar_pnl = (current_mid - entry_premium) * 100 * remaining

        logger.info(
            f"Heartbeat {symbol}: contract={contract_symbol}, entry={entry_premium:.2f}, "
            f"current={current_mid:.2f}, pct={pct_change:+.1%}, pnl=${dollar_pnl:+.0f}"
        )

        # ── Full take profit ──────────────────────────────
        if current_mid >= entry_premium * FULL_TP_PCT:
            ok = await close_option_position(symbol, remaining, f"Full TP at {pct_change:+.0%}")
            if ok:
                record_pnl(dollar_pnl)
                asyncio.create_task(asyncio.to_thread(
                    send_sms, "OPTIONS FULL TP",
                    f"{symbol} closed {remaining}x @ ${current_mid:.2f} | +{pct_change:.0%} | P&L ${dollar_pnl:+.0f}"
                ))
                results.append({"symbol": symbol, "action": "full_tp", "pnl": dollar_pnl})
            continue

        # ── Partial take profit ───────────────────────────
        if not trade.get("partial_tp_taken") and current_mid >= entry_premium * PARTIAL_TP_PCT:
            partial_qty = max(1, remaining // 2)
            partial_pnl = (current_mid - entry_premium) * 100 * partial_qty
            ok = await close_option_position(symbol, partial_qty, f"Partial TP at {pct_change:+.0%}")
            if ok:
                record_pnl(partial_pnl)
                trade["partial_tp_taken"] = True
                persist_option(symbol)
                asyncio.create_task(asyncio.to_thread(
                    send_sms, "OPTIONS PARTIAL TP",
                    f"{symbol} partial {partial_qty}x @ ${current_mid:.2f} | +{pct_change:.0%} | P&L ${partial_pnl:+.0f}"
                ))
                results.append({"symbol": symbol, "action": "partial_tp", "pnl": partial_pnl})
            continue

        # ── Stop loss ─────────────────────────────────────
        if current_mid <= entry_premium * STOP_LOSS_PCT:
            loss = (current_mid - entry_premium) * 100 * remaining
            ok   = await close_option_position(symbol, remaining, f"Stop loss at {pct_change:+.0%}")
            if ok:
                record_pnl(loss)
                asyncio.create_task(asyncio.to_thread(
                    send_sms, "OPTIONS STOP LOSS",
                    f"{symbol} stopped {remaining}x @ ${current_mid:.2f} | {pct_change:.0%} | P&L ${loss:+.0f}"
                ))
                results.append({"symbol": symbol, "action": "stop_loss", "pnl": loss})
            continue

        results.append({
            "symbol":      symbol,
            "contract":    contract_symbol,
            "current_mid": current_mid,
            "pct_change":  round(pct_change, 4),
            "pnl":         round(dollar_pnl, 2),
            "action":      "watching",
        })

    return {"status": "scan_completed", "positions": results, "daily_pnl": daily_pnl}

async def heartbeat_loop():
    while True:
        try:
            await monitor_options()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
