import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import smtplib
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Dict
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, TrailingStopOrderRequest
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trading_engine_monitor.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("LeveragedEngine")

SYMBOLS = {"SMCI", "MSTR", "TSLA", "COIN", "HOOD", "AMC"}
symbol_locks = {symbol: asyncio.Lock() for symbol in SYMBOLS}
EXECUTION_LOCK = asyncio.Lock()

MAX_TRADE_ALLOCATION = float(os.environ.get("MAX_TRADE_ALLOCATION", "10000"))
MAX_RISK_PER_TRADE = float(os.environ.get("MAX_RISK_PER_TRADE", "100"))
DAILY_LOSS_LIMIT = float(os.environ.get("DAILY_LOSS_LIMIT", "-300"))
MAX_ALERT_AGE_SECONDS = int(os.environ.get("MAX_ALERT_AGE_SECONDS", "10"))
MAX_SLIPPAGE_BPS = float(os.environ.get("MAX_SLIPPAGE_BPS", "50"))
IDEMPOTENCY_TTL_SECONDS = int(os.environ.get("IDEMPOTENCY_TTL_SECONDS", "300"))
ALPACA_PAPER = os.environ.get("ALPACA_PAPER", "true").lower() == "true"

MAX_BASE_CLIENT_ORDER_ID_LENGTH = 40
ENTRY_FILL_POLL_ATTEMPTS = int(os.environ.get("ENTRY_FILL_POLL_ATTEMPTS", "40"))
ENTRY_FILL_POLL_SECONDS = float(os.environ.get("ENTRY_FILL_POLL_SECONDS", "0.25"))
TRAIL_PRICE_ATR_MULTIPLE = float(os.environ.get("TRAIL_PRICE_ATR_MULTIPLE", "2.5"))
PARTIAL_TAKE_PROFIT_ENABLED = os.environ.get("PARTIAL_TAKE_PROFIT_ENABLED", "true").lower() == "true"
PARTIAL_TAKE_PROFIT_TRIGGER_ATR = float(os.environ.get("PARTIAL_TAKE_PROFIT_TRIGGER_ATR", "3.0"))
PARTIAL_TAKE_PROFIT_FRACTION = float(os.environ.get("PARTIAL_TAKE_PROFIT_FRACTION", "0.5"))
STATE_DB_PATH = os.environ.get("STATE_DB_PATH", "trading_state.sqlite3")
INTERNAL_HEARTBEAT_ENABLED = os.environ.get("INTERNAL_HEARTBEAT_ENABLED", "true").lower() == "true"
HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "5"))

LOW_VOL_ALLOCATION_SCALE = float(os.environ.get("LOW_VOL_ALLOCATION_SCALE", "0.5"))
LOW_VOL_TRAIL_ATR_MULTIPLE = float(os.environ.get("LOW_VOL_TRAIL_ATR_MULTIPLE", "1.5"))
LOW_VOL_TAKE_PROFIT_TRIGGER_ATR = float(os.environ.get("LOW_VOL_TAKE_PROFIT_TRIGGER_ATR", "1.5"))
LOW_VOL_TAKE_PROFIT_FRACTION = float(os.environ.get("LOW_VOL_TAKE_PROFIT_FRACTION", "0.75"))
LOW_VOL_THRESHOLD = float(os.environ.get("LOW_VOL_THRESHOLD", "1.0"))

MAX_LOSSES_PER_SYMBOL_PER_DAY = int(os.environ.get("MAX_LOSSES_PER_SYMBOL_PER_DAY", "3"))
LOSS_DECAY_FACTORS = [1.0, 0.5, 0.25]
BREAKEVEN_ATR_TRIGGER = float(os.environ.get("BREAKEVEN_ATR_TRIGGER", "1.5"))
NO_NEW_ENTRIES_AFTER_HOUR = int(os.environ.get("NO_NEW_ENTRIES_AFTER_HOUR", "15"))
NO_NEW_ENTRIES_AFTER_MINUTE = int(os.environ.get("NO_NEW_ENTRIES_AFTER_MINUTE", "0"))
TIME_DECAY_FULL_UNTIL_HOUR = int(os.environ.get("TIME_DECAY_FULL_UNTIL_HOUR", "11"))
TIME_DECAY_MID_UNTIL_HOUR = int(os.environ.get("TIME_DECAY_MID_UNTIL_HOUR", "13"))
TIME_DECAY_MID_SCALE = float(os.environ.get("TIME_DECAY_MID_SCALE", "0.75"))
TIME_DECAY_LATE_SCALE = float(os.environ.get("TIME_DECAY_LATE_SCALE", "0.5"))

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
GMAIL_USER = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
SMS_GATEWAY = os.environ.get("SMS_GATEWAY", "4436429291@tmomail.net")

trading_client = None
data_client = None
processed_alerts = {}
active_trades: Dict[str, Dict[str, object]] = {}
daily_loss_counts: Dict[str, int] = {}
daily_loss_date: str = ""
heartbeat_task = None


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    global heartbeat_task
    init_state_db()
    load_state_from_db()
    if INTERNAL_HEARTBEAT_ENABLED:
        heartbeat_task = asyncio.create_task(heartbeat_loop())
        logger.info(f"Internal heartbeat loop started: interval={HEARTBEAT_INTERVAL_SECONDS}s")
    try:
        yield
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                logger.info("Internal heartbeat loop stopped.")


app = FastAPI(title="9:30 AM Breakout Execution Engine v8.0-VolRatio-TimeDecay", lifespan=lifespan)


def init_state_db():
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_trades (
                symbol TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_alerts (
                alert_id TEXT PRIMARY KEY,
                seen_at REAL NOT NULL
            )
            """
        )


def load_state_from_db():
    active_trades.clear()
    processed_alerts.clear()
    now = time.time()
    try:
        with sqlite3.connect(STATE_DB_PATH) as conn:
            for symbol, data in conn.execute("SELECT symbol, data FROM active_trades"):
                try:
                    active_trades[symbol] = json.loads(data)
                except json.JSONDecodeError:
                    logger.warning(f"Ignoring corrupt active trade state for {symbol}.")

            expired_before = now - IDEMPOTENCY_TTL_SECONDS
            conn.execute("DELETE FROM processed_alerts WHERE seen_at < ?", (expired_before,))
            for alert_id, seen_at in conn.execute("SELECT alert_id, seen_at FROM processed_alerts"):
                processed_alerts[alert_id] = float(seen_at)
        logger.info(f"Loaded state: active_trades={len(active_trades)}, processed_alerts={len(processed_alerts)}")
    except Exception as e:
        logger.error(f"State load failed: {e}")


def persist_active_trade(symbol):
    trade = active_trades.get(symbol)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        if trade is None:
            conn.execute("DELETE FROM active_trades WHERE symbol = ?", (symbol,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO active_trades(symbol, data) VALUES (?, ?)",
                (symbol, json.dumps(trade)),
            )


def remove_active_trade(symbol):
    active_trades.pop(symbol, None)
    persist_active_trade(symbol)


def persist_processed_alert(alert_id, seen_at):
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO processed_alerts(alert_id, seen_at) VALUES (?, ?)",
            (alert_id, seen_at),
        )


def delete_processed_alert(alert_id):
    processed_alerts.pop(alert_id, None)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute("DELETE FROM processed_alerts WHERE alert_id = ?", (alert_id,))


@app.get("/health")
async def health_check():
    return {"status": "ok"}


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
        data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    return data_client


async def alpaca_call(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def send_sms_alert_sync(subject: str, message: str):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        logger.warning("SMS skipped: Gmail credentials missing.")
        return
    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = SMS_GATEWAY
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        logger.info(f"SMS Sent: {subject}")
    except Exception as e:
        logger.error(f"SMS Relay failed: {str(e)}")


def parse_timestamp(value):
    if not value:
        raise HTTPException(status_code=400, detail="Missing required alert_time.")
    try:
        normalized = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid alert_time.")


async def validate_market_hours(client):
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    if now_ny.weekday() >= 5:
        raise HTTPException(status_code=403, detail="Market is closed.")

    market_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    if not (market_open <= now_ny <= market_close):
        raise HTTPException(status_code=403, detail="Market is closed.")

    try:
        clock = await alpaca_call(client.get_clock)
        if not clock.is_open:
            raise HTTPException(status_code=403, detail="Alpaca reports market is closed.")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Market clock check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Market clock check failed.")


def validate_circuit_breaker(account):
    equity = float(account.equity)
    last_equity = float(account.last_equity)
    daily_pnl = equity - last_equity
    logger.info(f"Monitor Update - Current Daily P&L Balance: ${daily_pnl:,.2f}")

    if daily_pnl <= DAILY_LOSS_LIMIT:
        raise HTTPException(status_code=403, detail="Circuit Breaker active. Trading halted.")


async def latest_trade_price(symbol):
    try:
        trades = await alpaca_call(
            get_data_client().get_stock_latest_trade,
            StockLatestTradeRequest(symbol_or_symbols=symbol)
        )
        return float(trades[symbol].price)
    except Exception as e:
        logger.error(f"Latest trade lookup failed for {symbol}: {str(e)}")
        raise HTTPException(status_code=503, detail="Latest market price lookup failed.")


def build_alert_id(payload, symbol, side, received_at):
    explicit_id = payload.get("alert_id") or payload.get("id")
    if explicit_id:
        explicit_id = str(explicit_id)
        if len(explicit_id) <= MAX_BASE_CLIENT_ORDER_ID_LENGTH:
            return explicit_id
        return hashlib.sha256(explicit_id.encode("utf-8")).hexdigest()[:32]

    alert_time = payload.get("alert_time") or payload.get("timestamp") or received_at.isoformat()
    raw = f"{symbol}|{side}|{alert_time}|{payload.get('price')}|{payload.get('atr')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def remember_alert(alert_id):
    now = time.time()
    expired = [
        key for key, seen_at in processed_alerts.items()
        if now - seen_at > IDEMPOTENCY_TTL_SECONDS
    ]
    for key in expired:
        delete_processed_alert(key)

    if alert_id in processed_alerts:
        return False

    processed_alerts[alert_id] = now
    persist_processed_alert(alert_id, now)
    return True


def validate_alert_freshness(payload, received_at):
    raw = payload.get("alert_time") or payload.get("timestamp")
    if not raw:
        logger.info("Alert timestamp missing; using server receive time for compatibility.")
        return received_at

    alert_time = parse_timestamp(raw)
    age = (datetime.now(timezone.utc) - alert_time).total_seconds()
    if age < -5:
        raise HTTPException(status_code=400, detail="Alert timestamp is in the future.")
    if age > MAX_ALERT_AGE_SECONDS:
        raise HTTPException(status_code=409, detail="Stale alert rejected.")
    return alert_time


def validate_entry_payload(payload, side):
    if payload.get("atr") is None:
        raise HTTPException(status_code=400, detail="Missing required atr.")

    try:
        atr = float(payload.get("atr"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid atr.")

    if atr <= 0:
        raise HTTPException(status_code=400, detail="ATR must be greater than zero.")

    price = None
    if payload.get("price") is not None:
        try:
            price = float(payload.get("price"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid price.")
        if price <= 0:
            raise HTTPException(status_code=400, detail="Price must be greater than zero.")

    if side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail="Invalid entry side.")

    return price, atr


def get_time_of_day_scale():
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    hour = now_ny.hour
    minute = now_ny.minute

    cutoff_minutes = NO_NEW_ENTRIES_AFTER_HOUR * 60 + NO_NEW_ENTRIES_AFTER_MINUTE
    current_minutes = hour * 60 + minute
    if current_minutes >= cutoff_minutes:
        return 0.0

    if hour < TIME_DECAY_FULL_UNTIL_HOUR:
        return 1.0
    if hour < TIME_DECAY_MID_UNTIL_HOUR:
        return TIME_DECAY_MID_SCALE
    return TIME_DECAY_LATE_SCALE


def get_daily_loss_count(symbol):
    global daily_loss_counts, daily_loss_date
    ny_tz = ZoneInfo("America/New_York")
    today_str = datetime.now(ny_tz).strftime("%Y-%m-%d")

    if daily_loss_date != today_str:
        daily_loss_counts = {}
        daily_loss_date = today_str

    return daily_loss_counts.get(symbol, 0)


def get_loss_decay_factor(symbol):
    loss_count = get_daily_loss_count(symbol)
    if loss_count >= MAX_LOSSES_PER_SYMBOL_PER_DAY:
        return 0.0
    if loss_count < len(LOSS_DECAY_FACTORS):
        return LOSS_DECAY_FACTORS[loss_count]
    return 0.0


def record_symbol_loss(symbol):
    global daily_loss_counts, daily_loss_date
    ny_tz = ZoneInfo("America/New_York")
    today_str = datetime.now(ny_tz).strftime("%Y-%m-%d")

    if daily_loss_date != today_str:
        daily_loss_counts = {}
        daily_loss_date = today_str

    daily_loss_counts[symbol] = daily_loss_counts.get(symbol, 0) + 1
    logger.warning(f"Loss recorded for {symbol}: {daily_loss_counts[symbol]}/{MAX_LOSSES_PER_SYMBOL_PER_DAY} today")


def resolve_trade_params(vol_ratio):
    is_low_vol = vol_ratio < LOW_VOL_THRESHOLD
    if is_low_vol:
        return {
            "is_low_vol": True,
            "vol_ratio": vol_ratio,
            "allocation_scale": LOW_VOL_ALLOCATION_SCALE,
            "trail_atr_multiple": LOW_VOL_TRAIL_ATR_MULTIPLE,
            "take_profit_trigger_atr": LOW_VOL_TAKE_PROFIT_TRIGGER_ATR,
            "take_profit_fraction": LOW_VOL_TAKE_PROFIT_FRACTION,
        }
    return {
        "is_low_vol": False,
        "vol_ratio": vol_ratio,
        "allocation_scale": 1.0,
        "trail_atr_multiple": TRAIL_PRICE_ATR_MULTIPLE,
        "take_profit_trigger_atr": PARTIAL_TAKE_PROFIT_TRIGGER_ATR,
        "take_profit_fraction": PARTIAL_TAKE_PROFIT_FRACTION,
    }


async def validate_slippage(symbol, alert_price):
    market_price = await latest_trade_price(symbol)
    if alert_price is None:
        logger.info(f"Alert price missing for {symbol}; using latest market price and skipping slippage comparison.")
        return market_price, 0.0

    slippage_bps = abs(market_price - alert_price) / alert_price * 10000
    if slippage_bps > MAX_SLIPPAGE_BPS:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Slippage too high: alert={alert_price:.2f}, "
                f"market={market_price:.2f}, bps={slippage_bps:.1f}"
            ),
        )
    return market_price, slippage_bps


def calculate_position_size(account, market_price, trail_price, allocation_scale=1.0):
    buying_power = float(account.daytrading_buying_power)
    max_allocation = MAX_TRADE_ALLOCATION * allocation_scale
    allocation_cash = min(round(buying_power / 3, 2), max_allocation)
    allocation_qty = int(allocation_cash // market_price)
    risk_qty = int(MAX_RISK_PER_TRADE // trail_price)
    calculated_qty = min(allocation_qty, risk_qty)

    logger.info(
        f"Sizing: allocation_cash=${allocation_cash:,.2f}, allocation_qty={allocation_qty}, "
        f"risk_qty={risk_qty}, max_risk=${MAX_RISK_PER_TRADE:,.2f}, trail_price={trail_price:.2f}, "
        f"allocation_scale={allocation_scale:.2f}, final_qty={calculated_qty}"
    )
    return calculated_qty, allocation_cash, allocation_qty, risk_qty


async def get_open_symbol_orders(client, symbol):
    try:
        return await alpaca_call(
            client.get_orders,
            filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        )
    except TypeError:
        orders = await alpaca_call(client.get_orders, filter=GetOrdersRequest(status=QueryOrderStatus.OPEN))
        return [order for order in orders if order.symbol == symbol]


async def cancel_open_symbol_orders(client, symbol):
    orders = await get_open_symbol_orders(client, symbol)
    canceled = 0
    for order in orders:
        try:
            await alpaca_call(client.cancel_order_by_id, order.id)
            canceled += 1
            logger.info(f"Canceled open {symbol} order {order.id}.")
        except Exception as e:
            logger.warning(f"Failed to cancel {symbol} order {order.id}: {str(e)}")
    return canceled


async def close_symbol_position(client, symbol):
    await cancel_open_symbol_orders(client, symbol)
    last_error = None
    for _ in range(3):
        try:
            await alpaca_call(client.close_position, symbol)
            remove_active_trade(symbol)
            return True, None
        except Exception as e:
            last_error = e
            message = str(e)
            if "held_for_orders" in message or "insufficient qty available" in message:
                await asyncio.sleep(0.35)
                await cancel_open_symbol_orders(client, symbol)
                continue
            if "position not found" in message:
                remove_active_trade(symbol)
                return False, e
            return False, e
    return False, last_error


def find_protection_order(open_orders, symbol):
    for order in open_orders:
        order_type = getattr(getattr(order, "type", None), "value", getattr(order, "type", ""))
        if order.symbol == symbol and str(order_type) in {"stop", "trailing_stop"}:
            return order
    return None


def find_position(open_positions, symbol):
    return next((position for position in open_positions if position.symbol == symbol), None)


def position_abs_qty(position):
    return abs(int(float(position.qty)))


async def submit_native_trailing_stop(client, symbol, qty, exit_side, trail_price, client_order_id):
    trailing_request = TrailingStopOrderRequest(
        symbol=symbol,
        qty=qty,
        side=exit_side,
        trail_price=trail_price,
        time_in_force=TimeInForce.DAY,
        client_order_id=client_order_id,
    )
    return await alpaca_call(client.submit_order, order_data=trailing_request)


async def wait_for_entry_fill(client, order_id):
    last_order = None
    for _ in range(ENTRY_FILL_POLL_ATTEMPTS):
        last_order = await alpaca_call(client.get_order_by_id, order_id)
        status = str(getattr(getattr(last_order, "status", None), "value", getattr(last_order, "status", "")))
        if status == "filled":
            return last_order
        if status in {"rejected", "canceled", "expired"}:
            raise RuntimeError(f"Entry order {status}.")
        await asyncio.sleep(ENTRY_FILL_POLL_SECONDS)

    status = str(getattr(getattr(last_order, "status", None), "value", getattr(last_order, "status", "unknown")))
    raise TimeoutError(f"Entry order not filled before protection timeout. status={status}")


def order_decimal_value(order, attr_name, fallback=None):
    value = getattr(order, attr_name, None)
    if value in (None, ""):
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


async def take_partial_profit_and_rearm(client, pos, trade, protection_order):
    symbol = pos.symbol
    current_qty = position_abs_qty(pos)
    take_profit_fraction = float(trade.get("take_profit_fraction", PARTIAL_TAKE_PROFIT_FRACTION))
    partial_qty = int(current_qty * take_profit_fraction)
    if partial_qty <= 0 or current_qty - partial_qty <= 0:
        logger.info(f"Partial take-profit skipped for {symbol}: qty={current_qty}, partial_qty={partial_qty}.")
        return False

    position_side = str(getattr(pos, "side", trade["side"])).lower()
    exit_side = OrderSide.SELL if position_side == "long" else OrderSide.BUY
    alert_id = str(trade["alert_id"])
    trail_price = float(trade["trail_price"])

    await alpaca_call(client.cancel_order_by_id, protection_order.id)
    logger.info(f"Canceled {symbol} trailing stop {protection_order.id} before partial take-profit.")
    await asyncio.sleep(0.25)

    partial_order = await alpaca_call(
        client.submit_order,
        order_data=MarketOrderRequest(
            symbol=symbol,
            qty=partial_qty,
            side=exit_side,
            time_in_force=TimeInForce.DAY,
            client_order_id=f"{alert_id}-pt",
        ),
    )
    filled_partial = await wait_for_entry_fill(client, partial_order.id)
    filled_partial_qty = int(order_decimal_value(filled_partial, "filled_qty", partial_qty))

    refreshed_positions = await alpaca_call(client.get_all_positions)
    refreshed_pos = find_position(refreshed_positions, symbol)
    if not refreshed_pos:
        remove_active_trade(symbol)
        logger.info(f"Partial take-profit flattened {symbol}; no remaining position.")
        return True

    remaining_qty = position_abs_qty(refreshed_pos)
    if remaining_qty <= 0:
        remove_active_trade(symbol)
        return True

    new_trailing_order = await submit_native_trailing_stop(
        client=client,
        symbol=symbol,
        qty=remaining_qty,
        exit_side=exit_side,
        trail_price=trail_price,
        client_order_id=f"{alert_id}-trail2",
    )

    trade["partial_take_profit_taken"] = True
    trade["partial_take_profit_qty"] = filled_partial_qty
    trade["protection_order_id"] = str(new_trailing_order.id)
    persist_active_trade(symbol)
    logger.warning(
        f"PARTIAL TAKE PROFIT: {symbol} closed {filled_partial_qty} shares, "
        f"remaining={remaining_qty}, new_trailing_order={new_trailing_order.id}"
    )
    return True


@app.post("/webhook")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed JSON object payload.")

    logger.info(f"Webhook request received: {payload}")

    symbol = payload.get("symbol")
    side = str(payload.get("side", "")).lower()

    if symbol not in SYMBOLS:
        return JSONResponse(
            status_code=200,
            content={"status": "ignored", "reason": f"Asset {symbol} not in tracking watchlist matrix."},
        )
    if side not in {"buy", "sell", "exit"}:
        raise HTTPException(status_code=400, detail="Invalid or missing side.")

    if symbol_locks[symbol].locked():
        return {"status": "ignored", "reason": f"{symbol} is already processing"}

    async with symbol_locks[symbol]:
        return await process_symbol_alert(symbol, side, payload, background_tasks)


async def process_symbol_alert(symbol, side, payload, background_tasks):
    try:
        client = get_trading_client()
    except RuntimeError as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    await validate_market_hours(client)
    received_at = datetime.now(timezone.utc)
    alert_time = validate_alert_freshness(payload, received_at)
    alert_id = build_alert_id(payload, symbol, side, received_at)

    if not remember_alert(alert_id):
        return {"status": "ignored", "reason": "Duplicate alert", "alert_id": alert_id}

    if side == "exit":
        ok, err = await close_symbol_position(client, symbol)
        reason = payload.get("reason", "TradingView exit alert")
        if ok:
            logger.info(f"Liquidated {symbol}. reason={reason}, alert_id={alert_id}")
            background_tasks.add_task(send_sms_alert_sync, "STRATEGY EXIT", f"Flattened {symbol}: {reason}")
            return {
                "status": "success",
                "action": "flat",
                "alert_id": alert_id,
                "alert_time": alert_time.isoformat(),
                "reason": reason,
            }
        logger.warning(f"Manual exit request skipped for {symbol}: {err}")
        return {"status": "ignored", "reason": f"No active position or error: {str(err)}"}

    alert_price, atr_value = validate_entry_payload(payload, side)

    loss_decay = get_loss_decay_factor(symbol)
    if loss_decay <= 0.0:
        logger.warning(f"Symbol {symbol} blocked: {get_daily_loss_count(symbol)} losses today (max {MAX_LOSSES_PER_SYMBOL_PER_DAY}).")
        return {"status": "blocked", "reason": f"Max losses reached for {symbol} today", "alert_id": alert_id}

    time_scale = get_time_of_day_scale()
    if time_scale <= 0.0:
        logger.warning(f"No new entries after {NO_NEW_ENTRIES_AFTER_HOUR}:{NO_NEW_ENTRIES_AFTER_MINUTE:02d} ET. Blocking {symbol}.")
        return {"status": "blocked", "reason": "Past new-entry cutoff time", "alert_id": alert_id}

    vol_ratio = 1.0
    raw_vol_ratio = payload.get("vol_ratio")
    if raw_vol_ratio is not None:
        try:
            vol_ratio = float(raw_vol_ratio)
        except (TypeError, ValueError):
            vol_ratio = 1.0
    trade_params = resolve_trade_params(vol_ratio)
    trade_params["allocation_scale"] *= time_scale * loss_decay
    logger.info(
        f"Trade params for {symbol}: vol_ratio={vol_ratio:.2f}, "
        f"is_low_vol={trade_params['is_low_vol']}, "
        f"allocation_scale={trade_params['allocation_scale']:.2f} "
        f"(time_scale={time_scale:.2f}, loss_decay={loss_decay:.2f}, losses_today={get_daily_loss_count(symbol)}), "
        f"trail_atr_multiple={trade_params['trail_atr_multiple']:.2f}"
    )

    positions = await alpaca_call(client.get_all_positions)
    if any(position.symbol == symbol for position in positions):
        return {"status": "ignored", "reason": "Position already exists", "alert_id": alert_id}

    open_orders = await get_open_symbol_orders(client, symbol)
    if open_orders:
        return {"status": "ignored", "reason": "Open symbol orders already exist", "alert_id": alert_id}

    account = await alpaca_call(client.get_account)
    validate_circuit_breaker(account)

    if side == "sell":
        try:
            asset = await alpaca_call(client.get_asset, symbol)
            if not getattr(asset, "shortable", False):
                raise HTTPException(status_code=409, detail=f"{symbol} is not shortable.")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Shortability check failed for {symbol}: {str(e)}")
            raise HTTPException(status_code=503, detail="Shortability check failed.")

    market_price, slippage_bps = await validate_slippage(symbol, alert_price)
    trail_price = round(atr_value * trade_params["trail_atr_multiple"], 2)
    if trail_price <= 0:
        raise HTTPException(status_code=400, detail="Calculated trail price must be greater than zero.")
    calculated_qty, allocated_cash, allocation_qty, risk_qty = calculate_position_size(
        account=account,
        market_price=market_price,
        trail_price=trail_price,
        allocation_scale=trade_params["allocation_scale"],
    )

    if calculated_qty <= 0:
        return {
            "status": "failed",
            "reason": "Insufficient buying power or risk budget for share calculation",
            "alert_id": alert_id,
            "allocation_qty": allocation_qty,
            "risk_qty": risk_qty,
        }

    try:
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        exit_side = OrderSide.SELL if side == "buy" else OrderSide.BUY

        entry_request = MarketOrderRequest(
            symbol=symbol,
            qty=calculated_qty,
            side=order_side,
            time_in_force=TimeInForce.DAY,
            client_order_id=alert_id,
        )
        placed_order = await alpaca_call(client.submit_order, order_data=entry_request)
        logger.info(
            f"Entry order {placed_order.id} sent for {symbol}: qty={calculated_qty}, "
            f"side={side}, market_price={market_price:.2f}, slippage_bps={slippage_bps:.1f}, "
            f"alert_id={alert_id}. Awaiting fill before trailing stop."
        )

        filled_order = await wait_for_entry_fill(client, placed_order.id)
        filled_qty = int(order_decimal_value(filled_order, "filled_qty", calculated_qty))
        avg_fill_price = order_decimal_value(filled_order, "filled_avg_price", market_price)
        if filled_qty <= 0:
            raise RuntimeError("Entry filled with zero filled quantity.")

        trailing_order = await submit_native_trailing_stop(
            client=client,
            symbol=symbol,
            qty=filled_qty,
            exit_side=exit_side,
            trail_price=trail_price,
            client_order_id=f"{alert_id}-trail",
        )
        active_trades[symbol] = {
            "atr": atr_value,
            "side": "long" if side == "buy" else "short",
            "entry_price": avg_fill_price,
            "trail_price": trail_price,
            "alert_id": alert_id,
            "entry_order_id": str(placed_order.id),
            "protection_order_id": str(trailing_order.id),
            "partial_take_profit_taken": False,
            "breakeven_stop_applied": False,
            "vol_ratio": vol_ratio,
            "take_profit_trigger_atr": trade_params["take_profit_trigger_atr"],
            "take_profit_fraction": trade_params["take_profit_fraction"],
        }
        persist_active_trade(symbol)
        logger.info(
            f"Native trailing stop armed for {symbol}: qty={filled_qty}, "
            f"avg_fill={avg_fill_price:.4f}, trail_price={trail_price:.2f}, "
            f"vol_ratio={vol_ratio:.2f}, is_low_vol={trade_params['is_low_vol']}, "
            f"entry_order_id={placed_order.id}, protection_order_id={trailing_order.id}, alert_id={alert_id}"
        )
        vol_label = f" [LOW VOL {vol_ratio:.2f}]" if trade_params["is_low_vol"] else ""
        background_tasks.add_task(
            send_sms_alert_sync,
            "STRATEGY ENTRY",
            f"{symbol} {side.upper()} filled{vol_label}. qty={filled_qty}, trail=${trail_price:.2f}",
        )
        return {
            "status": "success",
            "action": "entered_with_native_trailing_stop",
            "order_id": str(placed_order.id),
            "protection_order_id": str(trailing_order.id),
            "alert_id": alert_id,
            "qty": filled_qty,
            "market_price": market_price,
            "avg_fill_price": avg_fill_price,
            "trail_price": trail_price,
            "vol_ratio": vol_ratio,
            "is_low_vol": trade_params["is_low_vol"],
            "take_profit": "none",
        }
    except Exception as e:
        logger.error(f"Alpaca order rejected for {symbol}: {str(e)}")
        delete_processed_alert(alert_id)
        try:
            if any(position.symbol == symbol for position in await alpaca_call(client.get_all_positions)):
                logger.error(f"{symbol} may be unprotected after entry/protection failure. Attempting emergency flatten.")
                await close_symbol_position(client, symbol)
        except Exception as flatten_err:
            logger.error(f"Emergency flatten failed for {symbol}: {flatten_err}")
        raise HTTPException(status_code=500, detail=f"Alpaca execution error: {str(e)}")


@app.post("/heartbeat")
async def monitor_and_adjust_stops():
    async with EXECUTION_LOCK:
        try:
            client = get_trading_client()
            if active_trades:
                try:
                    open_orders = await alpaca_call(
                        client.get_orders,
                        filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=list(active_trades.keys())),
                    )
                except TypeError:
                    open_orders = await alpaca_call(client.get_orders, filter=GetOrdersRequest(status=QueryOrderStatus.OPEN))
                    open_orders = [order for order in open_orders if order.symbol in active_trades]
            else:
                open_orders = []
            open_positions = await alpaca_call(client.get_all_positions)
            open_symbols = {position.symbol for position in open_positions}

            for symbol in list(active_trades):
                if symbol not in open_symbols:
                    trade = active_trades[symbol]
                    if not trade.get("breakeven_stop_applied"):
                        record_symbol_loss(symbol)
                        logger.info(f"Trade for {symbol} exited as a loss (never reached breakeven).")
                    else:
                        logger.info(f"Trade for {symbol} exited after breakeven (no loss counted).")
                    remove_active_trade(symbol)

            for pos in open_positions:
                symbol = pos.symbol
                if symbol not in active_trades:
                    continue

                trade = active_trades[symbol]
                entry_price = float(pos.avg_entry_price)
                current_price = float(pos.current_price)
                protection_order = find_protection_order(open_orders, symbol)
                if not protection_order:
                    logger.error(f"No open trailing protection order found for {symbol}. Attempting emergency flatten.")
                    await close_symbol_position(client, symbol)
                    continue

                atr = float(trade["atr"])
                position_side = str(getattr(pos, "side", trade["side"])).lower()
                favorable_move = current_price - entry_price if position_side == "long" else entry_price - current_price
                trail_price = float(trade["trail_price"])

                logger.info(
                    f"Heartbeat: {symbol} protected by order {protection_order.id}. "
                    f"entry={entry_price:.4f}, current={current_price:.4f}, "
                    f"trail_price={trail_price:.2f}, favorable_move={favorable_move:.2f}"
                )

                if not trade.get("breakeven_stop_applied") and favorable_move >= BREAKEVEN_ATR_TRIGGER * atr:
                    try:
                        await alpaca_call(client.cancel_order_by_id, protection_order.id)
                        await asyncio.sleep(0.25)
                        exit_side = OrderSide.SELL if position_side == "long" else OrderSide.BUY
                        new_trail = await submit_native_trailing_stop(
                            client=client,
                            symbol=symbol,
                            qty=position_abs_qty(pos),
                            exit_side=exit_side,
                            trail_price=trail_price,
                            client_order_id=f"{trade['alert_id']}-be",
                        )
                        trade["breakeven_stop_applied"] = True
                        trade["protection_order_id"] = str(new_trail.id)
                        persist_active_trade(symbol)
                        logger.info(
                            f"BREAKEVEN STOP: {symbol} moved {favorable_move:.2f} in favor. "
                            f"Replaced trailing stop with breakeven-anchored trail. new_order={new_trail.id}"
                        )
                        protection_order = new_trail
                    except Exception as be_err:
                        logger.error(f"Breakeven stop replacement failed for {symbol}: {be_err}")

                if not PARTIAL_TAKE_PROFIT_ENABLED or trade.get("partial_take_profit_taken"):
                    continue

                trigger_atr = float(trade.get("take_profit_trigger_atr", PARTIAL_TAKE_PROFIT_TRIGGER_ATR))
                trigger_move = trigger_atr * atr
                if favorable_move < trigger_move:
                    continue

                try:
                    await take_partial_profit_and_rearm(client, pos, trade, protection_order)
                except Exception as partial_err:
                    logger.error(f"Partial take-profit failed for {symbol}: {partial_err}. Attempting emergency flatten.")
                    await close_symbol_position(client, symbol)

            return {"status": "scan_completed"}

        except Exception as global_err:
            logger.error(f"Heartbeat execution error: {global_err}")
            return {"status": "failed", "error": str(global_err)}


async def heartbeat_loop():
    while True:
        try:
            await monitor_and_adjust_stops()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Internal heartbeat loop error: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
