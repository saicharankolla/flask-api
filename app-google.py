import asyncio
import hashlib
import logging
import os
import smtplib
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, StopLossRequest
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trading_engine_monitor.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("LeveragedEngine")

app = FastAPI(title="9:30 AM Breakout Execution Engine v6.0-LiveReady")

SYMBOLS = {"SMCI", "MSTR", "TSLA"}
symbol_locks = {symbol: asyncio.Lock() for symbol in SYMBOLS}

MAX_TRADE_ALLOCATION = float(os.environ.get("MAX_TRADE_ALLOCATION", "10000"))
DAILY_LOSS_LIMIT = float(os.environ.get("DAILY_LOSS_LIMIT", "-300"))
MAX_ALERT_AGE_SECONDS = int(os.environ.get("MAX_ALERT_AGE_SECONDS", "10"))
MAX_SLIPPAGE_BPS = float(os.environ.get("MAX_SLIPPAGE_BPS", "50"))
IDEMPOTENCY_TTL_SECONDS = int(os.environ.get("IDEMPOTENCY_TTL_SECONDS", "300"))
TAKE_PROFIT_ENABLED = False

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
GMAIL_USER = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
SMS_GATEWAY = os.environ.get("SMS_GATEWAY", "4436429291@tmomail.net")

trading_client = None
data_client = None
processed_alerts = {}


@app.get("/health")
async def health_check():
    return {"status": "ok"}


def get_trading_client():
    global trading_client
    if trading_client is None:
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY.")
        trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=False)
    return trading_client


def get_data_client():
    global data_client
    if data_client is None:
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY.")
        data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    return data_client


def send_sms_alert_sync(subject: str, message: str):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
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


def validate_market_hours(client):
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    if now_ny.weekday() >= 5:
        raise HTTPException(status_code=403, detail="Market is closed.")

    market_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    if not (market_open <= now_ny <= market_close):
        raise HTTPException(status_code=403, detail="Market is closed.")

    try:
        clock = client.get_clock()
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


def latest_trade_price(symbol):
    try:
        trades = get_data_client().get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols=symbol)
        )
        return float(trades[symbol].price)
    except Exception as e:
        logger.error(f"Latest trade lookup failed for {symbol}: {str(e)}")
        raise HTTPException(status_code=503, detail="Latest market price lookup failed.")


def build_alert_id(payload, symbol, side):
    explicit_id = payload.get("alert_id") or payload.get("id")
    if explicit_id:
        return str(explicit_id)

    alert_time = payload.get("alert_time") or payload.get("timestamp")
    raw = f"{symbol}|{side}|{alert_time}|{payload.get('price')}|{payload.get('atr')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def remember_alert(alert_id):
    now = time.time()
    expired = [
        key for key, seen_at in processed_alerts.items()
        if now - seen_at > IDEMPOTENCY_TTL_SECONDS
    ]
    for key in expired:
        del processed_alerts[key]

    if alert_id in processed_alerts:
        return False

    processed_alerts[alert_id] = now
    return True


def validate_alert_freshness(payload):
    alert_time = parse_timestamp(payload.get("alert_time") or payload.get("timestamp"))
    age = (datetime.now(timezone.utc) - alert_time).total_seconds()
    if age < -5:
        raise HTTPException(status_code=400, detail="Alert timestamp is in the future.")
    if age > MAX_ALERT_AGE_SECONDS:
        raise HTTPException(status_code=409, detail="Stale alert rejected.")
    return alert_time


def validate_entry_payload(payload, side):
    if payload.get("price") is None:
        raise HTTPException(status_code=400, detail="Missing required price.")
    if payload.get("atr") is None:
        raise HTTPException(status_code=400, detail="Missing required atr.")

    try:
        price = float(payload.get("price"))
        atr = float(payload.get("atr"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid price or atr.")

    if price <= 0:
        raise HTTPException(status_code=400, detail="Price must be greater than zero.")
    if atr <= 0:
        raise HTTPException(status_code=400, detail="ATR must be greater than zero.")

    if side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail="Invalid entry side.")

    return price, atr


def validate_slippage(symbol, alert_price):
    market_price = latest_trade_price(symbol)
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


@app.post("/webhook")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON Structure")

    logger.info(f"Webhook request received: {payload}")

    symbol = payload.get("symbol")
    side = payload.get("side")

    if symbol not in SYMBOLS:
        return {"status": "ignored", "reason": "Out of strategy scope"}
    if side not in {"buy", "sell", "exit"}:
        raise HTTPException(status_code=400, detail="Invalid or missing side.")

    lock = symbol_locks[symbol]
    if lock.locked():
        return {"status": "ignored", "reason": f"{symbol} is already processing"}

    async with lock:
        return await process_symbol_alert(symbol, side, payload, background_tasks)


async def process_symbol_alert(symbol, side, payload, background_tasks):
    try:
        client = get_trading_client()
    except RuntimeError as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    validate_market_hours(client)
    alert_time = validate_alert_freshness(payload)

    alert_id = build_alert_id(payload, symbol, side)
    if not remember_alert(alert_id):
        return {"status": "ignored", "reason": "Duplicate alert", "alert_id": alert_id}

    if side == "exit":
        try:
            open_orders = client.get_orders(
                filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
            )
            for order in open_orders:
                client.cancel_order_by_id(order.id)
            client.close_position(symbol)
            reason = payload.get("reason", "TradingView exit alert")
            logger.info(f"Liquidated {symbol}. reason={reason}, alert_id={alert_id}")
            background_tasks.add_task(send_sms_alert_sync, "STRATEGY EXIT", f"Flattened {symbol}: {reason}")
            return {
                "status": "success",
                "action": "flat",
                "alert_id": alert_id,
                "alert_time": alert_time.isoformat(),
                "reason": reason,
            }
        except Exception as e:
            return {"status": "ignored", "reason": f"No active position or error: {str(e)}"}

    alert_price, atr_value = validate_entry_payload(payload, side)

    positions = client.get_all_positions()
    if any(position.symbol == symbol for position in positions):
        return {"status": "ignored", "reason": "Position already exists", "alert_id": alert_id}

    account = client.get_account()
    validate_circuit_breaker(account)

    market_price, slippage_bps = validate_slippage(symbol, alert_price)
    buying_power = float(account.daytrading_buying_power)
    allocated_cash = min(round(buying_power / 3, 2), MAX_TRADE_ALLOCATION)
    calculated_qty = int(allocated_cash // market_price)

    if calculated_qty <= 0:
        return {
            "status": "failed",
            "reason": "Insufficient buying power for share calculation",
            "alert_id": alert_id,
        }

    order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
    stop_price = round(market_price - atr_value, 2) if side == "buy" else round(market_price + atr_value, 2)
    if stop_price <= 0:
        raise HTTPException(status_code=400, detail="Calculated stop price must be greater than zero.")

    entry_request = MarketOrderRequest(
        symbol=symbol,
        qty=calculated_qty,
        side=order_side,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.OTO,
        # No take-profit leg by design. Strategy exits come from TradingView exit alerts.
        stop_loss=StopLossRequest(stop_price=stop_price),
        client_order_id=alert_id,
    )

    try:
        placed_order = client.submit_order(order_data=entry_request)
        logger.info(
            f"LIVE OTO order sent for {symbol}: qty={calculated_qty}, "
            f"market_price={market_price:.2f}, stop={stop_price:.2f}, "
            f"slippage_bps={slippage_bps:.1f}, alert_id={alert_id}"
        )
        background_tasks.add_task(send_sms_alert_sync, "STRATEGY ENTRY", f"OTO order placed for {symbol}")
        return {
            "status": "success",
            "action": "oto_dispatched",
            "order_id": str(placed_order.id),
            "alert_id": alert_id,
            "qty": calculated_qty,
            "market_price": market_price,
            "stop_price": stop_price,
            "take_profit": "none" if not TAKE_PROFIT_ENABLED else "enabled",
        }
    except Exception as e:
        logger.error(f"Alpaca live order rejected for {symbol}: {str(e)}")
        processed_alerts.pop(alert_id, None)
        raise HTTPException(status_code=500, detail=f"Alpaca execution error: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
