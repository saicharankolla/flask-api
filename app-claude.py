import asyncio
import hashlib
import logging
import os
import smtplib
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Dict
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, OrderStatus, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import (
    GetOrdersRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLossRequest,
    TakeProfitRequest,
)
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

app = FastAPI(title="9:30 AM Breakout Execution Engine v6.0-LiveReady")

SYMBOLS = {"SMCI", "MSTR", "TSLA"}
symbol_locks = {symbol: asyncio.Lock() for symbol in SYMBOLS}

MAX_TRADE_ALLOCATION = float(os.environ.get("MAX_TRADE_ALLOCATION", "10000"))
DAILY_LOSS_LIMIT = float(os.environ.get("DAILY_LOSS_LIMIT", "-300"))
MAX_ALERT_AGE_SECONDS = int(os.environ.get("MAX_ALERT_AGE_SECONDS", "10"))
MAX_SLIPPAGE_BPS = float(os.environ.get("MAX_SLIPPAGE_BPS", "50"))
IDEMPOTENCY_TTL_SECONDS = int(os.environ.get("IDEMPOTENCY_TTL_SECONDS", "300"))
TAKE_PROFIT_ENABLED = os.environ.get("TAKE_PROFIT_ENABLED", "true").lower() == "true"
ATR_TAKE_PROFIT_MULTIPLE = float(os.environ.get("ATR_TAKE_PROFIT_MULTIPLE", "2.5"))

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
GMAIL_USER = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
SMS_GATEWAY = os.environ.get("SMS_GATEWAY", "4436429291@tmomail.net")

trading_client = None
data_client = None
processed_alerts = {}
active_trades_atr: Dict[str, float] = {}
EXECUTION_LOCK = asyncio.Lock()


@app.get("/health")
async def health_check():
    return {"status": "ok"}


def get_trading_client():
    global trading_client
    if trading_client is None:
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY.")
        trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
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
    raw = payload.get("alert_time") or payload.get("timestamp")
    if not raw:
        return None
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


# 3. Enhanced Complete Webhook Execution Framework
@app.post("/webhook")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed JSON object payload.")

    symbol = payload.get("symbol")
    side = payload.get("side", "").lower()

    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Asset {symbol} not in tracking watchlist matrix.")

    # Concurrency Lock Routing Strategy applied per distinct asset identifier
    async with symbol_locks[symbol]:
        try:
            client = get_trading_client()
        except RuntimeError as e:
            logger.error(str(e))
            raise HTTPException(status_code=500, detail=str(e))

        # Guardrail Layers Validation
        validate_market_hours(client)
        account = client.get_account()
        validate_circuit_breaker(account)
        validate_alert_freshness(payload)

        alert_id = build_alert_id(payload, symbol, side)
        if not remember_alert(alert_id):
            return JSONResponse(status_code=200, content={"status": "ignored", "reason": "Idempotent duplicate footprint detected."})

        # Process Explicit Signal Terminations
        if side == "exit":
            try:
                client.close_position(symbol)
                if symbol in active_trades_atr:
                    del active_trades_atr[symbol]
                return {"status": "liquidation_dispatched", "symbol": symbol}
            except Exception as exit_err:
                logger.warning(f"Manual exit request skipped for {symbol}: {exit_err}")
                return {"status": "no_active_position_to_terminate"}

        # Process Active Entries Path
        alert_price, atr_value = validate_entry_payload(payload, side)

        positions = client.get_all_positions()
        if any(position.symbol == symbol for position in positions):
            return {"status": "ignored", "reason": "Position already exists", "alert_id": alert_id}

        if alert_price is not None:
            market_price, calculated_bps = validate_slippage(symbol, alert_price)
        else:
            market_price = latest_trade_price(symbol)
            calculated_bps = 0.0
        daytrading_power = float(account.daytrading_buying_power)
        allocated_cash = min(round(daytrading_power / 3, 2), MAX_TRADE_ALLOCATION)
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

        take_profit_price = None
        if TAKE_PROFIT_ENABLED:
            take_profit_price = (
                round(market_price + atr_value * ATR_TAKE_PROFIT_MULTIPLE, 2)
                if side == "buy"
                else round(market_price - atr_value * ATR_TAKE_PROFIT_MULTIPLE, 2)
            )

        entry_request = MarketOrderRequest(
            symbol=symbol,
            qty=calculated_qty,
            side=order_side,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET if TAKE_PROFIT_ENABLED else OrderClass.OTO,
            stop_loss=StopLossRequest(stop_price=stop_price),
            take_profit=TakeProfitRequest(limit_price=take_profit_price) if TAKE_PROFIT_ENABLED else None,
            client_order_id=alert_id,
        )

        try:
            placed_order = client.submit_order(order_data=entry_request)
            active_trades_atr[symbol] = atr_value
            logger.info(
                f"LIVE order sent for {symbol}: qty={calculated_qty}, "
                f"market_price={market_price:.2f}, stop={stop_price:.2f}, "
                f"slippage_bps={calculated_bps:.1f}, alert_id={alert_id}"
            )
            background_tasks.add_task(send_sms_alert_sync, "STRATEGY ENTRY", f"Order placed for {symbol}")
            return {
                "status": "success",
                "action": "bracket_dispatched" if TAKE_PROFIT_ENABLED else "oto_dispatched",
                "order_id": str(placed_order.id),
                "alert_id": alert_id,
                "qty": calculated_qty,
                "market_price": market_price,
                "stop_price": stop_price,
                "take_profit_price": take_profit_price,
            }
        except Exception as e:
            logger.error(f"Alpaca order rejected for {symbol}: {str(e)}")
            processed_alerts.pop(alert_id, None)
            raise HTTPException(status_code=500, detail=f"Alpaca execution error: {str(e)}")


@app.post("/heartbeat")
async def monitor_and_adjust_breakeven():
    """
    Scans active positions. If a trade hits +1x ATR, finds the server-side
    bracket stop-loss order and updates its trigger price to breakeven.
    """
    async with EXECUTION_LOCK:
        try:
            client = get_trading_client()
            open_positions = client.get_all_positions()
            all_open_orders = client.get_orders(
                filter=GetOrdersRequest(status=QueryOrderStatus.OPEN)
            )

            for pos in open_positions:
                symbol = pos.symbol
                if symbol not in active_trades_atr:
                    continue

                entry_p = float(pos.avg_entry_price)
                current_p = float(pos.current_price)
                atr = active_trades_atr[symbol]
                target_milestone = entry_p + atr

                if current_p >= target_milestone:
                    logger.info(f"{symbol} hit 1x ATR milestone ({current_p} >= {target_milestone}). Adjusting stop...")

                    stop_order_id = next(
                        (o.id for o in all_open_orders if o.symbol == symbol and o.type.value == "stop"),
                        None,
                    )

                    if stop_order_id:
                        try:
                            client.replace_order_by_id(
                                order_id=stop_order_id,
                                order_data=ReplaceOrderRequest(stop_price=round(entry_p, 2)),
                            )
                            logger.warning(f"BREAKEVEN SET: {symbol} stop moved to ${round(entry_p, 2)}")
                            del active_trades_atr[symbol]
                        except Exception as replace_err:
                            logger.error(f"Failed to replace stop for {symbol}: {replace_err}")
                    else:
                        logger.warning(f"No open stop-loss order found for {symbol}")

            return {"status": "scan_completed"}

        except Exception as global_err:
            logger.error(f"Heartbeat execution error: {global_err}")
            return {"status": "failed", "error": str(global_err)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
