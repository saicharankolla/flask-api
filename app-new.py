import os
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, HTTPException
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, TrailingStopOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import smtplib
from email.mime.text import MIMEText

# --- 1. CONFIGURATION & LOGGING MONITOR SETUP ---
# Create a dedicated local file to track system health and order execution
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trading_engine_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LeveragedEngine")

app = FastAPI(title="9:30 AM Breakout Execution Engine v4")
trading_lock = asyncio.Lock()

# Environment Credentials Check
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
SMS_GATEWAY = "1234567890@tmomail.net"  # Replace with your phone's carrier gateway

# Force Paper Trading Mode for Monday's Test
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

# --- 2. AUTOMATED NOTIFICATION MODULE ---
def send_sms_alert(subject: str, message: str):
    """Sends free high-priority SMS notifications via Gmail SMTP gateway."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        logger.warning("SMS skipped: Email environmental credentials missing.")
        return
        
    try:
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = GMAIL_USER
        msg['To'] = SMS_GATEWAY
        
        with smtplib.SMTP_SSL('://gmail.com', 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        logger.info(f"SMS Alert Dispatched: {subject} -> {message}")
    except Exception as e:
        logger.error(f"Failed to dispatch SMS: {str(e)}")

# --- 3. OPERATIONAL RISK & TIMEZONE SAFEGUARDS ---
def enforce_market_hours() -> bool:
    """Blocks any execution before 9:30 AM EST or on weekends."""
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    
    # Block Weekends
    if now_ny.weekday() >= 5:
        return False
        
    # Block Pre-market (9:30 AM EST)
    market_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_ny < market_open:
        return False
        
    return True

def check_circuit_breaker() -> bool:
    """Enforces strict -$200 daily loss limits across active capital."""
    try:
        account = trading_client.get_account()
        equity = float(account.equity)
        last_equity = float(account.last_equity)
        daily_pnl = equity - last_equity
        
        logger.info(f"Monitor Update - Current Daily P&L Balance: ${daily_pnl:,.2f}")
        
        if daily_pnl <= -200.00:
            logger.critical(f"CIRCUIT BREAKER HIT: Daily P&L at ${daily_pnl:.2f}. Initiating liquidation.")
            # Hard E-Stop liquidation routine
            trading_client.close_all_positions(cancel_orders=True)
            send_sms_alert("CRITICAL STOP", f"Circuit Breaker Triggered! P&L: ${daily_pnl:.2f}. Account Liquidated.")
            return False
        return True
    except Exception as e:
        logger.error(f"Failed to pull circuit breaker account logs: {str(e)}")
        return False

# --- 4. LIFECYCLE WEBHOOK ENDPOINT ---
@app.post("/webhook")
async def process_tradingview_alert(request: Request):
    # Performance Monitoring: Capture exact timestamp webhook hit server
    ny_tz = ZoneInfo("America/New_York")
    receive_time = datetime.now(ny_tz)
    
    # Force single-threaded verification loop via async lock
    async with trading_lock:
        # Time Lock Check
        if not enforce_market_hours():
            logger.warning("Signal dropped: Webhook packet received outside market hours.")
            raise HTTPException(status_code=403, detail="Market is closed.")
            
        # Circuit Breaker Check
        if not check_circuit_breaker():
            raise HTTPException(status_code=403, detail="Circuit Breaker active. Trading halted.")
            
        # Process Payload
        try:
            payload = await request.json()
        except Exception:
            logger.error("Failed to parse incoming packet: Invalid JSON format.")
            raise HTTPException(status_code=400, detail="Invalid JSON text format.")
            
        symbol = payload.get("symbol")
        side = payload.get("side")       # "buy", "sell", or "exit"
        atr_value = payload.get("atr")   # Floating point value passed from indicator()
        
        # Guard against unmonitored assets (like your legacy COIN alerts)
        if symbol not in ["SMCI", "MSTR", "TSLA"]:
            logger.info(f"Signal ignored: Ticker {symbol} not in core strategy watchlist.")
            return {"status": "ignored", "reason": "Asset out of strategy bounds."}
            
        logger.info(f"Webhook Received for {symbol} | Side: {side} | Latency Timestamp: {receive_time.strftime('%H:%M:%S.%f')}")
        
        # Verify current position status
        positions = trading_client.get_all_positions()
        has_position = any(p.symbol == symbol for p in positions)
        
        # A. HANDLE POSITION EXITS ("exit")
        if side == "exit":
            if has_position:
                trading_client.close_position(symbol)
                logger.info(f"Execution Success: Closed active {symbol} position.")
                send_sms_alert("STRATEGY EXIT", f"Flat: {symbol} trailing stop or EOD hit.")
                return {"status": "success", "action": "liquidated"}
            logger.info(f"Zombie Signal Handled: Exit received for {symbol} but account already flat.")
            return {"status": "ignored", "reason": "Already flat"}
            
        # B. HANDLE POSITION ENTRIES ("buy" / "sell")
        if side in ["buy", "sell"]:
            if has_position:
                logger.warning(f"Signal Dropped: {symbol} entry alert fired, but position already open.")
                return {"status": "ignored", "reason": "Position exists"}
                
            # Compute 4:1 Leveraged Margin Sizing Allocation
            account = trading_client.get_account()
            buying_power = float(account.daytrading_buying_power)
            allocated_funds = buying_power / 3  # Split available intraded buying power equally
            
            logger.info(f"Sizing Engine: Allocating ${allocated_funds:,.2f} of Daytrading Margin to {symbol}")
            
            # Submit Market Order Entry
            order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
            entry_request = MarketOrderRequest(
                symbol=symbol,
                notional=allocated_funds,
                side=order_side,
                time_in_force=TimeInForce.DAY
            )
            submitted_entry = trading_client.submit_order(order_data=entry_request)
            logger.info(f"Order Sent: Entry Market order processed for {symbol}.")
            
            # Submit Automated Secondary ATR Trailing Order
            exit_side = OrderSide.SELL if side == "buy" else OrderSide.BUY
            trailing_request = TrailingStopOrderRequest(
                symbol=symbol,
                qty=submitted_entry.qty,
                side=exit_side,
                trail_price=float(atr_value),
                time_in_force=TimeInForce.DAY
            )
            trading_client.submit_order(order_data=trailing_request)
            logger.info(f"Protection Placed: Attached {atr_value} ATR trailing stop-loss to {symbol}.")
            
            send_sms_alert("STRATEGY ENTRY", f"Entered Leveraged {side.upper()} position on {symbol}.")
            return {"status": "success", "action": "entered"}

        return {"status": "rejected", "reason": "Malformed side instruction"}

# --- 5. SYSTEM HEARTBEAT INITIALIZATION ---
@app.on_event("startup")
async def system_heartbeat():
    """Fires an automated test text right when the server spins up to verify email/SMS connectivity."""
    logger.info("Initializing 4:1 Leveraged Breakout Engine Core Engine...")
    send_sms_alert("SYSTEM ONLINE", "Webhook server initialized. Ready for Monday market open.")
