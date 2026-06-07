import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify
import alpaca_trade_api as tradeapi

app = Flask(__name__)

# ==============================================================================
# 1. LIVE USER CREDENTIALS & NOTIFICATION CONFIGURATION
# ==============================================================================
# Alpaca Live API Keys
ALPACA_API_KEY     = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY  = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL     = "https://alpaca.markets"  # Swap to live URL when verified

# Free Gmail Notification Credentials
GMAIL_ADDRESS      = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")

# Target Recipient (Your specific mobile carrier gateway address or normal email)
# Examples: yournumber@txt.att.net | yournumber@vtext.com | yournumber@tmomail.net
NOTIFICATION_RECEIVER = "your_phone_number@vtext.com"

# Hardcoded Strategy Risk Parameters
MAX_STRATEGY_CAPITAL = 40000.00   # 4:1 Leveraged buying power based on your $10k cash
DAILY_LOSS_LIMIT     = -200.00    # Hard maximum loss threshold allowed per day

# ==============================================================================
# 2. COMPONENT INITIALIZATION & STATE MANAGEMENT
# ==============================================================================
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')

# Tracks intraday operational status dynamically (resets daily)
GLOBAL_STATE = {
    "trades_executed_today": 0,
    "max_trades_allowed": 2,
    "initial_balance_today": None,
    "system_active": True
}

def send_notification(subject, message_body):
    """Dispatches instant execution updates directly to your device for free."""
    try:
        msg = MIMEMultipart()
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = NOTIFICATION_RECEIVER
        msg['Subject'] = subject
        msg.attach(MIMEText(message_body, 'plain'))
        
        server = smtplib.SMTP('://gmail.com', 587)
        server.starttls()
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, NOTIFICATION_RECEIVER, msg.as_string())
        server.quit()
        print(f"Notification Sent: {subject}")
    except Exception as e:
        print(f"Notification System Failure: {e}")

def check_account_safety():
    """Monitors live balance equity to enforce the absolute -$200 stop loss."""
    if not GLOBAL_STATE["system_active"]:
        return False

    try:
        account = api.get_account()
        print(account)
        equity = float(account.equity)
        
        if GLOBAL_STATE["initial_balance_today"] is None:
            GLOBAL_STATE["initial_balance_today"] = equity
            
        current_pnl = equity - GLOBAL_STATE["initial_balance_today"]
        
        if current_pnl <= DAILY_LOSS_LIMIT:
            GLOBAL_STATE["system_active"] = False
            send_notification(
                "CRITICAL EMERGENCY CIRCUIT BREAKER", 
                f"Daily loss limit reached ({current_pnl:.2f}). System is fully locked out."
            )
            return False
        return True
    except Exception as e:
        print(f"Safety Check Exception: {e}")
        return False

# ==============================================================================
# 3. WEBHOOK RECEIVER & TRANSLATION PIPELINE
# ==============================================================================
@app.route('/webhook', methods=['POST'])
def handle_tradingview_alert():
    payload = request.json
    if not payload:
        return jsonify({"status": "rejected", "reason": "Payload empty"}), 400
        
    # Enforce strategy walls
    if not check_account_safety():
        return jsonify({"status": "blocked", "reason": "Risk protection locked"}), 403
        
    if GLOBAL_STATE["trades_executed_today"] >= GLOBAL_STATE["max_trades_allowed"]:
        return jsonify({"status": "blocked", "reason": "Maximum daily trade quota complete"}), 403

    # Clean incoming formatting data arrays
    try:
        ticker      = payload.get("ticker")  # Expects MSTR, TSLA, SMCI
        side        = payload.get("side")    # Expects "buy" (Long) or "sell" (Short)
        entry_price = float(payload.get("price"))
        atr         = float(payload.get("atr"))
    except (TypeError, ValueError) as e:
        return jsonify({"status": "error", "reason": f"Data conversion fault: {e}"}), 400

    # Calculate optimal share position size up to the $40,000 threshold
    shares = int(MAX_STRATEGY_CAPITAL // entry_price)
    if shares <= 0:
        return jsonify({"status": "error", "reason": "Asset premium exceeds total limit allocation"}), 400

    # Map bracket stop and limit take-profit distances
    stop_distance   = atr * 1.0
    profit_distance = atr * 1.5

    if side == "buy" or side == "Long":
        stop_loss_price   = round(entry_price - stop_distance, 2)
        take_profit_price = round(entry_price + profit_distance, 2)
        order_side        = "buy"
    else:  # Setups built for short positions
        stop_loss_price   = round(entry_price + stop_distance, 2)
        take_profit_price = round(entry_price - profit_distance, 2)
        order_side        = "sell"

    # Submit server-side bracket order directly to Alpaca infrastructure
    try:
        order = api.submit_order(
            symbol=ticker,
            qty=shares,
            side=order_side,
            type='market',
            time_in_force='day',
            order_class='bracket',
            take_profit={'limit_price': take_profit_price},
            stop_loss={'stop_price': stop_loss_price}
        )
        
        GLOBAL_STATE["trades_executed_today"] += 1
        
        # Build layout summary string
        alert_body = f"Trade Number {GLOBAL_STATE['trades_executed_today']} Active!\n" \
                     f"Action: {order_side.upper()} {shares} shares of {ticker}\n" \
                     f"Entry: ${entry_price:.2f}\n" \
                     f"Target Profit: ${takeProfit_price:.2f}\n" \
                     f"Hard Stop Loss: ${stop_loss_price:.2f}"
                     
        send_notification(f"ORDER FILLED: {ticker} {order_side.upper()}", alert_body)
        return jsonify({"status": "success", "order_id": order.id}), 200

    except Exception as e:
        error_msg = f"Alpaca server rejected order for {ticker}: {str(e)}"
        send_notification("TRADE FAILED", error_msg)
        return jsonify({"status": "failed", "error": str(e)}), 500

if __name__ == '__main__':
    # Start web container listener on local standard dev port 8080
    app.run(port=8080, debug=True)
