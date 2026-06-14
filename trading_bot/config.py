import os

# ── Alpaca credentials ────────────────────────────────────────────────────────
# Set these in your environment or a .env file:
#   export ALPACA_API_KEY="your_paper_key"
#   export ALPACA_SECRET_KEY="your_paper_secret"
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

# Paper trading endpoints
ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_URL = "https://data.alpaca.markets"

# ── Global risk settings ──────────────────────────────────────────────────────
RISK_PCT = 0.02          # Risk 2% of account equity per trade
EOD_EXIT_TIME = "15:50"  # Force close any open position at 3:50 PM ET

# ── Per-symbol strategy config ────────────────────────────────────────────────
# Each symbol runs its own set of strategies, each with independent position sizing.
SYMBOL_CONFIG = {
    "COIN": {
        "feed": "sip",
        "timeframe": "5Min",

        # Strategy 1: Opening Range Breakout (fires ~10:00-10:30 AM)
        # Backtest: 117 trades | 45% WR | +$51.09/share | MaxDD $31.89
        "orb": {
            "or_period_minutes": 30,   # Opening range window: 9:30-10:00 AM ET
            "min_or_range": 0.50,      # Skip days with OR range < $0.50
            "target_mult": 1.0,        # Target = entry ± (OR range × 1.0)
            "stop_mult": 0.50,         # Stop   = entry ∓ (OR range × 0.5)
        },

        # Strategy 2: First Candle Break SHORT (fires at 9:35 AM only)
        # Backtest: 43 trades | 37.2% WR | +$75.30/share | MaxDD $6.94 | Green all 7 months
        # Conditions: first candle must be RED + 9:35 bar breaks first candle low
        "fcb": {
            "atr_min_mult":    1.5,   # min_fc_range = 1.5 × opening_bar_ATR (dynamic)
            "atr_lookback":    14,    # trading days to compute opening bar ATR
            "max_range_pct":   0.03,  # skip FCB if first candle > 3% of price (capitulation day)
            "volume_spike_mult": 5.0, # skip if first candle volume > 5× avg opening bar volume
            "target_mult":     2.0,   # Target = entry - (FC range × 2.0)
            "stop_mult":       0.30,  # Stop   = entry + (FC range × 0.3)
        },

        # Strategy 3: VWAP Mean Reversion Scalping (1-min bars, runs 9:45-15:30)
        # 5-min proxy backtest: 2285 trades/124 days (~18/day), best params below
        # Re-run backtest_scalp.py after pulling 1-min data for accurate results
        "scalp": {
            "n_sigma":      2.5,    # VWAP SD band multiplier — replaces fixed deviation
            "min_deviation": 0.50,  # Floor: don't enter if σ is tiny (flat/quiet day)
            "target_pct":   0.75,   # Take profit at 75% of deviation (toward VWAP)
            "stop":         0.30,   # Fixed stop loss ($) beyond entry
            "cooldown_min": 2,      # Minutes to wait between trades
            "max_trades":   40,     # Circuit breaker: max trades per day
            "risk_pct":     0.005,  # Risk 0.5% of equity per trade (tight for high freq)
            "start_time":   "09:45",
            "end_time":     "15:30",
            "max_spread":   0.15,   # Skip entry if bid-ask spread > $0.15
        },
    },
}
