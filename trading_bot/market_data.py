"""
Market data utilities — fetch historical bars to compute dynamic parameters.
Called once at bot startup before strategies are initialized.
"""

import logging
import warnings
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def get_opening_bar_stats(
    symbol: str,
    api_key: str,
    secret_key: str,
    lookback_days: int = 14,
) -> tuple[Optional[float], Optional[float]]:
    """
    Fetch the last N trading days of 5-min bars and return:
      (avg_range, avg_volume) of the 9:30 opening bar.

    avg_range  → ATR baseline for FCB dynamic min-range filter
    avg_volume → baseline for FCB volume spike circuit breaker

    Returns (None, None) if data cannot be fetched — FCB disables itself safely.
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockBarsRequest
            from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        end   = datetime.now(ET).replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=lookback_days * 2)  # buffer for weekends/holidays

        client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=start,
            end=end,
            feed="sip",
            adjustment="raw",
        )
        bars = client.get_stock_bars(req)[symbol]

        opening_bars = [
            (bar.high - bar.low, bar.volume)
            for bar in bars
            if bar.timestamp.astimezone(ET).strftime("%H:%M") == "09:30"
        ]

        if not opening_bars:
            log.warning("[market_data] No 9:30 bars found for %s — FCB will be disabled", symbol)
            return None, None

        recent = opening_bars[-lookback_days:]
        atr        = sum(r for r, v in recent) / len(recent)
        avg_volume = sum(v for r, v in recent) / len(recent)
        log.info("[market_data] %s opening bar — ATR: $%.2f  avg_volume: %.0f  (over %d days)",
                 symbol, atr, avg_volume, len(recent))
        return atr, avg_volume

    except Exception as e:
        log.warning("[market_data] Could not fetch opening bar stats: %s — FCB will be disabled", e)
        return None, None
