"""
Pull 1-minute COIN bars from Alpaca and save to CSV.

Usage:
    export ALPACA_API_KEY="..."
    export ALPACA_SECRET_KEY="..."
    python scripts/pull_1min_data.py
"""

import csv
import os
import warnings
warnings.filterwarnings("ignore")

# pyenv macOS SSL fix: patch HTTPAdapter so all requests skip cert verification
import requests
from requests.adapters import HTTPAdapter
_orig_send = HTTPAdapter.send
def _no_verify_send(self, request, **kwargs):
    kwargs["verify"] = False
    return _orig_send(self, request, **kwargs)
HTTPAdapter.send = _no_verify_send

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

ET = ZoneInfo("America/New_York")

API_KEY    = os.environ.get("ALPACA_API_KEY", "")
SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
SYMBOL     = "COIN"
OUTPUT     = "COIN_1Min_sip.csv"

# Pull last 3 months of trading days
END   = date.today()
START = END - timedelta(days=95)   # ~3 months including weekends/holidays


def main():
    if not API_KEY or not SECRET_KEY:
        raise RuntimeError("Set ALPACA_API_KEY and ALPACA_SECRET_KEY env vars first.")

    client = StockHistoricalDataClient(api_key=API_KEY, secret_key=SECRET_KEY)

    print(f"Pulling {SYMBOL} 1-min bars from {START} to {END}...")

    req = StockBarsRequest(
        symbol_or_symbols=SYMBOL,
        timeframe=TimeFrame.Minute,
        start=datetime(START.year, START.month, START.day, tzinfo=ET),
        end=datetime(END.year,   END.month,   END.day,   tzinfo=ET),
        feed="sip",
        adjustment="raw",
    )

    bars = client.get_stock_bars(req)
    bar_list = bars[SYMBOL]

    print(f"  Downloaded {len(bar_list)} bars — writing to {OUTPUT}...")

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "timestamp_utc", "timestamp_et",
                         "trade_date_et", "time_et",
                         "open", "high", "low", "close",
                         "volume", "trade_count", "vwap",
                         "timeframe", "feed"])

        skipped = 0
        for bar in bar_list:
            ts_et  = bar.timestamp.astimezone(ET)
            t_et   = ts_et.time()

            # Keep only regular trading hours (9:30 - 16:00)
            from datetime import time
            if t_et < time(9, 30) or t_et >= time(16, 0):
                skipped += 1
                continue

            writer.writerow([
                SYMBOL,
                bar.timestamp.isoformat(),
                ts_et.isoformat(),
                ts_et.date().isoformat(),
                t_et.strftime("%H:%M"),
                f"{bar.open:.4f}",
                f"{bar.high:.4f}",
                f"{bar.low:.4f}",
                f"{bar.close:.4f}",
                int(bar.volume),
                int(bar.trade_count) if bar.trade_count else 0,
                f"{bar.vwap:.4f}" if bar.vwap else f"{bar.close:.4f}",
                "1Min",
                "sip",
            ])

    kept = len(bar_list) - skipped
    days = kept // 390 if kept >= 390 else 1
    print(f"  Wrote {kept} RTH bars (~{kept // max(days,1)} bars/day, ~{days} trading days)")
    print(f"  Skipped {skipped} non-RTH bars")
    print(f"Done → {OUTPUT}")


if __name__ == "__main__":
    main()
