"""
VWAP Mean Reversion Scalping Backtest

Works with either 1-min or 5-min CSV data.
Simulates entry at bar close, exit when target or stop is reached on a subsequent bar.

Usage:
    # 5-min prototype (rough):
    python scripts/backtest_scalp.py

    # 1-min precise (after pulling data):
    python scripts/backtest_scalp.py --csv COIN_1Min_sip.csv
"""

import csv
import sys
import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

# Add parent dir so we can import trading_bot
sys.path.insert(0, ".")
from trading_bot.strategies.coin_scalp import ScalpStrategy

ET = ZoneInfo("America/New_York")
SYMBOL = "COIN"
SLIPPAGE = 0.02   # $0.02/share assumed slippage per entry+exit


@dataclass
class FakeBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


def load_bars(csv_path: str, symbol: str = SYMBOL):
    bars = []
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            if r["symbol"] != symbol:
                continue
            bars.append(FakeBar(
                symbol=r["symbol"],
                timestamp=datetime.fromisoformat(r["timestamp_utc"].replace("Z", "+00:00")),
                open=float(r["open"]),   high=float(r["high"]),
                low=float(r["low"]),     close=float(r["close"]),
                volume=int(r["volume"]),
            ))
    bars.sort(key=lambda b: b.timestamp)
    return bars


def simulate_exit(bars_after, side, target, stop):
    """Check subsequent bars for target/stop hit. Returns (exit_price, outcome)."""
    for bar in bars_after:
        bar_t = bar.timestamp.astimezone(ET).time()
        # EOD exit
        if bar_t >= time(15, 30):
            return bar.close, "EOD"
        if side == "buy":
            if bar.high >= target: return target, "TP"
            if bar.low  <= stop:   return stop,   "SL"
        else:
            if bar.low  <= target: return target, "TP"
            if bar.high >= stop:   return stop,   "SL"
    return (bars_after[-1].close if bars_after else target), "EOD"


def run(csv_path: str):
    bars = load_bars(csv_path)
    print(f"\nLoaded {len(bars)} bars from {csv_path}")

    strategy = ScalpStrategy(symbol=SYMBOL, account_equity=25000.0)

    trades = []
    daily_counts = defaultdict(int)

    for i, bar in enumerate(bars):
        signal = strategy.on_bar(bar)
        if signal is None:
            continue

        date_str = bar.timestamp.astimezone(ET).date().isoformat()
        daily_counts[date_str] += 1

        exit_price, outcome = simulate_exit(bars[i+1:], signal.side, signal.target, signal.stop)

        raw_pnl = (exit_price - signal.entry) if signal.side == "buy" else (signal.entry - exit_price)
        net_pnl = raw_pnl - SLIPPAGE

        trades.append({
            "date":    date_str,
            "time":    bar.timestamp.astimezone(ET).strftime("%H:%M"),
            "side":    signal.side,
            "entry":   signal.entry,
            "target":  signal.target,
            "stop":    signal.stop,
            "vwap":    signal.vwap,
            "dev":     signal.deviation,
            "exit":    round(exit_price, 2),
            "outcome": outcome,
            "pnl":     round(net_pnl, 2),
        })

        strategy.mark_done()

    # ── Report ────────────────────────────────────────────────────────────────
    if not trades:
        print("No trades generated.")
        return

    wins    = [t for t in trades if t["pnl"] > 0]
    losses  = [t for t in trades if t["pnl"] <= 0]
    n       = len(trades)
    total   = sum(t["pnl"] for t in trades)
    days    = len(daily_counts)
    avg_day = n / days if days else 0

    running = peak = maxdd = 0.0
    for t in trades:
        running += t["pnl"]
        peak = max(peak, running)
        maxdd = max(maxdd, peak - running)

    monthly = defaultdict(lambda: {"pnl": 0.0, "n": 0, "w": 0})
    for t in trades:
        m = t["date"][:7]
        monthly[m]["pnl"] += t["pnl"]
        monthly[m]["n"]   += 1
        monthly[m]["w"]   += 1 if t["pnl"] > 0 else 0

    print(f"\n{'═'*62}")
    print(f"  COIN VWAP Scalp Backtest  ({csv_path})")
    print(f"{'═'*62}")
    print(f"  Trades    : {n}  over {days} days  ({avg_day:.1f}/day avg)")
    print(f"  Win rate  : {len(wins)}/{n} = {len(wins)/n*100:.1f}%")
    print(f"  Avg win   : ${sum(t['pnl'] for t in wins)/max(len(wins),1):+.2f}  "
          f"| Avg loss: ${sum(t['pnl'] for t in losses)/max(len(losses),1):+.2f}")
    print(f"  Total P&L : ${total:+.2f}/share  "
          f"| Avg/trade: ${total/n:+.2f}  | MaxDD: ${maxdd:.2f}")
    print(f"  (includes ${SLIPPAGE:.2f}/share slippage per trade)")

    print(f"\n  Monthly breakdown:")
    print(f"  {'Month':<10} {'Trades':>7} {'WR':>6} {'P&L':>10}  Bar")
    print(f"  {'─'*10} {'─'*7} {'─'*6} {'─'*10}")
    for m in sorted(monthly):
        md = monthly[m]
        wr = md["w"] / md["n"] * 100 if md["n"] else 0
        sign = "🟢" if md["pnl"] >= 0 else "🔴"
        bar  = "█" * min(int(abs(md["pnl"]) / 2), 20)
        print(f"  {m:<10} {md['n']:>7} {wr:>5.0f}%  ${md['pnl']:>+8.2f}  {sign} {bar}")

    # Outcome breakdown
    tp_n  = sum(1 for t in trades if t["outcome"] == "TP")
    sl_n  = sum(1 for t in trades if t["outcome"] == "SL")
    eod_n = sum(1 for t in trades if t["outcome"] == "EOD")
    print(f"\n  Exit breakdown: TP={tp_n} ({tp_n/n*100:.0f}%)  "
          f"SL={sl_n} ({sl_n/n*100:.0f}%)  EOD={eod_n} ({eod_n/n*100:.0f}%)")

    # Daily trade count distribution
    count_dist = defaultdict(int)
    for c in daily_counts.values(): count_dist[c] += 1
    print(f"\n  Trades/day distribution:")
    for c in sorted(count_dist):
        print(f"    {c:>3} trades: {count_dist[c]} days")

    print(f"\n  Last 10 trades:")
    for t in trades[-10:]:
        mk = "🟢" if t["pnl"] > 0 else "🔴"
        print(f"    {t['date']} {t['time']}  {t['side'].upper():<5}  "
              f"dev={t['dev']:+.2f}  in={t['entry']:.2f}  out={t['exit']:.2f}  "
              f"{t['outcome']:<4}  ${t['pnl']:>+.2f}  {mk}")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="ALL_5Min_sip_6mo_rth.csv",
                        help="CSV file with bars (default: 5-min CSV for prototype)")
    args = parser.parse_args()
    run(args.csv)
