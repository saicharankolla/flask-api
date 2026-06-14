"""
Backtest validator: feeds historical CSV bars through both live strategy engines
to confirm signal logic matches original backtest results.

Usage:
    python -m trading_bot.backtest
"""

import csv
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from .strategies.coin_orb import ORBStrategy
from .strategies.coin_fcb import FCBStrategy

logging.basicConfig(level=logging.WARNING)
ET = ZoneInfo("America/New_York")
CSV_PATH = "ALL_5Min_sip_6mo_rth.csv"
SYMBOL   = "COIN"


@dataclass
class FakeBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float


def load_bars(symbol=SYMBOL):
    bars = []
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            if r["symbol"] != symbol:
                continue
            bars.append(FakeBar(
                symbol=r["symbol"],
                timestamp=datetime.fromisoformat(r["timestamp_utc"].replace("Z", "+00:00")),
                open=float(r["open"]),   high=float(r["high"]),
                low=float(r["low"]),     close=float(r["close"]),
                volume=int(r["volume"]), vwap=float(r["vwap"]),
            ))
    return bars


def simulate_exit(bars_after, side, target, stop, entry):
    for fb in bars_after:
        t = fb.timestamp.astimezone(ET).strftime("%H:%M")
        if t >= "15:50":
            return fb.close, "EOD"
        if side == "buy":
            if fb.high >= target: return target, "TARGET"
            if fb.low  <= stop:   return stop,   "STOP"
        else:
            if fb.low  <= target: return target, "TARGET"
            if fb.high >= stop:   return stop,   "STOP"
    return (bars_after[-1].close if bars_after else entry), "EOD"


def run_strategy(bars, strategy, label):
    trades = []
    for i, bar in enumerate(bars):
        signal = strategy.on_bar(bar)
        if not signal:
            continue

        entry_date = bar.timestamp.astimezone(ET).date().isoformat()
        exit_price, outcome = simulate_exit(bars[i+1:], signal.side, signal.target, signal.stop, signal.entry)

        pnl = (exit_price - signal.entry) if signal.side == "buy" else (signal.entry - exit_price)
        trades.append({
            "date": entry_date,
            "side": signal.side,
            "entry": signal.entry,
            "target": signal.target,
            "stop": signal.stop,
            "exit": round(exit_price, 2),
            "outcome": outcome,
            "pnl": round(pnl, 2),
        })
        strategy.mark_done()

    return trades


def print_report(label, trades):
    if not trades:
        print(f"\n{label}: NO TRADES"); return

    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    n      = len(trades)
    total  = sum(t["pnl"] for t in trades)

    running = peak = maxdd = 0.0
    for t in trades:
        running += t["pnl"]
        if running > peak: peak = running
        if peak - running > maxdd: maxdd = peak - running

    monthly = defaultdict(float)
    for t in trades: monthly[t["date"][:7]] += t["pnl"]

    print(f"\n{'═'*58}")
    print(f"  {label}")
    print(f"{'═'*58}")
    print(f"  Trades    : {n}  |  {len(wins)}W – {len(losses)}L  ({len(wins)/n*100:.1f}% WR)")
    print(f"  Avg win   : ${sum(t['pnl'] for t in wins)/max(len(wins),1):+.2f}  |  Avg loss: ${sum(t['pnl'] for t in losses)/max(len(losses),1):+.2f}")
    print(f"  Total P&L : ${total:+.2f}/share  |  Avg/trade: ${total/n:+.2f}  |  MaxDD: ${maxdd:.2f}")

    print(f"\n  Monthly:")
    for m in sorted(monthly):
        sign = "🟢" if monthly[m] >= 0 else "🔴"
        bar  = "█" * min(int(abs(monthly[m]) / 1.5), 22)
        print(f"    {m}: {sign} ${monthly[m]:>+7.2f}  {bar}")

    print(f"\n  Last 5 trades:")
    for t in trades[-5:]:
        m = "🟢" if t["pnl"] > 0 else "🔴"
        print(f"    {t['date']}  {t['side'].upper():4}  "
              f"in={t['entry']:.2f} out={t['exit']:.2f}  {t['outcome']:<7}  ${t['pnl']:>+.2f}  {m}")


def run():
    bars = load_bars()

    orb = ORBStrategy(symbol=SYMBOL, account_equity=10000.0)
    fcb = FCBStrategy(symbol=SYMBOL, account_equity=10000.0)

    orb_trades = run_strategy(bars, orb, "ORB")
    fcb_trades = run_strategy(bars, fcb, "FCB SHORT")

    print_report("ORB 30-min", orb_trades)
    print_report("FCB SHORT (9:35 red candle)", fcb_trades)

    # Combined summary
    all_trades = orb_trades + fcb_trades
    all_trades.sort(key=lambda t: t["date"])
    total_pnl = sum(t["pnl"] for t in all_trades)
    wins_all  = sum(1 for t in all_trades if t["pnl"] > 0)
    n_all     = len(all_trades)

    monthly_combined = defaultdict(float)
    for t in all_trades: monthly_combined[t["date"][:7]] += t["pnl"]

    print(f"\n{'═'*58}")
    print(f"  COMBINED (ORB + FCB SHORT)")
    print(f"{'═'*58}")
    print(f"  Total trades : {n_all}  ({len(orb_trades)} ORB + {len(fcb_trades)} FCB)")
    print(f"  Win rate     : {wins_all/n_all*100:.1f}%")
    print(f"  Total P&L    : ${total_pnl:+.2f}/share")
    print(f"  Avg/trade    : ${total_pnl/n_all:+.2f}/share")
    print(f"\n  Monthly combined:")
    for m in sorted(monthly_combined):
        sign = "🟢" if monthly_combined[m] >= 0 else "🔴"
        bar  = "█" * min(int(abs(monthly_combined[m]) / 2), 22)
        print(f"    {m}: {sign} ${monthly_combined[m]:>+8.2f}  {bar}")


if __name__ == "__main__":
    run()
