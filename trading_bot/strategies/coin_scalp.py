"""
COIN VWAP Mean Reversion Scalping Strategy
-------------------------------------------
Trades 1-minute bars throughout the day (9:45 AM - 3:30 PM ET).

Logic:
  - Track running VWAP + volume-weighted standard deviation (σ) from 9:30 AM
  - Entry threshold = max(min_deviation, n_sigma × σ) — expands on volatile days
    so the bot doesn't catch knives on strong trending days
  - When price drops >= threshold below VWAP AND bar closes green → LONG
  - When price rises >= threshold above VWAP AND bar closes red  → SHORT
  - Target: recover 75% of the deviation back toward VWAP
  - Stop: fixed $0.30 beyond entry
  - Cooldown: 2 min between trades, max 40 trades/day
"""

import logging
from dataclasses import dataclass, field
from datetime import time, datetime, timedelta
from enum import Enum, auto
from typing import Optional
from zoneinfo import ZoneInfo

from ..config import SYMBOL_CONFIG

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class State(Enum):
    WAITING  = auto()
    IN_TRADE = auto()
    DONE     = auto()   # max trades hit or past end_time


@dataclass
class ScalpSignal:
    symbol: str
    side: str           # "buy" or "sell"
    entry: float
    target: float
    stop: float
    shares: int
    vwap: float
    deviation: float    # distance from VWAP at entry


@dataclass
class ScalpStrategy:
    symbol: str = "COIN"
    account_equity: float = 10000.0

    # config (loaded in __post_init__)
    n_sigma: float      = field(init=False)
    min_deviation: float = field(init=False)
    target_pct: float   = field(init=False)
    stop_amount: float  = field(init=False)
    cooldown_min: int   = field(init=False)
    max_trades: int     = field(init=False)
    risk_pct: float     = field(init=False)
    start_time: time    = field(init=False)
    end_time: time      = field(init=False)

    # running VWAP + variance accumulators
    _cum_vol: float    = field(init=False, default=0.0)
    _cum_pv: float     = field(init=False, default=0.0)   # Σ(price × volume)
    _cum_pv2: float    = field(init=False, default=0.0)   # Σ(price² × volume) for σ

    # state
    state: State                     = field(init=False, default=State.WAITING)
    trade_count: int                 = field(init=False, default=0)
    last_trade_time: Optional[datetime] = field(init=False, default=None)
    trade_date: Optional[str]        = field(init=False, default=None)

    def __post_init__(self):
        cfg = SYMBOL_CONFIG[self.symbol]["scalp"]
        self.n_sigma       = cfg["n_sigma"]
        self.min_deviation = cfg["min_deviation"]
        self.target_pct    = cfg["target_pct"]
        self.stop_amount   = cfg["stop"]
        self.cooldown_min  = cfg["cooldown_min"]
        self.max_trades    = cfg["max_trades"]
        self.risk_pct      = cfg["risk_pct"]
        h, m = cfg["start_time"].split(":")
        self.start_time = time(int(h), int(m))
        h, m = cfg["end_time"].split(":")
        self.end_time = time(int(h), int(m))

    def reset(self):
        self._cum_vol  = 0.0
        self._cum_pv   = 0.0
        self._cum_pv2  = 0.0
        self.state       = State.WAITING
        self.trade_count = 0
        self.last_trade_time = None
        log.info("[%s SCALP] Reset for new day", self.symbol)

    def mark_done(self):
        self.state = State.WAITING   # scalper resets to WAITING after each trade
        log.debug("[%s SCALP] Trade resolved, back to WAITING", self.symbol)

    def on_bar(self, bar) -> Optional[ScalpSignal]:
        bar_dt   = bar.timestamp.astimezone(ET)
        bar_date = bar_dt.date().isoformat()
        bar_t    = bar_dt.time()

        # New day reset
        if self.trade_date and self.trade_date != bar_date:
            self.reset()
        self.trade_date = bar_date

        # Update running VWAP + variance accumulators (all RTH bars from 9:30)
        typical_price  = (bar.high + bar.low + bar.close) / 3.0
        vol            = bar.volume or 1
        self._cum_pv  += typical_price * vol
        self._cum_pv2 += (typical_price ** 2) * vol
        self._cum_vol += vol
        vwap = self._cum_pv / self._cum_vol

        # Volume-weighted standard deviation of price from VWAP
        vwap_sq  = self._cum_pv2 / self._cum_vol
        sigma    = max(0.0, vwap_sq - vwap ** 2) ** 0.5

        # Dynamic threshold: expands on volatile/trending days to avoid knife-catching
        threshold = max(self.min_deviation, self.n_sigma * sigma)

        # State guard
        if self.state in (State.IN_TRADE, State.DONE):
            return None

        # Time filter
        if bar_t < self.start_time or bar_t >= self.end_time:
            return None

        # Max trades circuit breaker
        if self.trade_count >= self.max_trades:
            self.state = State.DONE
            log.info("[%s SCALP] Max trades (%d) hit for today", self.symbol, self.max_trades)
            return None

        # Cooldown check
        if self.last_trade_time is not None:
            elapsed = (bar_dt - self.last_trade_time).total_seconds() / 60.0
            if elapsed < self.cooldown_min:
                return None

        # Deviation from VWAP
        deviation = bar.close - vwap   # positive = above VWAP, negative = below

        # ── Long: price dipped below VWAP AND bar is green (bounce confirmation) ──
        if deviation <= -threshold and bar.close > bar.open:
            entry  = bar.close
            target = entry + abs(deviation) * self.target_pct
            stop   = entry - self.stop_amount
            shares = self._size_shares(entry, stop)
            if shares < 1:
                return None
            signal = ScalpSignal(
                symbol=self.symbol, side="buy",
                entry=round(entry, 2), target=round(target, 2), stop=round(stop, 2),
                shares=shares, vwap=round(vwap, 2), deviation=round(deviation, 2),
            )
            self.state = State.IN_TRADE
            self.trade_count += 1
            self.last_trade_time = bar_dt
            log.info("[%s SCALP] LONG  @%s  entry=%.2f  target=%.2f  stop=%.2f  "
                     "vwap=%.2f  dev=%.2f  σ=%.2f  threshold=%.2f  shares=%d  trade#%d",
                     self.symbol, bar_t, entry, target, stop, vwap, deviation,
                     sigma, threshold, shares, self.trade_count)
            return signal

        # ── Short: price rose above VWAP AND bar is red (rejection confirmation) ──
        if deviation >= threshold and bar.close < bar.open:
            entry  = bar.close
            target = entry - abs(deviation) * self.target_pct
            stop   = entry + self.stop_amount
            shares = self._size_shares(entry, stop)
            if shares < 1:
                return None
            signal = ScalpSignal(
                symbol=self.symbol, side="sell",
                entry=round(entry, 2), target=round(target, 2), stop=round(stop, 2),
                shares=shares, vwap=round(vwap, 2), deviation=round(deviation, 2),
            )
            self.state = State.IN_TRADE
            self.trade_count += 1
            self.last_trade_time = bar_dt
            log.info("[%s SCALP] SHORT @%s  entry=%.2f  target=%.2f  stop=%.2f  "
                     "vwap=%.2f  dev=%.2f  σ=%.2f  threshold=%.2f  shares=%d  trade#%d",
                     self.symbol, bar_t, entry, target, stop, vwap, deviation,
                     sigma, threshold, shares, self.trade_count)
            return signal

        return None

    def _size_shares(self, entry: float, stop: float) -> int:
        risk_per_share = abs(entry - stop)
        if risk_per_share <= 0:
            return 0
        return max(1, int(self.account_equity * self.risk_pct / risk_per_share))
