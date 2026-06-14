"""
COIN Opening Range Breakout Strategy
-------------------------------------
Tuned specifically for COIN based on 6 months of 5-min backtested data:
  - 117 trades | 47% win rate
  - Simple bracket exit: target (1x OR range) or stop (0.5x OR range)
  - EOD 3:50 PM: force-close any open position

State machine:
  BUILDING_OR  → 9:30-9:59 AM: accumulate bars, track OR high/low
  WATCHING     → 10:00 AM+: wait for first bar that breaks OR
  IN_TRADE     → after breakout: bracket order placed
  DONE         → trade fully resolved, reset next day
"""

import logging
from dataclasses import dataclass, field
from datetime import time
from enum import Enum, auto
from typing import Optional
from zoneinfo import ZoneInfo

from ..config import SYMBOL_CONFIG

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class State(Enum):
    BUILDING_OR = auto()
    WATCHING    = auto()
    IN_TRADE    = auto()
    DONE        = auto()


@dataclass
class TradeSignal:
    symbol: str
    side: str       # "buy" or "sell"
    entry: float
    target: float   # 1x OR range from entry
    stop: float     # 0.5x OR range from entry
    shares: int


@dataclass
class ORBStrategy:
    symbol: str = "COIN"
    account_equity: float = 10000.0

    # loaded from config
    or_period_minutes: int = field(init=False)
    min_or_range: float    = field(init=False)
    target_mult: float     = field(init=False)
    stop_mult: float       = field(init=False)
    risk_pct: float        = field(init=False)

    # state
    state: State         = field(init=False, default=State.BUILDING_OR)
    or_high: float       = field(init=False, default=0.0)
    or_low: float        = field(init=False, default=float("inf"))
    or_bars: list        = field(init=False, default_factory=list)
    trade_date: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        cfg = SYMBOL_CONFIG[self.symbol]["orb"]
        self.or_period_minutes = cfg["or_period_minutes"]
        self.min_or_range      = cfg["min_or_range"]
        self.target_mult       = cfg["target_mult"]
        self.stop_mult         = cfg["stop_mult"]
        from ..config import RISK_PCT
        self.risk_pct = RISK_PCT

    def reset(self):
        self.state   = State.BUILDING_OR
        self.or_high = 0.0
        self.or_low  = float("inf")
        self.or_bars = []
        log.info("[%s ORB] Reset for new day", self.symbol)

    def on_bar(self, bar) -> Optional[TradeSignal]:
        """
        Call with each incoming 5-min bar.
        Returns a TradeSignal on breakout detection, None otherwise.
        """
        bar_time = bar.timestamp.astimezone(ET)
        bar_date = bar_time.date().isoformat()

        if self.trade_date and self.trade_date != bar_date:
            self.reset()
        self.trade_date = bar_date

        if self.state == State.DONE:
            return None

        bar_clock = bar_time.time()
        total_mins = 9 * 60 + 30 + self.or_period_minutes
        or_end = time(total_mins // 60, total_mins % 60)

        # ── Phase 1: Build Opening Range ──────────────────────────────────────
        if self.state == State.BUILDING_OR:
            if bar_clock < or_end:
                self.or_bars.append(bar)
                self.or_high = max(self.or_high, bar.high)
                self.or_low  = min(self.or_low,  bar.low)
                log.debug("[%s ORB] OR bar %s H=%.2f L=%.2f  range=[%.2f-%.2f]",
                          self.symbol, bar_clock, bar.high, bar.low, self.or_low, self.or_high)
                return None
            else:
                or_range = self.or_high - self.or_low
                if or_range < self.min_or_range:
                    log.info("[%s ORB] OR range $%.2f < min $%.2f, skip today",
                             self.symbol, or_range, self.min_or_range)
                    self.state = State.DONE
                    return None
                self.state = State.WATCHING
                log.info("[%s ORB] OR locked: High=%.2f  Low=%.2f  Range=$%.2f",
                         self.symbol, self.or_high, self.or_low, or_range)

        # ── Phase 2: Watch for Breakout ───────────────────────────────────────
        if self.state == State.WATCHING:
            or_range = self.or_high - self.or_low

            if bar.high > self.or_high:
                signal = self._build_signal("buy", self.or_high, or_range)
                log.info("[%s ORB] LONG breakout @%s  Entry=%.2f  Target=%.2f  Stop=%.2f  Shares=%d",
                         self.symbol, bar_clock, signal.entry, signal.target, signal.stop, signal.shares)
                self.state = State.IN_TRADE
                return signal

            elif bar.low < self.or_low:
                signal = self._build_signal("sell", self.or_low, or_range)
                log.info("[%s ORB] SHORT breakout @%s  Entry=%.2f  Target=%.2f  Stop=%.2f  Shares=%d",
                         self.symbol, bar_clock, signal.entry, signal.target, signal.stop, signal.shares)
                self.state = State.IN_TRADE
                return signal

        return None

    def mark_done(self):
        self.state = State.DONE
        log.info("[%s ORB] Trade resolved, state=DONE", self.symbol)

    def _build_signal(self, side: str, entry: float, or_range: float) -> TradeSignal:
        if side == "buy":
            target = entry + or_range * self.target_mult
            stop   = entry - or_range * self.stop_mult
        else:
            target = entry - or_range * self.target_mult
            stop   = entry + or_range * self.stop_mult

        risk_per_share = abs(entry - stop)
        shares = max(1, int(self.account_equity * self.risk_pct / risk_per_share))

        return TradeSignal(
            symbol=self.symbol,
            side=side,
            entry=round(entry, 2),
            target=round(target, 2),
            stop=round(stop, 2),
            shares=shares,
        )
