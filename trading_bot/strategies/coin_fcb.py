"""
COIN First Candle Break SHORT (FCB SHORT)
------------------------------------------
Tuned specifically for COIN based on 6 months of 5-min backtested data:
  - 43 trades  |  37.2% win rate  |  +$75.30/share cumulative  |  MaxDD $6.94
  - Profitable all 7 months — highest consistency of any COIN strategy tested
  - Avg win: $6.51  |  Avg loss: -$1.07  |  R:R ~6:1

Rules:
  - First candle (9:30) must be RED (close < open) — bearish open
  - Second bar (9:35) must break below the 9:30 candle LOW → SHORT entry
  - Entry: 9:30 candle low
  - Target: entry - (9:30 range × 2.0)
  - Stop:   entry + (9:30 range × 0.3)
  - Only fires once at 9:35 — if 9:35 bar doesn't break the low, no trade today

Why this works on COIN:
  When COIN opens with a bearish first candle and immediately continues lower,
  it tends to follow through aggressively. The wide 2:1 target captures the
  full morning trend while the tight 0.3x stop limits damage on failed setups.

State machine:
  WAITING   → watching for 9:30 candle to complete
  TRIGGERED → 9:35 bar broke first candle low, bracket order placed
  DONE      → resolved (target/stop/EOD) or 9:35 bar did not break the low
"""

import logging
from dataclasses import dataclass, field
from datetime import time
from enum import Enum, auto
from typing import Optional
from zoneinfo import ZoneInfo

from ..config import RISK_PCT, SYMBOL_CONFIG

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class FCBState(Enum):
    WAITING   = auto()   # before 9:35 bar
    TRIGGERED = auto()   # short order placed
    DONE      = auto()   # finished for the day


@dataclass
class FCBSignal:
    symbol: str
    side: str = "sell"
    entry: float = 0.0
    target: float = 0.0
    stop: float = 0.0
    shares: int = 0
    fc_range: float = 0.0
    fc_open: float = 0.0
    fc_close: float = 0.0


@dataclass
class FCBStrategy:
    symbol: str = "COIN"
    account_equity: float = 10000.0
    opening_bar_atr: Optional[float] = None         # injected by market_data.get_opening_bar_stats
    opening_bar_avg_volume: Optional[float] = None  # injected by market_data.get_opening_bar_stats

    # config (loaded in __post_init__)
    _atr_min_mult:     float = field(init=False)
    _max_range_pct:    float = field(init=False)
    _volume_spike_mult: float = field(init=False)
    _target_mult:      float = field(init=False)
    _stop_mult:        float = field(init=False)

    state: FCBState = field(init=False, default=FCBState.WAITING)
    first_candle: Optional[dict] = field(init=False, default=None)
    trade_date: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        cfg = SYMBOL_CONFIG[self.symbol]["fcb"]
        self._atr_min_mult      = cfg["atr_min_mult"]
        self._max_range_pct     = cfg["max_range_pct"]
        self._volume_spike_mult = cfg["volume_spike_mult"]
        self._target_mult       = cfg["target_mult"]
        self._stop_mult         = cfg["stop_mult"]
        if self.opening_bar_atr:
            log.info("[%s FCB] Dynamic ATR filter: min_range=$%.2f (%.1f× ATR $%.2f)",
                     self.symbol, self._atr_min_mult * self.opening_bar_atr,
                     self._atr_min_mult, self.opening_bar_atr)
        else:
            log.info("[%s FCB] No ATR data — FCB disabled for safety (pass opening_bar_atr)", self.symbol)
        if self.opening_bar_avg_volume:
            log.info("[%s FCB] Volume spike guard: skip if opening volume > %.1f× avg (%.0f shares)",
                     self.symbol, self._volume_spike_mult, self.opening_bar_avg_volume)
        else:
            log.info("[%s FCB] No avg volume data — volume spike guard disabled", self.symbol)

    def reset(self):
        self.state        = FCBState.WAITING
        self.first_candle = None
        log.info("[%s FCB] Reset for new day", self.symbol)

    def on_bar(self, bar) -> Optional[FCBSignal]:
        """
        Call with each incoming 5-min bar.
        Returns an FCBSignal only on the 9:35 bar if setup is valid.
        """
        bar_time = bar.timestamp.astimezone(ET)
        bar_date = bar_time.date().isoformat()
        bar_clock = bar_time.strftime("%H:%M")

        # Auto-reset on new trading day
        if self.trade_date and self.trade_date != bar_date:
            self.reset()
        self.trade_date = bar_date

        if self.state == FCBState.DONE:
            return None

        # ── 9:30 bar: record the first candle ────────────────────────────────
        if bar_clock == "09:30":
            self.first_candle = {
                "open":   bar.open,
                "high":   bar.high,
                "low":    bar.low,
                "close":  bar.close,
                "range":  bar.high - bar.low,
                "volume": getattr(bar, "volume", 0),
            }
            log.debug(
                "[%s FCB] First candle: O=%.2f H=%.2f L=%.2f C=%.2f range=%.2f  %s",
                self.symbol, bar.open, bar.high, bar.low, bar.close,
                bar.high - bar.low,
                "RED" if bar.close < bar.open else "GREEN (skip)",
            )
            return None

        # ── 9:35 bar: check for SHORT setup ──────────────────────────────────
        if bar_clock == "09:35" and self.first_candle:
            fc = self.first_candle
            self.state = FCBState.DONE  # only one chance regardless of outcome

            # Filter 1: first candle must be bearish (red)
            if fc["close"] >= fc["open"]:
                log.info("[%s FCB] Skip — first candle is GREEN (%.2f → %.2f)", self.symbol, fc["open"], fc["close"])
                return None

            # Filter 2: dynamic ATR range floor — require meaningful first candle
            if self.opening_bar_atr is None:
                log.info("[%s FCB] Skip — no ATR data available", self.symbol)
                return None
            min_fc_range = self._atr_min_mult * self.opening_bar_atr
            if fc["range"] < min_fc_range:
                log.info("[%s FCB] Skip — first candle range $%.2f < ATR floor $%.2f",
                         self.symbol, fc["range"], min_fc_range)
                return None

            # Filter 3: capitulation guard — skip if first candle > 3% of price (runaway move)
            max_fc_range = self._max_range_pct * fc["close"]
            if fc["range"] > max_fc_range:
                log.info("[%s FCB] Skip — first candle range $%.2f exceeds %.0f%% cap $%.2f (capitulation day)",
                         self.symbol, fc["range"], self._max_range_pct * 100, max_fc_range)
                return None

            # Filter 4: volume spike guard — skip if first candle volume signals liquidity crisis
            if self.opening_bar_avg_volume and fc["volume"] > self._volume_spike_mult * self.opening_bar_avg_volume:
                log.info("[%s FCB] Skip — volume spike: %d shares = %.1f× avg %.0f (flash crash risk)",
                         self.symbol, fc["volume"],
                         fc["volume"] / self.opening_bar_avg_volume, self.opening_bar_avg_volume)
                return None

            # Filter 5: 9:35 bar must actually break below the first candle low
            if bar.low >= fc["low"]:
                log.info(
                    "[%s FCB] No break — 9:35 bar low %.2f did not breach FC low %.2f",
                    self.symbol, bar.low, fc["low"],
                )
                return None

            entry  = fc["low"]
            target = round(entry - fc["range"] * self._target_mult, 2)
            stop   = round(entry + fc["range"] * self._stop_mult,   2)

            risk_per_share = stop - entry
            shares = max(1, int(self.account_equity * RISK_PCT / risk_per_share))

            signal = FCBSignal(
                symbol=self.symbol,
                side="sell",
                entry=round(entry, 2),
                target=target,
                stop=stop,
                shares=shares,
                fc_range=round(fc["range"], 2),
                fc_open=fc["open"],
                fc_close=fc["close"],
            )
            self.state = FCBState.TRIGGERED
            log.info(
                "[%s FCB] SHORT signal at 09:35 | Entry=%.2f  Target=%.2f  Stop=%.2f  Shares=%d  FC_range=%.2f",
                self.symbol, signal.entry, signal.target, signal.stop, signal.shares, signal.fc_range,
            )
            return signal

        # After 9:35 without a first candle recorded — skip day
        if bar_clock > "09:35" and self.first_candle is None:
            self.state = FCBState.DONE

        return None

    def mark_done(self):
        self.state = FCBState.DONE
        log.info("[%s FCB] Trade resolved, state=DONE", self.symbol)
