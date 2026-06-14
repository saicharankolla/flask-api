"""
Flow Filter — daily directional bias from Unusual Whales options flow.

Architecture: asyncio.Queue decouples the UW WebSocket message intake
from the processing logic. The UW client calls enqueue(raw) — a non-blocking
O(1) put — so the Alpaca bar handler never competes for the event loop.
A dedicated process_queue_loop() background task drains the queue separately.

Bias rules:
  - Dominant direction (more alerts) wins; ties → NEUTRAL
  - Bias expires after expiry_hours with no confirming alert
  - Resets to NEUTRAL at start of each new trading day
  - Resets to NEUTRAL on WebSocket disconnect (safety default)
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

QUEUE_MAX = 50_000   # drop oldest if UW floods faster than we process (never in practice)


class Bias(Enum):
    NEUTRAL = "neutral"
    BULLISH = "bullish"
    BEARISH = "bearish"


class FlowFilter:
    def __init__(
        self,
        symbol: str = "COIN",
        min_premium: float = 200_000,
        max_dte: int = 30,
        expiry_hours: float = 2.0,
    ):
        self._symbol      = symbol.upper()
        self._min_premium = min_premium
        self._max_dte     = max_dte
        self._expiry      = timedelta(hours=expiry_hours)

        self._bias: Bias              = Bias.NEUTRAL
        self._bias_expires_at         = datetime.min.replace(tzinfo=ET)
        self._bull_count              = 0
        self._bear_count              = 0
        self._date: Optional[str]     = None
        self._lock                    = asyncio.Lock()
        self._queue: asyncio.Queue    = asyncio.Queue(maxsize=QUEUE_MAX)

    # ── Hot path: non-blocking enqueue (called from UW WebSocket handler) ─

    def enqueue(self, raw: str):
        """
        Drop raw JSON text onto the queue without awaiting.
        Called from inside the UW WebSocket async-for loop — must be O(1)
        so Alpaca bars are never delayed by flow message processing.
        """
        try:
            self._queue.put_nowait(raw)
        except asyncio.QueueFull:
            log.warning("[FlowFilter] Queue full (%d) — dropping oldest message", QUEUE_MAX)

    # ── Background worker (runs as separate asyncio task) ─────────────────

    async def process_queue_loop(self):
        """
        Drains the queue and updates bias. Runs as a standalone asyncio task
        so it never blocks the Alpaca bar processing path.
        """
        while True:
            try:
                raw = await self._queue.get()
                await self._process(raw)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("[FlowFilter] Queue processing error: %s", e)

    async def _process(self, raw: str):
        """Parse one flow-alerts record and update bias if it qualifies."""
        try:
            msg = json.loads(raw)
        except Exception:
            return

        # ── Ticker check ─────────────────────────────────────────────────
        ticker = (msg.get("ticker") or msg.get("underlying_symbol") or msg.get("symbol") or "").upper()
        if ticker != self._symbol:
            return

        # ── Premium (flow-alerts: total_premium in dollars; flow-recent: size×price×100) ──
        premium = float(msg.get("total_premium") or 0)
        if not premium:
            size  = float(msg.get("size") or msg.get("total_size") or msg.get("volume") or 0)
            price = float(msg.get("price") or 0)
            premium = size * price * 100
        if premium < self._min_premium:
            return

        # ── DTE: use field if present, otherwise compute from expiry date ──
        dte_raw = msg.get("dte") or msg.get("days_to_expiry")
        if dte_raw is not None:
            dte = int(dte_raw)
        else:
            expiry_str = msg.get("expiry") or ""
            if expiry_str:
                try:
                    expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=ET)
                    dte = (expiry_dt.date() - datetime.now(ET).date()).days
                except Exception:
                    dte = 999
            else:
                dte = 999
        if dte > self._max_dte:
            return

        # ── Call/Put: flow-alerts uses "type": "call"/"put" ──────────────
        is_call = msg.get("is_call")
        if is_call is None:
            opt = (
                msg.get("type")          # flow-alerts
                or msg.get("option_type") # flow-recent
                or msg.get("put_call")
                or ""
            ).upper()
            if opt in ("CALL", "C"):
                is_call = True
            elif opt in ("PUT", "P"):
                is_call = False
            else:
                log.debug("[FlowFilter] Unknown option type in msg — set LOG_UW_RAW=1 to inspect")
                return

        bias = Bias.BULLISH if is_call else Bias.BEARISH
        await self._update_bias(bias, premium)

    # ── Bias writes (async, locked) ───────────────────────────────────────

    async def _update_bias(self, bias: Bias, premium: float):
        async with self._lock:
            today = datetime.now(ET).date().isoformat()
            if self._date != today:
                self._reset_day(today)

            if bias == Bias.BULLISH:
                self._bull_count += 1
            else:
                self._bear_count += 1

            new_bias = (
                Bias.BULLISH if self._bull_count > self._bear_count else
                Bias.BEARISH if self._bear_count > self._bull_count else
                Bias.NEUTRAL
            )
            if new_bias != self._bias:
                log.info("[FlowFilter] Bias: %s → %s  (bull=%d bear=%d premium=$%.0f)",
                         self._bias.value, new_bias.value,
                         self._bull_count, self._bear_count, premium)
            self._bias            = new_bias
            self._bias_expires_at = datetime.now(ET) + self._expiry

    async def reset_to_neutral(self):
        """Safety reset on WebSocket disconnect — prevents stale bias gating trades."""
        async with self._lock:
            if self._bias != Bias.NEUTRAL:
                log.warning("[FlowFilter] WebSocket lost — resetting bias to NEUTRAL")
            self._bias            = Bias.NEUTRAL
            self._bias_expires_at = datetime.min.replace(tzinfo=ET)

    # ── Bias reads (sync — safe in asyncio single-threaded model) ─────────

    def allows(self, side: str) -> bool:
        """True if trade direction is permitted by current bias."""
        self._check_expiry()
        if self._bias == Bias.NEUTRAL:
            return True
        if self._bias == Bias.BULLISH:
            return side == "buy"
        return side == "sell"   # BEARISH

    @property
    def bias(self) -> str:
        self._check_expiry()
        return self._bias.value

    # ── Internal ──────────────────────────────────────────────────────────

    def _reset_day(self, date_str: str):
        self._bull_count          = 0
        self._bear_count          = 0
        self._bias                = Bias.NEUTRAL
        self._bias_expires_at     = datetime.min.replace(tzinfo=ET)
        self._date                = date_str
        log.info("[FlowFilter] New day %s — bias reset to NEUTRAL", date_str)

    def _check_expiry(self):
        now = datetime.now(ET)
        if self._bias_expires_at > datetime.min.replace(tzinfo=ET) and now > self._bias_expires_at:
            log.info("[FlowFilter] Bias expired — resetting to NEUTRAL")
            self._bias            = Bias.NEUTRAL
            self._bias_expires_at = datetime.min.replace(tzinfo=ET)
