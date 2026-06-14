"""
COIN Trading Bot — Main Entry Point

Runs three strategies concurrently on COIN:
  1. FCB SHORT  — 9:35 AM (first candle break, dynamic ATR filter)
  2. ORB        — ~10:00 AM (opening range breakout)
  3. VWAP Scalp — 9:45 AM–3:30 PM (mean reversion, VWAP SD bands)

All signals are gated through the Unusual Whales flow filter:
  - Bullish COIN flow → long trades only
  - Bearish COIN flow → short trades only
  - No signal / expired → trade both directions

Alpaca delivers 1-min bars via WebSocket. The bar aggregator synthesizes
5-min bars for ORB and FCB; the scalper receives raw 1-min bars directly.

Usage:
    python -m trading_bot.main

Required environment variables:
    ALPACA_API_KEY
    ALPACA_SECRET_KEY

Optional environment variables:
    UNUSUAL_WHALES_KEY      enable real-time flow filter (recommended)
    TASTYTRADE_USERNAME     Phase 2 options execution
    TASTYTRADE_PASSWORD     Phase 2 options execution
    LOG_UW_RAW=1            log raw Unusual Whales messages for debugging
    DRY_RUN=1               log all signals but skip order placement (safe testing)
"""

import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, time
from typing import List, Optional
from zoneinfo import ZoneInfo

from .alpaca_client import AlpacaClient
from .config import ALPACA_API_KEY, ALPACA_SECRET_KEY, EOD_EXIT_TIME
from .data_feeds.unusual_whales import UnusualWhalesClient
from .market_data import get_opening_bar_stats
from .strategies.coin_fcb import FCBStrategy
from .strategies.coin_orb import ORBStrategy
from .strategies.coin_scalp import ScalpStrategy
from .strategies.flow_filter import FlowFilter
from .tastytrade_client import TastytradeClient
from .trade_executor import TradeExecutor
from .trade_logger import TradeLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

SYMBOL   = "COIN"
DRY_RUN  = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
_EOD_H, _EOD_M = map(int, EOD_EXIT_TIME.split(":"))


# ── Synthetic 5-min bar ───────────────────────────────────────────────────────

@dataclass
class SyntheticBar:
    """5-min bar synthesized from five 1-min bars for ORB and FCB strategies."""
    symbol: str
    timestamp: object
    open: float
    high: float
    low: float
    close: float
    volume: int


# ── Bar aggregator ────────────────────────────────────────────────────────────

class BarAggregator:
    """
    Accumulates 1-min bars into time-aligned synthetic 5-min bars.
    Emits a synthetic bar at the START of each new 5-min period
    (i.e., when the first bar of the next period arrives).
    """

    def __init__(self, on_5min_bar):
        self._callback     = on_5min_bar
        self._bars: list   = []
        self._slot: Optional[int] = None  # current period's minute-of-day slot

    def on_bar(self, bar):
        bar_dt = bar.timestamp.astimezone(ET)
        minute = bar_dt.hour * 60 + bar_dt.minute
        slot   = (minute // 5) * 5         # floor to 5-min boundary

        if self._slot is None:
            self._slot = slot

        if slot != self._slot:
            if self._bars:
                self._emit()
            self._bars = []
            self._slot = slot

        self._bars.append(bar)

    def _emit(self):
        bars = self._bars
        synthetic = SyntheticBar(
            symbol    = bars[0].symbol,
            timestamp = bars[0].timestamp,  # start of the 5-min period
            open      = bars[0].open,
            high      = max(b.high for b in bars),
            low       = min(b.low  for b in bars),
            close     = bars[-1].close,
            volume    = sum(b.volume for b in bars),
        )
        self._callback(synthetic)


# ── EOD monitor ───────────────────────────────────────────────────────────────

async def eod_monitor(executors: List[TradeExecutor], client: AlpacaClient):
    """Poll every 30 seconds; close all positions at EOD_EXIT_TIME."""
    eod = time(_EOD_H, _EOD_M)
    while True:
        await asyncio.sleep(30)
        if datetime.now(ET).time() >= eod:
            log.info("EOD exit triggered at %s — closing all positions", EOD_EXIT_TIME)
            for ex in executors:
                ex.shutdown()
            client.close_position(SYMBOL)
            return


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 65)
    log.info("COIN Bot starting (paper trading)%s", "  *** DRY RUN — no orders ***" if DRY_RUN else "")
    log.info("  Strategy 1: FCB SHORT   — 09:35 (dynamic ATR filter)")
    log.info("  Strategy 2: ORB         — ~10:00 AM")
    log.info("  Strategy 3: VWAP Scalp  — 09:45–15:30 (SD bands)")
    log.info("=" * 65)

    client = AlpacaClient()
    equity = client.get_equity()
    log.info("Account equity: $%.2f", equity)

    # ── Fetch dynamic ATR + avg volume for FCB filters (pre-market, once) ─
    opening_atr, opening_avg_volume = get_opening_bar_stats(
        symbol=SYMBOL,
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
    )

    # ── Strategies ────────────────────────────────────────────────────────
    orb_strategy   = ORBStrategy(symbol=SYMBOL, account_equity=equity)
    fcb_strategy   = FCBStrategy(symbol=SYMBOL, account_equity=equity,
                                 opening_bar_atr=opening_atr,
                                 opening_bar_avg_volume=opening_avg_volume)
    scalp_strategy = ScalpStrategy(symbol=SYMBOL, account_equity=equity)

    # ── Executors + loggers ───────────────────────────────────────────────
    orb_executor   = TradeExecutor(client=client, strategy=orb_strategy,
                                   logger=TradeLogger(symbol=f"{SYMBOL}_orb"),   name="orb")
    fcb_executor   = TradeExecutor(client=client, strategy=fcb_strategy,
                                   logger=TradeLogger(symbol=f"{SYMBOL}_fcb"),   name="fcb")
    scalp_executor = TradeExecutor(client=client, strategy=scalp_strategy,
                                   logger=TradeLogger(symbol=f"{SYMBOL}_scalp"), name="scalp")
    all_executors  = [orb_executor, fcb_executor, scalp_executor]

    # ── Flow filter (gating all signals) ─────────────────────────────────
    flow_filter = FlowFilter(min_premium=200_000, max_dte=30, expiry_hours=2.0)

    # ── 5-min bar aggregator (for ORB + FCB) ─────────────────────────────
    def on_5min_bar(bar_5m):
        fcb_sig = fcb_strategy.on_bar(bar_5m)
        if fcb_sig:
            if flow_filter.allows(fcb_sig.side):
                if DRY_RUN:
                    log.info("[DRY RUN] FCB signal: %s %s entry=%.2f target=%.2f stop=%.2f",
                             fcb_sig.symbol, fcb_sig.side, fcb_sig.entry, fcb_sig.target, fcb_sig.stop)
                else:
                    fcb_executor.execute(fcb_sig)
            else:
                log.info("[%s FCB] Signal skipped — flow bias is %s", SYMBOL, flow_filter.bias)

        orb_sig = orb_strategy.on_bar(bar_5m)
        if orb_sig:
            if flow_filter.allows(orb_sig.side):
                if DRY_RUN:
                    log.info("[DRY RUN] ORB signal: %s %s entry=%.2f target=%.2f stop=%.2f",
                             orb_sig.symbol, orb_sig.side, orb_sig.entry, orb_sig.target, orb_sig.stop)
                else:
                    orb_executor.execute(orb_sig)
            else:
                log.info("[%s ORB] Signal skipped — flow bias is %s", SYMBOL, flow_filter.bias)

    aggregator = BarAggregator(on_5min_bar=on_5min_bar)

    # ── 1-min bar handler ─────────────────────────────────────────────────
    async def on_bar(bar):
        scalp_sig = scalp_strategy.on_bar(bar)
        if scalp_sig:
            if flow_filter.allows(scalp_sig.side):
                if DRY_RUN:
                    log.info("[DRY RUN] SCALP signal: %s %s entry=%.2f target=%.2f stop=%.2f",
                             scalp_sig.symbol, scalp_sig.side, scalp_sig.entry,
                             scalp_sig.target, scalp_sig.stop)
                    scalp_strategy.mark_done()
                else:
                    scalp_executor.execute(scalp_sig)
            else:
                log.info("[%s SCALP] Signal skipped — flow bias is %s", SYMBOL, flow_filter.bias)

        aggregator.on_bar(bar)

    # ── Graceful shutdown ─────────────────────────────────────────────────
    def handle_shutdown(signum, frame):
        log.info("Shutdown signal — closing all positions and exiting...")
        for ex in all_executors:
            ex.shutdown()
        client.close_position(SYMBOL)
        client.stop_stream()
        sys.exit(0)

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # ── Async event loop ──────────────────────────────────────────────────
    uw_key = os.environ.get("UNUSUAL_WHALES_KEY", "")
    tt_user = os.environ.get("TASTYTRADE_USERNAME", "")
    tt_pass = os.environ.get("TASTYTRADE_PASSWORD", "")

    # Phase 2: options client (disabled until credentials provided)
    TastytradeClient(username=tt_user, password=tt_pass, paper=True)

    async def run():
        client.subscribe_bars(SYMBOL, on_bar)
        client.subscribe_quotes(SYMBOL)   # NBBO quotes cached for scalp spread checks

        tasks = [
            asyncio.create_task(client.run_stream_async()),
            asyncio.create_task(eod_monitor(all_executors, client)),
            asyncio.create_task(flow_filter.process_queue_loop()),
        ]

        if uw_key:
            uw_client = UnusualWhalesClient(
                api_key=uw_key,
                flow_filter=flow_filter,
                symbol=SYMBOL,
                min_premium=200_000,
                max_dte=30,
            )
            tasks.append(asyncio.create_task(uw_client.run()))
            log.info("Unusual Whales REST poller enabled — COIN flow filter active (60s interval)")
        else:
            log.info("UNUSUAL_WHALES_KEY not set — flow filter NEUTRAL (all trades allowed)")

        log.info("Connecting to Alpaca WebSocket, waiting for 1-min bars on %s...", SYMBOL)
        await asyncio.gather(*tasks)

    asyncio.run(run())


if __name__ == "__main__":
    main()
