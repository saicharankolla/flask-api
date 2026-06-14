"""
Manages order lifecycle for both strategy types.

Both ORB and FCB use the same simple bracket order:
  - limit entry → take-profit + stop-loss bracket
  - EOD 3:50 PM: force-close if still open
"""

import logging
import threading
from typing import Optional
from zoneinfo import ZoneInfo
from datetime import datetime

from .alpaca_client import AlpacaClient
from .order_manager import StrategyOrderManager
from .strategies.coin_orb import ORBStrategy, TradeSignal as ORBSignal
from .strategies.coin_fcb import FCBStrategy, FCBSignal
from .strategies.coin_scalp import ScalpSignal
from .trade_logger import TradeLogger
from .config import EOD_EXIT_TIME, SYMBOL_CONFIG

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class TradeExecutor:
    def __init__(self, client: AlpacaClient, strategy, logger: TradeLogger, name: str = "unknown"):
        self.client   = client
        self.strategy = strategy
        self.logger   = logger
        self._om      = StrategyOrderManager(client._trading, strategy_name=name)

        self.active_signal           = None
        self.order_id: Optional[str] = None
        self._eod_timer: Optional[threading.Timer] = None

    # ── Public: receive signal from strategy ─────────────────────────────────

    def execute(self, signal):
        if self.active_signal is not None:
            log.warning("Signal received but trade already active — ignoring")
            return

        # ── Scalp: live spread check + marketable limit price ─────────────
        limit_price = signal.entry
        if isinstance(signal, ScalpSignal):
            limit_price = self._get_scalp_limit_price(signal)
            if limit_price is None:
                return   # spread too wide or quote unavailable — skip trade

        # Cross-liquidation / stacking guard — check Alpaca for existing position
        if self._om.check_position_conflict(signal.symbol, signal.side):
            self.strategy.mark_done()
            return

        self.active_signal = signal
        self.logger.log_entry(signal, order_id="pending")

        order = self._om.submit_bracket(
            symbol=signal.symbol,
            side=signal.side,
            qty=signal.shares,
            limit_price=limit_price,
            take_profit=signal.target,
            stop_loss=signal.stop,
        )
        if order is None:
            self.active_signal = None
            self.strategy.mark_done()
            return
        self.order_id = str(order.id)

        self._schedule_eod_exit(signal)

    def _get_scalp_limit_price(self, signal: ScalpSignal) -> Optional[float]:
        """
        Fetch live bid/ask, validate spread, return a marketable limit price:
          LONG:  ask + $0.05  (aggressive — ensures fill, caps slippage)
          SHORT: bid - $0.05

        Returns None if spread is too wide or quotes unavailable (skip trade).
        """
        max_spread = SYMBOL_CONFIG[signal.symbol]["scalp"].get("max_spread", 0.15)
        bid, ask   = self.client.get_latest_quote(signal.symbol)

        if bid is None or ask is None:
            # No live quote yet (first seconds of session) — fall back to bar close
            log.debug("[%s SCALP] No live quote yet — using bar close as limit", signal.symbol)
            return signal.entry

        spread = ask - bid
        if spread > max_spread:
            log.info("[%s SCALP] Spread $%.3f exceeds max $%.2f — skipping entry",
                     signal.symbol, spread, max_spread)
            self.strategy.mark_done()
            return None

        if signal.side == "buy":
            price = round(ask + 0.05, 2)
        else:
            price = round(bid - 0.05, 2)

        log.debug("[%s SCALP] Marketable limit: bid=%.2f ask=%.2f spread=%.3f → limit=%.2f",
                  signal.symbol, bid, ask, spread, price)
        return price

    # ── EOD forced close ──────────────────────────────────────────────────────

    def _schedule_eod_exit(self, signal):
        now = datetime.now(ET)
        eod_h, eod_m = map(int, EOD_EXIT_TIME.split(":"))
        eod_today = now.replace(hour=eod_h, minute=eod_m, second=0, microsecond=0)

        if now >= eod_today:
            self._force_close(signal, reason="already past EOD")
            return

        delay = (eod_today - now).total_seconds()
        self._eod_timer = threading.Timer(delay, self._force_close, args=(signal,))
        self._eod_timer.daemon = True
        self._eod_timer.start()
        log.info("[%s] EOD exit timer set for %s ET (%.0fs)", signal.symbol, EOD_EXIT_TIME, delay)

    def _force_close(self, signal, reason: str = "EOD"):
        if self.active_signal is None:
            return
        log.info("[%s] %s forced close", signal.symbol, reason)
        self.client.cancel_all_orders()
        self.client.close_position(signal.symbol)

        exit_price = signal.entry
        try:
            pos = self.client.get_position(signal.symbol)
            if pos:
                exit_price = float(pos.current_price)
        except Exception:
            pass

        self.logger.log_exit(signal, exit_price, outcome="EOD", notes=reason)
        self._reset_state()

    def _reset_state(self):
        self.active_signal = None
        self.order_id      = None
        self.strategy.mark_done()

    # ── External notifications ────────────────────────────────────────────────

    def notify_closed(self, exit_price: float, outcome: str):
        """Call when position is closed (stop hit or target filled)."""
        if self.active_signal is None:
            return
        if self._eod_timer:
            self._eod_timer.cancel()
        self.logger.log_exit(self.active_signal, exit_price, outcome=outcome)
        self._reset_state()

    def shutdown(self):
        if self._eod_timer:
            self._eod_timer.cancel()
        if self.active_signal:
            self._force_close(self.active_signal, reason="shutdown")
