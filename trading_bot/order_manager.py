"""
Strategy Order Manager
-----------------------
Wraps Alpaca bracket order placement with two protections:

1. client_order_id tagging: every order carries a prefix identifying its
   originating strategy (e.g. "scalp_a1b2c3d4"). This lets you filter
   the Alpaca order history by strategy and prevents ambiguity.

2. Position conflict guard: before placing any order, checks the current
   Alpaca position for the symbol. If a conflicting or stacked position
   already exists (owned by a different strategy), the order is skipped
   to prevent cross-liquidation.

   Cross-liquidation example blocked:
     Scalper holds +100 COIN (long). ORB fires SHORT signal.
     Without this guard, the sell order partially closes the scalper's
     position instead of opening an independent short.
"""

import logging
import uuid
from typing import Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, TakeProfitRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass

log = logging.getLogger(__name__)


class StrategyOrderManager:
    def __init__(self, trading_client: TradingClient, strategy_name: str):
        self._client = trading_client
        self._name   = strategy_name

    def _tag(self) -> str:
        """Unique order ID: '<strategy>_<8-char hex>' — visible in Alpaca dashboard."""
        return f"{self._name}_{uuid.uuid4().hex[:8]}"

    def check_position_conflict(self, symbol: str, side: str) -> bool:
        """
        Returns True if a conflicting or stacked position exists.
        Caller should skip the order when this returns True.
        """
        try:
            from alpaca.common.exceptions import APIError
            pos = self._client.get_open_position(symbol)
            qty = float(pos.qty)

            # Cross-liquidation: entering opposite direction to existing position
            if side == "buy" and qty < 0:
                log.warning("[%s] Cross-liquidation guard: existing SHORT %.0f shares — "
                            "skipping BUY signal", self._name, abs(qty))
                return True
            if side == "sell" and qty > 0:
                log.warning("[%s] Cross-liquidation guard: existing LONG %.0f shares — "
                            "skipping SELL signal", self._name, abs(qty))
                return True

            # Same-direction stacking: another strategy already holds this position
            log.warning("[%s] Position stacking guard: %s %.0f shares already open — "
                        "skipping to avoid double exposure", self._name, symbol, qty)
            return True

        except Exception:
            return False   # no position exists — safe to proceed

    def submit_bracket(
        self,
        symbol: str,
        side: str,           # "buy" or "sell"
        qty: int,
        limit_price: float,
        take_profit: float,
        stop_loss: float,
    ) -> Optional[object]:
        """
        Submit a tagged bracket order. Returns the Alpaca order object or None on failure.
        Does NOT check for conflicts — caller must call check_position_conflict() first.
        """
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        client_id  = self._tag()

        req = LimitOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            type="limit",
            time_in_force=TimeInForce.DAY,
            limit_price=round(limit_price, 2),
            order_class=OrderClass.BRACKET,
            client_order_id=client_id,
            take_profit=TakeProfitRequest(limit_price=round(take_profit, 2)),
            stop_loss=StopLossRequest(stop_price=round(stop_loss, 2)),
        )

        try:
            order = self._client.submit_order(req)
            log.info("[%s] Bracket submitted: %s %d %s @%.2f  TP=%.2f  SL=%.2f  "
                     "client_id=%s  order_id=%s",
                     self._name, side.upper(), qty, symbol, limit_price,
                     take_profit, stop_loss, client_id, order.id)
            return order
        except Exception as e:
            log.error("[%s] Order submission failed: %s", self._name, e)
            return None
