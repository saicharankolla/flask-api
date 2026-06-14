import logging
from typing import Callable, Optional, Tuple

from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    LimitOrderRequest,
    TakeProfitRequest,
    StopLossRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass

from .config import ALPACA_API_KEY, ALPACA_SECRET_KEY

log = logging.getLogger(__name__)


class AlpacaClient:
    def __init__(self):
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError(
                "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in environment."
            )
        self._trading = TradingClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_SECRET_KEY,
            paper=True,
        )
        self._stream = StockDataStream(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_SECRET_KEY,
            feed=DataFeed.SIP,
        )
        self._latest_quotes: dict = {}   # symbol → latest quote object

    # ── Account ───────────────────────────────────────────────────────────────

    def get_equity(self) -> float:
        return float(self._trading.get_account().equity)

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        qty: int,
        limit_price: float,
        take_profit: Optional[float] = None,
        stop_loss: Optional[float]   = None,
        order_class: str = "bracket",
    ):
        """
        Flexible limit order supporting bracket, oto (one-triggers-other), or simple.
        order_class: "bracket" | "oto" | "simple"
        """
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL

        oc = {"bracket": OrderClass.BRACKET,
              "oto":     OrderClass.OTO,
              "simple":  OrderClass.SIMPLE}.get(order_class, OrderClass.SIMPLE)

        kwargs = dict(
            symbol=symbol,
            qty=qty,
            side=order_side,
            type="limit",
            time_in_force=TimeInForce.DAY,
            limit_price=round(limit_price, 2),
            order_class=oc,
        )
        if take_profit is not None:
            kwargs["take_profit"] = TakeProfitRequest(limit_price=round(take_profit, 2))
        if stop_loss is not None:
            kwargs["stop_loss"] = StopLossRequest(stop_price=round(stop_loss, 2))

        order = self._trading.submit_order(LimitOrderRequest(**kwargs))
        log.info("Order submitted: %s %s %d @%.2f  TP=%s  SL=%s  class=%s  id=%s",
                 side.upper(), symbol, qty, limit_price,
                 f"{take_profit:.2f}" if take_profit else "—",
                 f"{stop_loss:.2f}"   if stop_loss   else "—",
                 order_class, order.id)
        return order

    def close_position(self, symbol: str):
        try:
            self._trading.close_position(symbol)
            log.info("Closed position: %s", symbol)
        except Exception as e:
            log.warning("close_position %s: %s", symbol, e)

    def cancel_all_orders(self):
        self._trading.cancel_orders()
        log.info("Cancelled all open orders")

    def get_position(self, symbol: str) -> Optional[object]:
        try:
            return self._trading.get_open_position(symbol)
        except Exception:
            return None

    # ── Streaming ─────────────────────────────────────────────────────────────

    def subscribe_bars(self, symbol: str, handler: Callable):
        self._stream.subscribe_bars(handler, symbol)
        log.info("Subscribed to 1-min bars for %s", symbol)

    def subscribe_quotes(self, symbol: str):
        """Subscribe to NBBO quotes, cached internally for spread checks."""
        async def _on_quote(q):
            self._latest_quotes[q.symbol] = q
        self._stream.subscribe_quotes(_on_quote, symbol)
        log.info("Subscribed to NBBO quotes for %s", symbol)

    def get_latest_quote(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        """Returns (bid_price, ask_price) or (None, None) if no quote received yet."""
        q = self._latest_quotes.get(symbol)
        if q is None:
            return None, None
        try:
            return float(q.bid_price), float(q.ask_price)
        except Exception:
            return None, None

    def run_stream(self):
        log.info("Starting websocket stream...")
        self._stream.run()

    async def run_stream_async(self):
        await self._stream._run_forever()

    def stop_stream(self):
        self._stream.stop()
