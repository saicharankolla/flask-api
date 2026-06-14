"""
Tastytrade Client — Phase 2 options execution.

Implements delta-based contract sizing so options risk stays within the
same 2% equity limit as the stock strategies. At COIN's typical IV of
60–100%+, ATM options cost $800–$1,500/contract — flat "1-2 contracts"
would risk 4–12% of a $25K account per trade. This sizing formula
prevents that.

Full execution requires Tastytrade developer API credentials:
  https://developer.tastytrade.com

Environment variables:
    TASTYTRADE_USERNAME   your Tastytrade login email
    TASTYTRADE_PASSWORD   your Tastytrade password

Set paper=True to use the Tastytrade sandbox environment.
"""

import math
import logging
from typing import Optional

log = logging.getLogger(__name__)


class TastytradeClient:
    def __init__(
        self,
        username: str = "",
        password: str = "",
        paper: bool = True,
    ):
        self._username = username
        self._password = password
        self._paper    = paper

        if self.enabled:
            env = "sandbox" if paper else "live"
            log.info("[Tastytrade] Initialized (%s) — Phase 2 options enabled", env)
        else:
            log.info("[Tastytrade] No credentials — Phase 2 options disabled")

    @property
    def enabled(self) -> bool:
        return bool(self._username and self._password)

    def size_contracts(
        self,
        equity: float,
        option_premium_per_share: float,
        risk_pct: float = 0.02,
        stop_pct: float = 0.50,
        slippage_buffer: float = 0.10,
    ) -> int:
        """
        Delta-based contract sizing with slippage buffer.

        Caps risk at risk_pct of equity. Assumes we exit if the option
        loses stop_pct of its value (50% stop = standard options discipline).
        slippage_buffer inflates the premium to account for ask flash risk
        in high-IV environments (COIN IV 60-100%+).

        Formula:
            max_loss          = equity × risk_pct
            effective_premium = premium_per_share × (1 + slippage_buffer)
            loss_per_contract = effective_premium × stop_pct × 100
            contracts         = floor(max_loss / loss_per_contract)

        Returns 0 if premium is too expensive to fit in risk budget.
        Caller must skip the trade when this returns 0.

        Example at $25K equity / 2% risk / $12 premium / 10% buffer:
            max_loss          = $500
            effective_premium = $12 × 1.10 = $13.20
            loss_per_contract = $13.20 × 0.50 × 100 = $660
            contracts         = floor(500 / 660) = 0  → skip trade
        """
        max_loss          = equity * risk_pct
        effective_premium = option_premium_per_share * (1 + slippage_buffer)
        loss_per_contract = effective_premium * stop_pct * 100

        if loss_per_contract <= 0:
            return 0

        contracts = math.floor(max_loss / loss_per_contract)

        if contracts < 1:
            log.info(
                "[Tastytrade] Premium $%.2f/share → loss/contract $%.0f exceeds "
                "risk budget $%.0f — skipping trade",
                option_premium_per_share, loss_per_contract, max_loss,
            )
        return contracts

    def get_option_premium(
        self,
        symbol: str,
        option_type: str,    # "call" or "put"
        strike: float,
        expiry: str,         # "YYYY-MM-DD"
    ) -> Optional[float]:
        """
        Fetch current mid-price of an option contract.
        Returns None if unavailable.

        TODO: implement Tastytrade market data API call.
        Reference: https://developer.tastytrade.com/open-api-spec/market-data/
        """
        if not self.enabled:
            return None
        log.debug("[Tastytrade] get_option_premium: %s %s %s %s", symbol, option_type, strike, expiry)
        # TODO: implement
        return None

    def place_order(
        self,
        symbol: str,
        option_type: str,          # "call" or "put"
        strike: float,
        expiry: str,               # "YYYY-MM-DD"
        contracts: int,
        side: str = "buy",
        fill_timeout_seconds: int = 5,
    ) -> Optional[int]:
        """
        Place an options order with partial fill protection.

        After submitting, waits fill_timeout_seconds then:
          - Fully filled    → return filled count
          - Partially filled → cancel remainder, return filled count
          - Unfilled         → cancel entirely, return 0

        Returns None if disabled. Returns 0 if order fails or times out unfilled.
        The stop-loss must be recalculated by the caller based on actual filled count.

        TODO: implement Tastytrade API calls when Phase 2 is ready.
        Reference: https://developer.tastytrade.com/open-api-spec/accounts-and-customers/
        """
        if not self.enabled:
            log.info("[Tastytrade] Disabled — skipping: %s %d× %s %s strike=%.2f exp=%s",
                     side.upper(), contracts, symbol, option_type.upper(), strike, expiry)
            return None

        if contracts < 1:
            log.info("[Tastytrade] contracts=0 — skipping (premium exceeds risk budget)")
            return None

        log.info("[Tastytrade] Submitting: %s %d× %s %s strike=%.2f exp=%s",
                 side.upper(), contracts, symbol, option_type.upper(), strike, expiry)

        # TODO: order_id = tastytrade_api.submit_order(...)
        order_id = None

        # Poll for fill up to fill_timeout_seconds
        import time
        deadline = time.monotonic() + fill_timeout_seconds
        filled   = 0
        while time.monotonic() < deadline:
            time.sleep(0.5)
            # TODO: filled = tastytrade_api.get_filled_quantity(order_id)
            break  # stub: treat as 0 filled

        remaining = contracts - filled
        if remaining > 0 and order_id is not None:
            log.info("[Tastytrade] Partial fill %d/%d — cancelling %d unfilled contracts",
                     filled, contracts, remaining)
            # TODO: tastytrade_api.cancel_order(order_id)

        if filled == 0:
            log.info("[Tastytrade] No fill after %ds — order cancelled", fill_timeout_seconds)
            return 0

        log.info("[Tastytrade] Filled %d/%d contracts", filled, contracts)
        return filled
