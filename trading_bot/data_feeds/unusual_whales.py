"""
Unusual Whales REST Poller
--------------------------
Polls GET /api/option-trades?ticker=COIN every POLL_INTERVAL seconds and
enqueues each new trade into FlowFilter's asyncio.Queue. All classification
and bias updating happens inside FlowFilter.process_queue_loop() — this
module only fetches and enqueues.

REST endpoint (basic plan):
  GET https://api.unusualwhales.com/api/option-trades?ticker=COIN&limit=50

If the endpoint path differs for your plan tier, set:
  UW_TRADES_PATH=/api/your/actual/path

Reliability:
  - Exponential backoff on HTTP errors (60s → 120s → 240s … 300s max)
  - Safety reset: FlowFilter → NEUTRAL on any fetch failure
  - Deduplication: tracks seen trade IDs so repeated polls never double-count
"""

import asyncio
import http.client
import json
import logging
import os
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

REST_HOST     = "api.unusualwhales.com"
# /api/stock/{ticker}/flow-alerts — pre-aggregated sweeps with total_premium in dollars.
# Override with UW_TRADES_PATH env var if your plan uses a different endpoint.
REST_PATH_TPL = os.environ.get("UW_TRADES_PATH", "/api/stock/{symbol}/flow-alerts")
POLL_INTERVAL = 60          # seconds — conservative for basic tier rate limits
LOG_RAW       = os.environ.get("LOG_UW_RAW", "").lower() in ("1", "true", "yes")
MAX_SEEN_IDS  = 1_000       # cap the dedup set so memory stays bounded


class UnusualWhalesClient:
    def __init__(self, api_key: str, flow_filter, symbol: str = "COIN",
                 min_premium: float = 500_000, max_dte: int = 30):
        self._api_key  = api_key
        self._filter   = flow_filter
        self._symbol   = symbol.upper()
        self._seen_ids: set = set()

    async def run(self):
        """Poll REST endpoint with exponential backoff on errors."""
        log.info("[UW REST] Poller started — %s every %ds", self._symbol, POLL_INTERVAL)
        error_count = 0
        while True:
            try:
                new_trades = await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch
                )
                for raw in new_trades:
                    self._filter.enqueue(raw)   # O(1), non-blocking
                if new_trades:
                    log.info("[UW REST] %s: %d new trade(s) enqueued", self._symbol, len(new_trades))
                else:
                    log.debug("[UW REST] %s: no new trades this poll", self._symbol)
                error_count = 0
                await asyncio.sleep(POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                error_count += 1
                backoff = min(POLL_INTERVAL * (2 ** min(error_count, 4)), 300)
                log.warning("[UW REST] Error (attempt %d): %s — resetting bias, retrying in %ds",
                            error_count, e, backoff)
                await self._filter.reset_to_neutral()
                await asyncio.sleep(backoff)

    def _fetch(self) -> list[str]:
        """
        Blocking HTTP fetch — runs in executor thread, never touches the event loop.
        Returns a list of JSON strings for trades not seen on previous polls.
        """
        path = REST_PATH_TPL.format(symbol=self._symbol)
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }

        conn = http.client.HTTPSConnection(REST_HOST, timeout=10)
        try:
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            raw_body = resp.read().decode("utf-8")
        finally:
            conn.close()

        if resp.status == 401:
            raise Exception("Unauthorized — check UNUSUAL_WHALES_KEY")
        if resp.status == 403:
            raise Exception("Forbidden — endpoint may require a higher plan tier")
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}: {raw_body[:200]}")

        if LOG_RAW:
            log.debug("[UW REST] RAW: %s", raw_body[:500])

        payload = json.loads(raw_body)
        trades  = payload if isinstance(payload, list) else payload.get("data", [])

        new_trades = []
        for trade in trades:
            trade_id = (
                trade.get("id")
                or trade.get("trade_id")
                or trade.get("flow_id")
                or f"{trade.get('ticker')}{trade.get('strike')}{trade.get('expiry')}{trade.get('timestamp')}"
            )
            if trade_id in self._seen_ids:
                continue
            self._seen_ids.add(trade_id)
            new_trades.append(json.dumps(trade))

        # Prevent unbounded growth — keep only the most recent IDs
        if len(self._seen_ids) > MAX_SEEN_IDS:
            self._seen_ids = set(list(self._seen_ids)[-MAX_SEEN_IDS:])

        return new_trades
