# COIN Algorithmic Trading Bot — Full Plan

## Overview

Automated trading system for **COIN (Coinbase Global)** stock using multiple layered strategies.
- **Broker**: Alpaca (paper + live, stocks only)
- **Data feed**: Alpaca websocket (1-min bars, real-time)
- **Flow intelligence**: Unusual Whales WebSocket (real-time options flow)
- **Options execution (Phase 2)**: Tastytrade
- **Language**: Python only (no Pine Script)
- **Account size**: $25K+ (PDT compliant)

---

## Architecture

```
Alpaca WebSocket (1-min bars, ~1s latency)
        │
        ├── VWAP Scalper ──────────────────────────────────── 15-25 trades/day
        │       └── Dynamic SD band threshold (2.5σ) — expands on volatile days
        │       └── Gated by FlowFilter before execution
        │
        ├── Bar Aggregator (5 × 1-min → 1 synthetic 5-min bar)
        │       ├── ORB Strategy ──────────────────────────── 0-1 trades/day
        │       └── FCB Strategy ──────────────────────────── 0-1 trades/day
        │               └── Dynamic ATR range filter (1.5× opening bar ATR)
        │               └── Volume spike circuit breaker (5× avg opening volume)
        │               └── Both gated by FlowFilter before execution
        │
Unusual Whales WebSocket (real-time push, ~1s latency)
        └── FlowFilter ──── daily bias: BULLISH / BEARISH / NEUTRAL
                └── Gates all signals — skips trades contradicting big money

Alpaca Trading Client
        └── Executes bracket orders (entry + take-profit + stop-loss)

[Phase 2] Tastytrade API
        └── Copies options flow trade with delta-based contract sizing
```

---

## Strategy 1: ORB (Opening Range Breakout)

**Fires**: Once per day, 10:00–10:30 AM ET
**Timeframe**: 5-min bars (synthetic, aggregated from 1-min)
**Backtest**: 117 trades | 45% WR | +$51.09/share total | MaxDD $31.89

### Rules
- Opening range = 9:30–10:00 AM high/low
- Skip if OR range < $0.50 (too tight)
- **LONG**: Price breaks above OR high → entry, target = entry + (OR range × 1.0), stop = entry − (OR range × 0.5)
- **SHORT**: Price breaks below OR low → entry, target = entry − (OR range × 1.0), stop = entry + (OR range × 0.5)
- Single bracket order (no partial exits)
- Position size: 2% equity risk per trade

### Config
```python
"orb": {
    "or_period_minutes": 30,
    "min_or_range": 0.50,
    "target_mult": 1.0,
    "stop_mult": 0.50,
}
```

---

## Strategy 2: FCB (First Candle Break)

**Fires**: Once per day, at 9:35 AM ET only
**Timeframe**: 5-min bars
**Backtest**: 43 trades | 37.2% WR | +$75.30/share total | MaxDD $6.94 | Green all 7 months

### Rules
- First candle = 9:30–9:35 AM bar
- First candle must be RED (close < open)
- 9:35 bar must break below first candle low
- **Dynamic ATR range floor**: `min_fc_range = 1.5 × opening_bar_ATR` (fetched pre-market from 14 days of history) — prevents entering on tiny candles relative to COIN's current volatility
- **Capitulation guard**: skip if first candle > 3% of price — avoids shorting the bottom of a $4+ gap-down
- **Volume spike circuit breaker**: skip if first candle volume > 5× avg opening bar volume — catches flash crashes masked by absorbed institutional orders that keep price range within 3% but signal liquidity crisis via extreme volume
- Target = entry − (FC range × 2.0), stop = entry + (FC range × 0.3)
- Disables ATR and volume guards safely if historical data unavailable

### Config
```python
"fcb": {
    "atr_min_mult":      1.5,   # min_fc_range = 1.5 × opening_bar_ATR (dynamic)
    "atr_lookback":      14,    # trading days to compute opening bar ATR
    "max_range_pct":     0.03,  # skip if first candle > 3% of price (capitulation)
    "volume_spike_mult": 5.0,   # skip if first candle volume > 5× avg opening volume
    "target_mult":       2.0,
    "stop_mult":         0.30,
}
```

---

## Strategy 3: VWAP Mean Reversion Scalping

**Fires**: 15–25 times per day, 9:45 AM–3:30 PM ET
**Timeframe**: 1-min bars (raw from websocket)
**Backtest**: 2,285 trades / 124 days (~18/day) on 5-min proxy — run 1-min backtest for accurate results

### Rules
- Track running VWAP + volume-weighted standard deviation (σ) from 9:30 AM
- **Dynamic threshold**: `threshold = max(min_deviation, n_sigma × σ)` — expands on trending/volatile days to prevent catching knives
  - Quiet day: σ ≈ $0.40 → threshold ≈ $1.00 (similar to before)
  - Normal day: σ ≈ $0.80 → threshold ≈ $2.00
  - Strong trend: σ ≈ $3.00 → threshold ≈ $7.50 (won't step in front of runaway move)
- **LONG**: Price drops ≥ threshold below VWAP AND bar closes green (bounce confirmation)
- **SHORT**: Price rises ≥ threshold above VWAP AND bar closes red (rejection confirmation)
- Target = entry ± (deviation × 0.75) toward VWAP
- Stop = fixed $0.30 beyond entry
- Cooldown: 2 min between trades | Max: 40 trades/day
- Position size: 0.5% equity risk per trade

### Config
```python
"scalp": {
    "n_sigma":       2.5,    # VWAP SD band multiplier
    "min_deviation": 0.50,   # floor: don't enter when σ is tiny (flat day)
    "target_pct":    0.75,
    "stop":          0.30,
    "cooldown_min":  2,
    "max_trades":    40,
    "risk_pct":      0.005,
    "start_time":    "09:45",
    "end_time":      "15:30",
    "max_spread":    0.15,
}
```

---

## Layer 4: Unusual Whales Flow Filter

**Purpose**: Use institutional options flow as a directional bias — skip trades contradicting big money positioning.

### How It Works
1. Connect to Unusual Whales WebSocket (`wss://api.unusualwhales.com/socket`) with Bearer token
2. Subscribe to `option_trades` channel — raw tick data, real-time push (~1s latency)
3. Classify each trade ourselves: ticker == "COIN" + premium ≥ $500K + DTE ≤ 30 days
4. CALL trade → BULLISH bias | PUT trade → BEARISH bias
5. Dominant direction (more alerts) wins; ties → NEUTRAL
6. Bias expires after 2 hours with no confirming alert
7. Every signal checked via `flow_filter.allows(side)` before execution
8. **Heartbeat**: explicit ping every 20s, 10s timeout → hard reconnect on silent dropout
9. **Safety reset**: bias → NEUTRAL on any disconnect (avoids stale directional filtering)
10. **asyncio.Queue pipeline**: UW handler calls `enqueue(raw)` — sync O(1), never awaits. A dedicated `process_queue_loop()` asyncio task drains the queue independently. Writes use `asyncio.Lock` — event loop never blocked by flow processing

### Signal Gating
| Flow Bias | Strategy wants LONG | Strategy wants SHORT |
|-----------|--------------------|--------------------|
| BULLISH   | ✅ Take it          | ❌ Skip             |
| BEARISH   | ❌ Skip             | ✅ Take it          |
| NEUTRAL   | ✅ Take it          | ✅ Take it          |

### Example Day
```
9:32 AM  UW: COIN $2.1M CALL SWEEP → bias = BULLISH
9:35 AM  FCB: first candle RED, would fire SHORT → SKIPPED (contra-bias)
10:05 AM ORB: COIN breaks OR high → LONG → TAKEN ✓
10:30 AM Scalp: COIN dips below VWAP, green bounce → LONG → TAKEN ✓
10:52 AM Scalp: COIN above VWAP, red rejection → would SHORT → SKIPPED ✓
11:15 AM Scalp: COIN dips below VWAP, green bounce → LONG → TAKEN ✓
```

---

## Phase 2: Options Execution via Tastytrade

### Delta-Based Contract Sizing
COIN IV routinely runs 60–100%+, making ATM options cost $800–$1,500/contract.
Flat "1-2 contracts" would risk 4–12% of a $25K account — violating the 2% risk rule.

**Formula (with 10% slippage buffer for COIN's high-IV ask flash risk):**
```python
max_loss          = equity × 0.02                        # $500 at $25K
effective_premium = premium × (1 + slippage_buffer)      # 10% buffer
loss_per_contract = effective_premium × 0.50 × 100       # 50% stop × 100 shares
contracts         = floor(max_loss / loss_per_contract)
# If contracts < 1 → skip trade (premium too expensive)
```

**Examples at $25K equity / 10% slippage buffer:**

| Option premium | Effective premium | Loss at 50% stop | Contracts | Action |
|---|---|---|---|---|
| $3.00/share | $3.30 | $165/contract | 3 | Trade |
| $5.00/share | $5.50 | $275/contract | 1 | Trade |
| $9.00/share | $9.90 | $495/contract | 1 | Trade |
| $10.00/share | $11.00 | $550/contract | 0 | **Skip** |

### Partial Fill Protection
Options can be illiquid — ordering 3 contracts doesn't guarantee 3 contracts fill.

1. Submit order → wait up to **5 seconds** for fill
2. If partial fill: **cancel remainder immediately** (don't chase)
3. Log actual filled count — stop-loss recalculated based on actual size
4. If zero fill: cancel entirely, skip trade

When UW fires a large COIN sweep → `size_contracts()` → `place_order()` via Tastytrade.
Runs in parallel with stock scalping.

---

## Files — Current State

| File | Status | Purpose |
|---|---|---|
| `trading_bot/config.py` | Done | All strategy parameters |
| `trading_bot/alpaca_client.py` | Done | WebSocket bars + NBBO quotes + orders |
| `trading_bot/market_data.py` | Done | Fetch opening bar ATR + avg volume for FCB filters |
| `trading_bot/strategies/coin_orb.py` | Done | ORB strategy |
| `trading_bot/strategies/coin_fcb.py` | Done | FCB — dynamic ATR + capitulation + volume spike guard |
| `trading_bot/strategies/coin_scalp.py` | Done | VWAP scalp — SD bands |
| `trading_bot/strategies/flow_filter.py` | Done | asyncio.Queue pipeline + bias (BULLISH/BEARISH/NEUTRAL) |
| `trading_bot/data_feeds/unusual_whales.py` | Done | UW WebSocket — option_trades channel + heartbeat |
| `trading_bot/order_manager.py` | Done | client_order_id tagging + cross-liquidation guard |
| `trading_bot/trade_executor.py` | Done | Bracket orders + scalp spread check + conflict guard |
| `trading_bot/tastytrade_client.py` | Done | Delta sizing (10% slippage buffer) + partial fill stub |
| `trading_bot/main.py` | Done | Full integration + dry run mode |
| `scripts/pull_1min_data.py` | Done | Pull historical 1-min bars |
| `scripts/backtest_scalp.py` | Done | Scalp backtest on 1-min CSV |

## Files — To Complete (Phase 2)

| File | Purpose |
|---|---|
| `trading_bot/tastytrade_client.py` | Wire actual Tastytrade API calls (stubs ready) |

---

## Execution Flow (Live Bot)

```
main.py starts
  │
  ├── Fetch opening bar ATR + avg volume (market_data.py) → passed to FCBStrategy
  ├── AlpacaClient.subscribe_bars("COIN")   → 1-min bars stream
  ├── AlpacaClient.subscribe_quotes("COIN") → NBBO quote cache for spread checks
  ├── flow_filter.process_queue_loop() — asyncio background task drains UW message queue
  ├── UnusualWhalesClient WebSocket connects (if UNUSUAL_WHALES_KEY set)
  │       └── option_trades channel, ping/pong heartbeat, enqueue(raw) O(1), bias reset on dropout
  │
  │   [Every minute, 1-min bar arrives from Alpaca]
  │
  ├── Bar → scalp_strategy.on_bar(bar_1m) → signal?
  │       └── flow_filter.allows(side)?
  │             └── DRY_RUN? log only : TradeExecutor
  │                   ├── check spread (live bid/ask) → marketable limit order
  │                   └── StrategyOrderManager.check_position_conflict() → skip if stacked
  │
  ├── Bar → BarAggregator
  │       └── Every 5 bars → emit synthetic 5-min bar
  │             ├── fcb_strategy.on_bar(bar_5m) → signal?
  │             │       └── flow_filter.allows(side)? → TradeExecutor
  │             │             └── check_position_conflict() → bracket order (tagged: fcb_<hex>)
  │             └── orb_strategy.on_bar(bar_5m) → signal?
  │                     └── flow_filter.allows(side)? → TradeExecutor
  │                           └── check_position_conflict() → bracket order (tagged: orb_<hex>)
  │
  ├── [Unusual Whales push, O(1) enqueue — never blocks Alpaca bars]
  │       └── enqueue(raw) → asyncio.Queue
  │             └── process_queue_loop() (background task) drains queue
  │                   └── filter COIN + $500K + DTE≤30 → FlowFilter._update_bias() (locked)
  │
  └── [Every day at 3:50 PM ET]
        └── Close all open positions (EOD exit)
```

---

## Risk Controls

| Control | Value | Notes |
|---|---|---|
| ORB/FCB risk per trade | 2% of equity | config.py RISK_PCT |
| Scalp risk per trade | 0.5% of equity | High frequency — kept tight |
| Max scalp trades/day | 40 | Circuit breaker |
| EOD force close | 3:50 PM ET | All positions |
| Min OR range (ORB) | $0.50 | Skip low-volatility days |
| FCB min range | 1.5 × opening ATR | Dynamic — pre-market fetch |
| FCB capitulation guard | > 3% of price | Skip explosive gap days |
| FCB volume spike guard | > 5× avg opening volume | Flash crash / liquidity crisis protection |
| VWAP SD threshold | 2.5σ (floor $0.50) | Dynamic — expands on trending days |
| Scalp entry | Marketable limit | ask+$0.05 long / bid-$0.05 short |
| Scalp spread check | < $0.15 | Live bid/ask from NBBO quote stream |
| Scalp cooldown | 2 min | Avoid re-entering same move |
| Flow filter | BULLISH/BEARISH/NEUTRAL | Skip contra-trend trades |
| UW dropout safety | Reset to NEUTRAL | Heartbeat + reconnect on silent drop |
| Cross-liquidation guard | Check open position before any order | Prevents ORB sell from closing scalper's long |
| Strategy order tagging | `client_order_id: <name>_<hex>` | Every order identifiable in Alpaca dashboard |
| Options slippage buffer | 10% on premium | Protects against ask flash at high IV |
| Options risk | 2% equity cap | Delta-based sizing, skip if 0 contracts |
| Options partial fill | 5s timeout + cancel | Prevents orphaned open positions |

---

## Environment Variables

```bash
# Required
export ALPACA_API_KEY="..."
export ALPACA_SECRET_KEY="..."

# Optional — enables real-time flow filter
export UNUSUAL_WHALES_KEY="..."

# Optional — enables Phase 2 options (stubs ready, API wiring TODO)
export TASTYTRADE_USERNAME="..."
export TASTYTRADE_PASSWORD="..."

# Debugging / testing
export LOG_UW_RAW=1    # print raw Unusual Whales WebSocket messages
export DRY_RUN=1       # log signals only, no order placement
```

## Recommended Testing Sequence

**Phase 0 — Dry run (3 days minimum)**
```bash
DRY_RUN=1 python -m trading_bot.main
```
Verify: synthetic 5-min bars aggregate correctly, FlowFilter toggles on COIN flow,
scalp signals fire with correct SD threshold, no unhandled exceptions.

**Phase 1 — Stock live (Alpaca paper)**
Remove `DRY_RUN`. Run on Alpaca paper account for 2+ weeks.
Target: 50+ executions, verify slippage and fills match backtest assumptions.

**Phase 2 — Options live (Tastytrade)**
Only after Phase 1 is profitable. Wire `tastytrade_client.py` stubs.
Start with 1 contract max regardless of sizing formula.

---

## P&L Estimates (Rough — 1-Min Backtest Pending)

### Per Trade (Scalping)
- Risk per trade: $125 (0.5% × $25K)
- Shares: ~416 (at $0.30 stop)
- Win target: $0.75/share → $312 per winning trade
- Loss: $0.30/share → $125 per losing trade
- At 50% win rate: expected value ≈ $83/trade

### Per Day
- 18 trades/day × $83 = **~$1,500 (backtest math)**
- Realistic (slippage, missed fills, bad days): **$500–$750/day**
- Bad trending days: can lose $300–$800

> ⚠️ Run the 1-min backtest before drawing conclusions. SD bands will reduce
> trade count vs the 5-min proxy — fewer but higher quality entries.
> Live performance is typically 35–50% of backtest performance.

---

## Backtest Steps (Before Going Live)

1. Pull 1-min data:
   ```bash
   export ALPACA_API_KEY="your_key"
   export ALPACA_SECRET_KEY="your_secret"
   python scripts/pull_1min_data.py
   ```

2. Run scalp backtest:
   ```bash
   python scripts/backtest_scalp.py --csv COIN_1Min_sip.csv
   ```

3. Tune SD band parameters if needed:
   - Try `n_sigma`: 2.0, 2.5, 3.0
   - Try `min_deviation`: 0.30, 0.50, 0.75
   - Pick combination with best Sharpe (return / max drawdown)

4. Validate ORB/FCB unchanged:
   ```bash
   python -m trading_bot.backtest
   ```

5. Paper trade minimum 2 weeks before going live

---

## Open Questions

1. **Flow filter sensitivity**: Is $500K the right premium threshold? Too high misses alerts; too low generates noise.
2. **Bias expiry**: 2-hour expiry — should morning flow (9:45 AM) still gate afternoon trades (2:30 PM)?
3. **Premium-weighted voting**: Currently dominant alert count wins — should larger premiums count more votes?
4. **UW field names**: `_handle()` in `unusual_whales.py` uses estimated field names from docs — verify against live messages on first run (`LOG_UW_RAW=1`). Adjust `put_call`, `total_premium`, `dte` keys if needed.
5. **Options copy trade timing (Phase 2)**: Enter immediately on sweep alert, or wait for stock price to confirm direction first?
6. **Tastytrade ATM vs OTM**: ATM (delta ~0.50, higher premium, fewer contracts) vs OTM (cheaper, more contracts within budget)?
7. **Correlation risk**: ORB + FCB + Scalper all on COIN — single-stock concentration risk acceptable?
