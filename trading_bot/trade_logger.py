import csv
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)

TRADES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "trades")
FIELDNAMES = [
    "date", "symbol", "direction", "or_range",
    "entry", "target", "stop", "shares",
    "exit_price", "outcome", "pnl_per_share", "total_pnl",
    "notes",
]


class TradeLogger:
    def __init__(self, symbol: str):
        self.symbol = symbol
        os.makedirs(TRADES_DIR, exist_ok=True)
        self.csv_path = os.path.join(TRADES_DIR, f"{symbol.lower()}_trades.csv")
        self._init_csv()

    def _init_csv(self):
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

    def log_entry(self, signal, order_id: str):
        log.info(
            "\033[96m[%s] ENTRY  %s  Entry=%.2f  Target=%.2f  Stop=%.2f  Shares=%d  OR=%.2f\033[0m",
            signal.symbol, signal.side.upper(),
            signal.entry, signal.target, signal.stop, signal.shares, signal.or_range,
        )

    def log_exit(self, signal, exit_price: float, outcome: str, notes: str = ""):
        pnl_per_share = (
            exit_price - signal.entry if signal.side == "buy"
            else signal.entry - exit_price
        )
        total_pnl = pnl_per_share * signal.shares
        color = "\033[92m" if total_pnl >= 0 else "\033[91m"

        log.info(
            "%s[%s] EXIT   %s  Exit=%.2f  P&L=%.2f/share  Total=$%.2f  [%s]\033[0m",
            color, signal.symbol, outcome.upper(),
            exit_price, pnl_per_share, total_pnl, notes,
        )

        row = {
            "date": datetime.now(ET).date().isoformat(),
            "symbol": signal.symbol,
            "direction": signal.side,
            "or_range": signal.or_range,
            "entry": signal.entry,
            "target": signal.target,
            "stop": signal.stop,
            "shares": signal.shares,
            "exit_price": round(exit_price, 2),
            "outcome": outcome,
            "pnl_per_share": round(pnl_per_share, 2),
            "total_pnl": round(total_pnl, 2),
            "notes": notes,
        }
        with open(self.csv_path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writerow(row)

        return total_pnl
