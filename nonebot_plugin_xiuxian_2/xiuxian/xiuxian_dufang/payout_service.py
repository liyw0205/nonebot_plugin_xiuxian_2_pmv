from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DufangPayoutResult:
    status: str
    wallet_stone: int
    gain: int
    loss: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DufangPayoutService:
    """Settle one pending unseal wager and its statistics atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> DufangPayoutResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dufang_payout_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,"
                "gain INTEGER NOT NULL,loss INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return DufangPayoutResult("duplicate", int(previous[0]), int(previous[1]), int(previous[2]))

    def settle(self, operation_id, bet_id, user_id, outcome, gain, requested_loss, settled_at) -> DufangPayoutResult:
        operation_id, bet_id, user_id = str(operation_id).strip(), str(bet_id).strip(), str(user_id)
        outcome, settled_at = str(outcome), str(settled_at)
        gain, requested_loss = int(gain), int(requested_loss)
        if not operation_id or not bet_id or outcome not in {"win", "loss"} or gain < 0 or requested_loss < 0:
            raise ValueError("valid operation, bet and payout values are required")
        # Request identity = bet + user; outcome/gain/loss stored as result.
        payload = json.dumps([bet_id, user_id], ensure_ascii=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dufang_payout_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,gain INTEGER NOT NULL,loss INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return DufangPayoutResult("state_changed", 0, 0, 0)
                    return DufangPayoutResult("duplicate", int(previous[1]), int(previous[2]), int(previous[3]))
                bet = conn.execute("SELECT user_id,status FROM dufang_bets WHERE bet_id=%s", (bet_id,)).fetchone()
                if bet is None or str(bet[0]) != user_id or str(bet[1]) != "pending":
                    conn.rollback()
                    return DufangPayoutResult("state_changed", 0, 0, 0)
                user = conn.execute("SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return DufangPayoutResult("user_missing", 0, 0, 0)
                wallet = int(user[0])
                actual_gain = gain if outcome == "win" else 0
                actual_loss = min(requested_loss, wallet) if outcome == "loss" else 0
                wallet = wallet + actual_gain - actual_loss
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (wallet, user_id))
                field, amount = ("profit", actual_gain) if outcome == "win" else ("loss", actual_loss)
                conn.execute(f"UPDATE player_data.unseal_data SET {db_backend.quote_ident(field)}=COALESCE({db_backend.quote_ident(field)},0)+%s,last_update=%s WHERE user_id=%s", (amount, settled_at, user_id))
                if conn.execute("UPDATE dufang_bets SET status=%s,settled_at=%s WHERE bet_id=%s AND status=%s", (outcome, settled_at, bet_id, "pending")).rowcount != 1:
                    conn.rollback()
                    return DufangPayoutResult("state_changed", 0, 0, 0)
                conn.execute("INSERT INTO dufang_payout_operations VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, wallet, actual_gain, actual_loss))
                conn.commit()
                return DufangPayoutResult("applied", wallet, actual_gain, actual_loss)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DufangPayoutResult", "DufangPayoutService"]
