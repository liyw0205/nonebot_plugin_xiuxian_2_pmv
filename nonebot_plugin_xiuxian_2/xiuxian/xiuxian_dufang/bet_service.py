from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DufangBetResult:
    status: str
    cost: int
    wallet_stone: int
    bet_id: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DufangBetService:
    """Charge an unseal wager and persist its pending state atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def place(self, operation_id, user_id, cost, placed_at) -> DufangBetResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        cost = int(cost)
        placed_at = str(placed_at).strip()
        if not operation_id or cost <= 0 or not placed_at:
            raise ValueError("operation id, positive cost and placement time are required")
        payload = json.dumps([user_id, cost, placed_at], ensure_ascii=True)

        def result(status, wallet_stone=0, bet_id=""):
            return DufangBetResult(status, cost if status in {"applied", "duplicate"} else 0, wallet_stone, bet_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dufang_bets ("
                    "bet_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cost INTEGER NOT NULL, "
                    "status TEXT NOT NULL, placed_at TEXT NOT NULL, settled_at TEXT)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dufang_bet_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, cost INTEGER NOT NULL, "
                    "wallet_stone INTEGER NOT NULL, bet_id TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,cost,wallet_stone,bet_id FROM dufang_bet_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return DufangBetResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                wallet_stone = int(user[0])
                if wallet_stone < cost:
                    conn.rollback()
                    return result("stone_insufficient", wallet_stone)

                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.unseal_data ("
                    "user_id TEXT PRIMARY KEY, count INTEGER, total_cost INTEGER, profit INTEGER, loss INTEGER, "
                    "shared_profit INTEGER, shared_loss INTEGER, received_profit INTEGER, received_loss INTEGER, last_update TEXT)"
                )
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                    (cost, user_id, cost),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", wallet_stone)
                conn.execute(
                    "INSERT INTO player_data.unseal_data (user_id,count,total_cost,last_update) VALUES (%s,1,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET count=COALESCE(count,0)+1, "
                    "total_cost=COALESCE(total_cost,0)+EXCLUDED.total_cost,last_update=EXCLUDED.last_update",
                    (user_id, cost, placed_at),
                )
                conn.execute(
                    "INSERT INTO dufang_bets VALUES (%s,%s,%s,%s,%s,NULL)",
                    (operation_id, user_id, cost, "pending", placed_at),
                )
                wallet_stone -= cost
                conn.execute(
                    "INSERT INTO dufang_bet_operations VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, cost, wallet_stone, operation_id),
                )
                conn.commit()
                return DufangBetResult("applied", cost, wallet_stone, operation_id)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DufangBetResult", "DufangBetService"]
