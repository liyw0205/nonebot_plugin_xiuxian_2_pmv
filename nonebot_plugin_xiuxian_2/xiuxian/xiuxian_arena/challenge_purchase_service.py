from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaChallengePurchaseResult:
    status: str
    amount: int
    cost: int
    stone: int
    bought: int
    extra: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaChallengePurchaseService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database = Path(game_database), Path(player_database)
        self._lock = lock or RLock()

    def purchase(self, operation_id, user_id, amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra = map(int, (amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra))
        if not operation_id or min(amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra) < 0 or amount == 0:
            raise ValueError("valid operation and purchase state are required")
        payload = json.dumps([user_id, amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra])
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS arena_challenge_purchase_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, amount INTEGER NOT NULL, cost INTEGER NOT NULL, stone INTEGER NOT NULL, bought INTEGER NOT NULL, extra INTEGER NOT NULL)")
                previous = conn.execute("SELECT payload,amount,cost,stone,bought,extra FROM arena_challenge_purchase_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    return ArenaChallengePurchaseResult("duplicate", *(map(int, previous[1:]))) if str(previous[0]) == payload else ArenaChallengePurchaseResult("state_changed", 0, 0, expected_stone, expected_bought, expected_extra)
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                arena = conn.execute("SELECT daily_challenge_buys,daily_extra_challenges FROM player_data.arena WHERE user_id=%s", (user_id,)).fetchone()
                if not user or not arena or tuple(map(int, arena)) != (expected_bought, expected_extra) or int(user[0]) != expected_stone:
                    conn.rollback(); return ArenaChallengePurchaseResult("state_changed", 0, 0, expected_stone, expected_bought, expected_extra)
                real_amount = min(amount, max(0, daily_limit - expected_bought))
                cost = real_amount * unit_cost
                if real_amount == 0:
                    conn.rollback(); return ArenaChallengePurchaseResult("limit_reached", 0, 0, expected_stone, expected_bought, expected_extra)
                if expected_stone < cost:
                    conn.rollback(); return ArenaChallengePurchaseResult("stone_insufficient", 0, 0, expected_stone, expected_bought, expected_extra)
                stone, bought, extra = expected_stone - cost, expected_bought + real_amount, expected_extra + real_amount
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (stone, user_id))
                conn.execute("UPDATE player_data.arena SET daily_challenge_buys=%s,daily_extra_challenges=%s WHERE user_id=%s", (bought, extra, user_id))
                conn.execute("INSERT INTO arena_challenge_purchase_operations VALUES (%s,%s,%s,%s,%s,%s,%s)", (operation_id, payload, real_amount, cost, stone, bought, extra))
                conn.commit(); return ArenaChallengePurchaseResult("applied", real_amount, cost, stone, bought, extra)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")
