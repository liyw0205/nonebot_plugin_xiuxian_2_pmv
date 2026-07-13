from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BossBattleCostResult:
    status: str
    stamina: int
    battle_count: int
    checked_at: str

    @property
    def succeeded(self) -> bool:
        return self.status == "applied"


class BossBattleCostService:
    """Consume the mutable player state required to start one boss battle."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def consume(
        self,
        operation_id,
        user_id,
        stamina_cost,
        battle_limit,
        expected_stamina,
        expected_hp,
        expected_exp,
        expected_battle_count,
        expected_checked_at,
        checked_at=None,
    ) -> BossBattleCostResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stamina_cost = int(stamina_cost)
        battle_limit = int(battle_limit)
        expected_stamina = int(expected_stamina)
        expected_hp = int(expected_hp)
        expected_exp = int(expected_exp)
        expected_battle_count = int(expected_battle_count)
        expected_checked_at = "" if expected_checked_at is None else str(expected_checked_at)
        checked_at = str(checked_at or datetime.now())
        if not operation_id or min(stamina_cost, battle_limit, expected_stamina, expected_hp, expected_exp, expected_battle_count) < 0:
            raise ValueError("valid operation and non-negative battle state are required")
        payload = json.dumps(
            [user_id, stamina_cost, battle_limit, expected_stamina, expected_hp, expected_exp,
             expected_battle_count, expected_checked_at],
            ensure_ascii=True,
        )

        def rejected(status: str) -> BossBattleCostResult:
            return BossBattleCostResult(status, expected_stamina, expected_battle_count, expected_checked_at)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS boss_battle_cost_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stamina INTEGER NOT NULL, "
                    "battle_count INTEGER NOT NULL, checked_at TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, stamina, battle_count, checked_at FROM boss_battle_cost_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return BossBattleCostResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(user_stamina, 0), COALESCE(hp, 0), COALESCE(exp, 0) "
                    "FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cooldown = conn.execute(
                    "SELECT COALESCE(last_check_info_time, '') FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name='boss'"
                ).fetchone()
                columns = ({str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(boss)").fetchall()}
                           if table else set())
                if user is None or cooldown is None:
                    conn.rollback()
                    return rejected("user_missing")
                if "boss_battle_count" not in columns:
                    conn.rollback()
                    return rejected("state_changed")
                count_row = conn.execute(
                    "SELECT COALESCE(boss_battle_count, 0) FROM player_data.boss WHERE user_id=%s", (user_id,)
                ).fetchone()
                current = (int(user[0]), int(user[1]), int(user[2]), int(count_row[0]) if count_row else 0, str(cooldown[0]))
                expected = (expected_stamina, expected_hp, expected_exp, expected_battle_count, expected_checked_at)
                if current != expected:
                    conn.rollback()
                    return rejected("state_changed")
                if expected_stamina < stamina_cost:
                    conn.rollback()
                    return rejected("stamina_insufficient")
                if expected_hp <= expected_exp / 10:
                    conn.rollback()
                    return rejected("hp_insufficient")
                if expected_battle_count >= battle_limit:
                    conn.rollback()
                    return rejected("limit_reached")

                stamina = expected_stamina - stamina_cost
                battle_count = expected_battle_count + 1
                if conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s AND COALESCE(user_stamina, 0)=%s "
                    "AND COALESCE(hp, 0)=%s AND COALESCE(exp, 0)=%s",
                    (stamina, user_id, expected_stamina, expected_hp, expected_exp),
                ).rowcount != 1:
                    conn.rollback()
                    return rejected("state_changed")
                if conn.execute(
                    "UPDATE user_cd SET last_check_info_time=%s WHERE user_id=%s AND COALESCE(last_check_info_time, '')=%s",
                    (checked_at, user_id, expected_checked_at),
                ).rowcount != 1:
                    conn.rollback()
                    return rejected("state_changed")
                if count_row is None:
                    conn.execute(
                        "INSERT INTO player_data.boss (user_id, boss_battle_count) VALUES (%s, %s)",
                        (user_id, battle_count),
                    )
                elif conn.execute(
                    "UPDATE player_data.boss SET boss_battle_count=%s WHERE user_id=%s AND COALESCE(boss_battle_count, 0)=%s",
                    (battle_count, user_id, expected_battle_count),
                ).rowcount != 1:
                    conn.rollback()
                    return rejected("state_changed")
                conn.execute(
                    "INSERT INTO boss_battle_cost_operations VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, stamina, battle_count, checked_at),
                )
                conn.commit()
                return BossBattleCostResult("applied", stamina, battle_count, checked_at)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BossBattleCostResult", "BossBattleCostService"]
