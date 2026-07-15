from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MixelixirHarvestLevelUpgradeResult:
    status: str
    cost: int = 0
    wallet_stone: int = 0
    level: int = 0
    experience: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MixelixirHarvestLevelUpgradeService:
    """Atomically charge stones and upgrade the herb harvest level."""

    _LEVEL_FIELD = "收取等级"
    _EXPERIENCE_FIELD = "炼丹经验"

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> MixelixirHarvestLevelUpgradeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_harvest_level_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,cost,wallet_stone,level,experience FROM mixelixir_harvest_level_upgrade_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirHarvestLevelUpgradeResult(
                "duplicate", int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
            )

    def upgrade(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_experience,
        expected_stone,
        next_level,
        cost,
    ) -> MixelixirHarvestLevelUpgradeResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, expected_experience, expected_stone, next_level, cost = map(
            int, (expected_level, expected_experience, expected_stone, next_level, cost)
        )
        if (
            not operation_id
            or min(expected_level, expected_experience, expected_stone) < 0
            or next_level != expected_level + 1
            or cost <= 0
        ):
            raise ValueError("valid operation, state snapshot, next level and cost are required")
        # Request identity only — expected level/exp/stone are concurrency checks.
        payload = json.dumps(
            [user_id, next_level, cost], ensure_ascii=True, separators=(",", ":"),
        )

        def result(status, *, stone=expected_stone, level=expected_level):
            return MixelixirHarvestLevelUpgradeResult(status, 0, stone, level, expected_experience)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_harvest_level_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                    "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,cost,wallet_stone,level,experience "
                    "FROM mixelixir_harvest_level_upgrade_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return MixelixirHarvestLevelUpgradeResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
                    )

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing", stone=0)

                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s",
                    ("mix_elixir_info",),
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return result("state_changed", stone=int(user[0]))
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if not {self._LEVEL_FIELD, self._EXPERIENCE_FIELD}.issubset(columns):
                    conn.rollback()
                    return result("state_changed", stone=int(user[0]))

                quoted_level = db_backend.quote_ident(self._LEVEL_FIELD)
                quoted_experience = db_backend.quote_ident(self._EXPERIENCE_FIELD)
                mix_state = conn.execute(
                    f"SELECT {quoted_level},{quoted_experience} FROM player_data.mix_elixir_info WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_stone = int(user[0])
                if (
                    mix_state is None
                    or current_stone != expected_stone
                    or (int(mix_state[0] or 0), int(mix_state[1] or 0))
                    != (expected_level, expected_experience)
                ):
                    conn.rollback()
                    return result("state_changed", stone=current_stone)
                if current_stone < cost:
                    conn.rollback()
                    return result("stone_insufficient", stone=current_stone)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone=%s AND stone>=%s",
                    (cost, user_id, expected_stone, cost),
                )
                upgraded = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {quoted_level}=%s "
                    f"WHERE user_id=%s AND CAST({quoted_level} AS INTEGER)=%s "
                    f"AND CAST({quoted_experience} AS INTEGER)=%s",
                    (str(next_level), user_id, expected_level, expected_experience),
                )
                if charged.rowcount != 1 or upgraded.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", stone=current_stone)

                wallet_stone = expected_stone - cost
                conn.execute(
                    "INSERT INTO mixelixir_harvest_level_upgrade_operations "
                    "(operation_id,payload,cost,wallet_stone,level,experience) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, cost, wallet_stone, next_level, expected_experience),
                )
                conn.commit()
                return MixelixirHarvestLevelUpgradeResult(
                    "applied", cost, wallet_stone, next_level, expected_experience
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["MixelixirHarvestLevelUpgradeResult", "MixelixirHarvestLevelUpgradeService"]
