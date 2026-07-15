from __future__ import annotations

import random
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class EffectUpgradeResult:
    status: str
    slot: int
    effect_type: int
    level: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"upgraded", "duplicate"}


class EffectUpgradeService:
    """Consume a scripture and upgrade one lowest-level effect atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> EffectUpgradeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_effect_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, max_effect_level INTEGER NOT NULL, "
                "choice_seed INTEGER NOT NULL, slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, "
                "level INTEGER NOT NULL)"
            )
            previous = conn.execute(
                "SELECT slot, effect_type, level FROM natal_effect_upgrade_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return EffectUpgradeResult("duplicate", int(previous[0]), int(previous[1]), int(previous[2]))

    def upgrade(self, operation_id, user_id, scripture_id, scripture_cost,
                max_slots, max_effect_level, choice_seed) -> EffectUpgradeResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        scripture_id = int(scripture_id)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        max_effect_level = int(max_effect_level)
        choice_seed = int(choice_seed)
        if not operation_id or scripture_cost <= 0 or max_slots <= 0 or max_effect_level <= 0:
            raise ValueError("valid operation and positive upgrade parameters are required")

        def result(status, slot=0, effect_type=0, level=0):
            return EffectUpgradeResult(status, int(slot), int(effect_type), int(level))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_effect_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                    "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, max_effect_level INTEGER NOT NULL, "
                    "choice_seed INTEGER NOT NULL, slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, "
                    "level INTEGER NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, scripture_id, scripture_cost, max_slots, max_effect_level, choice_seed, "
                    "slot, effect_type, level FROM natal_effect_upgrade_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user + scripture cost knobs; seed/outcome in op row.
                    if str(previous[0]) != user_id or int(previous[1]) != scripture_id or int(previous[2]) != scripture_cost:
                        return result("state_changed")
                    return result("duplicate", *previous[6:])

                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                required = {"form"}
                for slot in range(1, max_slots + 1):
                    required.update({f"effect{slot}_type", f"effect{slot}_level"})
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                fields = ["form"] + [
                    field for slot in range(1, max_slots + 1)
                    for field in (f"effect{slot}_type", f"effect{slot}_level")
                ]
                treasure = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields) +
                    " FROM player_data.natal_treasure WHERE user_id=%s", (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")
                candidates = []
                for slot in range(1, max_slots + 1):
                    effect_type = int(treasure[1 + (slot - 1) * 2] or 0)
                    level = int(treasure[2 + (slot - 1) * 2] or 0)
                    if effect_type > 0 and level < max_effect_level:
                        candidates.append((slot, effect_type, level))
                if not candidates:
                    conn.rollback()
                    return result("all_maxed")
                min_level = min(item[2] for item in candidates)
                lowest = [item for item in candidates if item[2] == min_level]
                slot, effect_type, level = random.Random(choice_seed).choice(lowest)
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                if item is None or int(item[0]) < scripture_cost:
                    conn.rollback()
                    return result("item_insufficient", slot, effect_type, level)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (scripture_cost, user_id, scripture_id, scripture_cost),
                )
                upgraded = conn.execute(
                    f"UPDATE player_data.natal_treasure SET {db_backend.quote_ident(f'effect{slot}_level')}=%s "
                    f"WHERE user_id=%s AND {db_backend.quote_ident(f'effect{slot}_level')}=%s",
                    (level + 1, user_id, level),
                )
                if consumed.rowcount != 1 or upgraded.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_effect_upgrade_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, scripture_id, scripture_cost, max_slots,
                     max_effect_level, choice_seed, slot, effect_type, level + 1),
                )
                conn.commit()
                return result("upgraded", slot, effect_type, level + 1)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["EffectUpgradeResult", "EffectUpgradeService"]
