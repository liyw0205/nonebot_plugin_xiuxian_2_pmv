from __future__ import annotations

import random
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class EngravingResult:
    status: str
    slot: int
    effect_type: int
    base_value: float

    @property
    def succeeded(self) -> bool:
        return self.status in {"engraved", "duplicate"}


class EngravingService:
    """Consume a scripture and engrave a distinct effect atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> EngravingResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_engraving_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                "slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
            )
            previous = conn.execute(
                "SELECT slot, effect_type, base_value FROM natal_engraving_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return EngravingResult("duplicate", int(previous[0]), int(previous[1]), float(previous[2]))

    def engrave(self, operation_id, user_id, scripture_id, scripture_cost,
                max_slots, effect_configs, fixed_base_effects, choice_seed) -> EngravingResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        scripture_id = int(scripture_id)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        choice_seed = int(choice_seed)
        configs = {
            int(key): (float(value[0]), float(value[1]))
            for key, value in effect_configs.items()
        }
        fixed = {int(value) for value in fixed_base_effects}
        if not operation_id or scripture_cost <= 0 or max_slots <= 0 or not configs:
            raise ValueError("valid operation, positive cost, slots and effect configs are required")

        def result(status, slot=0, effect_type=0, base_value=0.0):
            return EngravingResult(status, int(slot), int(effect_type), float(base_value))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_engraving_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                    "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                    "slot INTEGER NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, scripture_id, scripture_cost, max_slots, choice_seed, slot, effect_type, "
                    "base_value FROM natal_engraving_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != scripture_id or int(previous[2]) != scripture_cost:
                        return result("state_changed")
                    return result("duplicate", *previous[5:])

                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                required = {"form"}
                for slot in range(1, max_slots + 1):
                    required.update({f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level"})
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                type_fields = [f"effect{slot}_type" for slot in range(1, max_slots + 1)]
                treasure = conn.execute(
                    "SELECT form, " + ", ".join(db_backend.quote_ident(field) for field in type_fields) +
                    " FROM player_data.natal_treasure WHERE user_id=%s", (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")
                existing = {int(value) for value in treasure[1:] if int(value or 0) > 0}
                empty_slots = [index for index, value in enumerate(treasure[1:], 1) if int(value or 0) == 0]
                if not empty_slots:
                    conn.rollback()
                    return result("slots_full")
                available = sorted(set(configs) - existing)
                if not available:
                    conn.rollback()
                    return result("effect_exhausted")
                rng = random.Random(choice_seed)
                slot = empty_slots[0]
                effect_type = rng.choice(available)
                minimum, maximum = configs[effect_type]
                base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                if item is None or int(item[0]) < scripture_cost:
                    conn.rollback()
                    return result("item_insufficient", slot, effect_type, base_value)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=CAST(goods_num AS INTEGER)-%s "
                    "WHERE user_id=%s AND goods_id=%s AND CAST(COALESCE(goods_num,0) AS INTEGER)>=%s",
                    (scripture_cost, user_id, scripture_id, scripture_cost),
                )
                type_field = db_backend.quote_ident(f"effect{slot}_type")
                base_field = db_backend.quote_ident(f"effect{slot}_base_value")
                level_field = db_backend.quote_ident(f"effect{slot}_level")
                engraved = conn.execute(
                    f"UPDATE player_data.natal_treasure SET "
                    f"{type_field}=%s, {base_field}=%s, {level_field}=1 "
                    f"WHERE user_id=%s AND CAST(COALESCE({type_field}, 0) AS INTEGER)=0",
                    (effect_type, base_value, user_id),
                )
                if consumed.rowcount != 1 or engraved.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_engraving_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, scripture_id, scripture_cost, max_slots,
                     choice_seed, slot, effect_type, base_value),
                )
                conn.commit()
                return result("engraved", slot, effect_type, base_value)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["EngravingResult", "EngravingService"]
