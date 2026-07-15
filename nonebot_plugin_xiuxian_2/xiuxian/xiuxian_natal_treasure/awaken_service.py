from __future__ import annotations

import random
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AwakenResult:
    status: str
    form: int
    name: str
    effect_type: int
    base_value: float

    @property
    def succeeded(self) -> bool:
        return self.status in {"awakened", "duplicate"}


class AwakenService:
    """Create the first natal treasure state atomically and idempotently."""

    def __init__(self, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> AwakenResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_awaken_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, max_slots INTEGER NOT NULL, "
                "choice_seed INTEGER NOT NULL, form INTEGER NOT NULL, name TEXT NOT NULL, "
                "effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
            )
            previous = conn.execute(
                "SELECT form, name, effect_type, base_value FROM natal_awaken_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return AwakenResult("duplicate", int(previous[0]), str(previous[1]), int(previous[2]), float(previous[3]))

    def awaken(self, operation_id, user_id, max_slots, effect_configs,
               effect_names, fixed_base_effects, choice_seed) -> AwakenResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        max_slots = int(max_slots)
        choice_seed = int(choice_seed)
        configs = {
            int(key): (float(value[0]), float(value[1]))
            for key, value in effect_configs.items()
        }
        names = {
            int(key): tuple(str(name) for name in value)
            for key, value in effect_names.items()
        }
        fixed = {int(value) for value in fixed_base_effects}
        if (not operation_id or max_slots <= 0 or not configs
                or any(not names.get(effect_type) for effect_type in configs)):
            raise ValueError("valid operation and awaken parameters are required")

        def result(status, form=0, name="", effect_type=0, base_value=0.0):
            return AwakenResult(
                status, int(form), str(name), int(effect_type), float(base_value),
            )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_awaken_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, max_slots INTEGER NOT NULL, "
                    "choice_seed INTEGER NOT NULL, form INTEGER NOT NULL, name TEXT NOT NULL, "
                    "effect_type INTEGER NOT NULL, base_value REAL NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, max_slots, choice_seed, form, name, effect_type, base_value "
                    "FROM natal_awaken_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user_id only; seed/outcome stored in op row.
                    if str(previous[0]) != user_id:
                        return result("state_changed")
                    return result("duplicate", *previous[3:])

                required = {
                    "form", "name", "level", "exp", "max_exp",
                    "fate_revive_count", "immortal_revive_count",
                    "invincible_gain_count", "nirvana_revive_count",
                    "soul_return_revive_count", "charge_status",
                    "soul_summon_count", "enlightenment_count",
                }
                for slot in range(1, max_slots + 1):
                    required.update((
                        f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level",
                    ))
                if not conn.table_exists("natal_treasure"):
                    conn.rollback()
                    return result("treasure_missing")
                columns = set(conn.column_names("natal_treasure"))
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                current = conn.execute(
                    "SELECT form FROM natal_treasure WHERE user_id=%s", (user_id,),
                ).fetchone()
                if current is None:
                    conn.rollback()
                    return result("treasure_missing")
                if int(current[0] or 0) != 0:
                    conn.rollback()
                    return result("already_awakened")

                rng = random.Random(choice_seed)
                form = rng.randint(1, 4)
                effect_type = rng.choice(sorted(configs))
                name = rng.choice(names[effect_type])
                minimum, maximum = configs[effect_type]
                base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)
                assignments = [
                    ("form", form), ("name", name), ("level", 0),
                    ("exp", 0), ("max_exp", 100),
                    ("fate_revive_count", 0), ("immortal_revive_count", 0),
                    ("invincible_gain_count", 0), ("nirvana_revive_count", 0),
                    ("soul_return_revive_count", 0), ("charge_status", 0),
                    ("soul_summon_count", "{}"), ("enlightenment_count", "{}"),
                ]
                for slot in range(1, max_slots + 1):
                    assignments.extend((
                        (f"effect{slot}_type", effect_type if slot == 1 else 0),
                        (f"effect{slot}_base_value", base_value if slot == 1 else 0.0),
                        (f"effect{slot}_level", 1 if slot == 1 else 0),
                    ))
                updated = conn.execute(
                    "UPDATE natal_treasure SET " + ", ".join(
                        f"{db_backend.quote_ident(field)}=%s" for field, _ in assignments
                    ) + " WHERE user_id=%s AND COALESCE(form, 0)=0",
                    tuple(value for _, value in assignments) + (user_id,),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_awaken_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, max_slots, choice_seed,
                     form, name, effect_type, base_value),
                )
                conn.commit()
                return result("awakened", form, name, effect_type, base_value)
            except Exception:
                conn.rollback()
                raise


__all__ = ["AwakenResult", "AwakenService"]
