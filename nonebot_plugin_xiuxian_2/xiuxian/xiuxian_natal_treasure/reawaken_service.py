from __future__ import annotations

import random
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ReawakenResult:
    status: str
    form: int
    name: str
    effect_type: int
    base_value: float
    scripture_change: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"reawakened", "duplicate"}


class ReawakenService:
    """Reset a natal treasure and settle scripture refunds atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def reawaken(self, operation_id, user_id, scripture_id, scripture_name,
                 scripture_type, scripture_cost, max_slots, max_goods_num,
                 effect_configs, effect_names, fixed_base_effects,
                 choice_seed) -> ReawakenResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        scripture_id = int(scripture_id)
        scripture_name = str(scripture_name)
        scripture_type = str(scripture_type)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        max_goods_num = int(max_goods_num)
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
        if (not operation_id or scripture_cost < 0 or max_slots <= 0
                or max_goods_num <= 0 or not configs
                or any(not names.get(effect_type) for effect_type in configs)):
            raise ValueError("valid operation and reawaken parameters are required")

        def result(status, form=0, name="", effect_type=0,
                   base_value=0.0, change=0):
            return ReawakenResult(
                status, int(form), str(name), int(effect_type),
                float(base_value), int(change),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_reawaken_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, scripture_id INTEGER NOT NULL, "
                    "scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, choice_seed INTEGER NOT NULL, "
                    "form INTEGER NOT NULL, "
                    "name TEXT NOT NULL, effect_type INTEGER NOT NULL, base_value REAL NOT NULL, "
                    "scripture_change INTEGER NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, scripture_id, scripture_cost, max_slots, choice_seed, form, name, effect_type, "
                    "base_value, scripture_change FROM natal_reawaken_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    request = (user_id, scripture_id, scripture_cost, max_slots, choice_seed)
                    recorded = (str(previous[0]), *(int(value) for value in previous[1:5]))
                    if recorded != request:
                        return result("state_changed")
                    return result("duplicate", *previous[5:])

                required = {
                    "form", "name", "level", "exp", "max_exp",
                    "fate_revive_count", "immortal_revive_count",
                    "invincible_gain_count", "nirvana_revive_count",
                    "soul_return_revive_count", "charge_status",
                    "soul_summon_count", "enlightenment_count",
                }
                fields = ["form"]
                for slot in range(1, max_slots + 1):
                    slot_fields = (
                        f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level",
                    )
                    required.update(slot_fields)
                    fields.append(f"effect{slot}_level")
                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                if not required.issubset(columns):
                    conn.rollback()
                    return result("treasure_missing")
                treasure = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.natal_treasure WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if treasure is None or int(treasure[0] or 0) == 0:
                    conn.rollback()
                    return result("treasure_missing")

                refund = sum(max(0, int(level or 0) - 1) for level in treasure[1:])
                scripture_change = refund - scripture_cost
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                current_quantity = int(item[0] or 0) if item else 0
                if scripture_change < 0 and current_quantity < -scripture_change:
                    conn.rollback()
                    return result("item_insufficient", change=scripture_change)
                if scripture_change > 0 and current_quantity + scripture_change > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", change=scripture_change)

                rng = random.Random(choice_seed)
                form = rng.randint(1, 4)
                effect_type = rng.choice(sorted(configs))
                name = rng.choice(names[effect_type])
                minimum, maximum = configs[effect_type]
                base_value = minimum if effect_type in fixed else round(rng.uniform(minimum, maximum), 3)

                if scripture_change < 0:
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s "
                        "AND goods_num>=%s",
                        (-scripture_change, user_id, scripture_id, -scripture_change),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                elif scripture_change > 0:
                    if item is None:
                        back_columns = set(conn.column_names("back"))
                        insert_columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                        insert_values = [user_id, scripture_id, scripture_name, scripture_type, scripture_change]
                        if "bind_num" in back_columns:
                            insert_columns.append("bind_num")
                            insert_values.append(0)
                        conn.execute(
                            "INSERT INTO back (" + ", ".join(
                                db_backend.quote_ident(column) for column in insert_columns
                            ) + ") VALUES (" + ", ".join("%s" for _ in insert_columns) + ")",
                            tuple(insert_values),
                        )
                    else:
                        changed = conn.execute(
                            "UPDATE back SET goods_num=goods_num+%s WHERE user_id=%s AND goods_id=%s "
                            "AND goods_num+%s<=%s",
                            (scripture_change, user_id, scripture_id, scripture_change, max_goods_num),
                        )
                        if changed.rowcount != 1:
                            conn.rollback()
                            return result("state_changed")

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
                    "UPDATE player_data.natal_treasure SET " + ", ".join(
                        f"{db_backend.quote_ident(field)}=%s" for field, _ in assignments
                    ) + " WHERE user_id=%s AND form=%s",
                    tuple(value for _, value in assignments) + (user_id, int(treasure[0])),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_reawaken_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, scripture_id, scripture_cost, max_slots,
                     choice_seed, form, name, effect_type, base_value, scripture_change),
                )
                conn.commit()
                return result("reawakened", form, name, effect_type, base_value, scripture_change)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["ReawakenResult", "ReawakenService"]
