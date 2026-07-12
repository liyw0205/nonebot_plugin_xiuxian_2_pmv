from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ForgetEffectResult:
    status: str
    slot: int
    effect_type: int
    effect_level: int
    scripture_change: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"forgotten", "duplicate"}


class ForgetEffectService:
    """Forget one effect and settle its scripture change atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def forget(self, operation_id, user_id, effect_type, scripture_id,
               scripture_name, scripture_type, scripture_cost,
               max_slots, max_goods_num) -> ForgetEffectResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        effect_type = int(effect_type)
        scripture_id = int(scripture_id)
        scripture_name = str(scripture_name)
        scripture_type = str(scripture_type)
        scripture_cost = int(scripture_cost)
        max_slots = int(max_slots)
        max_goods_num = int(max_goods_num)
        if (not operation_id or effect_type <= 0 or scripture_cost < 0
                or max_slots <= 1 or max_goods_num <= 0):
            raise ValueError("valid operation and forget parameters are required")

        def result(status, slot=0, level=0, change=0):
            return ForgetEffectResult(
                status, int(slot), effect_type, int(level), int(change),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_forget_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, effect_type INTEGER NOT NULL, "
                    "scripture_id INTEGER NOT NULL, scripture_cost INTEGER NOT NULL, max_slots INTEGER NOT NULL, "
                    "slot INTEGER NOT NULL, effect_level INTEGER NOT NULL, scripture_change INTEGER NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT user_id, effect_type, scripture_id, scripture_cost, max_slots, slot, "
                    "effect_level, scripture_change FROM natal_forget_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    request = (user_id, effect_type, scripture_id, scripture_cost, max_slots)
                    recorded = (str(previous[0]), *(int(value) for value in previous[1:5]))
                    if recorded != request:
                        return result("state_changed")
                    return result("duplicate", previous[5], previous[6], previous[7])

                required = {"form"}
                fields = ["form"]
                for slot in range(1, max_slots + 1):
                    slot_fields = (
                        f"effect{slot}_type", f"effect{slot}_base_value", f"effect{slot}_level",
                    )
                    required.update(slot_fields)
                    fields.extend(slot_fields)
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

                occupied = 0
                target_slot = 0
                target_level = 0
                for slot in range(1, max_slots + 1):
                    offset = 1 + (slot - 1) * 3
                    current_type = int(treasure[offset] or 0)
                    if current_type > 0:
                        occupied += 1
                    if current_type == effect_type:
                        target_slot = slot
                        target_level = int(treasure[offset + 2] or 0)
                if target_slot == 0:
                    conn.rollback()
                    return result("effect_missing")
                if occupied <= 1:
                    conn.rollback()
                    return result("last_effect")

                scripture_change = max(0, target_level - 1) - scripture_cost
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, scripture_id),
                ).fetchone()
                current_quantity = int(item[0] or 0) if item else 0
                if scripture_change < 0 and current_quantity < -scripture_change:
                    conn.rollback()
                    return result("item_insufficient", target_slot, target_level, scripture_change)
                if scripture_change > 0 and current_quantity + scripture_change > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", target_slot, target_level, scripture_change)

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

                cleared = conn.execute(
                    f"UPDATE player_data.natal_treasure SET "
                    f"{db_backend.quote_ident(f'effect{target_slot}_type')}=0, "
                    f"{db_backend.quote_ident(f'effect{target_slot}_base_value')}=0.0, "
                    f"{db_backend.quote_ident(f'effect{target_slot}_level')}=0 "
                    f"WHERE user_id=%s AND {db_backend.quote_ident(f'effect{target_slot}_type')}=%s "
                    f"AND {db_backend.quote_ident(f'effect{target_slot}_level')}=%s",
                    (user_id, effect_type, target_level),
                )
                if cleared.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_forget_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, effect_type, scripture_id, scripture_cost,
                     max_slots, target_slot, target_level, scripture_change),
                )
                conn.commit()
                return result("forgotten", target_slot, target_level, scripture_change)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["ForgetEffectResult", "ForgetEffectService"]
