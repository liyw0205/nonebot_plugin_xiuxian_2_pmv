from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BlessedFlagReplaceResult:
    status: str
    user_id: str
    item_id: int
    previous_level: int = 0
    current_level: int = 0
    herb_speed: int = 0
    quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BlessedFlagReplaceService:
    """Replace a blessed-spot flag in one cross-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(values) -> str:
        return json.dumps(values, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blessed_flag_replace_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _saved(status: str, data: dict) -> BlessedFlagReplaceResult:
        return BlessedFlagReplaceResult(
            status, str(data["user_id"]), int(data["item_id"]),
            int(data["previous_level"]), int(data["current_level"]),
            int(data["herb_speed"]), int(data["quantity"]),
        )

    def replace(self, operation_id, user_id, item_id, target_level, herb_speed, *,
                expected_level, expected_herb_speed,
                expected_quantity) -> BlessedFlagReplaceResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id, target_level, herb_speed = int(item_id), int(target_level), int(herb_speed)
        expected_level = int(expected_level)
        expected_herb_speed = int(expected_herb_speed)
        expected_quantity = int(expected_quantity)
        if item_id <= 0 or min(target_level, herb_speed, expected_quantity) < 0:
            raise ValueError("item, level, speed and quantity must be valid")
        payload = self._payload([
            user_id, item_id, target_level, herb_speed, expected_level,
            expected_herb_speed, expected_quantity,
        ])

        def result(status, previous_level=expected_level, quantity=0):
            return BlessedFlagReplaceResult(
                status, user_id, item_id, int(previous_level), target_level,
                herb_speed, int(quantity),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM blessed_flag_replace_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return self._saved("duplicate", json.loads(str(previous[1])))

                user = conn.execute(
                    "SELECT COALESCE(blessed_spot_flag,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if int(user[0] or 0) == 0:
                    conn.rollback()
                    return result("blessed_spot_missing")
                buff = conn.execute(
                    "SELECT COALESCE(blessed_spot,0) FROM BuffInfo WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return result("buff_missing")
                current_level = int(buff[0] or 0)
                if current_level != expected_level:
                    conn.rollback()
                    return result("state_changed", current_level)
                if target_level < current_level:
                    conn.rollback()
                    return result("downgrade", current_level)
                if target_level == current_level:
                    conn.rollback()
                    return result("same_level", current_level)

                inventory = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                current_quantity = int(inventory[0] or 0) if inventory else 0
                if current_quantity != expected_quantity:
                    conn.rollback()
                    return result("state_changed", current_level)
                if current_quantity < 1:
                    conn.rollback()
                    return result("item_missing", current_level)
                try:
                    speed_row = conn.execute(
                        f"SELECT {db_backend.quote_ident('药材速度')} FROM "
                        "player_data.mix_elixir_info WHERE user_id=%s", (user_id,),
                    ).fetchone()
                except db_backend.OperationalError:
                    conn.rollback()
                    return result("mix_elixir_missing", current_level)
                if speed_row is None:
                    conn.rollback()
                    return result("mix_elixir_missing", current_level)
                if int(speed_row[0] or 0) != expected_herb_speed:
                    conn.rollback()
                    return result("state_changed", current_level)

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-1"]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                        "WHEN COALESCE(bind_num,0)>=1 THEN COALESCE(bind_num,0)-1 "
                        "ELSE MIN(COALESCE(bind_num,0),goods_num-1) END"
                    )
                if "update_time" in columns:
                    updates.append("update_time=CURRENT_TIMESTAMP")
                if "action_time" in columns:
                    updates.append("action_time=CURRENT_TIMESTAMP")
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} WHERE user_id=%s "
                    "AND goods_id=%s AND goods_num=%s AND goods_num>=1",
                    (user_id, item_id, expected_quantity),
                )
                level_updated = conn.execute(
                    "UPDATE BuffInfo SET blessed_spot=%s WHERE user_id=%s "
                    "AND COALESCE(blessed_spot,0)=%s",
                    (target_level, user_id, expected_level),
                )
                speed_column = db_backend.quote_ident("药材速度")
                speed_updated = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {speed_column}=%s "
                    f"WHERE user_id=%s AND CAST(COALESCE({speed_column},0) AS INTEGER)=%s",
                    (str(herb_speed), user_id, expected_herb_speed),
                )
                if any(change.rowcount != 1 for change in
                       (consumed, level_updated, speed_updated)):
                    conn.rollback()
                    return result("state_changed", current_level)
                saved = {
                    "user_id": user_id, "item_id": item_id,
                    "previous_level": current_level, "current_level": target_level,
                    "herb_speed": herb_speed, "quantity": 1,
                }
                conn.execute(
                    "INSERT INTO blessed_flag_replace_operations "
                    "(operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, ensure_ascii=True)),
                )
                conn.commit()
                return self._saved("applied", saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["BlessedFlagReplaceResult", "BlessedFlagReplaceService"]
