from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


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


class BlessedFlagReplaceSqlRepository:
    """Atomically replace a blessed-spot flag across game and player DBs."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _payload(values: list[object]) -> str:
        return json.dumps(values, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _saved(status: str, data: dict[str, object]) -> BlessedFlagReplaceResult:
        return BlessedFlagReplaceResult(
            status,
            str(data["user_id"]),
            int(data["item_id"]),
            int(data["previous_level"]),
            int(data["current_level"]),
            int(data["herb_speed"]),
            int(data["quantity"]),
        )

    def replace(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        target_level: int,
        herb_speed: int,
        *,
        expected_level: int,
        expected_herb_speed: int,
        expected_quantity: int,
    ) -> BlessedFlagReplaceResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id, target_level, herb_speed = int(item_id), int(target_level), int(herb_speed)
        expected_level = int(expected_level)
        expected_herb_speed = int(expected_herb_speed)
        expected_quantity = int(expected_quantity)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if item_id <= 0 or min(target_level, herb_speed, expected_quantity) < 0:
            raise ValueError("item, level, speed and quantity must be valid")
        payload = self._payload(
            [
                user_id,
                item_id,
                target_level,
                herb_speed,
                expected_level,
                expected_herb_speed,
                expected_quantity,
            ]
        )

        def result(status: str, previous_level: int = expected_level, quantity: int = 0):
            return BlessedFlagReplaceResult(
                status,
                user_id,
                item_id,
                int(previous_level),
                target_level,
                herb_speed,
                int(quantity),
            )

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            previous = uow.query_one(
                "SELECT payload,result_json FROM blessed_flag_replace_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return result("state_changed")
                return self._saved("duplicate", json.loads(str(previous["result_json"])))

            user = uow.query_one(
                "SELECT COALESCE(blessed_spot_flag,0) AS enabled "
                "FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return result("user_missing")
            if int(user["enabled"] or 0) == 0:
                return result("blessed_spot_missing")

            buff = uow.query_one(
                "SELECT COALESCE(blessed_spot,0) AS level FROM BuffInfo WHERE user_id=?",
                (user_id,),
            )
            if buff is None:
                return result("buff_missing")
            current_level = int(buff["level"] or 0)
            if current_level != expected_level:
                return result("state_changed", current_level)
            if target_level < current_level:
                return result("downgrade", current_level)
            if target_level == current_level:
                return result("same_level", current_level)

            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS quantity FROM back "
                "WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            current_quantity = int(inventory["quantity"] or 0) if inventory else 0
            if current_quantity != expected_quantity:
                return result("state_changed", current_level)
            if current_quantity < 1:
                return result("item_missing", current_level)

            speed_column = '"药材速度"'
            speed_row = uow.query_one(
                f"SELECT {speed_column} AS herb_speed FROM player_data.mix_elixir_info "
                "WHERE user_id=?",
                (user_id,),
            )
            if speed_row is None:
                return result("mix_elixir_missing", current_level)
            if int(speed_row["herb_speed"] or 0) != expected_herb_speed:
                return result("state_changed", current_level)

            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
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
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(updates)} WHERE user_id=? "
                "AND goods_id=? AND goods_num=? AND goods_num>=1",
                (user_id, item_id, expected_quantity),
            )
            level_updated = uow.execute(
                "UPDATE BuffInfo SET blessed_spot=? WHERE user_id=? "
                "AND COALESCE(blessed_spot,0)=?",
                (target_level, user_id, expected_level),
            )
            speed_updated = uow.execute(
                f"UPDATE player_data.mix_elixir_info SET {speed_column}=? "
                f"WHERE user_id=? AND CAST(COALESCE({speed_column},0) AS INTEGER)=?",
                (str(herb_speed), user_id, expected_herb_speed),
            )
            if any(change.rowcount != 1 for change in (consumed, level_updated, speed_updated)):
                return result("state_changed", current_level)

            saved = {
                "user_id": user_id,
                "item_id": item_id,
                "previous_level": current_level,
                "current_level": target_level,
                "herb_speed": herb_speed,
                "quantity": 1,
            }
            uow.execute(
                "INSERT INTO blessed_flag_replace_operations "
                "(operation_id,payload,result_json) VALUES(?,?,?)",
                (operation_id, payload, json.dumps(saved, ensure_ascii=True)),
            )
            return self._saved("applied", saved)


__all__ = ["BlessedFlagReplaceResult", "BlessedFlagReplaceSqlRepository"]
