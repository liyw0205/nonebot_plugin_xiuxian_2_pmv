from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_wash_domain import AccessoryWashChange


class AccessoryWashSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode(value: Any, expected_type: type) -> Any:
        if not value:
            return expected_type()
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, expected_type) else expected_type()

    @classmethod
    def _result(cls, status: str, result_json: str) -> AccessoryWashChange:
        value = json.loads(result_json)
        return AccessoryWashChange(status, str(value.get("action", "wash")), str(value.get("user_id", "")), int(value.get("affected", 0)), int(value.get("stone_delta", 0)), value.get("accessory"))

    def replay(self, operation_id: str) -> AccessoryWashChange | None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one("SELECT action,result_json FROM accessory_transaction_operations WHERE operation_id=?", (str(operation_id).strip(),))
            if row is None or str(row["action"]) != "wash":
                return None
            return self._result("duplicate", str(row["result_json"]))

    def wash(self, operation_id: str, user_id: str, uid: str, expected_accessory: dict[str, Any], expected_stones: int, stone_id: int, stone_cost: int, updated_accessory: dict[str, Any]) -> AccessoryWashChange:
        operation_id, user_id, uid = str(operation_id).strip(), str(user_id), str(uid)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = {"user_id": user_id, "uid": uid, "expected_accessory": expected_accessory, "expected_stones": int(expected_stones), "stone_id": int(stone_id), "stone_cost": int(stone_cost)}
        payload_json = self._json(payload)
        with AttachedDatabaseUnitOfWork(self.game_database, attachments={"player_data": self.player_database}, immediate=True) as uow:
            previous = uow.query_one("SELECT action,payload,result_json FROM accessory_transaction_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["action"]) != "wash" or str(previous["payload"]) != payload_json:
                    return AccessoryWashChange("state_changed", "wash", user_id)
                return self._result("duplicate", str(previous["result_json"]))
            row = uow.query_one("SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?", (user_id,))
            if row is None:
                return AccessoryWashChange("accessory_missing", "wash", user_id)
            equipped, bag = self._decode(row["equipped"], dict), self._decode(row["bag"], list)
            where, key, current = None, None, None
            for index, item in enumerate(bag):
                if isinstance(item, dict) and str(item.get("uid", "")) == uid:
                    where, key, current = "bag", index, item
                    break
            if current is None:
                for slot, item in equipped.items():
                    if isinstance(item, dict) and str(item.get("uid", "")) == uid:
                        where, key, current = "equipped", slot, item
                        break
            if current is None:
                return AccessoryWashChange("accessory_missing", "wash", user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryWashChange("state_changed", "wash", user_id)
            stones = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, int(stone_id)))
            if stones is None or int(stones["goods_num"] or 0) != int(expected_stones):
                return AccessoryWashChange("state_changed", "wash", user_id)
            changed = uow.execute("UPDATE back SET goods_num=goods_num-?,bind_num=MIN(COALESCE(bind_num,0),goods_num-?) WHERE user_id=? AND goods_id=? AND goods_num>=?", (int(stone_cost), int(stone_cost), user_id, int(stone_id), int(stone_cost)))
            if changed.rowcount != 1:
                return AccessoryWashChange("item_insufficient", "wash", user_id)
            if where == "bag":
                bag[key] = updated_accessory
            else:
                equipped[key] = updated_accessory
            uow.execute("UPDATE player_data.player_accessory SET equipped=?,bag=? WHERE user_id=?", (json.dumps(equipped, ensure_ascii=False), json.dumps(bag, ensure_ascii=False), user_id))
            result_json = self._json({"action": "wash", "user_id": user_id, "affected": 1, "stone_delta": -int(stone_cost), "accessory": updated_accessory, "details": None})
            uow.execute("INSERT INTO accessory_transaction_operations(operation_id,action,payload,result_json) VALUES(?,?,?,?)", (operation_id, "wash", payload_json, result_json))
            return AccessoryWashChange("applied", "wash", user_id, 1, -int(stone_cost), updated_accessory)


__all__ = ["AccessoryWashSqlRepository"]
