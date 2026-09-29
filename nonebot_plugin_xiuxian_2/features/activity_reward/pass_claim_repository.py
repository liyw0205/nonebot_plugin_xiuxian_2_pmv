from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ActivityPassClaimResult:
    status: str
    rewards: tuple[tuple[int, str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


class ActivityPassClaimRepository:
    """Grant pass rewards in game_db and retry the legacy claim projection."""

    def __init__(self, game_database: str | Path, activity_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.activity_database = str(activity_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _assert_schema(uow: DatabaseUnitOfWork) -> None:
        required = {
            "activity_pass_reward_claim_operations": {
                "operation_id", "payload", "request_json", "result_json", "result_status", "status", "updated_at",
            },
            "activity_pass_reward_claim_reservations": {
                "activity_key", "user_id", "level", "operation_id",
            },
        }
        for table, columns in required.items():
            actual = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            if not columns.issubset(actual):
                raise RuntimeError(f"activity_reward.006 schema_missing: {table}")

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)

    @staticmethod
    def _normalize(
        operation_id: str,
        user_id: str,
        activity_key: str,
        current_level: int,
        rewards: Any,
        max_goods_num: int,
    ) -> tuple[str, dict[str, Any]]:
        operation_id, user_id, activity_key = map(str, (operation_id, user_id, activity_key))
        current_level, max_goods_num = int(current_level), int(max_goods_num)
        stone = 0
        items: dict[int, dict[str, Any]] = {}
        levels = []
        seen = set()
        for raw in rewards:
            level = int(raw["level"])
            if level <= 0 or level in seen or level > current_level:
                raise ValueError("reward levels must be unique and claimable")
            seen.add(level)
            name = str(raw.get("name") or "等级奖励")
            reward_text = str(raw.get("reward") or "")
            reward_items = []
            for raw_item in raw.get("reward_items") or ():
                item = dict(raw_item)
                quantity = int(item.get("quantity", 0))
                if quantity <= 0:
                    raise ValueError("reward quantity must be positive")
                if str(item.get("type")) == "stone":
                    stone += quantity
                    reward_items.append({"type": "stone", "quantity": quantity})
                    continue
                item_id = int(item.get("id", 0))
                item_name = str(item.get("name") or "")
                item_type = str(item.get("type") or "")
                if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                    item_type = "技能"
                elif item_type in {"法器", "防具"}:
                    item_type = "装备"
                if item_id <= 0 or not item_name or not item_type:
                    raise ValueError("complete activity pass item rewards are required")
                metadata = (item_name, item_type)
                existing = items.get(item_id)
                if existing is not None and (existing["name"], existing["type"]) != metadata:
                    raise ValueError("conflicting reward metadata")
                if existing is None:
                    existing = {"id": item_id, "name": item_name, "type": item_type, "quantity": 0}
                    items[item_id] = existing
                existing["quantity"] += quantity
                reward_items.append({
                    "id": item_id, "name": item_name, "type": item_type, "quantity": quantity,
                })
            levels.append({
                "level": level, "name": name, "reward": reward_text, "reward_items": reward_items,
            })
        levels.sort(key=lambda row: row["level"])
        if not operation_id.strip() or not user_id.strip() or not activity_key or not levels or max_goods_num < 0:
            raise ValueError("valid activity pass claim is required")
        item_rows = [items[key] for key in sorted(items)]
        rewards_result = [[row["level"], row["name"], row["reward"]] for row in levels]
        payload = _json([
            user_id, activity_key, current_level, rewards_result, stone,
            [[item["id"], item["name"], item["type"], item["quantity"]] for item in item_rows],
            max_goods_num,
        ])
        request = {
            "user_id": user_id,
            "activity_key": activity_key,
            "current_level": current_level,
            "levels": levels,
            "stone": stone,
            "items": item_rows,
            "max_goods_num": max_goods_num,
            "rewards": rewards_result,
        }
        return payload, request

    @staticmethod
    def _decode(row: Mapping[str, Any], *, replay: bool = False) -> ActivityPassClaimResult:
        rewards = tuple(tuple(item) for item in json.loads(str(row["result_json"] or "[]")))
        status = str(row["result_status"])
        if replay and status == "applied":
            status = "duplicate"
        return ActivityPassClaimResult(status, rewards)

    def get_result(
        self,
        operation_id: str,
        user_id: str | None = None,
        *,
        payload: str | None = None,
    ) -> ActivityPassClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            request = json.loads(str(row["request_json"]))
            if user_id is not None and str(request["user_id"]) != str(user_id):
                return ActivityPassClaimResult("operation_conflict")
            if payload is not None and str(row["payload"]) != payload:
                return ActivityPassClaimResult("operation_conflict")
            if row["status"] not in {"applied", "rejected"}:
                return None
            return self._decode(row, replay=True)

    def pending_status(self, operation_id: str, user_id: str | None = None) -> str | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json,status FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            request = json.loads(str(row["request_json"]))
            if user_id is not None and str(request["user_id"]) != str(user_id):
                return "operation_conflict"
            status = str(row["status"])
            return status if status in {"started", "granted"} else None

    def validate_claim(self, operation_id, user_id, activity_key, current_level, rewards, max_goods_num) -> str:
        return self._normalize(operation_id, user_id, activity_key, current_level, rewards, max_goods_num)[0]

    def prepare_operation(self, uow, operation_id, user_id, activity_key, current_level, rewards, max_goods_num):
        payload, request = self._normalize(
            operation_id, user_id, activity_key, current_level, rewards, max_goods_num
        )
        self._assert_schema(uow)
        operation_id = str(operation_id)
        operation = uow.query_one(
            "SELECT payload,request_json,result_json,result_status,status "
            "FROM activity_pass_reward_claim_operations WHERE operation_id=?",
            (operation_id,),
        )
        status = "started"
        if operation is not None:
            status = str(operation["status"])
            if status in {"started", "granted"}:
                saved = json.loads(str(operation["request_json"]))
                if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                    return ActivityPassClaimResult("operation_conflict"), False
                request = saved
            elif str(operation["payload"]) != payload:
                return ActivityPassClaimResult("operation_conflict"), False
            else:
                return self._decode(operation, replay=True), False
            if status not in {"started", "granted"}:
                return self._decode(operation, replay=True), False
            if str(operation["payload"]) != payload and status not in {"started", "granted"}:
                return ActivityPassClaimResult("operation_conflict"), False
        else:
            uow.execute(
                "INSERT INTO activity_pass_reward_claim_operations"
                "(operation_id,payload,request_json,result_json,result_status,status) "
                "VALUES(?,?,?,'[]','started','started')",
                (operation_id, payload, _json(request)),
            )
        for level in request["levels"]:
            identity = (request["activity_key"], request["user_id"], int(level["level"]))
            uow.execute(
                "INSERT OR IGNORE INTO activity_pass_reward_claim_reservations"
                "(activity_key,user_id,level,operation_id) VALUES(?,?,?,?)",
                (*identity, operation_id),
            )
            owner = uow.query_one(
                "SELECT operation_id FROM activity_pass_reward_claim_reservations "
                "WHERE activity_key=? AND user_id=? AND level=?",
                identity,
            )
            if owner is None or str(owner["operation_id"]) != operation_id:
                return self._reject(uow, operation_id, "claim_in_progress"), False
        return None, operation is None

    @staticmethod
    def _reject(uow, operation_id: str, status: str) -> ActivityPassClaimResult:
        uow.execute(
            "UPDATE activity_pass_reward_claim_operations SET status='rejected',result_status=?,"
            "result_json='[]',updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND status='started'",
            (status, operation_id),
        )
        uow.execute(
            "DELETE FROM activity_pass_reward_claim_reservations WHERE operation_id=?",
            (operation_id,),
        )
        return ActivityPassClaimResult(status)

    def _legacy_claimable(self, request: Mapping[str, Any]) -> bool:
        with DatabaseUnitOfWork(self.activity_database, read_only=True) as uow:
            balance = uow.query_one(
                "SELECT level FROM activity_pass_balance WHERE activity_key=? AND user_id=?",
                (request["activity_key"], request["user_id"]),
            )
            if balance is None or int(balance["level"] or 0) != int(request["current_level"]):
                return False
            for reward in request["levels"]:
                claimed = uow.query_one(
                    "SELECT 1 AS present FROM activity_pass_reward_claim "
                    "WHERE activity_key=? AND user_id=? AND level=?",
                    (request["activity_key"], request["user_id"], int(reward["level"])),
                )
                if claimed is not None:
                    return False
        return True

    def claim(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        current_level: int,
        rewards: Any,
        max_goods_num: int,
        *,
        newly_prepared: bool = False,
    ) -> ActivityPassClaimResult:
        operation_id = str(operation_id).strip()
        payload, request = self._normalize(
            operation_id, user_id, activity_key, current_level, rewards, max_goods_num
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            self._assert_schema(uow)
            operation = uow.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None:
                uow.execute(
                    "INSERT INTO activity_pass_reward_claim_operations"
                    "(operation_id,payload,request_json,result_json,result_status,status) "
                    "VALUES(?,?,?,'[]','started','started')",
                    (operation_id, payload, _json(request)),
                )
                operation = uow.query_one(
                    "SELECT payload,request_json,result_json,result_status,status "
                    "FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
            elif str(operation["status"]) in {"started", "granted"}:
                request = json.loads(str(operation["request_json"]))
                payload = str(operation["payload"])
            elif str(operation["payload"]) != payload:
                return ActivityPassClaimResult("operation_conflict")
            elif str(operation["status"]) in {"applied", "rejected"}:
                return self._decode(operation, replay=True)
            status = str(operation["status"])
        created = operation is None or newly_prepared

        if status == "started":
            if not self._legacy_claimable(request):
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    return self._reject(uow, operation_id, "state_changed")
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                operation = uow.query_one(
                    "SELECT status FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if operation is None:
                    raise RuntimeError("activity pass claim operation is missing")
                if operation["status"] == "started":
                    if uow.query_one(
                        "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (request["user_id"],)
                    ) is None:
                        return self._reject(uow, operation_id, "user_missing")
                    for item in request["items"]:
                        row = uow.query_one(
                            "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                            (request["user_id"], int(item["id"])),
                        )
                        if (int(row["quantity"]) if row else 0) + int(item["quantity"]) > int(request["max_goods_num"]):
                            return self._reject(uow, operation_id, "inventory_full")
                    now = self.clock.now().isoformat()
                    if int(request["stone"]):
                        changed = uow.execute(
                            "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? WHERE user_id=?",
                            (int(request["stone"]), request["user_id"]),
                        )
                        if changed.rowcount != 1:
                            return self._reject(uow, operation_id, "user_missing")
                    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
                    for item in request["items"]:
                        existing = uow.query_one(
                            "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                            (request["user_id"], int(item["id"])),
                        )
                        if existing is None:
                            names = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                            values: list[Any] = [request["user_id"], item["id"], item["name"], item["type"], item["quantity"]]
                            for field in ("create_time", "update_time"):
                                if field in columns:
                                    names.append(field)
                                    values.append(now)
                            if "bind_num" in columns:
                                names.append("bind_num")
                                values.append(item["quantity"])
                            uow.execute(
                                f"INSERT INTO back({','.join(names)}) VALUES({','.join('?' for _ in values)})",
                                tuple(values),
                            )
                        else:
                            assignments = "goods_name=?,goods_type=?,goods_num=COALESCE(goods_num,0)+?"
                            values = [item["name"], item["type"], item["quantity"]]
                            if "update_time" in columns:
                                assignments += ",update_time=?"
                                values.append(now)
                            if "bind_num" in columns:
                                assignments += ",bind_num=COALESCE(bind_num,0)+?"
                                values.append(item["quantity"])
                            values.extend((request["user_id"], item["id"]))
                            uow.execute(
                                f"UPDATE back SET {assignments} WHERE user_id=? AND goods_id=?",
                                tuple(values),
                            )
                    uow.execute(
                        "UPDATE activity_pass_reward_claim_operations SET status='granted',result_status='applied',"
                        "result_json=?,updated_at=? WHERE operation_id=? AND status='started'",
                        (_json(request["rewards"]), now, operation_id),
                    )
                    status = "granted"

        if status == "granted":
            self._finalize_legacy_state(request)
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.execute(
                    "UPDATE activity_pass_reward_claim_operations SET status='applied',updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=? AND status='granted'",
                    (operation_id,),
                )
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            final = uow.query_one(
                "SELECT result_json,result_status,status FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
        if final is None:
            raise RuntimeError("activity pass claim operation disappeared")
        return self._decode(final, replay=not created and final["status"] == "applied")

    def reconcile(self, operation_id: str) -> ActivityPassClaimResult:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json FROM activity_pass_reward_claim_operations WHERE operation_id=?",
                (str(operation_id),),
            )
        if row is None:
            raise RuntimeError("activity pass claim operation is missing")
        request = json.loads(str(row["request_json"]))
        rewards = tuple(request["levels"])
        return self.claim(
            str(operation_id), request["user_id"], request["activity_key"],
            request["current_level"], rewards, request["max_goods_num"],
        )

    def _finalize_legacy_state(self, request: Mapping[str, Any]) -> None:
        now = self.clock.now().isoformat()
        levels = [int(row["level"]) for row in request["levels"]]
        with DatabaseUnitOfWork(self.activity_database, immediate=True) as uow:
            existing = uow.query_all(
                "SELECT level FROM activity_pass_reward_claim WHERE activity_key=? AND user_id=? "
                f"AND level IN ({','.join('?' for _ in levels)})",
                (request["activity_key"], request["user_id"], *levels),
            )
            existing_levels = {int(row["level"]) for row in existing}
            if existing_levels and existing_levels != set(levels):
                raise RuntimeError("activity pass claim projection is partially committed")
            for level in levels:
                if level in existing_levels:
                    continue
                uow.execute(
                    "INSERT INTO activity_pass_reward_claim(activity_key,user_id,level,create_time) "
                    "VALUES(?,?,?,?)",
                    (request["activity_key"], request["user_id"], level, now),
                )


__all__ = ["ActivityPassClaimRepository", "ActivityPassClaimResult"]
