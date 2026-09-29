from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ActivityTaskClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


class ActivityTaskClaimRepository:
    """Grant activity-task assets in game_db and retry the legacy state projection."""

    def __init__(self, game_database: str | Path, activity_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.activity_database = str(activity_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _assert_schema(uow: DatabaseUnitOfWork) -> None:
        required = {
            "activity_task_reward_claim_operations": {
                "operation_id", "payload", "request_json", "result_json",
                "result_status", "status", "updated_at",
            },
            "activity_task_reward_claim_reservations": {
                "activity_key", "user_id", "scope_type", "scope_key", "task_key", "operation_id",
            },
        }
        for table, columns in required.items():
            actual = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            if not columns.issubset(actual):
                raise RuntimeError(f"activity_reward.004 schema_missing: {table}")

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)

    @staticmethod
    def _normalize(
        operation_id: str,
        user_id: str,
        activity_key: str,
        tasks: Any,
        max_goods_num: int,
    ) -> tuple[str, dict[str, Any]]:
        operation_id, user_id, activity_key = map(str, (operation_id, user_id, activity_key))
        max_goods_num = int(max_goods_num)
        normalized = []
        seen = set()
        stone = 0
        items: dict[int, dict[str, Any]] = {}
        for raw in tasks:
            task_key, scope_type, scope_key, target, reward_text, rewards, name = raw
            identity = (str(scope_type), str(scope_key), str(task_key))
            if (
                not str(task_key).strip()
                or not str(scope_type).strip()
                or not str(scope_key).strip()
                or int(target) <= 0
                or not str(name).strip()
                or identity in seen
            ):
                raise ValueError("valid unique activity task claims are required")
            seen.add(identity)
            normalized_rewards = []
            for raw_reward in rewards:
                reward = dict(raw_reward)
                quantity = int(reward.get("quantity", 0))
                if quantity <= 0:
                    raise ValueError("activity task reward quantities must be positive")
                if str(reward.get("type")) == "stone":
                    stone += quantity
                    normalized_rewards.append({"type": "stone", "quantity": quantity})
                    continue
                item_id = int(reward.get("id", 0))
                item_name = str(reward.get("name", ""))
                item_type = str(reward.get("type", ""))
                if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                    item_type = "技能"
                elif item_type in {"法器", "防具"}:
                    item_type = "装备"
                if item_id <= 0 or not item_name or not item_type:
                    raise ValueError("complete activity task item rewards are required")
                current = items.get(item_id)
                if current is not None and (current["name"], current["type"]) != (item_name, item_type):
                    raise ValueError("conflicting activity task reward metadata")
                if current is None:
                    current = {"id": item_id, "name": item_name, "type": item_type, "quantity": 0}
                    items[item_id] = current
                current["quantity"] += quantity
                normalized_rewards.append({
                    "id": item_id, "name": item_name, "type": item_type, "quantity": quantity,
                })
            normalized.append({
                "key": str(task_key), "scope_type": str(scope_type), "scope_key": str(scope_key),
                "target": int(target), "reward_text": str(reward_text), "name": str(name),
                "rewards": normalized_rewards,
            })
        if not operation_id.strip() or not user_id.strip() or not activity_key or not normalized or max_goods_num < 0:
            raise ValueError("valid activity task claim is required")
        item_rows = [items[key] for key in sorted(items)]
        payload = _json([
            user_id, activity_key,
            [[task["key"], task["scope_type"], task["scope_key"], task["target"], task["reward_text"], task["name"]]
             for task in normalized],
            stone,
            [[item["id"], item["name"], item["type"], item["quantity"]] for item in item_rows],
            max_goods_num,
        ])
        request = {
            "user_id": user_id, "activity_key": activity_key, "tasks": normalized,
            "stone": stone, "items": item_rows, "max_goods_num": max_goods_num,
            "rewards": [[task["name"], task["reward_text"]] for task in normalized],
        }
        return payload, request

    @staticmethod
    def _decode_result(row: Mapping[str, Any], *, replay: bool = False) -> ActivityTaskClaimResult:
        rewards = tuple(tuple(item) for item in json.loads(str(row["result_json"] or "[]")))
        status = str(row["result_status"])
        if replay and status == "applied":
            status = "duplicate"
        return ActivityTaskClaimResult(status, rewards)

    def get_result(self, operation_id: str, user_id: str | None = None) -> ActivityTaskClaimResult | None:
        if not str(operation_id).strip():
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT payload,result_json,result_status,status FROM activity_task_reward_claim_operations "
                "WHERE operation_id=?", (str(operation_id).strip(),),
            )
            if row is None:
                return None
            payload = json.loads(str(row["payload"]))
            if user_id is not None and str(payload[0]) != str(user_id):
                return ActivityTaskClaimResult("operation_conflict")
            if row["status"] not in {"applied", "rejected"}:
                return None
            return self._decode_result(row, replay=True)

    def validate_claim(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        tasks: Any,
        max_goods_num: int,
    ) -> None:
        self._normalize(operation_id, user_id, activity_key, tasks, max_goods_num)

    def prepare_operation(
        self,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        user_id: str,
        activity_key: str,
        tasks: Any,
        max_goods_num: int,
    ) -> tuple[ActivityTaskClaimResult | None, bool]:
        payload, request = self._normalize(operation_id, user_id, activity_key, tasks, max_goods_num)
        self._assert_schema(uow)
        operation = uow.query_one(
            "SELECT payload,request_json,result_json,result_status,status "
            "FROM activity_task_reward_claim_operations WHERE operation_id=?",
            (str(operation_id),),
        )
        status = "started"
        if operation is not None:
            status = str(operation["status"])
            if status in {"started", "granted"}:
                saved = json.loads(str(operation["request_json"]))
                if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                    return ActivityTaskClaimResult("operation_conflict"), False
                request = saved
            if str(operation["payload"]) != payload:
                if status not in {"started", "granted"}:
                    return ActivityTaskClaimResult("operation_conflict"), False
            elif status not in {"started", "granted"}:
                return self._decode_result(operation, replay=True), False
        else:
            uow.execute(
                "INSERT INTO activity_task_reward_claim_operations"
                "(operation_id,payload,request_json,result_json,result_status,status) "
                "VALUES(?,?,?,'[]','started','started')",
                (str(operation_id), payload, _json(request)),
            )
        created = operation is None
        if status != "granted":
            for task in request["tasks"]:
                identity = (
                    request["activity_key"], request["user_id"],
                    task["scope_type"], task["scope_key"], task["key"],
                )
                uow.execute(
                    "INSERT OR IGNORE INTO activity_task_reward_claim_reservations"
                    "(activity_key,user_id,scope_type,scope_key,task_key,operation_id) VALUES(?,?,?,?,?,?)",
                    (*identity, str(operation_id)),
                )
                owner = uow.query_one(
                    "SELECT operation_id FROM activity_task_reward_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND scope_type=? AND scope_key=? AND task_key=?",
                    identity,
                )
                if owner is None or str(owner["operation_id"]) != str(operation_id):
                    return self._reject(uow, str(operation_id), "claim_in_progress"), False
        return None, created

    def _legacy_claimable(self, request: Mapping[str, Any]) -> bool:
        with DatabaseUnitOfWork(self.activity_database, read_only=True) as uow:
            for task in request["tasks"]:
                row = uow.query_one(
                    "SELECT progress,target,claimed FROM activity_task_progress "
                    "WHERE activity_key=? AND user_id=? AND scope_type=? AND scope_key=? AND task_key=?",
                    (request["activity_key"], request["user_id"], task["scope_type"], task["scope_key"], task["key"]),
                )
                if row is None or int(row["claimed"] or 0) or int(row["progress"] or 0) < int(task["target"]):
                    return False
        return True

    @staticmethod
    def _reject(uow: DatabaseUnitOfWork, operation_id: str, status: str) -> ActivityTaskClaimResult:
        uow.execute(
            "UPDATE activity_task_reward_claim_operations SET status='rejected',result_status=?,"
            "result_json='[]',updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND status='started'",
            (status, operation_id),
        )
        uow.execute(
            "DELETE FROM activity_task_reward_claim_reservations WHERE operation_id=?",
            (operation_id,),
        )
        return ActivityTaskClaimResult(status)

    def claim(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        tasks: Any,
        max_goods_num: int,
        *,
        newly_prepared: bool = False,
    ) -> ActivityTaskClaimResult:
        operation_id = str(operation_id).strip()
        payload, request = self._normalize(operation_id, user_id, activity_key, tasks, max_goods_num)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            self._assert_schema(uow)
            operation = uow.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_task_reward_claim_operations WHERE operation_id=?", (operation_id,),
            )
            if operation is not None:
                if operation["status"] in {"started", "granted"}:
                    saved = json.loads(str(operation["request_json"]))
                    if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                        return ActivityTaskClaimResult("operation_conflict")
                    payload = str(operation["payload"])
                    request = saved
                elif str(operation["payload"]) != payload:
                    return ActivityTaskClaimResult("operation_conflict")
                if operation["status"] in {"applied", "rejected"}:
                    return self._decode_result(operation, replay=True)
            else:
                uow.execute(
                    "INSERT INTO activity_task_reward_claim_operations"
                    "(operation_id,payload,request_json,result_json,result_status,status) "
                    "VALUES(?,?,?,'[]','started','started')",
                    (operation_id, payload, _json(request)),
                )
            for task in request["tasks"]:
                uow.execute(
                    "INSERT OR IGNORE INTO activity_task_reward_claim_reservations"
                    "(activity_key,user_id,scope_type,scope_key,task_key,operation_id) VALUES(?,?,?,?,?,?)",
                    (request["activity_key"], request["user_id"], task["scope_type"], task["scope_key"], task["key"], operation_id),
                )
                owner = uow.query_one(
                    "SELECT operation_id FROM activity_task_reward_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND scope_type=? AND scope_key=? AND task_key=?",
                    (request["activity_key"], request["user_id"], task["scope_type"], task["scope_key"], task["key"]),
                )
                if owner is None or str(owner["operation_id"]) != operation_id:
                    return self._reject(uow, operation_id, "claim_in_progress")
            status = str(operation["status"]) if operation is not None else "started"
        created = operation is None or newly_prepared

        if status == "started":
            if not self._legacy_claimable(request):
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    return self._reject(uow, operation_id, "state_changed")
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                operation = uow.query_one(
                    "SELECT status FROM activity_task_reward_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if operation is None:
                    raise RuntimeError("activity task claim operation is missing")
                if operation["status"] == "started":
                    if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (request["user_id"],)) is None:
                        return self._reject(uow, operation_id, "user_missing")
                    for item in request["items"]:
                        row = uow.query_one(
                            "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                            (request["user_id"], item["id"]),
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
                            (request["user_id"], item["id"]),
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
                        "UPDATE activity_task_reward_claim_operations SET status='granted',result_status='applied',"
                        "result_json=?,updated_at=? WHERE operation_id=? AND status='started'",
                        (_json(request["rewards"]), now, operation_id),
                    )
                    status = "granted"
                else:
                    status = str(operation["status"])

        if status == "granted":
            self._finalize_legacy_state(request)
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.execute(
                    "UPDATE activity_task_reward_claim_operations SET status='applied',updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=? AND status='granted'",
                    (operation_id,),
                )
        with DatabaseUnitOfWork(self.game_database) as uow:
            final = uow.query_one(
                "SELECT result_json,result_status,status FROM activity_task_reward_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
        if final is None:
            raise RuntimeError("activity task claim operation disappeared")
        return self._decode_result(final, replay=not created and final["status"] == "applied")

    def reconcile(self, operation_id: str) -> ActivityTaskClaimResult:
        with DatabaseUnitOfWork(self.game_database) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json,status FROM activity_task_reward_claim_operations WHERE operation_id=?",
                (str(operation_id),),
            )
        if row is None:
            raise RuntimeError("activity task claim operation is missing")
        request = json.loads(str(row["request_json"]))
        tasks = tuple(
            (
                task["key"], task["scope_type"], task["scope_key"], task["target"],
                task["reward_text"], tuple(task["rewards"]), task["name"],
            )
            for task in request["tasks"]
        )
        return self.claim(
            str(operation_id), request["user_id"], request["activity_key"],
            tasks, request["max_goods_num"],
        )

    def _finalize_legacy_state(self, request: Mapping[str, Any]) -> None:
        now = self.clock.now().isoformat()
        with DatabaseUnitOfWork(self.activity_database, immediate=True) as uow:
            for task in request["tasks"]:
                parameters = (
                    request["activity_key"], request["user_id"], task["scope_type"],
                    task["scope_key"], task["key"],
                )
                row = uow.query_one(
                    "SELECT progress,target,claimed FROM activity_task_progress "
                    "WHERE activity_key=? AND user_id=? AND scope_type=? AND scope_key=? AND task_key=?",
                    parameters,
                )
                if row is None or int(row["progress"] or 0) < int(task["target"]):
                    raise RuntimeError(f"activity task state unavailable after reward grant: {task['key']}")
                existing_log = uow.query_one(
                    "SELECT 1 AS present FROM activity_task_claim_log WHERE activity_key=? AND user_id=? "
                    "AND scope_type=? AND scope_key=? AND task_key=? LIMIT 1",
                    parameters,
                )
                if int(row["claimed"] or 0):
                    if existing_log is None:
                        raise RuntimeError(f"activity task claim log missing after reward grant: {task['key']}")
                    continue
                changed = uow.execute(
                    "UPDATE activity_task_progress SET claimed=1,claim_time=?,update_time=?,target=? "
                    "WHERE activity_key=? AND user_id=? "
                    "AND scope_type=? AND scope_key=? AND task_key=? AND claimed=0 AND progress>=?",
                    (now, now, task["target"], *parameters, task["target"]),
                )
                if changed.rowcount != 1:
                    raise RuntimeError(f"activity task state changed after reward grant: {task['key']}")
                uow.execute(
                    "INSERT INTO activity_task_claim_log(activity_key,user_id,scope_type,scope_key,task_key,reward,create_time) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (*parameters, task["reward_text"], now),
                )


__all__ = ["ActivityTaskClaimRepository", "ActivityTaskClaimResult"]
