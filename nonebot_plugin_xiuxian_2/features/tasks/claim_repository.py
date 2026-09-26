from __future__ import annotations

import json
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .claim_domain import TaskClaimResult


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _decode(value: object, default: dict | list) -> dict | list:
    if value in (None, ""):
        return default
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return decoded if isinstance(decoded, type(default)) else default


def normalize_claim_tasks(tasks: Any, cycles: tuple[str, ...]) -> tuple[dict, ...]:
    normalized = []
    seen = set()
    for raw in tasks:
        task = dict(raw)
        key = str(task.get("key", "")).strip()
        cycle = str(task.get("cycle", "")).strip()
        name = str(task.get("name", "")).strip()
        target = int(task.get("target", 0))
        rewards = dict(task.get("rewards") or {})
        unsupported = {
            reward_key
            for reward_key, value in rewards.items()
            if reward_key != "items" and int(value or 0) != 0
        }
        if unsupported:
            raise ValueError("unsupported task reward types: " + ",".join(sorted(unsupported)))
        items = []
        for raw_item in rewards.get("items", ()) or ():
            item = dict(raw_item)
            normalized_item = {
                "id": int(item.get("id", 0)),
                "name": str(item.get("name", "")).strip(),
                "type": str(item.get("type", "")).strip(),
                "amount": int(item.get("amount", 0)),
                "bind_flag": int(item.get("bind_flag", 1)),
            }
            if (
                normalized_item["id"] <= 0
                or not normalized_item["name"]
                or not normalized_item["type"]
                or normalized_item["amount"] <= 0
                or normalized_item["bind_flag"] not in {0, 1}
            ):
                raise ValueError("complete positive task item rewards are required")
            items.append(normalized_item)
        if not key or not name or cycle not in cycles or target <= 0 or (cycle, key) in seen:
            raise ValueError("valid unique task definitions are required")
        seen.add((cycle, key))
        normalized.append({"key": key, "cycle": cycle, "name": name, "target": target, "items": items})
    return tuple(normalized)


class TaskClaimGameRepository:
    """Owns durable checkpoints and item rewards in game_db."""

    @staticmethod
    def get(uow: DatabaseUnitOfWork, operation_id: str) -> Mapping[str, Any] | None:
        return uow.query_one(
            "SELECT * FROM task_reward_claim_operations WHERE operation_id = ?",
            (operation_id,),
        )

    @staticmethod
    def create(
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        payload: str,
        request_json: str,
        now: str,
    ) -> None:
        uow.execute(
            "INSERT INTO task_reward_claim_operations"
            "(operation_id,payload,request_json,prepared_json,result_json,result_status,status,created_at,updated_at) "
            "VALUES(?,?,?,'{}','[]','started','started',?,?)",
            (operation_id, payload, request_json, now, now),
        )

    @staticmethod
    def save_prepared(
        uow: DatabaseUnitOfWork, operation_id: str, prepared: Mapping[str, Any], now: str
    ) -> None:
        uow.execute(
            "UPDATE task_reward_claim_operations SET prepared_json=?,updated_at=? "
            "WHERE operation_id=? AND status='started'",
            (_json(dict(prepared)), now, operation_id),
        )

    @staticmethod
    def reject(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        status: str,
        now: str,
    ) -> None:
        uow.execute(
            "UPDATE task_reward_claim_operations SET status='rejected',result_status=?,"
            "result_json='[]',updated_at=? WHERE operation_id=? AND status='started'",
            (status, now, operation_id),
        )

    @staticmethod
    def _store_item(
        uow: DatabaseUnitOfWork,
        user_id: str,
        item: Mapping[str, Any],
        max_goods_num: int,
        now: str,
    ) -> None:
        row = uow.query_one(
            "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
            "WHERE user_id=? AND goods_id=?",
            (user_id, item["id"]),
        )
        previous = int(row["goods_num"]) if row else 0
        final = previous + int(item["amount"])
        if final > max_goods_num:
            raise OverflowError("task reward inventory is full")
        columns = {
            str(column["name"]) for column in uow.query_all("PRAGMA table_info(back)")
        }
        if row is None:
            names = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
            values: list[Any] = [user_id, item["id"], item["name"], item["type"], item["amount"]]
            for field, value in (("create_time", now), ("update_time", now)):
                if field in columns:
                    names.append(field)
                    values.append(value)
            bound_amount = int(item["amount"]) if int(item["bind_flag"]) else 0
            if "bind_num" in columns:
                names.append("bind_num")
                values.append(bound_amount)
            uow.execute(
                f"INSERT INTO back({','.join(names)}) VALUES({','.join('?' for _ in values)})",
                tuple(values),
            )
            return
        assignments = "goods_name=?,goods_type=?,goods_num=?,update_time=?"
        values = [item["name"], item["type"], final, now]
        bound_amount = int(item["amount"]) if int(item["bind_flag"]) else 0
        if "bind_num" in columns and bound_amount:
            assignments += ",bind_num=COALESCE(bind_num,0)+?"
            values.append(bound_amount)
        values.extend((user_id, item["id"], previous))
        changed = uow.execute(
            f"UPDATE back SET {assignments} WHERE user_id=? AND goods_id=? "
            "AND COALESCE(goods_num,0)=?",
            tuple(values),
        )
        if changed.rowcount != 1:
            raise RuntimeError("task reward inventory changed")

    def grant(self, uow: DatabaseUnitOfWork, operation_id: str, now: str) -> TaskClaimResult:
        operation = self.get(uow, operation_id)
        if operation is None:
            raise RuntimeError("task reward operation is missing")
        status = str(operation["status"])
        if status in {"granted", "applied"}:
            tasks = tuple(_decode(operation["result_json"], []))
            return TaskClaimResult("granted", tasks)
        if status == "rejected":
            return TaskClaimResult(str(operation["result_status"]), ())
        prepared = _decode(operation["prepared_json"], {})
        if not isinstance(prepared, dict):
            raise RuntimeError("invalid prepared task reward operation")
        user_id = str(prepared["user_id"])
        if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
            self.reject(uow, operation_id, "user_missing", now)
            return TaskClaimResult("user_missing")
        tasks = tuple(prepared.get("tasks", ()))
        totals = tuple(prepared.get("totals", ()))
        max_goods_num = int(prepared["max_goods_num"])
        for item in totals:
            current = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                "WHERE user_id=? AND goods_id=?",
                (user_id, int(item["id"])),
            )
            if (int(current["goods_num"]) if current else 0) + int(item["amount"]) > max_goods_num:
                self.reject(uow, operation_id, "inventory_full", now)
                return TaskClaimResult("inventory_full")
        for item in totals:
            self._store_item(uow, user_id, item, max_goods_num, now)
        if tasks:
            item_delta = [
                {key: value for key, value in item.items() if key != "bound_amount"}
                for item in totals
            ]
            uow.execute(
                "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                "VALUES(?,'xiuxian_task','claim_task_reward',?,?,?,?)",
                (
                    user_id,
                    _json(item_delta),
                    _json({"tasks": [task["key"] for task in tasks]}),
                    operation_id,
                    now,
                ),
            )
        uow.execute(
            "UPDATE task_reward_claim_operations SET status='granted',result_status='applied',"
            "result_json=?,updated_at=? WHERE operation_id=? AND status='started'",
            (_json(tasks), now, operation_id),
        )
        return TaskClaimResult("granted", tasks)

    @staticmethod
    def finalize(uow: DatabaseUnitOfWork, operation_id: str, now: str) -> None:
        uow.execute(
            "UPDATE task_reward_claim_operations SET status='applied',updated_at=? "
            "WHERE operation_id=? AND status='granted'",
            (now, operation_id),
        )


class TaskClaimPlayerRepository:
    """Owns task progress reservations and claim receipts in player_db."""

    @staticmethod
    def has_operation(uow: DatabaseUnitOfWork, operation_id: str) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM task_reward_claim_player_operations WHERE operation_id=?",
            (operation_id,),
        ) is not None

    @staticmethod
    def prepare(
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        payload: str,
        context: Mapping[str, Any],
    ) -> TaskClaimResult:
        previous = uow.query_one(
            "SELECT payload,prepared_json,status,result_status,result_json "
            "FROM task_reward_claim_player_operations WHERE operation_id=?",
            (operation_id,),
        )
        if previous is not None:
            if str(previous["payload"]) != payload:
                return TaskClaimResult("operation_conflict")
            if previous["status"] == "rejected":
                return TaskClaimResult(str(previous["result_status"]), tuple(_decode(previous["result_json"], [])))
            prepared = _decode(previous["prepared_json"], {})
            return TaskClaimResult(str(previous["status"]), tuple(prepared.get("tasks", ())))

        user_id = str(context["user_id"])
        cycles = tuple(str(cycle) for cycle in context["cycles"])
        periods = {str(key): str(value) for key, value in dict(context["periods"]).items()}
        task_defs = tuple(dict(task) for task in context["tasks"])
        fields = [
            f"{cycle}_{suffix}"
            for cycle in ("daily", "weekly")
            for suffix in ("period", "progress", "claimed")
        ]
        row = uow.query_one(
            "SELECT " + ",".join(fields) + " FROM xiuxian_tasks WHERE user_id=?",
            (user_id,),
        )
        if row is None:
            uow.execute("INSERT INTO xiuxian_tasks(user_id) VALUES(?)", (user_id,))
            values = {field: None for field in fields}
        else:
            values = dict(row)
        eligible = []
        for cycle in cycles:
            if str(values.get(f"{cycle}_period") or "") != periods[cycle]:
                progress, claimed = {}, []
            else:
                progress = _decode(values.get(f"{cycle}_progress"), {})
                claimed = _decode(values.get(f"{cycle}_claimed"), [])
            claimed_set = {str(key) for key in claimed}
            for task in task_defs:
                if (
                    task["cycle"] == cycle
                    and int(progress.get(task["key"], 0) or 0) >= int(task["target"])
                    and task["key"] not in claimed_set
                ):
                    eligible.append(task)
                    claimed_set.add(task["key"])

        for task in eligible:
            reservation = uow.query_one(
                "SELECT operation_id FROM task_reward_claim_reservations "
                "WHERE user_id=? AND cycle=? AND period=? AND task_key=?",
                (user_id, task["cycle"], periods[task["cycle"]], task["key"]),
            )
            if reservation is not None and str(reservation["operation_id"]) != operation_id:
                return TaskClaimResult("claim_in_progress")

        totals: dict[int, dict[str, Any]] = {}
        for task in eligible:
            for item in task["items"]:
                total = totals.get(int(item["id"]))
                if total is None:
                    total = dict(item)
                    total["bound_amount"] = int(item["amount"]) if int(item["bind_flag"]) else 0
                    totals[int(item["id"])] = total
                else:
                    if (total["name"], total["type"]) != (item["name"], item["type"]):
                        raise ValueError("conflicting task item metadata")
                    total["amount"] += int(item["amount"])
                    if int(item["bind_flag"]):
                        total["bound_amount"] += int(item["amount"])
        prepared = {
            "user_id": user_id,
            "cycles": list(cycles),
            "periods": periods,
            "tasks": [
                {"key": task["key"], "cycle": task["cycle"], "name": task["name"], "items": task["items"]}
                for task in eligible
            ],
            "totals": list(totals.values()),
            "max_goods_num": int(context["max_goods_num"]),
        }
        uow.execute(
            "INSERT INTO task_reward_claim_player_operations"
            "(operation_id,user_id,payload,prepared_json,status,result_status,result_json) "
            "VALUES(?,?,?,?, 'reserved','started','[]')",
            (operation_id, user_id, payload, _json(prepared)),
        )
        for task in eligible:
            uow.execute(
                "INSERT INTO task_reward_claim_reservations(user_id,cycle,period,task_key,operation_id) "
                "VALUES(?,?,?,?,?)",
                (user_id, task["cycle"], periods[task["cycle"]], task["key"], operation_id),
            )
        return TaskClaimResult("reserved", tuple(prepared["tasks"]))

    @staticmethod
    def finalize(uow: DatabaseUnitOfWork, operation_id: str) -> TaskClaimResult:
        operation = uow.query_one(
            "SELECT user_id,prepared_json,status,result_status,result_json "
            "FROM task_reward_claim_player_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None:
            raise RuntimeError("player task claim reservation is missing")
        if operation["status"] == "applied":
            return TaskClaimResult("applied", tuple(_decode(operation["result_json"], [])))
        if operation["status"] == "rejected":
            return TaskClaimResult(str(operation["result_status"]), tuple(_decode(operation["result_json"], [])))
        prepared = _decode(operation["prepared_json"], {})
        user_id = str(operation["user_id"])
        row = uow.query_one(
            "SELECT daily_period,daily_claimed,weekly_period,weekly_claimed "
            "FROM xiuxian_tasks WHERE user_id=?",
            (user_id,),
        )
        if row is not None:
            for cycle in prepared["cycles"]:
                if str(row.get(f"{cycle}_period") or "") != str(prepared["periods"][cycle]):
                    continue
                claimed = [str(key) for key in _decode(row.get(f"{cycle}_claimed"), [])]
                for task in prepared["tasks"]:
                    if task["cycle"] == cycle and task["key"] not in claimed:
                        claimed.append(task["key"])
                uow.execute(
                    f"UPDATE xiuxian_tasks SET {cycle}_claimed=? WHERE user_id=?",
                    (_json(claimed), user_id),
                )
        uow.execute("DELETE FROM task_reward_claim_reservations WHERE operation_id=?", (operation_id,))
        uow.execute(
            "UPDATE task_reward_claim_player_operations SET status='applied',result_status='applied',"
            "result_json=? WHERE operation_id=?",
            (_json(prepared["tasks"]), operation_id),
        )
        return TaskClaimResult("applied", tuple(prepared["tasks"]))

    @staticmethod
    def reject(uow: DatabaseUnitOfWork, operation_id: str, status: str) -> None:
        uow.execute("DELETE FROM task_reward_claim_reservations WHERE operation_id=?", (operation_id,))
        uow.execute(
            "UPDATE task_reward_claim_player_operations SET status='rejected',result_status=?,result_json='[]' "
            "WHERE operation_id=? AND status='reserved'",
            (status, operation_id),
        )
