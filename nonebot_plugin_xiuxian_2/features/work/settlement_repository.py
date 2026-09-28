from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


def _cd_time_matches(actual: Any, expected: Any) -> bool:
    """Treat blank legacy timestamps as non-blocking while matching real values."""
    blank = {"", "0", "none", "null", "nil", "undefined"}
    if actual is None or expected is None:
        return True
    if isinstance(actual, datetime) or isinstance(expected, datetime):
        return actual == expected
    actual_text, expected_text = str(actual).strip(), str(expected).strip()
    if not actual_text or not expected_text:
        return True
    if actual_text.lower() in blank or expected_text.lower() in blank:
        return True
    return actual_text == expected_text


def _payload_matches(stored: Any, expected: str) -> bool:
    try:
        return json.loads(str(stored)) == json.loads(expected)
    except (TypeError, ValueError):
        return False


class WorkSettlementResult:
    def __init__(
        self,
        status: str,
        exp: int = 0,
        item_awarded: bool = False,
        result: dict | None = None,
        *,
        success_kind: str = "",
        item_msg: str = "",
        scheduled_time: str = "",
    ) -> None:
        self.status = status
        self.exp = exp
        self.item_awarded = item_awarded
        self.result = result or {}
        self.success_kind = success_kind or str(self.result.get("success_kind") or "")
        self.item_msg = item_msg or str(self.result.get("item_msg") or "")
        self.scheduled_time = scheduled_time or str(self.result.get("scheduled_time") or "")

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkSettlementSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='work_settlement_operations'"
        )
        if table is None:
            return False
        columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(work_settlement_operations)")
        }
        return {"operation_id", "payload", "exp", "item_awarded", "result_json"}.issubset(columns)

    def settle(
        self,
        operation_id: str,
        user_id: str,
        expected_work: Mapping[str, object],
        exp_gain: int,
        item: Mapping[str, object] | None,
        max_exp: int,
        max_goods_num: int = 0,
        **kwargs,
    ) -> WorkSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = dict(expected_work or {})
        exp_gain, max_exp, max_goods_num = int(exp_gain), int(max_exp), int(max_goods_num)
        success_kind = str(kwargs.get("success_kind") or "")
        item_msg = str(kwargs.get("item_msg") or "")
        reward = None
        if item:
            item_id = item.get("id", item.get("goods_id"))
            item_name = item.get("name", item.get("goods_name"))
            item_type = item.get("type", item.get("goods_type"))
            if item_id is None or item_name is None or item_type is None:
                raise ValueError("reward id, name and type are required")
            reward = (int(item_id), str(item_name), str(item_type), max(1, int(item.get("quantity", 1))))
        if not operation_id or exp_gain < 0 or max_exp < 0 or max_goods_num < 0 or not expected.get("scheduled_time"):
            raise ValueError("valid operation, work state and rewards are required")
        payload = json.dumps([user_id], ensure_ascii=False, separators=(",", ":"))
        if not Path(self.database).is_file():
            return WorkSettlementResult("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorkSettlementResult("schema_missing")
            previous = uow.query_one("SELECT payload,exp,item_awarded,result_json FROM work_settlement_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if not _payload_matches(previous["payload"], payload):
                    return WorkSettlementResult("state_changed")
                result = json.loads(previous["result_json"] or "{}")
                return WorkSettlementResult(
                    "duplicate",
                    int(previous["exp"]),
                    bool(previous["item_awarded"]),
                    result,
                )
            work = uow.query_one("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,))
            if work is None or int(work["type"] or 0) != 2:
                return WorkSettlementResult("state_changed")
            if str(work["scheduled_time"] or "") != str(expected.get("scheduled_time") or ""):
                return WorkSettlementResult("state_changed")
            if not _cd_time_matches(work["create_time"], expected.get("create_time")):
                return WorkSettlementResult("state_changed")
            user = uow.query_one("SELECT COALESCE(exp,0) AS exp FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return WorkSettlementResult("user_missing")
            current_exp = int(user["exp"])
            applied_exp = max(0, min(exp_gain, max_exp - current_exp))
            item_awarded = False
            result = {
                "success_kind": success_kind,
                "item_msg": item_msg,
                "scheduled_time": str(expected.get("scheduled_time") or ""),
            }
            if reward is not None:
                item_id, item_name, item_type, quantity = reward
                existing = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                current_goods = int(existing["goods_num"] or 0) if existing else 0
                if max_goods_num > 0 and current_goods + quantity > max_goods_num:
                    return WorkSettlementResult("inventory_full")
                item_awarded = True
                if existing is None:
                    uow.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                        "VALUES(?,?,?,?,?,0)",
                        (user_id, item_id, item_name, item_type, quantity),
                    )
                else:
                    uow.execute(
                        "UPDATE back SET goods_num=COALESCE(goods_num,0)+? "
                        "WHERE user_id=? AND goods_id=?",
                        (quantity, user_id, item_id),
                    )
            uow.execute(
                "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL) "
                "WHERE user_id=?",
                (applied_exp, user_id),
            )
            uow.execute(
                "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=?",
                (user_id,),
            )
            result["exp"] = applied_exp
            uow.execute(
                "INSERT INTO work_settlement_operations "
                "(operation_id,payload,exp,item_awarded,result_json) VALUES(?,?,?,?,?)",
                (operation_id, payload, applied_exp, int(item_awarded), json.dumps(result, ensure_ascii=False)),
            )
            return WorkSettlementResult(
                "applied",
                applied_exp,
                item_awarded,
                result,
                success_kind=success_kind,
                item_msg=item_msg,
                scheduled_time=str(expected.get("scheduled_time") or ""),
            )


__all__ = ["WorkSettlementSqlRepository", "WorkSettlementResult"]
