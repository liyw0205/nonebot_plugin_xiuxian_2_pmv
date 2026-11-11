from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork
from .collect_synthesis import reject
from .redemption import ActivityRedemptionRejected, require_redemption_write


class ActivityCollectSynthesisSqlRepository:
    """Convert the chosen tokens with a receipt in the same game_db transaction."""

    operation_table = "activity_collect_synthesis_operations"
    required = {
        operation_table: {"operation_id", "payload", "result_json", "created_at"},
        "activity_collect_inventory": {"activity_key", "user_id", "word_char", "count", "update_time"},
        "user_xiuxian": {"user_id"},
    }

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @classmethod
    def schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return all(
            columns <= {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            for table, columns in cls.required.items()
        )

    @staticmethod
    def identity(user_id, query):
        return json.dumps([str(user_id), str(query)], ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def decode(row, user_id: str, query: str) -> dict:
        try:
            identity = json.loads(row["payload"])
            if not isinstance(identity, list) or len(identity) != 2 or not all(isinstance(v, str) for v in identity):
                raise ValueError("invalid synthesis identity")
            if identity != [str(user_id), str(query)]:
                return reject("operation_conflict")
            data = json.loads(row["result_json"])
            if not isinstance(data, dict) or data.get("status") != "applied":
                raise ValueError("invalid synthesis receipt")
            if not all(isinstance(data.get(k), str) and data[k] for k in ("activity_key", "source", "target", "message")):
                raise ValueError("invalid synthesis result")
            if len(data["source"]) != 1 or len(data["target"]) != 1 or data["source"] == data["target"]:
                raise ValueError("invalid synthesis tokens")
            if query.split(maxsplit=2)[:2] != [data["source"], data["target"]]:
                raise ValueError("synthesis result does not match request")
            if type(data.get("cost")) is not int or not 2 <= data["cost"] <= 100:
                raise ValueError("invalid synthesis cost")
            if any(type(data.get(k)) is not int or data[k] < 0 for k in ("source_left", "target_count")) or data["target_count"] < 1:
                raise ValueError("invalid synthesis balances")
        except (TypeError, ValueError, KeyError) as exc:
            raise ActivityRedemptionRejected("receipt_corrupt") from exc
        return {**data, "status": "duplicate"}

    def lookup_receipt(self, operation_id: str, user_id: str, query: str) -> dict | None:
        if not self.database.is_file():
            return reject("schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self.schema_ready(uow):
                return reject("schema_missing")
            row = uow.query_one(f"SELECT payload,result_json FROM {self.operation_table} WHERE operation_id=?", (operation_id,))
        return self.decode(row, user_id, query) if row is not None else None

    @staticmethod
    def count(uow: DatabaseUnitOfWork, activity_key: str, user_id: str, char: str) -> int:
        row = uow.query_one(
            "SELECT count FROM activity_collect_inventory WHERE activity_key=? AND user_id=? AND word_char=?",
            (activity_key, user_id, char),
        )
        value = as_int_like(row["count"], None) if row is not None else 0
        if value is None or value < 0:
            raise ActivityRedemptionRejected("collect_synthesis_inventory_invalid")
        return value

    def convert(
        self, operation_id: str, user_id: str, query: str, activity_key: str,
        activity_name: str, source: str, target: str, cost: int,
    ) -> dict:
        if (
            not operation_id or not user_id or not activity_key
            or len(source) != 1 or len(target) != 1 or source == target
            or not 2 <= cost <= 100
        ):
            raise ValueError("valid synthesis request is required")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self.schema_ready(uow):
                return reject("schema_missing")
            row = uow.query_one(f"SELECT payload,result_json FROM {self.operation_table} WHERE operation_id=?", (operation_id,))
            if row is not None:
                return self.decode(row, user_id, query)
            if not uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=? LIMIT 1", (user_id,)):
                return reject("user_missing")
            owned = self.count(uow, activity_key, user_id, source)
            if owned < cost:
                return reject("collect_synthesis_tokens_insufficient", f"源字【{source}】不足：需要 {cost} 张，当前 {owned} 张")
            target_count = self.count(uow, activity_key, user_id, target) + 1
            source_left = owned - cost
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            changed = uow.execute(
                "UPDATE activity_collect_inventory SET count=?,update_time=? "
                "WHERE activity_key=? AND user_id=? AND word_char=? AND xiuxian_int_cmp(count,?)=0",
                (source_left, now, activity_key, user_id, source, owned),
            )
            require_redemption_write(changed, "collect_synthesis_source_write_rejected")
            changed = uow.execute(
                "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count,update_time) "
                "VALUES(?,?,?,?,?) ON CONFLICT(activity_key,user_id,word_char) DO UPDATE SET count=excluded.count,update_time=excluded.update_time",
                (activity_key, user_id, target, target_count, now),
            )
            require_redemption_write(changed, "collect_synthesis_target_write_rejected")
            # INTEGER affinity can round huge balances to REAL; never grant a free token.
            if (
                self.count(uow, activity_key, user_id, source) != source_left
                or self.count(uow, activity_key, user_id, target) != target_count
            ):
                raise ActivityRedemptionRejected("collect_synthesis_precision_insufficient")
            data = {
                "status": "applied", "activity_key": activity_key, "source": source,
                "target": target, "cost": cost, "source_left": source_left, "target_count": target_count,
                "message": (
                    f"【{activity_name}】换字成功：消耗 {source}x{cost}，获得 {target}x1\n"
                    f"字牌余额：{source}x{source_left}、{target}x{target_count}\n"
                    "可查看活动背包，集齐后使用活动兑换。"
                ),
            }
            changed = uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload,result_json,created_at) VALUES(?,?,?,?)",
                (operation_id, self.identity(user_id, query), json.dumps(data, ensure_ascii=True, separators=(",", ":")), now),
            )
            require_redemption_write(changed, "collect_synthesis_receipt_write_rejected")
            return data
