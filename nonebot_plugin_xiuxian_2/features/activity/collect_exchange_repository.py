from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ActivityCollectExchangeResult:
    status: str
    claim_count: int = 0
    missing: tuple[tuple[str, int], ...] = ()
    rewards: tuple[str, ...] = ()
    response: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivityCollectExchangeSqlRepository:
    """Atomically spend collect tokens and grant rewards in game_db."""

    operation_table = "activity_collect_exchange_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _semantic_value(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): ActivityCollectExchangeSqlRepository._semantic_value(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [ActivityCollectExchangeSqlRepository._semantic_value(item) for item in value]
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else value
        text = str(value).strip()
        if not text:
            return ""
        if re.fullmatch(r"[+-]?\d+", text):
            try:
                return int(text)
            except (ValueError, OverflowError):
                return text
        if re.fullmatch(
            r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", text
        ):
            try:
                number = Decimal(text)
                return int(number) if number == number.to_integral_value() else float(number)
            except (InvalidOperation, ValueError, OverflowError):
                return text
        return text

    @classmethod
    def _payload_matches(cls, stored: Any, expected: str) -> bool:
        if isinstance(stored, (bytes, bytearray)):
            stored = stored.decode("utf-8", errors="replace")
        if isinstance(stored, str):
            text = stored.strip()
            if text.startswith(("[", "{")):
                try:
                    stored = json.loads(text)
                except json.JSONDecodeError:
                    pass
        return cls._semantic_value(stored) == cls._semantic_value(json.loads(expected))

    def lookup_receipt(
        self, operation_id: str, user_id: str
    ) -> ActivityCollectExchangeResult | None:
        if not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            table = uow.query_one(
                "SELECT 1 AS found FROM sqlite_master WHERE type='table' AND name=?",
                (self.operation_table,),
            )
            if table is None:
                return None
            previous = uow.query_one(
                f"SELECT payload,result_json FROM {self.operation_table} WHERE operation_id=?",
                (str(operation_id).strip(),),
            )
        if previous is None:
            return None
        try:
            payload = json.loads(str(previous["payload"]))
            previous_result = json.loads(str(previous["result_json"]))
            if not isinstance(payload, list) or len(payload) < 3:
                raise ValueError("invalid operation payload")
            if not isinstance(previous_result, list) or len(previous_result) < 2:
                raise ValueError("invalid operation result")
            claim_count = int(previous_result[0])
            rewards = tuple(str(value) for value in previous_result[1])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("invalid activity collect exchange receipt") from exc
        if str(payload[0]) != str(user_id):
            return ActivityCollectExchangeResult("operation_conflict")
        response = str(previous_result[2]) if len(previous_result) > 2 else ""
        if not response:
            response = f"集字兑换成功：{payload[2]}"
            if rewards:
                response += "\n" + "，".join(rewards)
        return ActivityCollectExchangeResult(
            "duplicate", claim_count, rewards=rewards, response=response
        )

    @staticmethod
    def _assert_tables(uow: DatabaseUnitOfWork) -> None:
        required = {
            "activity_collect_inventory",
            "activity_collect_claim",
            "activity_collect_exchange_operations",
            "user_xiuxian",
            "back",
        }
        existing = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError(
                "activity_state.001 schema_missing: " + ", ".join(missing)
            )

    @staticmethod
    def _reward_rows(
        rewards: Any,
    ) -> tuple[int, tuple[tuple[int, str, str, int], ...], tuple[str, ...]]:
        stone = 0
        items: dict[int, list[Any]] = {}
        descriptions: list[str] = []
        for reward in rewards:
            quantity = int(reward["quantity"])
            if quantity <= 0:
                raise ValueError("reward quantity must be positive")
            descriptions.append(
                str(reward.get("desc") or f"获得 {reward.get('name', '')}x{quantity}")
            )
            if str(reward["type"]) == "stone":
                stone += quantity
                continue
            item_id = int(reward["id"])
            item_type = str(reward["type"])
            if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                item_type = "技能"
            elif item_type in {"法器", "防具"}:
                item_type = "装备"
            metadata = [str(reward["name"]), item_type]
            if item_id in items and items[item_id][:2] != metadata:
                raise ValueError("conflicting reward metadata")
            items.setdefault(item_id, metadata + [0])[2] += quantity
        item_rows = tuple(
            (item_id, values[0], values[1], values[2])
            for item_id, values in sorted(items.items())
        )
        return stone, item_rows, tuple(descriptions)

    def exchange(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        phrase: str,
        required_tokens: dict[str, int],
        limit: int,
        rewards: Any,
        max_goods_num: int,
        activity_name: str = "",
        phrase_name: str = "",
    ) -> ActivityCollectExchangeResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key, phrase = map(str, (user_id, activity_key, phrase))
        limit, max_goods_num = int(limit), int(max_goods_num)
        token_rows = tuple(
            sorted(
                (str(word_char), int(quantity))
                for word_char, quantity in dict(required_tokens).items()
            )
        )
        if (
            not operation_id
            or not activity_key
            or not phrase
            or not token_rows
            or any(not word_char or quantity <= 0 for word_char, quantity in token_rows)
            or limit < 0
            or max_goods_num < 0
        ):
            raise ValueError("valid collect exchange is required")

        stone, item_rows, reward_descriptions = self._reward_rows(rewards)
        payload = json.dumps(
            [user_id, activity_key, phrase, token_rows, limit, stone, item_rows, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_tables(uow)
            previous = uow.query_one(
                f"SELECT payload,result_json FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if not self._payload_matches(previous["payload"], payload):
                    return ActivityCollectExchangeResult("operation_conflict")
                previous_result = json.loads(str(previous["result_json"]))
                response = str(previous_result[2]) if len(previous_result) > 2 else ""
                return ActivityCollectExchangeResult(
                    "duplicate",
                    int(previous_result[0]),
                    rewards=tuple(previous_result[1]),
                    response=response,
                )

            if uow.query_one(
                "SELECT 1 AS found FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return ActivityCollectExchangeResult("user_missing")

            claim_row = uow.query_one(
                "SELECT COALESCE(count,0) AS count FROM activity_collect_claim "
                "WHERE activity_key=? AND user_id=? AND phrase=?",
                (activity_key, user_id, phrase),
            )
            claim_count = int((claim_row or {}).get("count") or 0)
            if limit > 0 and claim_count >= limit:
                return ActivityCollectExchangeResult("limit_reached", claim_count)

            missing = []
            for word_char, quantity in token_rows:
                inventory = uow.query_one(
                    "SELECT COALESCE(count,0) AS count FROM activity_collect_inventory "
                    "WHERE activity_key=? AND user_id=? AND word_char=?",
                    (activity_key, user_id, word_char),
                )
                owned = int((inventory or {}).get("count") or 0)
                if owned < quantity:
                    missing.append((word_char, quantity - owned))
            if missing:
                return ActivityCollectExchangeResult(
                    "tokens_insufficient", claim_count, tuple(missing)
                )

            for item_id, _, _, quantity in item_rows:
                inventory = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                if (int((inventory or {}).get("goods_num") or 0) + quantity) > max_goods_num:
                    return ActivityCollectExchangeResult("inventory_full", claim_count)

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for word_char, quantity in token_rows:
                changed = uow.execute(
                    "UPDATE activity_collect_inventory SET count=count-?,update_time=? "
                    "WHERE activity_key=? AND user_id=? AND word_char=? AND count>=?",
                    (quantity, now, activity_key, user_id, word_char, quantity),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("collect inventory state changed")
            uow.execute(
                "INSERT INTO activity_collect_claim(activity_key,user_id,phrase,count,update_time) "
                "VALUES(?,?,?,1,?) ON CONFLICT(activity_key,user_id,phrase) DO UPDATE SET "
                "count=activity_collect_claim.count+1,update_time=excluded.update_time",
                (activity_key, user_id, phrase, now),
            )
            claim_count += 1

            if stone:
                uow.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? "
                    "WHERE user_id=?",
                    (stone, user_id),
                )
            for item_id, name, item_type, quantity in item_rows:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                    "goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                    "update_time=excluded.update_time",
                    (user_id, item_id, name, item_type, quantity, now, now, quantity),
                )

            uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload,result_json) "
                "VALUES(?,?,?)",
                (
                    operation_id,
                    payload,
                    json.dumps(
                        [
                            claim_count,
                            reward_descriptions,
                            self._success_response(
                                activity_name, phrase_name, phrase, reward_descriptions
                            ),
                        ],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                ),
            )
        return ActivityCollectExchangeResult(
            "applied",
            claim_count,
            rewards=reward_descriptions,
            response=self._success_response(
                activity_name, phrase_name, phrase, reward_descriptions
            ),
        )

    @staticmethod
    def _success_response(
        activity_name: str,
        phrase_name: str,
        phrase: str,
        rewards: tuple[str, ...],
    ) -> str:
        title = (
            f"{activity_name}兑换成功：{phrase_name}"
            if activity_name and phrase_name
            else f"集字兑换成功：{phrase}"
        )
        return title + ("\n" + "，".join(rewards) if rewards else "")


__all__ = ["ActivityCollectExchangeResult", "ActivityCollectExchangeSqlRepository"]
