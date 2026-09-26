from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


_NUMERIC_TEXT = re.compile(r"^[+-]?\d+$")


def _semantic_payload(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        text = value.strip()
        if text[:1] in "[{":
            try:
                value = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                return text
        elif _NUMERIC_TEXT.fullmatch(text):
            try:
                return int(text)
            except ValueError:
                return text
    if isinstance(value, Mapping):
        return {str(key): _semantic_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_semantic_payload(item) for item in value]
    return value


def operation_payload_matches(stored: Any, expected: Any) -> bool:
    """Compare old/new JSON payloads without depending on the legacy package."""
    return _semantic_payload(stored) == _semantic_payload(expected)


@dataclass(frozen=True)
class TrainingPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    points: int = 0
    purchased: int = 0
    inventory: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _normalize_weekly(value: Any, today: date) -> dict[str, int | str]:
    if isinstance(value, str):
        try:
            value = json.loads(value) if value else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            value = {}
    if not isinstance(value, Mapping):
        value = {}
    try:
        reset = _as_date(value.get("_last_reset"))
    except (TypeError, ValueError):
        reset = None
    if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
        return {"_last_reset": today.isoformat()}
    weekly: dict[str, int | str] = {"_last_reset": reset.isoformat()}
    for raw_key, raw_amount in value.items():
        key = str(raw_key)
        if key == "_last_reset":
            continue
        try:
            amount = int(raw_amount)
        except (TypeError, ValueError):
            continue
        if amount >= 0:
            weekly[key] = amount
    return weekly


class TrainingPurchaseSqlRepository:
    """Atomically exchange training points for one inventory item."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _payload(
        user_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        unit_cost: int,
        weekly_limit: int,
        expected_points: int,
        weekly: Mapping[str, Any],
        max_goods_num: int,
        bind_flag: int,
    ) -> str:
        return json.dumps(
            [
                user_id,
                item_id,
                item_name,
                item_type,
                quantity,
                unit_cost,
                weekly_limit,
                expected_points,
                dict(weekly),
                max_goods_num,
                bind_flag,
            ],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(
        status: str,
        quantity: int,
        unit_cost: int,
        *,
        points: int,
        purchased: int = 0,
        inventory: int = 0,
    ) -> TrainingPurchaseResult:
        succeeded = status in {"applied", "duplicate"}
        return TrainingPurchaseResult(
            status,
            quantity if succeeded else 0,
            quantity * unit_cost if succeeded else 0,
            int(points),
            int(purchased),
            int(inventory),
        )

    def purchase(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        unit_cost: int,
        weekly_limit: int,
        expected_points: int,
        expected_weekly_purchases: Any,
        max_goods_num: int,
        bind_flag: int = 1,
        today: Any = None,
    ) -> TrainingPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        item_id, quantity, unit_cost, weekly_limit = map(
            int, (item_id, quantity, unit_cost, weekly_limit)
        )
        expected_points, max_goods_num = map(int, (expected_points, max_goods_num))
        item_name, item_type = str(item_name), str(item_type)
        bind_flag = 1 if int(bind_flag) == 1 else 0
        if today is None:
            raw_weekly = expected_weekly_purchases
            if isinstance(raw_weekly, str):
                try:
                    raw_weekly = json.loads(raw_weekly) if raw_weekly else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw_weekly = {}
            today = raw_weekly.get("_last_reset") if isinstance(raw_weekly, Mapping) else None
        try:
            today = _as_date(today) if today is not None else self.clock.now().date()
        except (TypeError, ValueError):
            raise ValueError("today must be an ISO date") from None
        weekly = _normalize_weekly(expected_weekly_purchases, today)
        if (
            not operation_id
            or not item_name
            or item_id <= 0
            or quantity <= 0
            or unit_cost < 0
            or weekly_limit < 0
            or expected_points < 0
            or max_goods_num < 0
        ):
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        payload = self._payload(
            user_id,
            item_id,
            item_name,
            item_type,
            quantity,
            unit_cost,
            weekly_limit,
            expected_points,
            weekly,
            max_goods_num,
            bind_flag,
        )

        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            game_tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            player_tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM player_data.sqlite_master WHERE type='table'"
                )
            }
            if not {"user_xiuxian", "back", "training_purchase_operations"}.issubset(game_tables):
                return self._result("schema_missing", quantity, unit_cost, points=expected_points)
            if "training" not in player_tables:
                return self._result("schema_missing", quantity, unit_cost, points=expected_points)
            operation_columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(training_purchase_operations)")
            }
            required_operation_columns = {
                "operation_id", "payload", "quantity", "cost", "points", "purchased", "inventory"
            }
            if not required_operation_columns.issubset(operation_columns):
                return self._result("schema_missing", quantity, unit_cost, points=expected_points)
            previous = uow.query_one(
                "SELECT payload,quantity,cost,points,purchased,inventory "
                "FROM training_purchase_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if not operation_payload_matches(previous["payload"], payload):
                    return self._result("state_changed", quantity, unit_cost, points=expected_points)
                return TrainingPurchaseResult(
                    "duplicate",
                    int(previous["quantity"]),
                    int(previous["cost"]),
                    int(previous["points"]),
                    int(previous["purchased"]),
                    int(previous["inventory"]),
                )

            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return self._result("user_missing", quantity, unit_cost, points=expected_points)
            training_columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA player_data.table_info(training)")
            }
            if not {"points", "weekly_purchases"}.issubset(training_columns):
                return self._result("state_changed", quantity, unit_cost, points=expected_points)
            training = uow.query_one(
                "SELECT COALESCE(points,0) AS points,COALESCE(weekly_purchases,'{}') AS weekly "
                "FROM player_data.training WHERE user_id=?",
                (user_id,),
            )
            if training is None:
                return self._result("state_changed", quantity, unit_cost, points=expected_points)
            current_weekly = _normalize_weekly(training["weekly"], today)
            if int(training["points"]) != expected_points or current_weekly != weekly:
                return self._result("state_changed", quantity, unit_cost, points=expected_points)
            purchased = int(weekly.get(str(item_id), 0))
            if purchased + quantity > weekly_limit:
                return self._result(
                    "limit_reached", quantity, unit_cost, points=expected_points, purchased=purchased
                )
            cost = quantity * unit_cost
            if expected_points < cost:
                return self._result(
                    "points_insufficient", quantity, unit_cost, points=expected_points, purchased=purchased
                )

            back_columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")
            }
            inventory_row = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            inventory = int(inventory_row["goods_num"]) if inventory_row else 0
            if inventory + quantity > max_goods_num:
                return self._result(
                    "inventory_full", quantity, unit_cost, points=expected_points,
                    purchased=purchased, inventory=inventory,
                )
            points, purchased, inventory = expected_points - cost, purchased + quantity, inventory + quantity
            weekly[str(item_id)] = purchased
            weekly_json = json.dumps(weekly, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            changed = uow.execute(
                "UPDATE player_data.training SET points=?,weekly_purchases=? "
                "WHERE user_id=? AND COALESCE(points,0)=?",
                (points, weekly_json, user_id, expected_points),
            )
            if changed.rowcount != 1:
                raise RuntimeError("training points changed during purchase")

            timestamp = self.clock.now().isoformat(sep=" ", timespec="seconds")
            if inventory_row is None:
                fields = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                values: list[Any] = [user_id, item_id, item_name, item_type, quantity]
                if "create_time" in back_columns:
                    fields.append("create_time")
                    values.append(timestamp)
                if "update_time" in back_columns:
                    fields.append("update_time")
                    values.append(timestamp)
                if "bind_num" in back_columns:
                    fields.append("bind_num")
                    values.append(quantity if bind_flag else 0)
                uow.execute(
                    f"INSERT INTO back({','.join(fields)}) VALUES({','.join('?' for _ in values)})",
                    tuple(values),
                )
            else:
                assignments = "goods_num=goods_num+?"
                values = [quantity]
                if "goods_name" in back_columns:
                    assignments = "goods_name=?,goods_type=?," + assignments
                    values = [item_name, item_type, *values]
                if "bind_num" in back_columns and bind_flag:
                    assignments += ",bind_num=COALESCE(bind_num,0)+?"
                    values.append(quantity)
                if "update_time" in back_columns:
                    assignments += ",update_time=?"
                    values.append(timestamp)
                values.extend((user_id, item_id))
                if uow.execute(
                    f"UPDATE back SET {assignments} WHERE user_id=? AND goods_id=?",
                    tuple(values),
                ).rowcount != 1:
                    raise RuntimeError("training inventory changed during purchase")

            uow.execute(
                "INSERT INTO training_purchase_operations"
                "(operation_id,payload,quantity,cost,points,purchased,inventory) VALUES(?,?,?,?,?,?,?)",
                (operation_id, payload, quantity, cost, points, purchased, inventory),
            )
            return TrainingPurchaseResult("applied", quantity, cost, points, purchased, inventory)


__all__ = ["TrainingPurchaseResult", "TrainingPurchaseSqlRepository"]
