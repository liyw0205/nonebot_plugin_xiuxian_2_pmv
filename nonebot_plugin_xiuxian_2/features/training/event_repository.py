from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


class TrainingEventSqlRepository:
    """Persist one training event across the game and player databases.

    Random event selection and reward formatting remain in the legacy adapter;
    this repository owns the state/CAS, inventory, resources and statistics
    write once the adapter has produced an explicit snapshot.
    """

    _FIELDS = (
        "progress",
        "last_time",
        "points",
        "completed",
        "max_progress",
        "last_event",
        "weekly_purchases",
    )

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _state_value(field: str, value: Any) -> str:
        if field == "weekly_purchases":
            raw = value
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw) if raw else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw = {}
            if not isinstance(raw, dict):
                raw = {}
            return json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return str(value)

    @classmethod
    def _state_matches(cls, current: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
        for field in cls._FIELDS:
            actual = current.get(field)
            wanted = expected.get(field)
            if field == "weekly_purchases":
                try:
                    actual_value = json.loads(str(actual)) if actual not in (None, "") else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    actual_value = {}
                if actual_value == wanted:
                    continue
            elif str(actual) == str(wanted):
                continue
            return False
        return True

    @staticmethod
    def _payload(
        user_id: str,
        expected_state: Mapping[str, Any],
        state: Mapping[str, Any],
        expected_user: Mapping[str, Any],
        stone_delta: int,
        exp_delta: int,
        hp_delta: int,
        rewards: tuple[tuple[int, str, str, int], ...],
        max_goods_num: int,
    ) -> str:
        return json.dumps(
            [
                user_id,
                dict(expected_state),
                dict(state),
                dict(expected_user),
                stone_delta,
                exp_delta,
                hp_delta,
                rewards,
                max_goods_num,
            ],
            ensure_ascii=True,
            sort_keys=True,
            default=str,
        )

    @staticmethod
    def _duplicate(previous: Mapping[str, Any], user_id: str) -> dict[str, Any]:
        if str(previous.get("payload", "")) == "":
            return {"status": "operation_conflict", "message": ""}
        try:
            stored = json.loads(str(previous["payload"]))
            if str(stored[0]) != user_id or not isinstance(stored[2], dict):
                return {"status": "operation_conflict", "message": ""}
            fallback = str(stored[2].get("last_event", ""))
        except (IndexError, TypeError, ValueError, json.JSONDecodeError):
            return {"status": "operation_conflict", "message": ""}
        result_json = previous.get("result_json")
        if result_json:
            try:
                result = json.loads(str(result_json))
                if isinstance(result, dict) and result.get("message") is not None:
                    fallback = str(result.get("message", fallback))
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        return {"status": "duplicate", "message": fallback}

    @staticmethod
    def _inventory_columns(uow: AttachedDatabaseUnitOfWork) -> set[str]:
        return {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}

    @classmethod
    def _apply_item(
        cls,
        uow: AttachedDatabaseUnitOfWork,
        *,
        user_id: str,
        item_id: int,
        name: str,
        item_type: str,
        amount: int,
        timestamp: str,
        columns: set[str],
    ) -> None:
        row = uow.query_one(
            "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
            (user_id, item_id),
        )
        if amount > 0 and row is None:
            fields = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
            values: list[Any] = [user_id, item_id, name, item_type, amount]
            if "create_time" in columns:
                fields.append("create_time")
                values.append(timestamp)
            if "update_time" in columns:
                fields.append("update_time")
                values.append(timestamp)
            if "bind_num" in columns:
                fields.append("bind_num")
                values.append(amount)
            uow.execute(
                f"INSERT INTO back({','.join(fields)}) VALUES({','.join('?' for _ in values)})",
                tuple(values),
            )
            return
        current = int(row["goods_num"]) if row else 0
        if current + amount < 0:
            raise ValueError("item_missing")
        if "bind_num" in columns:
            if amount > 0:
                assignment = "goods_num=goods_num+?,bind_num=COALESCE(bind_num,0)+?"
                values: list[Any] = [amount, amount]
            else:
                assignment = "goods_num=goods_num+?,bind_num=MIN(COALESCE(bind_num,0),goods_num+?)"
                values = [amount, amount]
        else:
            assignment = "goods_num=goods_num+?"
            values = [amount]
        if "goods_name" in columns:
            assignment = "goods_name=?,goods_type=?," + assignment
            values = [name, item_type, *values]
        if "update_time" in columns:
            assignment += ",update_time=?"
            values.append(timestamp)
        values.extend((user_id, item_id))
        changed = uow.execute(
            f"UPDATE back SET {assignment} WHERE user_id=? AND goods_id=?",
            tuple(values),
        )
        if changed.rowcount != 1:
            raise RuntimeError("training inventory changed")

    def apply(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_state: Mapping[str, Any],
        state: Mapping[str, Any],
        expected_user: Mapping[str, Any],
        stone_delta: int = 0,
        exp_delta: int = 0,
        hp_delta: int = 0,
        items: Any = (),
        max_goods_num: int = 0,
    ) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        expected_state, state, expected_user = dict(expected_state), dict(state), dict(expected_user)
        stone_delta, exp_delta, hp_delta, max_goods_num = map(
            int, (stone_delta, exp_delta, hp_delta, max_goods_num)
        )
        rewards = tuple(
            (
                int(item["id"]),
                str(item["name"]),
                str(item["type"]),
                int(item["amount"]),
            )
            for item in items
            if int(item["amount"]) != 0
        )
        payload = self._payload(
            user_id,
            expected_state,
            state,
            expected_user,
            stone_delta,
            exp_delta,
            hp_delta,
            rewards,
            max_goods_num,
        )
        timestamp = str(state.get("last_time") or "")
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
            if not {"user_xiuxian", "back", "training_event_operations"}.issubset(game_tables):
                return {"status": "schema_missing", "message": "训练事件 schema 尚未迁移。"}
            if not {"training", "statistics"}.issubset(player_tables):
                return {"status": "schema_missing", "message": "训练玩家 schema 尚未迁移。"}
            stats_columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA player_data.table_info(statistics)")
            }
            if "历练次数" not in stats_columns:
                return {"status": "schema_missing", "message": "训练统计 schema 尚未迁移。"}

            operation_columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(training_event_operations)")
            }
            result_column = ",result_json" if "result_json" in operation_columns else ""
            previous = uow.query_one(
                f"SELECT payload{result_column} FROM training_event_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return {"status": "operation_conflict", "message": ""}
                return self._duplicate(previous, user_id)

            user = uow.query_one(
                "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            wanted = tuple(int(expected_user.get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
            actual_user = (
                tuple(int(user[key] or 0) for key in ("stone", "exp", "hp", "mp"))
                if user is not None
                else None
            )
            if actual_user != wanted:
                return {"status": "state_changed", "message": "玩家状态已变化，请重新开始历练。"}

            current = uow.query_one(
                "SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases "
                "FROM player_data.training WHERE user_id=?",
                (user_id,),
            )
            if current is None or not self._state_matches(current, expected_state):
                return {"status": "state_changed", "message": "历练状态已变化，请重新开始。"}

            columns = self._inventory_columns(uow)
            for item_id, _, _, amount in rewards:
                inventory = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                count = int(inventory["goods_num"]) if inventory else 0
                if count + amount < 0:
                    return {"status": "item_missing", "message": "历练物品数量不足。"}
                if amount > 0 and count + amount > max_goods_num:
                    return {"status": "inventory_full", "message": "背包容量不足。"}
            if wanted[0] + stone_delta < 0 or wanted[1] + exp_delta < 0 or wanted[2] + hp_delta < 0:
                return {"status": "resource_missing", "message": "玩家资源不足。"}

            assignments = ",".join(f'"{field}"=?' for field in self._FIELDS)
            changed = uow.execute(
                f"UPDATE player_data.training SET {assignments} WHERE user_id=?",
                tuple(self._state_value(field, state.get(field)) for field in self._FIELDS) + (user_id,),
            )
            if changed.rowcount != 1:
                raise RuntimeError("training state changed during event")
            uow.execute(
                "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+?,exp=COALESCE(exp,0)+?,"
                "hp=COALESCE(hp,0)+? WHERE user_id=?",
                (stone_delta, exp_delta, hp_delta, user_id),
            )
            for item_id, name, item_type, amount in rewards:
                self._apply_item(
                    uow,
                    user_id=user_id,
                    item_id=item_id,
                    name=name,
                    item_type=item_type,
                    amount=amount,
                    timestamp=timestamp,
                    columns=columns,
                )
            uow.execute(
                'INSERT INTO player_data.statistics(user_id,"历练次数") VALUES(?,1) '
                'ON CONFLICT(user_id) DO UPDATE SET "历练次数"=COALESCE(player_data.statistics."历练次数",0)+1',
                (user_id,),
            )
            message = str(state.get("last_event", ""))
            if "result_json" in operation_columns:
                uow.execute(
                    "INSERT INTO training_event_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                    (
                        operation_id,
                        payload,
                        json.dumps(
                            {"status": "applied", "message": message}, ensure_ascii=False
                        ),
                    ),
                )
            else:
                # A pre-migration two-column table can still receive a
                # compatible payload; startup migration will add the richer
                # result projection on the next normal boot.
                uow.execute(
                    "INSERT INTO training_event_operations(operation_id,payload) VALUES(?,?)",
                    (operation_id, payload),
                )
            return {"status": "applied", "message": message}


__all__ = ["TrainingEventSqlRepository"]
