from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class MixelixirRefineRewardResult:
    def __init__(
        self,
        status: str,
        reward_id: int = 0,
        reward_name: str = "",
        reward_quantity: int = 0,
        *,
        reason: str = "",
        subject: str = "",
    ) -> None:
        self.status = status
        self.reward_id = reward_id
        self.reward_name = reward_name
        self.reward_quantity = reward_quantity
        self.reason = reason
        self.subject = subject


class _KnownWriteRejection(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.subject = subject


class MixelixirRefineRewardSqlRepository:
    _MIX_FIELDS = ("丹药控火", "炼丹记录", "炼丹经验")

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _state_value(field: str, value) -> str:
        if field == "炼丹记录":
            raw = value
            if raw is None or raw == "" or raw == "0":
                raw = {}
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError):
                    raw = {}
            if not isinstance(raw, dict):
                raw = {}
            normalized = {}
            for key, item in raw.items():
                if isinstance(item, dict):
                    try:
                        count = int(item.get("num", 0) or 0)
                    except (TypeError, ValueError):
                        count = 0
                    normalized[str(key)] = {"name": str(item.get("name", "") or ""), "num": count}
                else:
                    normalized[str(key)] = {"name": str(item), "num": 0}
            return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if value is None or value == "":
            return "0"
        try:
            return str(int(value))
        except (TypeError, ValueError):
            try:
                return str(int(float(str(value))))
            except (TypeError, ValueError):
                return str(value)

    @classmethod
    def _state_tuple(cls, values) -> tuple[str, ...]:
        return tuple(cls._state_value(field, value) for field, value in zip(cls._MIX_FIELDS, values))

    def latest_ready_task(self, user_id: str, recipe_key: str | None = None) -> str | None:
        user_id = str(user_id)
        recipe_key = str(recipe_key or "").strip()
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mixelixir_refine_tasks'"
            ) is None:
                return None
            if recipe_key:
                row = uow.query_one(
                    "SELECT task_id FROM mixelixir_refine_tasks "
                    "WHERE user_id=? AND status='ready' AND recipe_key=? "
                    "ORDER BY created_at DESC,task_id DESC LIMIT 1",
                    (user_id, recipe_key),
                )
            else:
                row = uow.query_one(
                    "SELECT task_id FROM mixelixir_refine_tasks "
                    "WHERE user_id=? AND status='ready' ORDER BY created_at DESC,task_id DESC LIMIT 1",
                    (user_id,),
                )
        return None if row is None else str(row["task_id"])

    def claim(self, operation_id: str, user_id: str, task_id: str, max_goods_num: int) -> MixelixirRefineRewardResult:
        operation_id, user_id, task_id = str(operation_id).strip(), str(user_id), str(task_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or not task_id or max_goods_num <= 0:
            raise ValueError("valid operation, task and capacity are required")
        if not Path(self.player_database).is_file():
            return MixelixirRefineRewardResult("schema_missing")
        payload = json.dumps([user_id, task_id], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            required_tables = {"user_xiuxian", "back", "mixelixir_refine_tasks", "mixelixir_refine_reward_operations"}
            game_tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            player_tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM player_data.sqlite_master WHERE type='table'")
            }
            if not required_tables.issubset(game_tables) or not {"mix_elixir_info", "statistics"}.issubset(player_tables):
                return MixelixirRefineRewardResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,reward_id,reward_name,reward_quantity "
                "FROM mixelixir_refine_reward_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return MixelixirRefineRewardResult("operation_conflict")
                return MixelixirRefineRewardResult(
                    "duplicate",
                    int(previous["reward_id"]),
                    str(previous["reward_name"]),
                    int(previous["reward_quantity"]),
                )

            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return MixelixirRefineRewardResult("user_missing")
            task = uow.query_one(
                "SELECT status,reward_id,reward_name,reward_quantity,expected_mix_state,updated_mix_state "
                "FROM mixelixir_refine_tasks WHERE task_id=? AND user_id=?",
                (task_id, user_id),
            )
            if task is None:
                return MixelixirRefineRewardResult("task_missing")
            if str(task["status"]) != "ready":
                return MixelixirRefineRewardResult("state_changed")
            try:
                expected = json.loads(str(task["expected_mix_state"]))
                updated = json.loads(str(task["updated_mix_state"]))
                expected_values = tuple(expected[field] for field in self._MIX_FIELDS)
                updated_values = tuple(updated[field] for field in self._MIX_FIELDS)
            except (TypeError, ValueError, KeyError):
                return MixelixirRefineRewardResult("state_changed")

            reward_id = int(task["reward_id"])
            reward_name = str(task["reward_name"])
            quantity = int(task["reward_quantity"])
            if reward_id <= 0 or quantity <= 0 or not reward_name:
                return MixelixirRefineRewardResult("state_changed")

            mix_columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA player_data.table_info(mix_elixir_info)")
            }
            if not set(self._MIX_FIELDS).issubset(mix_columns):
                return MixelixirRefineRewardResult("state_changed")
            stats_columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(statistics)")
            }
            if "炼丹次数" not in stats_columns:
                return MixelixirRefineRewardResult("schema_missing")
            quoted_fields = ",".join(f'"{field}"' for field in self._MIX_FIELDS)
            current = uow.query_one(
                f"SELECT {quoted_fields} FROM player_data.mix_elixir_info WHERE user_id=?",
                (user_id,),
            )
            if current is None or self._state_tuple(current[field] for field in self._MIX_FIELDS) != self._state_tuple(expected_values):
                return MixelixirRefineRewardResult("state_changed")

            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, reward_id),
            )
            if (int(inventory["goods_num"]) if inventory else 0) + quantity > max_goods_num:
                return MixelixirRefineRewardResult("inventory_full")

            back_columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")
            }
            has_bind_num = "bind_num" in back_columns
            current_inventory = uow.query_one(
                "SELECT goods_name,goods_type,COALESCE(goods_num,0) AS goods_num"
                + (",COALESCE(bind_num,0) AS bind_num" if has_bind_num else "")
                + " FROM back WHERE user_id=? AND goods_id=?",
                (user_id, reward_id),
            )
            previous_stats = uow.query_one(
                'SELECT COALESCE("炼丹次数",0) AS count FROM player_data.statistics WHERE user_id=?',
                (user_id,),
            )
            expected_inventory_num = (int(current_inventory["goods_num"]) if current_inventory else 0) + quantity
            expected_bind_num = int(current_inventory["bind_num"]) if current_inventory and has_bind_num else 0
            previous_count = int(previous_stats["count"]) if previous_stats else 0
            updated_state = tuple(
                self._state_value(field, updated[field]) for field in self._MIX_FIELDS
            )
            expected_inventory = (reward_name, "丹药", expected_inventory_num)
            if has_bind_num:
                expected_inventory += (expected_bind_num,)
            try:
                with uow.savepoint("mixelixir_refine_reward"):
                    if current_inventory is None:
                        insert_columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                        values = [user_id, reward_id, reward_name, "丹药", quantity]
                        if has_bind_num:
                            insert_columns.append("bind_num")
                            values.append(0)
                        columns_sql = ",".join(f'"{column}"' for column in insert_columns)
                        placeholders = ",".join("?" for _ in values)
                        changed = uow.execute(
                            f"INSERT INTO back({columns_sql}) VALUES({placeholders})",
                            tuple(values),
                        )
                    else:
                        changed = uow.execute(
                            "UPDATE back SET goods_name=?,goods_type='丹药',goods_num=goods_num+? "
                            "WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)=?",
                            (reward_name, quantity, user_id, reward_id, int(current_inventory["goods_num"])),
                        )
                    if changed.rowcount != 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_inventory_write_rejected", f"item:{reward_id}"
                        )
                    saved_inventory = uow.query_one(
                        "SELECT goods_name,goods_type,COALESCE(goods_num,0) AS goods_num"
                        + (",COALESCE(bind_num,0) AS bind_num" if has_bind_num else "")
                        + " FROM back WHERE user_id=? AND goods_id=?",
                        (user_id, reward_id),
                    )
                    actual_inventory = None if saved_inventory is None else (
                        str(saved_inventory["goods_name"]),
                        str(saved_inventory["goods_type"]),
                        int(saved_inventory["goods_num"]),
                    ) + ((int(saved_inventory["bind_num"]),) if has_bind_num else ())
                    if actual_inventory != expected_inventory:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_inventory_projection_mismatch", f"item:{reward_id}"
                        )

                    assignments = ",".join(f'"{field}"=?' for field in self._MIX_FIELDS)
                    changed = uow.execute(
                        f"UPDATE player_data.mix_elixir_info SET {assignments} WHERE user_id=?",
                        updated_state + (user_id,),
                    )
                    if changed.rowcount != 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_player_write_rejected", "player_mixelixir"
                        )
                    saved_mix = uow.query_one(
                        f"SELECT {quoted_fields} FROM player_data.mix_elixir_info WHERE user_id=?",
                        (user_id,),
                    )
                    if saved_mix is None or self._state_tuple(
                        saved_mix[field] for field in self._MIX_FIELDS
                    ) != self._state_tuple(updated_state):
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_player_projection_mismatch", "player_mixelixir"
                        )

                    changed = uow.execute(
                        'INSERT INTO player_data.statistics(user_id,"炼丹次数") VALUES(?,1) '
                        'ON CONFLICT(user_id) DO UPDATE SET "炼丹次数"='
                        'COALESCE(player_data.statistics."炼丹次数",0)+1',
                        (user_id,),
                    )
                    if changed.rowcount != 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_statistics_write_rejected", "player_statistics"
                        )
                    saved_stats = uow.query_one(
                        'SELECT COALESCE("炼丹次数",0) AS count FROM player_data.statistics WHERE user_id=?',
                        (user_id,),
                    )
                    if saved_stats is None or int(saved_stats["count"]) != previous_count + 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_statistics_projection_mismatch", "player_statistics"
                        )

                    changed = uow.execute(
                        "UPDATE mixelixir_refine_tasks SET status='claimed',claimed_at=CURRENT_TIMESTAMP "
                        "WHERE task_id=? AND user_id=? AND status='ready'",
                        (task_id, user_id),
                    )
                    if changed.rowcount != 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_task_write_rejected", "task"
                        )
                    saved_task = uow.query_one(
                        "SELECT status,claimed_at FROM mixelixir_refine_tasks WHERE task_id=? AND user_id=?",
                        (task_id, user_id),
                    )
                    if saved_task is None or saved_task["status"] != "claimed" or not saved_task["claimed_at"]:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_task_projection_mismatch", "task"
                        )

                    changed = uow.execute(
                        "INSERT INTO mixelixir_refine_reward_operations "
                        "(operation_id,payload,task_id,reward_id,reward_name,reward_quantity) VALUES(?,?,?,?,?,?)",
                        (operation_id, payload, task_id, reward_id, reward_name, quantity),
                    )
                    if changed.rowcount != 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_receipt_write_rejected", "receipt"
                        )
                    saved_receipt = uow.query_one(
                        "SELECT payload,task_id,reward_id,reward_name,reward_quantity "
                        "FROM mixelixir_refine_reward_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if saved_receipt is None or tuple(saved_receipt.values()) != (
                        payload, task_id, reward_id, reward_name, quantity
                    ):
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_receipt_projection_mismatch", "receipt"
                        )

                    final_inventory = uow.query_one(
                        "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                        (user_id, reward_id),
                    )
                    if final_inventory is None or int(final_inventory["goods_num"]) != expected_inventory_num:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_inventory_projection_mismatch", f"item:{reward_id}"
                        )
                    final_mix = uow.query_one(
                        f"SELECT {quoted_fields} FROM player_data.mix_elixir_info WHERE user_id=?",
                        (user_id,),
                    )
                    if final_mix is None or self._state_tuple(
                        final_mix[field] for field in self._MIX_FIELDS
                    ) != self._state_tuple(updated_state):
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_player_projection_mismatch", "player_mixelixir"
                        )
                    final_stats = uow.query_one(
                        'SELECT COALESCE("炼丹次数",0) AS count FROM player_data.statistics WHERE user_id=?',
                        (user_id,),
                    )
                    if final_stats is None or int(final_stats["count"]) != previous_count + 1:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_statistics_projection_mismatch", "player_statistics"
                        )
                    final_task = uow.query_one(
                        "SELECT status,claimed_at FROM mixelixir_refine_tasks WHERE task_id=? AND user_id=?",
                        (task_id, user_id),
                    )
                    if final_task is None or final_task["status"] != "claimed" or not final_task["claimed_at"]:
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_task_projection_mismatch", "task"
                        )
                    final_receipt = uow.query_one(
                        "SELECT payload,task_id,reward_id,reward_name,reward_quantity "
                        "FROM mixelixir_refine_reward_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if final_receipt is None or tuple(final_receipt.values()) != (
                        payload, task_id, reward_id, reward_name, quantity
                    ):
                        raise _KnownWriteRejection(
                            "mixelixir_refine_reward_receipt_projection_mismatch", "receipt"
                        )
            except _KnownWriteRejection as rejected:
                return MixelixirRefineRewardResult(
                    "write_rejected",
                    reward_id,
                    reward_name,
                    quantity,
                    reason=rejected.reason,
                    subject=rejected.subject,
                )
            return MixelixirRefineRewardResult("applied", reward_id, reward_name, quantity)


__all__ = ["MixelixirRefineRewardSqlRepository", "MixelixirRefineRewardResult"]
