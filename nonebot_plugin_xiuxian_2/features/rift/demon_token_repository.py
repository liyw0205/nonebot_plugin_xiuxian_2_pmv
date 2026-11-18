from __future__ import annotations

from ...core.numeric import as_int_like
from dataclasses import replace as _reason_replace
from ...core.snapshot_reason import first_difference

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from ...infrastructure.database.uow import DatabaseUnitOfWork


@dataclass(frozen=True)
class RiftDemonTokenBattleResult:
    status: str
    explore_count: int = 0
    message: str = ""
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class _DemonTokenWriteRejected(Exception):
    def __init__(self, result: RiftDemonTokenBattleResult) -> None:
        self.result = result


class RiftDemonTokenBattleSqlRepository:
    """Persist a pre-rolled demon-token battle across game and player data."""

    operation_table = "rift_demon_token_battle_operations"
    supported_statistics = frozenset({"秘境打怪", "秘境次数", "rift_combat"})

    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()

    def replay(self, operation_id: str) -> RiftDemonTokenBattleResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            exists = uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (self.operation_table,),
            )
            if exists is None:
                return None
            row = uow.query_one(
                f"SELECT explore_count,message FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            return RiftDemonTokenBattleResult(
                "duplicate", int(row["explore_count"]), str(row["message"])
            )

    @staticmethod
    def _progress_reward(expected_count: int, reward: Any) -> tuple[int, str, str, int] | None:
        if not 0 <= expected_count <= 9:
            raise ValueError("expected explore count must be between 0 and 9")
        if expected_count != 9:
            if reward is not None:
                raise ValueError("progress reward must match the tenth completion")
            return None
        if not isinstance(reward, dict):
            raise ValueError("progress reward must match the tenth completion")
        try:
            normalized = (
                int(reward["id"]),
                str(reward["name"]).strip(),
                str(reward["type"]).strip(),
                int(reward.get("amount", 1)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("progress reward must be complete") from exc
        if normalized[0] <= 0 or not normalized[1] or not normalized[2] or normalized[3] <= 0:
            raise ValueError("progress reward must be complete and positive")
        return normalized

    def settle(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        expected_rift: dict[str, Any],
        expected_user: dict[str, Any],
        expected_explore_count: int,
        outcome: dict[str, Any],
        max_goods_num: int,
    ) -> RiftDemonTokenBattleResult:
        operation_id, user_id, item_id = str(operation_id).strip(), str(user_id), int(item_id)
        expected_count, max_goods_num = int(expected_explore_count), int(max_goods_num)
        rift_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
        expected = tuple(as_int_like(expected_user.get(key)) for key in ("stone", "exp", "hp", "mp"))
        delta = tuple(as_int_like(outcome.get("delta", {}).get(key)) for key in ("stone", "exp", "hp", "mp"))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item.get("amount", 1)))
            for item in outcome.get("items", ())
            if int(item.get("amount", 1)) > 0
        )
        progress_reward = self._progress_reward(expected_count, outcome.get("progress_reward"))
        if progress_reward is not None:
            rewards += (progress_reward,)
        statistics = tuple(
            sorted(
                (str(key), int(value))
                for key, value in outcome.get("statistics", {}).items()
                if int(value)
            )
        )
        message = str(outcome.get("message", ""))
        if not operation_id or not user_id or item_id <= 0 or max_goods_num < 0:
            raise ValueError("valid operation, user, item and snapshots are required")
        unsupported = {key for key, _ in statistics} - self.supported_statistics
        if unsupported:
            raise ValueError(f"unsupported rift statistics: {sorted(unsupported)}")
        payload = json.dumps(
            [user_id, item_id, rift_snapshot, expected, expected_count, delta, rewards, statistics, message, max_goods_num],
            ensure_ascii=True,
            sort_keys=True,
        )

        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            required_game_tables = {"rift_entries", "user_cd", "user_xiuxian", "back", self.operation_table}
            game_tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            player_tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM player_data.sqlite_master WHERE type='table'")
            }
            if not required_game_tables.issubset(game_tables) or not {"rift", "statistics"}.issubset(player_tables):
                return RiftDemonTokenBattleResult("schema_missing")
            rift_columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(rift)")
            }
            statistics_columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(statistics)")
            }
            if "explore_count" not in rift_columns or not (set(key for key, _ in statistics) <= statistics_columns):
                return RiftDemonTokenBattleResult("schema_missing")

            old = uow.query_one(
                f"SELECT payload,explore_count,message FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                if str(old["payload"]) == payload:
                    return RiftDemonTokenBattleResult(
                        "duplicate", int(old["explore_count"]), str(old["message"])
                    )
                return _reason_replace(RiftDemonTokenBattleResult("state_changed"), reason="operation_conflict", subject="")

            entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
            cd = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?", (user_id,))
            user = uow.query_one(
                "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if entry is None or str(entry["status"]) != "active" or cd is None or int(cd["type"]) != 3:
                return RiftDemonTokenBattleResult("not_active")
            if json.loads(str(entry["rift_data"])) != json.loads(rift_snapshot) or user is None or tuple(map(as_int_like, user.values())) != expected:
                return _reason_replace(RiftDemonTokenBattleResult("state_changed"), reason="rift_snapshot_changed" if json.loads(str(entry["rift_data"])) != json.loads(rift_snapshot) else "user_missing" if user is None else first_difference(tuple(map(as_int_like,user.values())), expected, ("stone_changed","exp_changed","hp_changed","mp_changed")), subject="")

            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None or int(item["goods_num"]) < 1:
                return RiftDemonTokenBattleResult("item_missing")
            back_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            if "bind_num" in back_columns:
                item = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num "
                    "FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
            before_item_goods = int(item["goods_num"])
            before_item_bind = int(item.get("bind_num", 0) or 0) if "bind_num" in item else 0
            count_row = uow.query_one(
                'SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,)
            )
            current_count = int(count_row["explore_count"] or 0) if count_row else 0
            if current_count != expected_count:
                return _reason_replace(RiftDemonTokenBattleResult("state_changed"), reason="rift_explore_count_changed", subject="")
            if expected[0] + delta[0] < 0:
                return RiftDemonTokenBattleResult("resource_missing", reason="stone_insufficient")

            totals: dict[int, int] = {}
            metadata: dict[int, tuple[str, str]] = {}
            for reward_id, name, item_type, amount in rewards:
                totals[reward_id] = totals.get(reward_id, 0) + amount
                old_metadata = metadata.setdefault(reward_id, (name, item_type))
                if old_metadata != (name, item_type):
                    raise ValueError("conflicting reward metadata")
            reward_snapshots: dict[int, dict[str, Any] | None] = {}
            for reward_id, amount in totals.items():
                row = uow.query_one(
                    "SELECT goods_num,goods_name,goods_type" + (",bind_num" if "bind_num" in back_columns else "")
                    + " FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, reward_id),
                )
                reward_snapshots[reward_id] = dict(row) if row is not None else None
                current_amount = int(row["goods_num"] or 0) if row else 0
                consumed_amount = 1 if reward_id == item_id else 0
                if current_amount - consumed_amount + amount > max_goods_num:
                    return RiftDemonTokenBattleResult("inventory_full")

            bind_update = ",bind_num=MIN(MAX(COALESCE(bind_num,0)-1,0),goods_num-1)" if "bind_num" in back_columns else ""
            new_count = 0 if expected_count + 1 >= 10 else expected_count + 1
            expected_user = (
                expected[0] + delta[0],
                max(0, expected[1] + delta[1]),
                max(1, expected[2] + delta[2]),
                max(1, expected[3] + delta[3]),
            )
            entry_snapshot = json.loads(str(entry["rift_data"]))
            stats_row_exists = uow.query_one(
                "SELECT user_id FROM player_data.statistics WHERE user_id=?", (user_id,)
            ) is not None
            stats_before = {
                key: uow.query_one(
                    f'SELECT "{key}" AS value FROM player_data.statistics WHERE user_id=?',
                    (user_id,),
                )
                for key, _ in statistics
            }

            try:
                with uow.savepoint("rift_demon_token"):
                    consumed = uow.execute(
                        "UPDATE back SET goods_num=goods_num-1" + bind_update +
                        " WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)>=1",
                        (user_id, item_id),
                    )
                    saved_item = uow.query_one(
                        "SELECT goods_num" + (",bind_num" if "bind_num" in back_columns else "")
                        + " FROM back WHERE user_id=? AND goods_id=?",
                        (user_id, item_id),
                    )
                    expected_item_bind = min(max(before_item_bind - 1, 0), before_item_goods - 1)
                    if (
                        consumed.rowcount != 1
                        or saved_item is None
                        or int(saved_item["goods_num"] or 0) != before_item_goods - 1
                        or ("bind_num" in back_columns and int(saved_item["bind_num"] or 0) != expected_item_bind)
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult(
                                "state_changed", expected_count,
                                reason="rift_demon_token_item_write_rejected", subject=user_id,
                            )
                        )

                    changed_user = uow.execute(
                        "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')),exp=?,hp=?,mp=? WHERE user_id=?",
                        (delta[0], expected_user[1], expected_user[2], expected_user[3], user_id),
                    )
                    saved_user = uow.query_one(
                        "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,)
                    )
                    if (
                        changed_user.rowcount != 1
                        or saved_user is None
                        or tuple(as_int_like(saved_user[key]) for key in ("stone", "exp", "hp", "mp")) != expected_user
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult(
                                "state_changed", expected_count,
                                reason="rift_demon_token_user_write_rejected", subject=user_id,
                            )
                        )

                    now = self.clock.now().isoformat()
                    for reward_id, amount in totals.items():
                        name, item_type = metadata[reward_id]
                        insert_columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time"]
                        values: list[Any] = [user_id, reward_id, name, item_type, amount, now, now]
                        bind_sql = ""
                        if "bind_num" in back_columns:
                            insert_columns.append("bind_num")
                            values.append(amount)
                            bind_sql = ",bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num"
                        columns_sql = ",".join(f'"{column}"' for column in insert_columns)
                        placeholders = ",".join("?" for _ in values)
                        written = uow.execute(
                            f"INSERT INTO back({columns_sql}) VALUES({placeholders}) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num," +
                            "update_time=EXCLUDED.update_time" + bind_sql,
                            tuple(values),
                        )
                        saved_reward = uow.query_one(
                            "SELECT goods_num,goods_name,goods_type" + (",bind_num" if "bind_num" in back_columns else "")
                            + " FROM back WHERE user_id=? AND goods_id=?",
                            (user_id, reward_id),
                        )
                        previous = reward_snapshots[reward_id]
                        previous_goods = int(previous["goods_num"] or 0) if previous else 0
                        previous_bind = int(previous.get("bind_num", 0) or 0) if previous else 0
                        expected_goods = previous_goods - (1 if reward_id == item_id else 0) + amount
                        consumed_bind = (
                            min(max(previous_bind - 1, 0), previous_goods - 1)
                            if reward_id == item_id
                            else previous_bind
                        )
                        expected_bind = consumed_bind + amount
                        if (
                            written.rowcount != 1
                            or saved_reward is None
                            or int(saved_reward["goods_num"] or 0) != expected_goods
                            or str(saved_reward["goods_name"]) != name
                            or str(saved_reward["goods_type"]) != item_type
                            or ("bind_num" in back_columns and int(saved_reward["bind_num"] or 0) != expected_bind)
                        ):
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult(
                                    "state_changed", expected_count,
                                    reason="rift_demon_token_reward_write_rejected", subject=str(reward_id),
                                )
                            )

                    changed = uow.execute(
                        'UPDATE player_data.rift SET "explore_count"=? WHERE user_id=?', (new_count, user_id)
                    )
                    if count_row is None:
                        if changed.rowcount != 0:
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_progress_write_rejected", subject=user_id)
                            )
                        inserted = uow.execute(
                            'INSERT INTO player_data.rift(user_id,"explore_count") VALUES(?,?)',
                            (user_id, new_count),
                        )
                        if inserted.rowcount != 1:
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_progress_write_rejected", subject=user_id)
                            )
                    elif changed.rowcount != 1:
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_progress_write_rejected", subject=user_id)
                        )
                    saved_progress = uow.query_one(
                        'SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,)
                    )
                    if saved_progress is None or as_int_like(saved_progress["explore_count"]) != new_count:
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_progress_write_rejected", subject=user_id)
                        )

                    for key, amount in statistics:
                        quoted_key = f'"{key}"'
                        before_stat = stats_before[key]
                        before_value = as_int_like(before_stat["value"]) if before_stat is not None else 0
                        changed = uow.execute(
                            f"UPDATE player_data.statistics SET {quoted_key}=COALESCE({quoted_key},0)+? WHERE user_id=?",
                            (amount, user_id),
                        )
                        if not stats_row_exists:
                            if changed.rowcount != 0:
                                raise _DemonTokenWriteRejected(
                                    RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_statistics_write_rejected", subject=key)
                                )
                            inserted = uow.execute(
                                f"INSERT INTO player_data.statistics(user_id,{quoted_key}) VALUES(?,?)",
                                (user_id, amount),
                            )
                            if inserted.rowcount != 1:
                                raise _DemonTokenWriteRejected(
                                    RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_statistics_write_rejected", subject=key)
                                )
                            stats_row_exists = True
                        elif changed.rowcount != 1:
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_statistics_write_rejected", subject=key)
                            )
                        saved_stat = uow.query_one(
                            f'SELECT "{key}" AS value FROM player_data.statistics WHERE user_id=?', (user_id,)
                        )
                        if saved_stat is None or as_int_like(saved_stat["value"]) != before_value + amount:
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_statistics_write_rejected", subject=key)
                            )

                    changed_entry = uow.execute("UPDATE rift_entries SET status='settled' WHERE user_id=?", (user_id,))
                    saved_entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
                    if (
                        changed_entry.rowcount != 1
                        or saved_entry is None
                        or str(saved_entry["status"]) != "settled"
                        or json.loads(str(saved_entry["rift_data"])) != entry_snapshot
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_entry_write_rejected", subject=user_id)
                        )

                    changed_cd = uow.execute(
                        "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=? AND type=3",
                        (user_id,),
                    )
                    saved_cd = uow.query_one(
                        "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,)
                    )
                    if (
                        changed_cd.rowcount != 1
                        or saved_cd is None
                        or as_int_like(saved_cd["type"]) != 0
                        or as_int_like(saved_cd["create_time"]) != 0
                        or saved_cd["scheduled_time"] is not None
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_cooldown_write_rejected", subject=user_id)
                        )

                    receipt = uow.execute(
                        f"INSERT INTO {self.operation_table}(operation_id,payload,explore_count,message) VALUES(?,?,?,?)",
                        (operation_id, payload, new_count, message),
                    )
                    saved_receipt = uow.query_one(
                        f"SELECT payload,explore_count,message FROM {self.operation_table} WHERE operation_id=?",
                        (operation_id,),
                    )
                    if (
                        receipt.rowcount != 1
                        or saved_receipt is None
                        or str(saved_receipt["payload"]) != payload
                        or as_int_like(saved_receipt["explore_count"]) != new_count
                        or str(saved_receipt["message"]) != message
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_receipt_write_rejected", subject=operation_id)
                        )

                    # Receipt triggers run inside this transaction too. Re-read
                    # every projection so a late rewrite cannot look successful.
                    saved_item = uow.query_one(
                        "SELECT goods_num" + (",bind_num" if "bind_num" in back_columns else "")
                        + " FROM back WHERE user_id=? AND goods_id=?",
                        (user_id, item_id),
                    )
                    if (
                        saved_item is None
                        or int(saved_item["goods_num"] or 0) != before_item_goods - 1 + totals.get(item_id, 0)
                        or (
                            "bind_num" in back_columns
                            and int(saved_item["bind_num"] or 0) != expected_item_bind + totals.get(item_id, 0)
                        )
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_item_write_rejected", subject=user_id)
                        )
                    saved_user = uow.query_one(
                        "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,)
                    )
                    if saved_user is None or tuple(as_int_like(saved_user[key]) for key in ("stone", "exp", "hp", "mp")) != expected_user:
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_user_write_rejected", subject=user_id)
                        )
                    for reward_id, amount in totals.items():
                        saved_reward = uow.query_one(
                            "SELECT goods_num,goods_name,goods_type" + (",bind_num" if "bind_num" in back_columns else "")
                            + " FROM back WHERE user_id=? AND goods_id=?",
                            (user_id, reward_id),
                        )
                        previous = reward_snapshots[reward_id]
                        previous_goods = int(previous["goods_num"] or 0) if previous else 0
                        previous_bind = int(previous.get("bind_num", 0) or 0) if previous else 0
                        expected_goods = previous_goods - (1 if reward_id == item_id else 0) + amount
                        consumed_bind = min(max(previous_bind - 1, 0), previous_goods - 1) if reward_id == item_id else previous_bind
                        expected_bind = consumed_bind + amount
                        name, item_type = metadata[reward_id]
                        if (
                            saved_reward is None
                            or int(saved_reward["goods_num"] or 0) != expected_goods
                            or str(saved_reward["goods_name"]) != name
                            or str(saved_reward["goods_type"]) != item_type
                            or ("bind_num" in back_columns and int(saved_reward["bind_num"] or 0) != expected_bind)
                        ):
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_reward_write_rejected", subject=str(reward_id))
                            )
                    saved_progress = uow.query_one(
                        'SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,)
                    )
                    if saved_progress is None or as_int_like(saved_progress["explore_count"]) != new_count:
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_progress_write_rejected", subject=user_id)
                        )
                    for key, amount in statistics:
                        saved_stat = uow.query_one(
                            f'SELECT "{key}" AS value FROM player_data.statistics WHERE user_id=?', (user_id,)
                        )
                        before_stat = stats_before[key]
                        before_value = as_int_like(before_stat["value"]) if before_stat is not None else 0
                        if saved_stat is None or as_int_like(saved_stat["value"]) != before_value + amount:
                            raise _DemonTokenWriteRejected(
                                RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_statistics_write_rejected", subject=key)
                            )
                    saved_entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
                    if (
                        saved_entry is None
                        or str(saved_entry["status"]) != "settled"
                        or json.loads(str(saved_entry["rift_data"])) != entry_snapshot
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_entry_write_rejected", subject=user_id)
                        )
                    saved_cd = uow.query_one(
                        "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,)
                    )
                    if (
                        saved_cd is None
                        or as_int_like(saved_cd["type"]) != 0
                        or as_int_like(saved_cd["create_time"]) != 0
                        or saved_cd["scheduled_time"] is not None
                    ):
                        raise _DemonTokenWriteRejected(
                            RiftDemonTokenBattleResult("state_changed", expected_count, reason="rift_demon_token_cooldown_write_rejected", subject=user_id)
                        )
            except _DemonTokenWriteRejected as rejected:
                return rejected.result
            return RiftDemonTokenBattleResult("applied", new_count, message)


__all__ = ["RiftDemonTokenBattleResult", "RiftDemonTokenBattleSqlRepository"]
