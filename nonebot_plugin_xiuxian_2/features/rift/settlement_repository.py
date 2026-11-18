from __future__ import annotations

from ...core.numeric import as_int_like
from dataclasses import replace as _reason_replace
from ...core.snapshot_reason import first_difference

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .demon_token_repository import RiftDemonTokenBattleSqlRepository


@dataclass(frozen=True)
class RiftSettlementResult:
    status: str
    explore_count: int = 0
    message: str = ""
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class _RiftSettlementWriteRejected(Exception):
    def __init__(self, result: RiftSettlementResult) -> None:
        self.result = result


class RiftSettlementSqlRepository(RiftDemonTokenBattleSqlRepository):
    """Persist a pre-rolled ordinary rift settlement across both databases."""

    operation_table = "rift_settlement_operations"

    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any | None = None) -> None:
        super().__init__(game_database, player_database, clock=clock or SystemClock())

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value or "").strip()
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        parsed = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        parsed = None
                if parsed is None:
                    return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _elapsed_minutes(self, started_at: Any) -> int | None:
        started = self._parse_datetime(started_at)
        now = self._parse_datetime(self.clock.now())
        if started is None or now is None:
            return None
        now = now.astimezone(started.tzinfo)
        return max(0, int((now - started).total_seconds() // 60))

    def settle(
        self,
        operation_id: str,
        user_id: str,
        expected_rift: dict[str, Any],
        expected_user: dict[str, Any],
        expected_explore_count: int,
        outcome: dict[str, Any],
        max_goods_num: int,
    ) -> RiftSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
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
            sorted((str(key), int(value)) for key, value in outcome.get("statistics", {}).items() if int(value))
        )
        message = str(outcome.get("message", ""))
        if not operation_id or not user_id or expected_count < 0 or max_goods_num < 0 or not message:
            raise ValueError("valid operation, user, outcome and snapshots are required")
        unsupported = {key for key, _ in statistics} - self.supported_statistics
        if unsupported:
            raise ValueError(f"unsupported rift statistics: {sorted(unsupported)}")
        payload = json.dumps(
            [user_id, rift_snapshot, expected, expected_count, delta, rewards, statistics, message, max_goods_num],
            ensure_ascii=True,
            sort_keys=True,
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
                for row in uow.query_all("SELECT name FROM player_data.sqlite_master WHERE type='table'")
            }
            required_game = {"rift_entries", "user_cd", "user_xiuxian", "back", self.operation_table}
            if not required_game.issubset(game_tables) or not {"rift", "statistics"}.issubset(player_tables):
                return RiftSettlementResult("schema_missing")
            rift_columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(rift)")}
            statistics_columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(statistics)")}
            if "explore_count" not in rift_columns or not (set(key for key, _ in statistics) <= statistics_columns):
                return RiftSettlementResult("schema_missing")

            old = uow.query_one(
                f"SELECT payload,explore_count,message FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                if str(old["payload"]) == payload:
                    return RiftSettlementResult("duplicate", int(old["explore_count"]), str(old["message"]))
                return _reason_replace(RiftSettlementResult("state_changed"), reason="operation_conflict", subject="")

            entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
            cd = uow.query_one("SELECT COALESCE(type,0) AS type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,))
            user = uow.query_one("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,))
            if entry is None or str(entry["status"]) != "active" or cd is None or int(cd["type"]) != 3:
                return RiftSettlementResult("not_active")
            try:
                expected_duration = int(expected_rift.get("time", 0))
                scheduled_duration = int(cd["scheduled_time"])
            except (TypeError, ValueError):
                return _reason_replace(RiftSettlementResult("state_changed"), reason="rift_duration_invalid", subject="")
            elapsed = self._elapsed_minutes(cd["create_time"])
            if expected_duration <= 0 or scheduled_duration != expected_duration or elapsed is None:
                return _reason_replace(RiftSettlementResult("state_changed"), reason="rift_duration_invalid" if expected_duration <= 0 else "rift_duration_changed" if scheduled_duration != expected_duration else "rift_start_invalid", subject="")
            if elapsed < expected_duration:
                return RiftSettlementResult("not_ready")
            if json.loads(str(entry["rift_data"])) != json.loads(rift_snapshot) or user is None or tuple(map(as_int_like, user.values())) != expected:
                return _reason_replace(RiftSettlementResult("state_changed"), reason="rift_snapshot_changed" if json.loads(str(entry["rift_data"])) != json.loads(rift_snapshot) else "user_missing" if user is None else first_difference(tuple(map(as_int_like,user.values())), expected, ("stone_changed","exp_changed","hp_changed","mp_changed")), subject="")

            count_row = uow.query_one('SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,))
            current_count = int(count_row["explore_count"] or 0) if count_row else 0
            if current_count != expected_count:
                return _reason_replace(RiftSettlementResult("state_changed"), reason="rift_explore_count_changed", subject="")
            if expected[0] + delta[0] < 0:
                return RiftSettlementResult("resource_missing", reason="stone_insufficient")

            totals: dict[int, int] = {}
            metadata: dict[int, tuple[str, str]] = {}
            for reward_id, name, item_type, amount in rewards:
                totals[reward_id] = totals.get(reward_id, 0) + amount
                previous = metadata.setdefault(reward_id, (name, item_type))
                if previous != (name, item_type):
                    raise ValueError("conflicting reward metadata")
            back_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            reward_snapshots: dict[int, dict[str, Any] | None] = {}
            for reward_id, amount in totals.items():
                row = uow.query_one(
                    "SELECT goods_num,goods_name,goods_type" + (",bind_num" if "bind_num" in back_columns else "")
                    + " FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, reward_id),
                )
                reward_snapshots[reward_id] = dict(row) if row is not None else None
                if (int(row["goods_num"] or 0) if row else 0) + amount > max_goods_num:
                    return RiftSettlementResult("inventory_full")

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
                    f'SELECT "{key}" AS value FROM player_data.statistics WHERE user_id=?', (user_id,)
                )
                for key, _ in statistics
            }

            try:
                with uow.savepoint("rift_settlement"):
                    changed_user = uow.execute(
                        "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')),exp=?,hp=?,mp=? WHERE user_id=?",
                        (delta[0], expected_user[1], expected_user[2], expected_user[3], user_id),
                    )
                    saved_user = uow.query_one("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,))
                    if (
                        changed_user.rowcount != 1
                        or saved_user is None
                        or tuple(as_int_like(saved_user[key]) for key in ("stone", "exp", "hp", "mp")) != expected_user
                    ):
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_user_write_rejected", subject=user_id)
                        )

                    now = self.clock.now().isoformat()
                    for reward_id, amount in totals.items():
                        name, item_type = metadata[reward_id]
                        columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time"]
                        values: list[Any] = [user_id, reward_id, name, item_type, amount, now, now]
                        bind_sql = ""
                        if "bind_num" in back_columns:
                            columns.append("bind_num")
                            values.append(amount)
                            bind_sql = ",bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num"
                        columns_sql = ",".join(f'"{column}"' for column in columns)
                        placeholders = ",".join("?" for _ in values)
                        written = uow.execute(
                            f"INSERT INTO back({columns_sql}) VALUES({placeholders}) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,"
                            "update_time=EXCLUDED.update_time" + bind_sql,
                            tuple(values),
                        )
                        saved_reward = uow.query_one(
                            "SELECT goods_num,goods_name,goods_type" + (",bind_num" if "bind_num" in back_columns else "")
                            + " FROM back WHERE user_id=? AND goods_id=?", (user_id, reward_id)
                        )
                        previous = reward_snapshots[reward_id]
                        before_goods = int(previous["goods_num"] or 0) if previous else 0
                        before_bind = int(previous.get("bind_num", 0) or 0) if previous else 0
                        if (
                            written.rowcount != 1
                            or saved_reward is None
                            or int(saved_reward["goods_num"] or 0) != before_goods + amount
                            or str(saved_reward["goods_name"]) != name
                            or str(saved_reward["goods_type"]) != item_type
                            or ("bind_num" in back_columns and int(saved_reward["bind_num"] or 0) != before_bind + amount)
                        ):
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_reward_write_rejected", subject=str(reward_id))
                            )

                    changed = uow.execute('UPDATE player_data.rift SET "explore_count"=? WHERE user_id=?', (new_count, user_id))
                    if count_row is None:
                        if changed.rowcount != 0:
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_progress_write_rejected", subject=user_id)
                            )
                        inserted = uow.execute('INSERT INTO player_data.rift(user_id,"explore_count") VALUES(?,?)', (user_id, new_count))
                        if inserted.rowcount != 1:
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_progress_write_rejected", subject=user_id)
                            )
                    elif changed.rowcount != 1:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_progress_write_rejected", subject=user_id)
                        )
                    saved_progress = uow.query_one('SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,))
                    if saved_progress is None or as_int_like(saved_progress["explore_count"]) != new_count:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_progress_write_rejected", subject=user_id)
                        )

                    for key, amount in statistics:
                        before_stat = stats_before[key]
                        before_value = as_int_like(before_stat["value"]) if before_stat is not None else 0
                        changed = uow.execute(
                            f'UPDATE player_data.statistics SET "{key}"=COALESCE("{key}",0)+? WHERE user_id=?',
                            (amount, user_id),
                        )
                        if not stats_row_exists:
                            if changed.rowcount != 0:
                                raise _RiftSettlementWriteRejected(
                                    RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_statistics_write_rejected", subject=key)
                                )
                            inserted = uow.execute(f'INSERT INTO player_data.statistics(user_id,"{key}") VALUES(?,?)', (user_id, amount))
                            if inserted.rowcount != 1:
                                raise _RiftSettlementWriteRejected(
                                    RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_statistics_write_rejected", subject=key)
                                )
                            stats_row_exists = True
                        elif changed.rowcount != 1:
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_statistics_write_rejected", subject=key)
                            )
                        saved_stat = uow.query_one(f'SELECT "{key}" AS value FROM player_data.statistics WHERE user_id=?', (user_id,))
                        if saved_stat is None or as_int_like(saved_stat["value"]) != before_value + amount:
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_statistics_write_rejected", subject=key)
                            )

                    changed_entry = uow.execute("UPDATE rift_entries SET status='settled' WHERE user_id=?", (user_id,))
                    saved_entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
                    if changed_entry.rowcount != 1 or saved_entry is None or str(saved_entry["status"]) != "settled" or json.loads(str(saved_entry["rift_data"])) != entry_snapshot:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_entry_write_rejected", subject=user_id)
                        )
                    changed_cd = uow.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=? AND type=3", (user_id,))
                    saved_cd = uow.query_one("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,))
                    if changed_cd.rowcount != 1 or saved_cd is None or as_int_like(saved_cd["type"]) != 0 or as_int_like(saved_cd["create_time"]) != 0 or saved_cd["scheduled_time"] is not None:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_cooldown_write_rejected", subject=user_id)
                        )
                    receipt = uow.execute(
                        f"INSERT INTO {self.operation_table}(operation_id,payload,explore_count,message) VALUES(?,?,?,?)",
                        (operation_id, payload, new_count, message),
                    )
                    saved_receipt = uow.query_one(f"SELECT payload,explore_count,message FROM {self.operation_table} WHERE operation_id=?", (operation_id,))
                    if receipt.rowcount != 1 or saved_receipt is None or str(saved_receipt["payload"]) != payload or as_int_like(saved_receipt["explore_count"]) != new_count or str(saved_receipt["message"]) != message:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_receipt_write_rejected", subject=operation_id)
                        )

                    # Recheck projections after the receipt trigger has run.
                    saved_user = uow.query_one("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,))
                    if saved_user is None or tuple(as_int_like(saved_user[key]) for key in ("stone", "exp", "hp", "mp")) != expected_user:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_user_write_rejected", subject=user_id)
                        )
                    saved_progress = uow.query_one('SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,))
                    if saved_progress is None or as_int_like(saved_progress["explore_count"]) != new_count:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_progress_write_rejected", subject=user_id)
                        )
                    for reward_id, amount in totals.items():
                        saved_reward = uow.query_one("SELECT goods_num,goods_name,goods_type" + (",bind_num" if "bind_num" in back_columns else "") + " FROM back WHERE user_id=? AND goods_id=?", (user_id, reward_id))
                        previous = reward_snapshots[reward_id]
                        before_goods = int(previous["goods_num"] or 0) if previous else 0
                        before_bind = int(previous.get("bind_num", 0) or 0) if previous else 0
                        name, item_type = metadata[reward_id]
                        if saved_reward is None or int(saved_reward["goods_num"] or 0) != before_goods + amount or str(saved_reward["goods_name"]) != name or str(saved_reward["goods_type"]) != item_type or ("bind_num" in back_columns and int(saved_reward["bind_num"] or 0) != before_bind + amount):
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_reward_write_rejected", subject=str(reward_id))
                            )
                    for key, amount in statistics:
                        saved_stat = uow.query_one(f'SELECT "{key}" AS value FROM player_data.statistics WHERE user_id=?', (user_id,))
                        before_stat = stats_before[key]
                        before_value = as_int_like(before_stat["value"]) if before_stat is not None else 0
                        if saved_stat is None or as_int_like(saved_stat["value"]) != before_value + amount:
                            raise _RiftSettlementWriteRejected(
                                RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_statistics_write_rejected", subject=key)
                            )
                    saved_entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
                    if saved_entry is None or str(saved_entry["status"]) != "settled" or json.loads(str(saved_entry["rift_data"])) != entry_snapshot:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_entry_write_rejected", subject=user_id)
                        )
                    saved_cd = uow.query_one("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,))
                    if saved_cd is None or as_int_like(saved_cd["type"]) != 0 or as_int_like(saved_cd["create_time"]) != 0 or saved_cd["scheduled_time"] is not None:
                        raise _RiftSettlementWriteRejected(
                            RiftSettlementResult("state_changed", expected_count, reason="rift_settlement_cooldown_write_rejected", subject=user_id)
                        )
            except _RiftSettlementWriteRejected as rejected:
                return rejected.result
            return RiftSettlementResult("applied", new_count, message)


__all__ = ["RiftSettlementResult", "RiftSettlementSqlRepository"]
