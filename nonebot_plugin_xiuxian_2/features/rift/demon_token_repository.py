from __future__ import annotations

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

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


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
        expected = tuple(int(expected_user.get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        delta = tuple(int(outcome.get("delta", {}).get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
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
                return RiftDemonTokenBattleResult("state_changed")

            entry = uow.query_one("SELECT rift_data,status FROM rift_entries WHERE user_id=?", (user_id,))
            cd = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?", (user_id,))
            user = uow.query_one(
                "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if entry is None or str(entry["status"]) != "active" or cd is None or int(cd["type"]) != 3:
                return RiftDemonTokenBattleResult("not_active")
            if json.loads(str(entry["rift_data"])) != json.loads(rift_snapshot) or user is None or tuple(map(int, user.values())) != expected:
                return RiftDemonTokenBattleResult("state_changed")

            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None or int(item["goods_num"]) < 1:
                return RiftDemonTokenBattleResult("item_missing")
            count_row = uow.query_one(
                'SELECT "explore_count" FROM player_data.rift WHERE user_id=?', (user_id,)
            )
            current_count = int(count_row["explore_count"] or 0) if count_row else 0
            if current_count != expected_count:
                return RiftDemonTokenBattleResult("state_changed")
            if expected[0] + delta[0] < 0:
                return RiftDemonTokenBattleResult("resource_missing")

            totals: dict[int, int] = {}
            metadata: dict[int, tuple[str, str]] = {}
            for reward_id, name, item_type, amount in rewards:
                totals[reward_id] = totals.get(reward_id, 0) + amount
                old_metadata = metadata.setdefault(reward_id, (name, item_type))
                if old_metadata != (name, item_type):
                    raise ValueError("conflicting reward metadata")
            for reward_id, amount in totals.items():
                row = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, reward_id),
                )
                current_amount = int(row["goods_num"]) if row else 0
                consumed_amount = 1 if reward_id == item_id else 0
                if current_amount - consumed_amount + amount > max_goods_num:
                    return RiftDemonTokenBattleResult("inventory_full")

            back_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            bind_update = ",bind_num=MIN(MAX(COALESCE(bind_num,0)-1,0),goods_num-1)" if "bind_num" in back_columns else ""
            consumed = uow.execute(
                "UPDATE back SET goods_num=goods_num-1" + bind_update +
                " WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)>=1",
                (user_id, item_id),
            )
            if consumed.rowcount != 1:
                return RiftDemonTokenBattleResult("item_missing")

            uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL),exp=?,hp=?,mp=? WHERE user_id=?",
                (delta[0], max(0, expected[1] + delta[1]), max(1, expected[2] + delta[2]), max(1, expected[3] + delta[3]), user_id),
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
                uow.execute(
                    f"INSERT INTO back({columns_sql}) VALUES({placeholders}) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num," +
                    "update_time=EXCLUDED.update_time" + bind_sql,
                    tuple(values),
                )

            new_count = 0 if expected_count + 1 >= 10 else expected_count + 1
            changed = uow.execute(
                'UPDATE player_data.rift SET "explore_count"=? WHERE user_id=?', (new_count, user_id)
            )
            if changed.rowcount == 0:
                uow.execute(
                    'INSERT INTO player_data.rift(user_id,"explore_count") VALUES(?,?)',
                    (user_id, new_count),
                )
            for key, amount in statistics:
                quoted_key = f'"{key}"'
                changed = uow.execute(
                    f"UPDATE player_data.statistics SET {quoted_key}=COALESCE({quoted_key},0)+? WHERE user_id=?",
                    (amount, user_id),
                )
                if changed.rowcount == 0:
                    uow.execute(
                        f"INSERT INTO player_data.statistics(user_id,{quoted_key}) VALUES(?,?)",
                        (user_id, amount),
                    )
            uow.execute("UPDATE rift_entries SET status='settled' WHERE user_id=?", (user_id,))
            uow.execute(
                "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=? AND type=3",
                (user_id,),
            )
            uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload,explore_count,message) VALUES(?,?,?,?)",
                (operation_id, payload, new_count, message),
            )
            return RiftDemonTokenBattleResult("applied", new_count, message)


__all__ = ["RiftDemonTokenBattleResult", "RiftDemonTokenBattleSqlRepository"]
