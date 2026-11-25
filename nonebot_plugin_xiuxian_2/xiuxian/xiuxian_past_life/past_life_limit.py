"""
前尘往事 - 状态管理
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from ...paths import get_paths
from ..xiuxian_utils.shared_read_cache import process_read_cache, shared_snapshot_key
from .past_life_state import PAST_LIFE_FIELDS, new_default_state

_player_data_manager_instance = None


def _player_data_manager():
    global _player_data_manager_instance
    if _player_data_manager_instance is None:
        _player_data_manager_instance = PlayerDataManager()
    return _player_data_manager_instance

FIELDS = list(PAST_LIFE_FIELDS)

REFRESH_INTERVAL_HOURS = 12
MAX_ENDINGS_LOG = 10


class PastLifeLimit:
    def __init__(self, *, clock=None):
        self.table_name = "past_life"
        self.clock = clock or SystemClock()

    def _default_state(self):
        return new_default_state()

    def get_user_state(self, user_id):
        user_id = str(user_id)
        record = _player_data_manager().get_fields(user_id, self.table_name)
        if not record:
            return self._default_state()

        state = {}
        for f in FIELDS:
            val = record.get(f)
            if val is None:
                val = self._default_state().get(f)
            if isinstance(val, str):
                try:
                    val = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
            state[f] = val
        return state

    def get_all_field_data(self, field):
        """Read a past-life column without creating missing tables or fields."""
        database = Path(get_paths().player_db)
        if not database.is_file():
            return []

        field_name = str(field or "").strip()
        if not field_name:
            return []

        with DatabaseUnitOfWork(database, read_only=True) as uow:
            table_name = self.table_name
            table = uow.query_one(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND lower(name)=lower(?) LIMIT 1",
                (table_name,),
            )
            if table is None:
                return []

            columns = {
                str(row["name"]).casefold(): str(row["name"])
                for row in uow.query_all(
                    f"PRAGMA table_info({self._quote_identifier(table_name)})"
                )
            }
            column_name = columns.get(field_name.casefold())
            if column_name is None or "user_id" not in columns:
                return []

            rows = uow.query_all(
                f"SELECT {self._quote_identifier(columns['user_id'])} AS user_id, "
                f"{self._quote_identifier(column_name)} AS value "
                f"FROM {self._quote_identifier(table_name)}"
            )

        result = []
        for row in rows:
            value = row["value"]
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append((row["user_id"], value))
        return result

    def public_rank_snapshot(self, limit: int = 30):
        limit = max(1, min(int(limit), 30))
        player_database = Path(get_paths().player_db)
        game_database = Path(get_paths().game_db)
        key = shared_snapshot_key(
            player_database,
            "past-life-rank-v1",
            shared_snapshot_key(game_database, "file-identity"),
            limit,
        )

        def load_rows():
            if not player_database.is_file():
                return {"has_scores": False, "rows": []}

            with DatabaseUnitOfWork(player_database, read_only=True) as uow:
                table = uow.query_one(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND lower(name)=lower(?) LIMIT 1",
                    (self.table_name,),
                )
                if table is None:
                    return {"has_scores": False, "rows": []}
                table_name = str(table["name"])
                columns = {
                    str(row["name"]).casefold(): str(row["name"])
                    for row in uow.query_all(
                        f"PRAGMA table_info({self._quote_identifier(table_name)})"
                    )
                }
                user_column = columns.get("user_id")
                score_column = columns.get("best_score")
                if user_column is None or score_column is None:
                    return {"has_scores": False, "rows": []}

                scores = uow.query_all(
                    f"SELECT {self._quote_identifier(user_column)} AS user_id, "
                    f"{self._quote_identifier(score_column)} AS value "
                    f"FROM {self._quote_identifier(table_name)}"
                )
                ordered_scores = []
                for row in scores:
                    value = row["value"]
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    if isinstance(value, (int, float)) and value > 0:
                        ordered_scores.append((str(row["user_id"]), value))
                ordered = sorted(
                    ordered_scores, key=lambda item: item[1], reverse=True
                )[:limit]
                if not ordered:
                    return {"has_scores": False, "rows": []}

                user_ids = list(dict.fromkeys(user_id for user_id, _ in ordered))
                endings: dict[str, Any] = {}
                ending_column = columns.get("best_ending")
                if ending_column is not None:
                    for offset in range(0, len(user_ids), 400):
                        batch = user_ids[offset : offset + 400]
                        placeholders = ",".join("?" for _ in batch)
                        state_rows = uow.query_all(
                            f"SELECT {self._quote_identifier(user_column)} AS user_id, "
                            f"{self._quote_identifier(ending_column)} AS best_ending "
                            f"FROM {self._quote_identifier(table_name)} "
                            f"WHERE {self._quote_identifier(user_column)} IN ({placeholders}) "
                            "ORDER BY rowid ASC",
                            tuple(batch),
                        )
                        for row in state_rows:
                            user_id = str(row["user_id"])
                            ending = row["best_ending"]
                            if ending is None:
                                ending = self._default_state()["best_ending"]
                            elif isinstance(ending, str):
                                try:
                                    ending = json.loads(ending)
                                except (json.JSONDecodeError, TypeError):
                                    pass
                            endings.setdefault(user_id, ending)

                names: dict[str, Any] = {}
                if game_database.is_file():
                    uow.attach_database(game_database, "game_data", read_only=True)
                    game_tables = {
                        str(row["name"]).casefold()
                        for row in uow.query_all(
                            "SELECT name FROM game_data.sqlite_master WHERE type='table'"
                        )
                    }
                    if "user_xiuxian" in game_tables:
                        game_columns = {
                            str(row["name"]).casefold(): str(row["name"])
                            for row in uow.query_all(
                                'PRAGMA game_data.table_info("user_xiuxian")'
                            )
                        }
                        game_user_column = game_columns.get("user_id")
                        game_name_column = game_columns.get("user_name")
                        if game_user_column and game_name_column:
                            for offset in range(0, len(user_ids), 400):
                                batch = user_ids[offset : offset + 400]
                                placeholders = ",".join("?" for _ in batch)
                                name_rows = uow.query_all(
                                    f"SELECT {self._quote_identifier(game_user_column)} AS user_id, "
                                    f"{self._quote_identifier(game_name_column)} AS user_name "
                                    "FROM game_data.user_xiuxian "
                                    f"WHERE {self._quote_identifier(game_user_column)} IN ({placeholders}) "
                                    "ORDER BY rowid ASC",
                                    tuple(batch),
                                )
                                for row in name_rows:
                                    names.setdefault(str(row["user_id"]), row["user_name"])

            rows = [
                (
                    index,
                    names[user_id],
                    score,
                    endings.get(user_id, self._default_state()["best_ending"]),
                )
                for index, (user_id, score) in enumerate(ordered, 1)
                if user_id in names
            ]
            return {"has_scores": bool(ordered), "rows": rows}

        return process_read_cache.get_or_load(
            key,
            database_path=player_database,
            ttl=60 * 60,
            loader=load_rows,
            snapshot=True,
            dependency_databases=(game_database,),
        )

    @staticmethod
    def _quote_identifier(name):
        return '"' + str(name).replace('"', '""') + '"'

    def _parse_run_time(self, value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        return None

    def normalize_endings_log(self, endings_log):
        """按时间从旧到新整理结局记录，只保留最近十世。"""
        if not isinstance(endings_log, list):
            return []

        entries = [
            (index, entry) for index, entry in enumerate(endings_log)
            if isinstance(entry, dict)
        ]

        def sort_key(item):
            index, entry = item
            run_time = self._parse_run_time(entry.get("time"))
            return run_time or datetime.min, index

        ordered = [entry for _, entry in sorted(entries, key=sort_key)]
        return ordered[-MAX_ENDINGS_LOG:]

    def _get_refresh_slot_start(self, now=None):
        """前尘刷新段：每日 00:00 与 12:00。"""
        now = now or self.clock.now()
        refresh_hour = 12 if now.hour >= 12 else 0
        return now.replace(hour=refresh_hour, minute=0, second=0, microsecond=0)

    def get_refresh_slot_start(self, now=None):
        return self._get_refresh_slot_start(now)

    def _get_next_refresh_time(self, now=None):
        now = now or self.clock.now()
        return self._get_refresh_slot_start(now) + timedelta(hours=REFRESH_INTERVAL_HOURS)

    def get_next_available_time(self, user_id):
        state = self.get_user_state(user_id)
        last = self._parse_run_time(state.get("last_run_time"))
        if not last:
            return None
        now = self.clock.now()
        if last < self._get_refresh_slot_start(now):
            return None
        return self._get_next_refresh_time(now)

    def check_cooldown(self, user_id):
        """检查刷新段：每日 00:00 / 12:00 刷新，每段可完成一次。"""
        state = self.get_user_state(user_id)
        last = self._parse_run_time(state.get("last_run_time"))
        if not last:
            return True
        return last < self._get_refresh_slot_start()

    def get_cooldown_remaining(self, user_id):
        """返回距离下次前尘刷新剩余分钟。"""
        next_time = self.get_next_available_time(user_id)
        if not next_time:
            return 0
        remaining = next_time - self.clock.now()
        seconds = int(remaining.total_seconds())
        if seconds <= 0:
            return 0
        return max(1, (seconds + 59) // 60)

    def get_cooldown_text(self, user_id):
        """返回冷却剩余时间和准确开启时间文案。"""
        remaining = self.get_cooldown_remaining(user_id)
        if remaining <= 0:
            return "现在可以再次开启前尘往事。"

        hours = remaining // 60
        mins = remaining % 60
        time_parts = []
        if hours:
            time_parts.append(f"{hours}小时")
        if mins:
            time_parts.append(f"{mins}分钟")
        if not time_parts:
            time_parts.append("不到1分钟")

        next_time = self.get_next_available_time(user_id)
        if next_time:
            available_at = next_time.strftime("%Y-%m-%d %H:%M")
            return f"距离下次前尘刷新还需{''.join(time_parts)}，可于{available_at}后再次开启。"
        return f"距离下次前尘刷新还需{''.join(time_parts)}。"

past_life_limit = PastLifeLimit()
