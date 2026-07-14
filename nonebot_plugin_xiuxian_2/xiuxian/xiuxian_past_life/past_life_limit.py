"""
前尘往事 - 状态管理
"""
import json
from datetime import datetime, timedelta
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from .past_life_state import PAST_LIFE_FIELDS, new_default_state

player_data_manager = PlayerDataManager()

FIELDS = list(PAST_LIFE_FIELDS)

REFRESH_INTERVAL_HOURS = 12
MAX_ENDINGS_LOG = 10


class PastLifeLimit:
    def __init__(self):
        self.table_name = "past_life"

    def _default_state(self):
        return new_default_state()

    def get_user_state(self, user_id):
        user_id = str(user_id)
        record = player_data_manager.get_fields(user_id, self.table_name)
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
        now = now or datetime.now()
        refresh_hour = 12 if now.hour >= 12 else 0
        return now.replace(hour=refresh_hour, minute=0, second=0, microsecond=0)

    def get_refresh_slot_start(self, now=None):
        return self._get_refresh_slot_start(now)

    def _get_next_refresh_time(self, now=None):
        now = now or datetime.now()
        return self._get_refresh_slot_start(now) + timedelta(hours=REFRESH_INTERVAL_HOURS)

    def get_next_available_time(self, user_id):
        state = self.get_user_state(user_id)
        last = self._parse_run_time(state.get("last_run_time"))
        if not last:
            return None
        now = datetime.now()
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
        remaining = next_time - datetime.now()
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
