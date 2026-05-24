"""
前尘往事 - 状态管理
"""
import json
from datetime import datetime, timedelta
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

player_data_manager = PlayerDataManager()

FIELDS = [
    "state",            # 0=空闲, 1=等待投胎, 2=等待选择, 3=已完成
    "stage",            # 当前幕数(0-9)
    "alloc",            # 初始分配 {"悟性":x,"机缘":x,...}
    "accumulated",      # 累计属性 {"悟性":x,...}
    "talent",           # 天赋名
    "total_score",      # 总分
    "event_indices",    # 每幕选中的事件索引
    "event_snapshots",  # 每幕选中的事件快照
    "early_death_rolls",  # 已判定过的提前终局风险
    "history",          # 选择历史
    "last_run_time",    # 上次运行时间
    "total_runs",       # 总运行次数
    "best_ending",      # 最佳结局名
    "best_score",       # 最高分
    "endings_log",      # 结局记录
]

COOLDOWN_HOURS = 12


class PastLifeLimit:
    def __init__(self):
        self.table_name = "past_life"

    def _default_state(self):
        return {
            "state": 0,
            "stage": 0,
            "alloc": {},
            "accumulated": {"悟性": 0, "机缘": 0, "根骨": 0, "气运": 0, "心性": 0},
            "talent": "",
            "total_score": 0,
            "event_indices": [],
            "event_snapshots": [],
            "early_death_rolls": {},
            "history": [],
            "last_run_time": None,
            "total_runs": 0,
            "best_ending": "",
            "best_score": 0,
            "endings_log": [],
        }

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

    def save_user_state(self, user_id, state):
        user_id = str(user_id)
        for f in FIELDS:
            v = state.get(f, self._default_state().get(f))
            player_data_manager.update_or_write_data(
                user_id, self.table_name, f, v, data_type="TEXT"
            )

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

    def get_next_available_time(self, user_id):
        state = self.get_user_state(user_id)
        last = self._parse_run_time(state.get("last_run_time"))
        if not last:
            return None
        return last + timedelta(hours=COOLDOWN_HOURS)

    def check_cooldown(self, user_id):
        """检查冷却（完成一次前尘后12小时）"""
        state = self.get_user_state(user_id)
        last = self._parse_run_time(state.get("last_run_time"))
        if not last:
            return True
        return datetime.now() >= (last + timedelta(hours=COOLDOWN_HOURS))

    def get_cooldown_remaining(self, user_id):
        """返回剩余冷却分钟（完成一次前尘后12小时）"""
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
            return f"距离下次投胎还需{''.join(time_parts)}，可于{available_at}后再次开启。"
        return f"距离下次投胎还需{''.join(time_parts)}。"

    def save_run_result(self, user_id, ending_name, score):
        """记录一次完成的前世"""
        state = self.get_user_state(user_id)
        state["total_runs"] = state.get("total_runs", 0) + 1

        log = state.get("endings_log", [])
        if not isinstance(log, list):
            log = []
        log.insert(0, {
            "name": ending_name,
            "score": score,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        state["endings_log"] = log[:20]

        if score > state.get("best_score", 0):
            state["best_score"] = score
            state["best_ending"] = ending_name

        state["last_run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state["state"] = 0
        self.save_user_state(user_id, state)

    def reset_user_state(self, user_id, clear_history=False):
        """
        重置用户前尘状态
        clear_history=False: 仅重置当前流程/冷却，保留历史
        clear_history=True:  完全清空前尘数据
        """
        state = self.get_user_state(user_id)
        default = self._default_state()

        # 重置当前流程相关
        state["state"] = 0
        state["stage"] = 0
        state["alloc"] = {}
        state["accumulated"] = default["accumulated"]
        state["talent"] = ""
        state["total_score"] = 0
        state["event_indices"] = []
        state["event_snapshots"] = []
        state["early_death_rolls"] = {}
        state["history"] = []
        state["last_run_time"] = None  # 清冷却

        if clear_history:
            state["total_runs"] = 0
            state["best_ending"] = ""
            state["best_score"] = 0
            state["endings_log"] = []

        self.save_user_state(user_id, state)

    def reset_all_user_state(self, clear_history=False):
        """重置所有已有前尘记录的用户状态。"""
        records = player_data_manager.get_all_records(self.table_name)
        count = 0
        for record in records:
            user_id = record.get("user_id")
            if not user_id:
                continue
            self.reset_user_state(user_id, clear_history=clear_history)
            count += 1
        return count


past_life_limit = PastLifeLimit()
