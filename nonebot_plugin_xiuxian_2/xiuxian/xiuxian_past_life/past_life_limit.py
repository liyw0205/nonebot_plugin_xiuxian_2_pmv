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
    "history",          # 选择历史
    "last_run_time",    # 上次运行时间
    "total_runs",       # 总运行次数
    "best_ending",      # 最佳结局名
    "best_score",       # 最高分
    "endings_log",      # 结局记录
]


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

    def check_cooldown(self, user_id):
        """检查冷却（12小时一次）"""
        state = self.get_user_state(user_id)
        last = state.get("last_run_time")
        if not last:
            return True
        if isinstance(last, str):
            try:
                last = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return True
        if isinstance(last, datetime):
            return datetime.now() >= (last + timedelta(hours=12))
        return True

    def get_cooldown_remaining(self, user_id):
        """返回剩余冷却分钟（12小时）"""
        state = self.get_user_state(user_id)
        last = state.get("last_run_time")
        if not last:
            return 0
        if isinstance(last, str):
            try:
                last = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return 0
        if isinstance(last, datetime):
            end_time = last + timedelta(hours=12)
            remaining = end_time - datetime.now()
            return max(0, int(remaining.total_seconds() // 60))
        return 0

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
        state["history"] = []
        state["last_run_time"] = None  # 清冷却

        if clear_history:
            state["total_runs"] = 0
            state["best_ending"] = ""
            state["best_score"] = 0
            state["endings_log"] = []

        self.save_user_state(user_id, state)


past_life_limit = PastLifeLimit()