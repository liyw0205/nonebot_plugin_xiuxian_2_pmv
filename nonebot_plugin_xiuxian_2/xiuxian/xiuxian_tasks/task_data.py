from dataclasses import dataclass
from datetime import datetime
from typing import Any

from nonebot.log import logger

from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from ..xiuxian_utils.xiuxian2_handle import OtherSet, PlayerDataManager, XiuxianDateManage


@dataclass(frozen=True)
class TaskDefinition:
    key: str
    cycle: str
    name: str
    desc: str
    target: int
    events: tuple[str, ...]
    rewards: dict[str, Any]


TASKS: tuple[TaskDefinition, ...] = (
    TaskDefinition(
        key="daily_sign",
        cycle="daily",
        name="今日问道",
        desc="完成修仙签到 1 次",
        target=1,
        events=("sign_in",),
        rewards={"items": [{"id": 20005, "amount": 1}]},
    ),
    TaskDefinition(
        key="daily_out_closing",
        cycle="daily",
        name="闭关归元",
        desc="完成出关或虚神界出关 1 次",
        target=1,
        events=("out_closing", "xu_out_closing"),
        rewards={"items": [{"id": 18076, "amount": 1}]},
    ),
    TaskDefinition(
        key="daily_work",
        cycle="daily",
        name="悬赏历练",
        desc="结算悬赏令 1 次",
        target=1,
        events=("work",),
        rewards={"items": [{"id": 20015, "amount": 1}]},
    ),
    TaskDefinition(
        key="daily_boss",
        cycle="daily",
        name="斩妖除魔",
        desc="讨伐世界BOSS 1 次",
        target=1,
        events=("boss",),
        rewards={"items": [{"id": 20023, "amount": 2}]},
    ),
    TaskDefinition(
        key="weekly_sign",
        cycle="weekly",
        name="七日勤修",
        desc="本周完成修仙签到 6 次",
        target=6,
        events=("sign_in",),
        rewards={"items": [{"id": 18057, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_out_closing",
        cycle="weekly",
        name="道心不辍",
        desc="本周完成出关或虚神界出关 7 次",
        target=7,
        events=("out_closing", "xu_out_closing"),
        rewards={"items": [{"id": 18082, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_work",
        cycle="weekly",
        name="悬赏达人",
        desc="本周结算悬赏令 25 次",
        target=25,
        events=("work",),
        rewards={"items": [{"id": 18117, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_boss",
        cycle="weekly",
        name="伏魔周行",
        desc="本周讨伐世界BOSS 150 次",
        target=150,
        events=("boss",),
        rewards={"items": [{"id": 18134, "amount": 1}]},
    ),
)


TASKS_BY_CYCLE: dict[str, tuple[TaskDefinition, ...]] = {
    "daily": tuple(task for task in TASKS if task.cycle == "daily"),
    "weekly": tuple(task for task in TASKS if task.cycle == "weekly"),
}


class XiuxianTaskManager:
    table_name = "xiuxian_tasks"

    def __init__(self):
        self.player_data_manager = PlayerDataManager()
        self.sql_message = XiuxianDateManage()
        self.items = Items()

    @staticmethod
    def _now() -> datetime:
        return datetime.now()

    @classmethod
    def _period_key(cls, cycle: str) -> str:
        now = cls._now()
        if cycle == "weekly":
            iso_year, iso_week, _ = now.isocalendar()
            return f"{iso_year}-W{iso_week:02d}"
        return now.strftime("%Y-%m-%d")

    @staticmethod
    def _period_field(cycle: str) -> str:
        return f"{cycle}_period"

    @staticmethod
    def _progress_field(cycle: str) -> str:
        return f"{cycle}_progress"

    @staticmethod
    def _claimed_field(cycle: str) -> str:
        return f"{cycle}_claimed"

    def _get_field(self, user_id: str, field: str, default: Any = None):
        value = self.player_data_manager.get_field_data(user_id, self.table_name, field)
        return default if value is None else value

    def _write_field(self, user_id: str, field: str, value: Any):
        self.player_data_manager.update_or_write_data(user_id, self.table_name, field, value)

    def _ensure_cycle_state(self, user_id: str, cycle: str) -> tuple[dict[str, int], list[str], str]:
        user_id = str(user_id)
        period_key = self._period_key(cycle)
        period_field = self._period_field(cycle)
        progress_field = self._progress_field(cycle)
        claimed_field = self._claimed_field(cycle)
        stored_period = self._get_field(user_id, period_field, "")

        if stored_period != period_key:
            self._write_field(user_id, period_field, period_key)
            self._write_field(user_id, progress_field, {})
            self._write_field(user_id, claimed_field, [])
            return {}, [], period_key

        progress = self._get_field(user_id, progress_field, {})
        claimed = self._get_field(user_id, claimed_field, [])
        if not isinstance(progress, dict):
            progress = {}
        if not isinstance(claimed, list):
            claimed = []
        return progress, [str(item) for item in claimed], period_key

    @staticmethod
    def _legacy_reward_text(rewards: dict[str, Any]) -> list[str]:
        parts = []
        if rewards.get("stone", 0) > 0:
            parts.append(f"灵石{number_to(rewards['stone'])}")
        if rewards.get("exp", 0) > 0:
            parts.append(f"修为{number_to(rewards['exp'])}")
        return parts

    def _item_reward_text(self, rewards: dict[str, Any]) -> list[str]:
        parts = []
        for reward in rewards.get("items", []) or []:
            item_id = reward.get("id")
            amount = int(reward.get("amount", 1) or 1)
            item_info = self.items.get_data_by_item_id(item_id)
            item_name = item_info.get("name") if item_info else f"未知物品{item_id}"
            parts.append(f"{item_name}x{amount}")
        return parts

    def _reward_text(self, rewards: dict[str, Any]) -> str:
        parts = self._item_reward_text(rewards)
        parts.extend(self._legacy_reward_text(rewards))
        return "、".join(parts) if parts else "无"

    def record_progress(self, user_id: str, event_key: str, amount: int = 1) -> list[str]:
        user_id = str(user_id)
        amount = max(1, int(amount))
        completed: list[str] = []

        with self.player_data_manager.lock:
            for cycle, tasks in TASKS_BY_CYCLE.items():
                matched_tasks = [task for task in tasks if event_key in task.events]
                if not matched_tasks:
                    continue

                progress, claimed, _ = self._ensure_cycle_state(user_id, cycle)
                changed = False
                for task in matched_tasks:
                    old_value = int(progress.get(task.key, 0) or 0)
                    new_value = min(old_value + amount, task.target)
                    if new_value != old_value:
                        progress[task.key] = new_value
                        changed = True
                    if old_value < task.target <= new_value and task.key not in claimed:
                        completed.append(task.name)

                if changed:
                    self._write_field(user_id, self._progress_field(cycle), progress)

        return completed

    def build_status_message(self, user_id: str, cycle: str | None = None) -> str:
        cycles = [cycle] if cycle in TASKS_BY_CYCLE else ["daily", "weekly"]
        title = "修仙任务" if cycle is None else ("每日任务" if cycle == "daily" else "周常任务")
        msg_lines = [f"【{title}】"]

        with self.player_data_manager.lock:
            for item_cycle in cycles:
                progress, claimed, period_key = self._ensure_cycle_state(str(user_id), item_cycle)
                claimed_set = set(claimed)
                cycle_name = "每日" if item_cycle == "daily" else "周常"
                msg_lines.append(f"\n{cycle_name}进度：{period_key}")
                for task in TASKS_BY_CYCLE[item_cycle]:
                    current = min(int(progress.get(task.key, 0) or 0), task.target)
                    if task.key in claimed_set:
                        state = "已领取"
                    elif current >= task.target:
                        state = "可领取"
                    else:
                        state = "进行中"
                    msg_lines.append(
                        f"{state}｜{task.name} {current}/{task.target}\n"
                        f"  {task.desc}\n"
                        f"  奖励：{self._reward_text(task.rewards)}"
                    )

        msg_lines.append("\n发送【领取任务奖励】领取已完成奖励。")
        return "\n".join(msg_lines)

    def _grant_exp(self, user_id: str, exp: int) -> int:
        user_info = self.sql_message.get_user_info_with_id(user_id)
        if not user_info:
            return 0

        exp = max(0, int(exp))
        current_exp = int(user_info.get("exp", 0) or 0)
        max_exp = int(OtherSet().set_closing_type(user_info["level"])) * XiuConfig().closing_exp_upper_limit
        grant_exp = min(exp, max(max_exp - current_exp, 0))
        if grant_exp <= 0:
            return 0

        self.sql_message.update_exp(user_id, grant_exp)
        self.sql_message.update_power2(user_id)
        return grant_exp

    def _grant_rewards(self, user_id: str, rewards: dict[str, Any]) -> dict[str, Any]:
        granted = {"stone": 0, "exp": 0, "items": []}
        stone = int(rewards.get("stone", 0) or 0)
        exp = int(rewards.get("exp", 0) or 0)

        if stone > 0:
            self.sql_message.update_ls(user_id, stone, 1)
            granted["stone"] = stone
        if exp > 0:
            granted["exp"] = self._grant_exp(user_id, exp)

        for reward in rewards.get("items", []) or []:
            item_id = reward.get("id")
            amount = max(1, int(reward.get("amount", 1) or 1))
            item_info = self.items.get_data_by_item_id(item_id)
            if not item_info:
                logger.warning(f"任务奖励物品不存在：{item_id}")
                continue
            self.sql_message.send_back(
                user_id,
                int(item_id),
                item_info["name"],
                item_info["type"],
                amount,
                1,
            )
            granted["items"].append({"name": item_info["name"], "amount": amount})

        return granted

    def claim_rewards(self, user_id: str, cycle: str | None = None) -> str:
        user_id = str(user_id)
        cycles = [cycle] if cycle in TASKS_BY_CYCLE else ["daily", "weekly"]
        claimed_tasks: list[tuple[TaskDefinition, dict[str, Any]]] = []

        with self.player_data_manager.lock:
            for item_cycle in cycles:
                progress, claimed, _ = self._ensure_cycle_state(user_id, item_cycle)
                claimed_set = set(claimed)
                newly_claimed: list[str] = []
                for task in TASKS_BY_CYCLE[item_cycle]:
                    current = int(progress.get(task.key, 0) or 0)
                    if current >= task.target and task.key not in claimed_set:
                        granted = self._grant_rewards(user_id, task.rewards)
                        claimed_tasks.append((task, granted))
                        newly_claimed.append(task.key)

                if newly_claimed:
                    claimed.extend(newly_claimed)
                    self._write_field(user_id, self._claimed_field(item_cycle), claimed)

        if not claimed_tasks:
            return "当前没有可领取的任务奖励。"

        lines = ["任务奖励领取成功："]
        for task, granted in claimed_tasks:
            reward_parts = []
            for item in granted.get("items", []) or []:
                reward_parts.append(f"{item['name']}x{item['amount']}")
            if granted.get("stone", 0) > 0:
                reward_parts.append(f"灵石{number_to(granted['stone'])}")
            if granted.get("exp", 0) > 0:
                reward_parts.append(f"修为{number_to(granted['exp'])}")
            if task.rewards.get("exp", 0) > 0 and granted.get("exp", 0) <= 0:
                reward_parts.append("修为已达上限")
            lines.append(f"- {task.name}：{'、'.join(reward_parts) if reward_parts else '无'}")
        return "\n".join(lines)


task_manager = XiuxianTaskManager()


def record_task_progress(user_id: str, event_key: str, amount: int = 1) -> list[str]:
    try:
        return task_manager.record_progress(user_id, event_key, amount)
    except Exception as e:
        logger.warning(f"记录修仙任务进度失败：user_id={user_id}, event={event_key}, error={e}")
        return []
