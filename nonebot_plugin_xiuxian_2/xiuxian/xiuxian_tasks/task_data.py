from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from nonebot.log import logger

from ...paths import get_paths
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from .transaction_service import (
    TaskProgressEventResult,
    TaskProgressEventService,
    TaskRewardClaimService,
)


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
        key="daily_sect_task_complete",
        cycle="daily",
        name="同门小务",
        desc="完成宗门任务 1 次",
        target=1,
        events=("sect_task_complete",),
        rewards={"items": [{"id": 20015, "amount": 1}]},
    ),
    TaskDefinition(
        key="daily_pet_travel_claim",
        cycle="daily",
        name="灵宠归来",
        desc="领取宠物游历 1 次",
        target=1,
        events=("pet_travel_claim",),
        rewards={"items": [{"id": 20005, "amount": 1}]},
    ),
    TaskDefinition(
        key="daily_dongfu_harvest",
        cycle="daily",
        name="洞府经营",
        desc="完成洞府收获 1 次",
        target=1,
        events=("dongfu_harvest",),
        rewards={"items": [{"id": 18076, "amount": 1}]},
    ),
    TaskDefinition(
        key="daily_map_mission_complete",
        cycle="daily",
        name="寻踪问路",
        desc="完成地图委托 1 次",
        target=1,
        events=("map_mission_complete",),
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
        desc="本周累计修炼、出关或虚神界出关 7200 分钟",
        target=7200,
        events=("cultivation_time", "out_closing", "xu_out_closing"),
        rewards={"items": [{"id": 18173, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_work",
        cycle="weekly",
        name="悬赏达人",
        desc="本周结算悬赏令 25 次",
        target=25,
        events=("work",),
        rewards={"items": [{"id": 18172, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_boss",
        cycle="weekly",
        name="伏魔周行",
        desc="本周讨伐世界BOSS 150 次",
        target=150,
        events=("boss",),
        rewards={"items": [{"id": 18173, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_sect_task_complete",
        cycle="weekly",
        name="宗门勤务",
        desc="本周完成宗门任务 15 次",
        target=15,
        events=("sect_task_complete",),
        rewards={"items": [{"id": 18057, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_map_mission_complete",
        cycle="weekly",
        name="山河踏遍",
        desc="本周完成地图委托 20 次",
        target=20,
        events=("map_mission_complete",),
        rewards={"items": [{"id": 18172, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_elixir_or_dongfu",
        cycle="weekly",
        name="炼丹不辍",
        desc="本周炼丹或洞府收获累计 20 次",
        target=20,
        events=("dongfu_harvest", "mix_elixir_complete"),
        rewards={"items": [{"id": 18173, "amount": 1}]},
    ),
    TaskDefinition(
        key="weekly_dungeon_clear",
        cycle="weekly",
        name="组队试炼",
        desc="本周完成副本 10 次",
        target=10,
        events=("dungeon_clear",),
        rewards={"items": [{"id": 18173, "amount": 1}]},
    ),
)


TASKS_BY_CYCLE: dict[str, tuple[TaskDefinition, ...]] = {
    "daily": tuple(task for task in TASKS if task.cycle == "daily"),
    "weekly": tuple(task for task in TASKS if task.cycle == "weekly"),
}


def _normalize_progress_events(events) -> tuple[tuple[str, int], ...]:
    normalized = []
    seen = set()
    for raw_key, raw_amount in events:
        key = str(raw_key).strip()
        amount = max(0, int(raw_amount))
        if not key or amount <= 0 or key in seen:
            continue
        seen.add(key)
        normalized.append((key, amount))
    return tuple(normalized)


class XiuxianTaskManager:
    table_name = "xiuxian_tasks"

    def __init__(self):
        self.items = Items()
        self.progress_event_service = TaskProgressEventService(get_paths().player_db)
        self.reward_claim_service = TaskRewardClaimService(
            get_paths().game_db, get_paths().player_db
        )

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

    @staticmethod
    def _task_updates(events: tuple[tuple[str, int], ...]) -> tuple[dict[str, Any], ...]:
        updates = []
        for cycle in ("daily", "weekly"):
            for task in TASKS_BY_CYCLE[cycle]:
                amount = sum(
                    event_amount
                    for event_key, event_amount in events
                    if event_key in task.events
                )
                if amount > 0:
                    updates.append(
                        {
                            "key": task.key,
                            "cycle": task.cycle,
                            "name": task.name,
                            "target": task.target,
                            "amount": amount,
                        }
                    )
        return tuple(updates)

    def record_progress_event(
        self,
        user_id: str,
        events,
        operation_id: str | None = None,
    ) -> TaskProgressEventResult:
        normalized_events = _normalize_progress_events(events)
        if not normalized_events:
            return TaskProgressEventResult("ignored")
        operation_id = str(operation_id or "").strip()
        if not operation_id:
            operation_id = f"task-progress:untracked:{uuid4().hex}"
        task_updates = self._task_updates(normalized_events)
        cycles = {task["cycle"] for task in task_updates}
        periods = {cycle: self._period_key(cycle) for cycle in cycles}
        return self.progress_event_service.record(
            operation_id,
            str(user_id),
            normalized_events,
            periods,
            task_updates,
        )

    def record_progress(
        self,
        user_id: str,
        event_key: str,
        amount: int = 1,
        operation_id: str | None = None,
    ) -> list[str]:
        return list(
            self.record_progress_event(
                user_id, ((event_key, amount),), operation_id
            ).completed
        )

    def build_status_message(self, user_id: str, cycle: str | None = None) -> str:
        cycles = [cycle] if cycle in TASKS_BY_CYCLE else ["daily", "weekly"]
        title = "修仙任务" if cycle is None else ("每日任务" if cycle == "daily" else "周常任务")
        msg_lines = [f"【{title}】"]

        states = self.progress_event_service.get_states(
            str(user_id), {item_cycle: self._period_key(item_cycle) for item_cycle in cycles}
        )
        for item_cycle in cycles:
            progress, claimed, period_key = states[item_cycle]
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

    def _claim_task_snapshots(self, cycles: list[str]) -> list[dict[str, Any]]:
        snapshots = []
        for cycle in cycles:
            for task in TASKS_BY_CYCLE[cycle]:
                rewards = dict(task.rewards)
                items = []
                for reward in rewards.get("items", []) or []:
                    item_id = int(reward.get("id", 0))
                    item_info = self.items.get_data_by_item_id(item_id)
                    if not item_info:
                        raise ValueError(f"任务奖励物品不存在：{item_id}")
                    items.append(
                        {
                            "id": item_id,
                            "name": item_info["name"],
                            "type": item_info["type"],
                            "amount": int(reward.get("amount", 1) or 1),
                            "bind_flag": int(reward.get("bind_flag", 1)),
                        }
                    )
                rewards["items"] = items
                snapshots.append(
                    {
                        "key": task.key,
                        "cycle": task.cycle,
                        "name": task.name,
                        "target": task.target,
                        "rewards": rewards,
                    }
                )
        return snapshots

    def claim_rewards(
        self, operation_id: str, user_id: str, cycle: str | None = None
    ) -> str:
        user_id = str(user_id)
        cycles = [cycle] if cycle in TASKS_BY_CYCLE else ["daily", "weekly"]
        result = self.reward_claim_service.claim(
            operation_id,
            user_id,
            cycles,
            {item_cycle: self._period_key(item_cycle) for item_cycle in cycles},
            self._claim_task_snapshots(cycles),
            XiuConfig().max_goods_num,
        )
        if result.status == "operation_conflict":
            return "本次任务领奖与已记录事件冲突，请重新执行指令。"
        if result.status == "inventory_full":
            return "背包容量不足，任务奖励尚未领取。"
        if result.status == "user_missing":
            return "未找到角色信息，无法领取任务奖励。"
        if not result.tasks:
            return "当前没有可领取的任务奖励。"

        lines = ["任务奖励领取成功："]
        for task in result.tasks:
            reward_parts = []
            for item in task.get("items", ()):
                reward_parts.append(f"{item['name']}x{item['amount']}")
            lines.append(
                f"- {task['name']}：{'、'.join(reward_parts) if reward_parts else '无'}"
            )
        msg = "\n".join(lines)
        if result.status == "duplicate":
            msg += "\n该领奖请求已经处理，无需重复提交。"
        return msg


task_manager = XiuxianTaskManager()


def record_task_progress_event(
    user_id: str,
    events,
    operation_id: str | None = None,
) -> list[str]:
    normalized_events = _normalize_progress_events(events)
    if not normalized_events:
        return []
    messages: list[str] = []
    task_status = "failed"
    try:
        result = task_manager.record_progress_event(
            user_id, normalized_events, operation_id
        )
        task_status = result.status
        messages.extend(result.completed)
    except Exception as e:
        logger.warning(
            f"记录修仙任务进度失败：user_id={user_id}, events={normalized_events}, error={e}"
        )
    if task_status != "operation_conflict":
        for event_key, amount in normalized_events:
            try:
                from ..xiuxian_activity.service import record_activity_event

                messages.extend(record_activity_event(user_id, event_key, amount))
            except Exception as e:
                logger.warning(
                    f"记录活动事件失败：user_id={user_id}, event={event_key}, error={e}"
                )
    return messages


def record_task_progress(
    user_id: str,
    event_key: str,
    amount: int = 1,
    operation_id: str | None = None,
) -> list[str]:
    return record_task_progress_event(
        user_id, ((event_key, amount),), operation_id
    )
