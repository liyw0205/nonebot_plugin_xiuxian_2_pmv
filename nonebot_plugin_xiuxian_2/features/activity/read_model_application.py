from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from .read_model_repository import ActivityReadModelSqlRepository


class ActivityReadModelApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: ActivityReadModelSqlRepository | None = None,
        config_loader: Callable[[], dict[str, Any]] | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or ActivityReadModelSqlRepository(database)
        self.config_loader = config_loader
        self.clock = clock or SystemClock()

    def _config(self) -> dict[str, Any]:
        if self.config_loader is not None:
            return self.config_loader()
        from ...xiuxian.xiuxian_activity.activity_config import load_config

        return load_config()

    def task_progress_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import _activity_config_key
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _task_scope_key,
            get_activity_tasks,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text
        from ...xiuxian.xiuxian_activity.activity_views import (
            _activity_event_text,
            _scope_label,
            _task_status_text,
        )

        config = self._config()
        tasks = get_activity_tasks(config)
        lines = [f"【{config.get('name', '节日签到活动')} · 活动任务】"]
        if not tasks:
            lines.append("暂无活动任务")
            return "\n".join(lines)

        now = self.clock.now().astimezone().replace(tzinfo=None)
        activity_key = _activity_config_key(config)
        progress_map = self.repository.task_progress(activity_key, str(user_id))
        for scope_type in ("daily", "weekly"):
            scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
            if not scope_tasks:
                continue
            scope_key = _task_scope_key(scope_type, now)
            lines.extend(("", f"【{_scope_label(scope_type)}目标】"))
            for task in scope_tasks:
                state = progress_map.get((scope_type, scope_key, task["key"]), {})
                target = max(1, _as_int(task.get("target"), 1))
                progress = min(target, max(0, _as_int(state.get("progress"), 0)))
                event_text = _activity_event_text(task.get("events"))
                description = _clean_text(task.get("description"))
                if not description:
                    description = (
                        f"{event_text} {target} 次" if event_text else f"目标 {target}"
                    )
                lines.append(
                    f"- {task['name']}："
                    f"{_task_status_text(progress, target, bool(state.get('claimed')))}，"
                    f"{description}，奖励：{task.get('reward') or '暂无奖励'}"
                )
        lines.extend(("", "领奖：活动任务领取（自动领取全部可领任务）"))
        return "\n".join(lines).strip()

    def task_catalog_text(self) -> str:
        from ...xiuxian.xiuxian_activity.activity_rules import _activity_pass_config
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int
        from ...xiuxian.xiuxian_activity.activity_views import (
            ACTIVITY_EVENT_LABELS,
            _format_activity_task,
        )

        config = self._config()
        lines = [f"【{config.get('name', '节日签到活动')} · 任务】"]
        daily_tasks = config.get("daily_tasks") or []
        if daily_tasks:
            lines.extend(("", "【每日活动任务】"))
            lines.extend(
                _format_activity_task(task)
                for task in daily_tasks
                if isinstance(task, dict)
            )
        else:
            lines.extend(("", "暂无每日活动任务"))

        weekly_tasks = config.get("weekly_tasks") or []
        if weekly_tasks:
            lines.extend(("", "【周常活动任务】"))
            lines.extend(
                _format_activity_task(task)
                for task in weekly_tasks
                if isinstance(task, dict)
            )

        pass_config = _activity_pass_config(config)
        if pass_config.get("enabled"):
            lines.extend(("", f"【{pass_config['name']}】"))
            lines.append(
                f"每 {pass_config['level_exp']}{pass_config['exp_name']} 提升 1 级"
            )
            rules = pass_config.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+"
                    f"{_as_int(rule.get('exp'))}"
                    for rule in rules
                )
                lines.append(f"来源：{rule_text}")
            lines.append("命令：活动战令 / 活动战令领取")
        return "\n".join(lines).strip()

    def sign_rank_text(self, limit: int = 10) -> str:
        config = self._config()
        rows = self.repository.sign_rank(limit)
        lines = [f"【{config.get('name', '节日签到活动')}排行】"]
        if not rows:
            lines.append("暂无排行数据")
            return "\n".join(lines)
        for index, row in enumerate(rows, 1):
            lines.append(
                f"{index}. {row['display_name']} 累计签到 "
                f"{max(0, int(row.get('sign_days', 0) or 0))} 天"
            )
        return "\n".join(lines)

    def pass_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            _activity_elapsed_days,
        )
        from ...xiuxian.xiuxian_activity.activity_rules import _activity_pass_config
        from ...xiuxian.xiuxian_activity.activity_utils import _as_float, _as_int
        from ...xiuxian.xiuxian_activity.activity_views import ACTIVITY_EVENT_LABELS

        config = self._config()
        pass_config = _activity_pass_config(config)
        lines = [f"【{pass_config['name']}】"]
        if not pass_config.get("enabled"):
            lines.append("活动战令未开启")
            return "\n".join(lines)

        balance = self.repository.pass_summary(
            _activity_config_key(config),
            str(user_id),
            level_exp=pass_config["level_exp"],
            max_level=pass_config["max_level"],
        )
        lines.extend(
            (
                f"等级：{balance['level']}/{balance['max_level']}",
                f"{pass_config['exp_name']}：{balance['exp']}/{balance['level_exp']}"
                f"（累计 {balance['total_exp']}）",
            )
        )

        elapsed_day = _activity_elapsed_days(
            config, self.clock.now().astimezone().replace(tzinfo=None)
        )
        enabled = bool(pass_config.get("catchup_enabled"))
        start_day = max(1, _as_int(pass_config.get("catchup_start_day"), 5))
        level_gap = max(1, _as_int(pass_config.get("catchup_level_gap"), 3))
        multiplier = max(1.0, _as_float(pass_config.get("catchup_multiplier"), 1.5))
        gap = max(0, balance["highest_level"] - balance["level"])
        catchup_active = (
            enabled
            and elapsed_day >= start_day
            and gap >= level_gap
            and multiplier > 1.0
        )
        if enabled:
            if catchup_active:
                lines.append(
                    f"追赶加成：已触发 {multiplier:.2f}x，当前落后最高等级 {gap} 级"
                )
            else:
                lines.append(
                    f"追赶加成：第{start_day}天后、落后{level_gap}级时触发"
                )

        rules = pass_config.get("event_rules") or []
        if rules:
            rule_text = "、".join(
                f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+"
                f"{_as_int(rule.get('exp'))}"
                for rule in rules
            )
            lines.append(f"获取来源：{rule_text}")
        rewards = pass_config.get("level_rewards") or []
        if rewards:
            lines.extend(("", "【等级奖励】"))
            for reward in rewards:
                level = _as_int(reward.get("level"), 0)
                if level <= 0:
                    continue
                if level in balance["claimed_levels"]:
                    status = "已领取"
                elif balance["level"] >= level:
                    status = "可领取"
                else:
                    status = "未达成"
                lines.append(
                    f"- Lv.{level} {reward.get('name') or '等级奖励'}：{status}，"
                    f"{reward.get('reward') or '暂无奖励'}"
                )
        lines.extend(("", "领奖：活动战令领取（自动领取全部可领等级奖励）"))
        return "\n".join(lines).strip()


__all__ = ["ActivityReadModelApplication"]
