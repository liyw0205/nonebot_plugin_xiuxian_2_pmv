from __future__ import annotations

from copy import deepcopy
from typing import Any

from ...xiuxian.xiuxian_activity.activity_config import (
    DEFAULT_ACTIVITY_PASS,
    _migrate_config,
)
from ...xiuxian.xiuxian_activity.activity_utils import _clean_text
from .config_repository import ActivityConfigSqlRepository


class ActivityConfigApplication:
    """Mutate the versioned campaign configuration without touching game_db."""

    def __init__(self, repository: ActivityConfigSqlRepository | None = None) -> None:
        self.repository = repository or ActivityConfigSqlRepository()

    def set_enabled(
        self,
        *,
        operation_id: str,
        enabled: bool,
        target: str | None = None,
        operator_id: str = "",
    ) -> str:
        operation_id = str(operation_id or "").strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        target_text = _clean_text(target)
        enabled = bool(enabled)
        action_text = "开启" if enabled else "关闭"
        identity = {
            "action": "toggle",
            "enabled": enabled,
            "operator_id": str(operator_id),
            "target": target_text,
        }
        previous = self.repository.replay(operation_id, identity)
        if previous is not None:
            if previous.status == "operation_conflict":
                return "同一消息事件不能用于不同的活动配置操作"
            return previous.result_text

        state = self.repository.read()
        config, _ = _migrate_config(deepcopy(state.config))

        def commit(message: str) -> str:
            result = self.repository.replace(
                operation_id,
                identity,
                state.revision,
                config,
                result_text=message,
            )
            if result.status == "operation_conflict":
                return "同一消息事件不能用于不同的活动配置操作"
            if result.status == "state_changed":
                return "活动配置已更新，请重新载入后再操作"
            if not result.succeeded:
                return "活动配置保存失败：写入冲突"
            return result.result_text

        if not target_text:
            config["enabled"] = enabled
            return commit(f"已{action_text}签到活动")

        if target_text in {"全部", "所有", "all", "ALL"}:
            config["enabled"] = enabled
            for activity in config.get("gameplay_activities") or []:
                if isinstance(activity, dict):
                    activity["enabled"] = enabled
            extensions = config.setdefault("extensions", {})
            if isinstance(extensions, dict):
                activity_pass = extensions.setdefault(
                    "activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS)
                )
                if isinstance(activity_pass, dict):
                    activity_pass["enabled"] = enabled
            return commit(f"已{action_text}全部活动")

        if target_text in {"签到", "节日签到", "签到活动"}:
            config["enabled"] = enabled
            return commit(f"已{action_text}签到活动")

        if target_text in {"玩法", "玩法活动"}:
            changed_count = 0
            for activity in config.get("gameplay_activities") or []:
                if isinstance(activity, dict):
                    activity["enabled"] = enabled
                    changed_count += 1
            message = (
                f"已{action_text}{changed_count}个玩法活动"
                if changed_count
                else "当前没有配置玩法活动"
            )
            return commit(message)

        if target_text in {"战令", "活动战令", "通行证", "活动通行证", "活跃"}:
            extensions = config.setdefault("extensions", {})
            if not isinstance(extensions, dict):
                extensions = {}
                config["extensions"] = extensions
            activity_pass = extensions.setdefault(
                "activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS)
            )
            if not isinstance(activity_pass, dict):
                activity_pass = deepcopy(DEFAULT_ACTIVITY_PASS)
                extensions["activity_pass"] = activity_pass
            activity_pass["enabled"] = enabled
            return commit(f"已{action_text}活动战令")

        type_targets = {
            "集字": "collect_words",
            "集字活动": "collect_words",
            "积分": "event_points",
            "积分活动": "event_points",
            "活动积分": "event_points",
            "活动商店": "event_points",
            "首领": "activity_boss",
            "活动首领": "activity_boss",
            "BOSS": "activity_boss",
            "boss": "activity_boss",
        }
        if target_text in type_targets:
            target_type = type_targets[target_text]
            changed_count = 0
            for activity in config.get("gameplay_activities") or []:
                if (
                    isinstance(activity, dict)
                    and _clean_text(activity.get("type"), "collect_words") == target_type
                ):
                    activity["enabled"] = enabled
                    changed_count += 1
            message = (
                f"已{action_text}{changed_count}个{target_text}"
                if changed_count
                else f"当前没有配置{target_text}"
            )
            return commit(message)

        for activity in config.get("gameplay_activities") or []:
            if not isinstance(activity, dict):
                continue
            names = {
                _clean_text(activity.get("key")),
                _clean_text(activity.get("name")),
                _clean_text(activity.get("template_key")),
                _clean_text(activity.get("type")),
            }
            if target_text in names:
                activity["enabled"] = enabled
                return commit(f"已{action_text}{target_text}")
        return commit(f"未找到活动：{target_text}")


__all__ = ["ActivityConfigApplication"]
