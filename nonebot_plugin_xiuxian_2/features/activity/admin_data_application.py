from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .admin_data_repository import ActivityAdminDataSqlRepository


def _today_str() -> str:
    from ...xiuxian.xiuxian_activity.activity_storage import today_str

    return today_str()


def _now_str() -> str:
    from ...xiuxian.xiuxian_activity.activity_storage import now_str

    return now_str()


class ActivityAdminDataApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: ActivityAdminDataSqlRepository | None = None,
        config_loader: Callable[[], dict[str, Any]] | None = None,
        today_provider: Callable[[], str] | None = None,
        timestamp_provider: Callable[[], str] | None = None,
    ) -> None:
        self.repository = repository or ActivityAdminDataSqlRepository(database)
        self.config_loader = config_loader
        self.today_provider = today_provider or _today_str
        self.timestamp_provider = timestamp_provider or _now_str

    def _config(self) -> dict[str, Any]:
        if self.config_loader is not None:
            return self.config_loader()
        from ...xiuxian.xiuxian_activity.activity_config import load_config

        return load_config()

    def overview(
        self,
        *,
        activity_key: str | None = None,
        user_id: str | None = None,
        limit: Any = 10,
    ) -> dict[str, Any]:
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            _activity_elapsed_days,
            activity_runtime_state,
            activity_state,
        )
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _activity_pass_config,
            _task_scope_key,
            get_activity_tasks,
            get_gameplay_activities,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text

        config = self._config()
        key_filter = _clean_text(activity_key)
        uid = _clean_text(user_id)
        row_limit = max(1, min(_as_int(limit, 10), 50))
        activities = []
        for activity in get_gameplay_activities(config):
            key = str(activity.get("key") or "")
            if key_filter and key != key_filter:
                continue
            active, reason = activity_state(activity)
            activities.append(
                {
                    **activity,
                    "state": "进行中" if active else reason,
                }
            )

        activity_key_for_main = _activity_config_key(config)
        pass_config = _activity_pass_config(config)
        result = self.repository.overview_snapshot(
            activities=activities,
            user_id=uid,
            limit=row_limit,
            today=self.today_provider(),
            activity_key=activity_key_for_main,
            daily_task_key=_task_scope_key("daily"),
            weekly_task_key=_task_scope_key("weekly"),
            tasks=get_activity_tasks(config),
            pass_config=pass_config,
            elapsed_days=_activity_elapsed_days(config),
        )

        task_states = {
            (str(row["scope_type"]), str(row["scope_key"]), str(row["task_key"])): row
            for row in result.pop("task_user_rows")
        }
        if uid:
            task_defs = get_activity_tasks(config)
            result["tasks"]["user"] = [
                {
                    "task_key": task["key"],
                    "name": task["name"],
                    "scope_type": task["scope_type"],
                    "scope_key": _task_scope_key(task["scope_type"]),
                    "progress": min(
                        max(1, _as_int(task.get("target"), 1)),
                        max(
                            0,
                            _as_int(
                                task_states.get(
                                    (
                                        task["scope_type"],
                                        _task_scope_key(task["scope_type"]),
                                        task["key"],
                                    ),
                                    {},
                                ).get("progress"),
                                0,
                            ),
                        ),
                    ),
                    "target": max(1, _as_int(task.get("target"), 1)),
                    "claimed": bool(
                        _as_int(
                            task_states.get(
                                (
                                    task["scope_type"],
                                    _task_scope_key(task["scope_type"]),
                                    task["key"],
                                ),
                                {},
                            ).get("claimed"),
                            0,
                        )
                    ),
                }
                for task in task_defs
            ]

        pass_data = result.pop("pass")
        elapsed_days = result.pop("pass_elapsed_days")
        if pass_data.get("enabled") and uid:
            user_state = pass_data.pop("user_state", {})
            level_exp = max(1, _as_int(pass_data.get("level_exp"), 100))
            max_level = max(1, _as_int(pass_data.get("max_level"), 12))
            total_exp = max(0, _as_int(user_state.get("total_exp"), 0))
            level = min(total_exp // level_exp, max_level)
            current_exp = level_exp if level >= max_level else max(0, total_exp - level * level_exp)
            pass_data["user"] = {
                "exp": current_exp,
                "total_exp": total_exp,
                "level": level,
                "level_exp": level_exp,
                "max_level": max_level,
            }

            catchup = pass_data["catchup"]
            start_day = max(1, _as_int(catchup.get("start_day"), 5))
            level_gap = max(1, _as_int(catchup.get("level_gap"), 3))
            multiplier = max(1.0, float(catchup.get("multiplier") or 1.5))
            highest_level = max(0, _as_int(pass_data["highest_level"], 0))
            gap = max(0, highest_level - level)
            active = bool(catchup.get("enabled")) and elapsed_days >= start_day and gap >= level_gap and multiplier > 1.0
            pass_data["user_catchup"] = {
                "enabled": bool(catchup.get("enabled")),
                "active": active,
                "elapsed_day": elapsed_days,
                "start_day": start_day,
                "level_gap": level_gap,
                "catchup_multiplier": multiplier,
                "highest_level": highest_level,
                "user_level": level,
                "gap": gap,
                "multiplier": multiplier if active else 1.0,
            }
        result["activity_pass"] = pass_data
        result["runtime"] = activity_runtime_state(config)
        return result

    def reset(self, scope: str | None, activity_key: str | None = None) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import _activity_config_key
        from ...xiuxian.xiuxian_activity.activity_utils import _clean_text

        deleted = self.repository.reset(
            _clean_text(scope, "activity"),
            _clean_text(activity_key),
            _activity_config_key(self._config()),
        )
        return f"已清理活动数据，影响记录 {deleted} 条"

    def adjust(
        self,
        *,
        adjust_type: str,
        activity_key: str | None,
        user_id: str | None,
        word_char: str | None,
        amount: Any,
    ) -> dict[str, Any]:
        from ...xiuxian.xiuxian_activity.activity_config import _activity_config_key
        from ...xiuxian.xiuxian_activity.activity_rules import _activity_pass_config, get_gameplay_activities
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text

        kind = _clean_text(adjust_type)
        uid = _clean_text(user_id)
        delta = _as_int(amount, 0)
        if kind not in {"points", "word", "pass_exp"}:
            raise ValueError("调整类型无效")
        if not uid:
            raise ValueError("请输入用户ID")
        if delta == 0:
            raise ValueError("调整数量不能为 0")

        config = self._config()
        timestamp = self.timestamp_provider()
        if kind == "pass_exp":
            pass_config = _activity_pass_config(config)
            if not pass_config.get("enabled"):
                raise ValueError("活动战令未开启")
            return self.repository.adjust_pass_exp(
                _activity_config_key(config),
                uid,
                delta,
                level_exp=pass_config["level_exp"],
                max_level=pass_config["max_level"],
                timestamp=timestamp,
            )

        key = _clean_text(activity_key)
        activity = next(
            (item for item in get_gameplay_activities(config) if item.get("key") == key),
            None,
        )
        expected_type = "event_points" if kind == "points" else "collect_words"
        if not activity or activity.get("type") != expected_type:
            raise ValueError("请选择积分活动" if kind == "points" else "请选择集字活动")

        if kind == "points":
            return self.repository.adjust_points(key, uid, delta, timestamp)
        char = _clean_text(word_char)
        if not char:
            raise ValueError("请输入字牌")
        return self.repository.adjust_collect_word(key, uid, char[0], delta, timestamp)


__all__ = ["ActivityAdminDataApplication"]
