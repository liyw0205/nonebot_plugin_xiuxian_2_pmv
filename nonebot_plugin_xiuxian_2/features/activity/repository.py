from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort


class ActivityRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("activity", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity", handlers={
            "claim_activity_rewards": self._claim_all,
            "claim_activity_tasks": self._claim_tasks,
            "claim_activity_pass_rewards": self._claim_pass,
            "claim_sign": self._claim_sign,
            "claim_collect_phrase": self._claim_collect,
            "claim_point_shop_item": self._claim_shop,
            "activity_boss.claim_boss_rewards": self._claim_boss,
            "activity_boss.fight_cooperative_boss": self._fight_boss,
            "activity_boss.use_item_on_boss": self._use_boss_item,
            "set_enabled": self._set_enabled,
        })
        self.database = str(database)

    def _claim_all(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_activity_rewards
        return claim_activity_rewards(str(kwargs.get("user_id", "")), str(kwargs.get("operation_id", "")))

    def _claim_tasks(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_activity_tasks
        return claim_activity_tasks(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _claim_pass(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_activity_pass_rewards
        return claim_activity_pass_rewards(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _claim_sign(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_sign
        return claim_sign(str(kwargs.get("user_id", "")), str(kwargs.get("operation_id", "")))

    def _claim_collect(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_collect_phrase
        return claim_collect_phrase(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _claim_shop(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_point_shop_item
        return claim_point_shop_item(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _claim_boss(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.activity_boss import claim_boss_rewards
        return claim_boss_rewards(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")))

    def _fight_boss(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.activity_boss import fight_cooperative_boss
        return fight_cooperative_boss(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _use_boss_item(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.activity_boss import use_item_on_boss
        return use_item_on_boss(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _set_enabled(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import set_enabled
        return set_enabled(
            bool(kwargs.get("enabled")),
            kwargs.get("target"),
            operation_id=str(kwargs.get("operation_id", "")),
            operator_id=str(kwargs.get("operator_id", "")),
        )


__all__ = ["ActivityRepository"]
