from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan
from .application import PackageRewardApplication


def handle_package_reward(application: PackageRewardApplication, **kwargs: Any) -> ReplyPlan:
    return application.reply(**kwargs)


__all__ = ["handle_package_reward"]
