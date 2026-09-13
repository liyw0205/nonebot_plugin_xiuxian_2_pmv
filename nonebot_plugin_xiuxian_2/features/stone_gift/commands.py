from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan
from .application import StoneGiftApplication


def handle_stone_gift(application: StoneGiftApplication, **kwargs: Any) -> ReplyPlan:
    return application.reply(**kwargs)


__all__ = ["handle_stone_gift"]
