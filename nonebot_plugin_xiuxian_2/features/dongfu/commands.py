from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan


def build_reply(application: Any, **kwargs: Any) -> ReplyPlan:
    return application.reply(**kwargs)


__all__ = ["build_reply"]
