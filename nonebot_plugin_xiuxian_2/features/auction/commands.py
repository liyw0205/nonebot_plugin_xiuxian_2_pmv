"""Command adapter entry point for auction bidding."""

from ...core.result import ReplyPlan


def build_reply(application, **kwargs) -> ReplyPlan:
    return application.reply(**kwargs)


__all__ = ["build_reply"]
