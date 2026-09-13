from ...core.result import ReplyPlan


def build_reply(application, *, action: str, **kwargs) -> ReplyPlan:
    return application.reply(action=action, **kwargs)


__all__ = ["build_reply"]
