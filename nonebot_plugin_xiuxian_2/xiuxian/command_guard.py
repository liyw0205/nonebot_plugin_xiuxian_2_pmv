"""Always answer: turn a raising command handler into one explicit reply.

`xiuxian_pet` got this guard in v1.2.16 after the log showed
`Running Matcher(... xiuxian_pet, lineno=93) failed.` with the player receiving
nothing at all.  The same shape exists in other command modules: their handlers
call the application layer directly, so any unexpected exception ends as a bare
`Running Matcher ... failed` line while the player waits for a reply that never
comes.

Evidence (two xiu2 logs, 33 `Running Matcher ... failed` lines, 26 of them
outside the pet module): `虚神界闭关` (8), `双修` (7), `传承祈愿` (4),
`抑制黑暗动乱` (1) and `日常` (1).  The historical causes were fixed separately;
this module fixes the *silent* structure so the next unknown failure still gets
an answer.

Design is deliberately narrow:

* success path is a pass-through (identical replies and side effects);
* on failure it logs the traceback and sends exactly one line that never contains
  a traceback, SQL fragment or file path;
* `BaseException` descendants (e.g. ``asyncio.CancelledError``) are not swallowed;
* ``functools.wraps`` keeps nonebot's dependency injection intact --
  ``inspect.signature`` (and therefore ``get_typed_signature``) follows
  ``__wrapped__``.
"""

from __future__ import annotations

import functools
from typing import Any, Awaitable, Callable

from nonebot.log import logger

from ..core.errors import ConflictError, DomainError, ValidationError
from ..infrastructure.database import is_database_busy

GENERIC_FAILURE_TEXT = "操作失败，本次未结算；如持续失败请联系管理员。"
BUSY_FAILURE_TEXT = "数据繁忙，本次未结算，请稍后重试。"
CONFLICT_FAILURE_TEXT = "上一次请求仍在处理中，本次未结算，请稍后重试。"
VALIDATION_FAILURE_TEXT = "操作参数无效，本次未结算。"


def _looks_busy(exc: BaseException) -> bool:
    """Busy/locked detection that also works without ``sqlite_errorcode``.

    ``is_database_busy`` inspects the driver-provided error code, which is absent
    on errors re-raised or constructed by callers.  The wording must not depend
    on that detail, so fall back to the driver's message.
    """

    if is_database_busy(exc):
        return True
    text = str(exc).lower()
    return (
        "database is locked" in text
        or "database table is locked" in text
        or "database is busy" in text
    )


def failure_text(exc: BaseException) -> str:
    """Map an exception to one short, leak-free hint."""

    if isinstance(exc, ConflictError):
        return CONFLICT_FAILURE_TEXT
    if isinstance(exc, ValidationError):
        return VALIDATION_FAILURE_TEXT
    if isinstance(exc, DomainError):
        return GENERIC_FAILURE_TEXT
    if _looks_busy(exc):
        return BUSY_FAILURE_TEXT
    return GENERIC_FAILURE_TEXT


def failure_reply(action: str, exc: BaseException) -> str:
    """Full reply line for one failed command."""

    return f"{action}未完成：{failure_text(exc)}"


def _argument(args: tuple[Any, ...], kwargs: dict[str, Any], index: int, name: str) -> Any:
    if name in kwargs:
        return kwargs[name]
    if len(args) > index:
        return args[index]
    return None


def guard_command(
    action: str,
    *,
    send: Callable[..., Awaitable[Any]] | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Make ``action`` answer the player when its handler raises."""

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - answering is the point
                bot = _argument(args, kwargs, 0, "bot")
                event = _argument(args, kwargs, 1, "event")
                logger.opt(exception=exc).error(
                    f"[command] {action} handler failed ({type(exc).__name__}); replied with a hint"
                )
                sender = send
                if sender is None:
                    from .xiuxian_utils.utils import handle_send

                    sender = handle_send
                try:
                    await sender(bot, event, failure_reply(action, exc))
                except Exception:  # noqa: BLE001 - delivery must not mask the original
                    logger.opt(exception=True).error(
                        f"[command] {action} failure reply was not delivered"
                    )
                return None

        return wrapper

    return decorator


__all__ = [
    "BUSY_FAILURE_TEXT",
    "CONFLICT_FAILURE_TEXT",
    "GENERIC_FAILURE_TEXT",
    "VALIDATION_FAILURE_TEXT",
    "failure_reply",
    "failure_text",
    "guard_command",
]
