"""User-visible failure replies for pet commands.

Production evidence (``/root/xiu2/logs/xiu2.log-20261103-033001.gz``, 2026-11-01)::

    11-01 20:20:25 [ERROR] nonebot | Running Matcher(type='message',
        module=...xiuxian.xiuxian_pet, lineno=93) failed.
    ...
    sqlite3.IntegrityError: UNIQUE constraint failed:
        player_pet_item.user_id, player_pet_item.uid

``lineno=93`` is ``pet_egg = on_command("砸蛋", ...)``.  The matcher raised, nonebot
logged the failure and the player received **no reply at all** -- no "未结算", no
hint, nothing.  ``d64ce230`` removed one cause of that raise (pet UID collisions),
but every remaining exception on any pet command still ends in silence: the pet
module calls ``pet_application.*`` from twelve sites and handled no exceptions.

This module turns that silence into one explicit reply.  It is deliberately
narrow:

* the guard wraps a command handler; on success it is a pass-through (identical
  replies and side effects);
* on failure it logs the exception (so the traceback is still available) and
  sends exactly one user-facing line that never contains a traceback, SQL
  fragment or file path;
* control-flow interrupts (``BaseException`` descendants such as
  ``asyncio.CancelledError``) are not swallowed.
"""

from __future__ import annotations

import functools
from typing import Any, Awaitable, Callable

from nonebot.log import logger

from ...core.errors import ConflictError, DomainError, ValidationError
from ...infrastructure.database import is_database_busy

GENERIC_FAILURE_TEXT = "宠物操作失败，本次未结算；如持续失败请联系管理员。"
BUSY_FAILURE_TEXT = "宠物数据繁忙，本次未结算，请稍后重试。"
CONFLICT_FAILURE_TEXT = "上一次宠物请求仍在处理中，本次未结算，请稍后重试。"
VALIDATION_FAILURE_TEXT = "宠物操作参数无效，本次未结算。"


def _looks_busy(exc: BaseException) -> bool:
    """Busy/locked detection that also works without ``sqlite_errorcode``.

    ``is_database_busy`` inspects the driver-provided error code, which is absent
    on errors re-raised/constructed by callers.  The wording choice must not
    depend on that detail, so fall back to the driver's message.
    """

    if is_database_busy(exc):
        return True
    text = str(exc).lower()
    return "database is locked" in text or "database table is locked" in text or "database is busy" in text


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
    """Full reply line for one failed pet command."""

    return f"{action}未完成：{failure_text(exc)}"


def _argument(args: tuple[Any, ...], kwargs: dict[str, Any], index: int, name: str) -> Any:
    if name in kwargs:
        return kwargs[name]
    if len(args) > index:
        return args[index]
    return None


def guard_pet_handler(
    action: str,
    *,
    send: Callable[..., Awaitable[Any]] | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Make ``action`` answer the player when its handler raises.

    ``functools.wraps`` keeps the original signature visible to nonebot's
    dependency injection, so ``CommandArg``/``Bot``/``Event`` parameters keep
    working unchanged.  ``send`` is injectable for tests; by default it resolves
    the project's ``handle_send`` lazily (avoids an import cycle at module load).
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - the point is to answer
                bot = _argument(args, kwargs, 0, "bot")
                event = _argument(args, kwargs, 1, "event")
                logger.opt(exception=exc).error(
                    f"[pet] {action} handler failed ({type(exc).__name__}); replying with a hint"
                )
                sender = send
                if sender is None:
                    from ..xiuxian_utils.utils import handle_send

                    sender = handle_send
                try:
                    await sender(bot, event, failure_reply(action, exc))
                except Exception:  # noqa: BLE001 - delivery must not mask the original
                    logger.opt(exception=True).error(f"[pet] {action} failure reply was not delivered")
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
    "guard_pet_handler",
]
