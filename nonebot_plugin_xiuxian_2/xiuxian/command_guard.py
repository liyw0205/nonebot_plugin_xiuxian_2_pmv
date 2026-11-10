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
import sqlite3
from uuid import uuid4
from typing import Any, Awaitable, Callable

from nonebot.exception import MatcherException, ProcessException
from nonebot.log import logger

from ..core.errors import ConflictError, DomainError, OperationConflictError, ValidationError
from ..core.player_reasons import reason_text
from ..infrastructure.database import is_database_busy

GENERIC_FAILURE_TEXT = "处理或回复时发生内部异常，请查询结果并联系管理员核对。"
BUSY_FAILURE_TEXT = "数据库繁忙，请稍后查询结果再操作。"
CONFLICT_FAILURE_TEXT = "请求发生冲突，请查询当前操作结果。"
VALIDATION_FAILURE_TEXT = "命令参数未通过校验，请检查命令格式。"


def _looks_busy(exc: BaseException) -> bool:
    """Busy/locked detection that also works without ``sqlite_errorcode``.

    ``is_database_busy`` inspects the driver-provided error code, which is absent
    on errors re-raised or constructed by callers.  The wording must not depend
    on that detail, so fall back to the driver's message.
    """

    if is_database_busy(exc):
        return True
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return (
        "database is locked" in text
        or "database table is locked" in text
        or "database is busy" in text
    )


def failure_text(exc: BaseException) -> str:
    """Map an exception to one short, leak-free hint."""

    if isinstance(exc, OperationConflictError):
        return reason_text("operation_conflict")
    if isinstance(exc, DomainError):
        known = reason_text(exc.details.get("reason", "")) or reason_text(exc.code)
        if known:
            label = _subject_label(exc.details.get("subject", ""))
            return (label + "，" if label else "") + known
    if isinstance(exc, ConflictError):
        return CONFLICT_FAILURE_TEXT
    if isinstance(exc, ValidationError):
        return VALIDATION_FAILURE_TEXT
    if isinstance(exc, DomainError):
        return GENERIC_FAILURE_TEXT
    if _looks_busy(exc):
        return BUSY_FAILURE_TEXT
    return GENERIC_FAILURE_TEXT


def failure_reply(action: str, exc: BaseException, *, reference: str | None = None) -> str:
    """Full reply line for one failed command."""

    text = failure_text(exc)
    suffix = f"（定位号：{reference or uuid4().hex[:12]}）" if text == GENERIC_FAILURE_TEXT else ""
    return f"{action}处理提示：{text}{suffix}"


def result_failure(action: str, result, *, messages: dict[str, str] | None = None, subjects: dict[str, str] | None = None) -> str:
    data = getattr(result, "data", None)
    if isinstance(data, dict):
        result = {"status": getattr(result, "code", "") or result.status, **data}
    if isinstance(result, dict):
        status = result.get("status", "")
        code = result.get("reason") or status
        subject = result.get("subject", "")
    else:
        status = result.status
        code = getattr(result, "reason", "") or status
        subject = getattr(result, "subject", "")
    text = (messages or {}).get(code) or reason_text(code)
    if text:
        label = {str(key): value for key, value in (subjects or {}).items()}.get(str(subject), "")
        if not label:
            label = _subject_label(subject)
        if str(subject).startswith("plot:") and str(subject)[5:].isdigit():
            label = str(subject)[5:] + "号灵田"
        return f"{action}：{label + '，' if label else ''}{text}"
    return report_exception(action, RuntimeError(f"unmapped result status={status!r} reason={code!r}"))


def _subject_label(subject) -> str:
    return {"visitor": "来访方", "target": "目标方", "source": "材料", "destination": "目标", "sender": "赠送方", "recipient": "接收方", "actor": "发起方", "challenger": "挑战者", "opponent": "对手", "sect": "宗门", "mentor": "师父", "apprentice": "徒弟"}.get(str(subject), "")


def report_exception(action: str, exc: BaseException, *, phase: str = "处理或回复") -> str:
    reference = uuid4().hex[:12]
    logger.opt(exception=exc).error(f"[command] {action} phase={phase} reference={reference}")
    return failure_reply(action, exc, reference=reference)


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
            except (MatcherException, ProcessException):
                raise
            except Exception as exc:  # noqa: BLE001 - answering is the point
                bot = _argument(args, kwargs, 0, "bot")
                event = _argument(args, kwargs, 1, "event")
                reply = report_exception(action, exc)
                sender = send
                if sender is None:
                    from .xiuxian_utils.utils import handle_send

                    sender = handle_send
                try:
                    await sender(bot, event, reply)
                except (MatcherException, ProcessException):
                    raise
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
    "report_exception",
    "result_failure",
]
