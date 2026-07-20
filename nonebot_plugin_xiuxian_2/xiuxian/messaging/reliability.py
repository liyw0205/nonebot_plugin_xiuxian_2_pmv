from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Literal


DeliveryErrorKind = Literal[
    "audit_rejected",
    "invalid_request",
    "rate_limited",
    "retryable",
    "unauthorized",
    "unsupported",
    "unknown",
]


def _exception_code(exc: BaseException) -> int | None:
    for name in ("code", "retcode", "status_code"):
        try:
            value = getattr(exc, name, None)
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def is_msg_seq_conflict(exc: BaseException) -> bool:
    code = _exception_code(exc)
    text = str(exc).lower()
    return code == 40054005 or any(
        marker in text for marker in ("40054005", "消息被去重", "msgseq", "msg_seq")
    )


def is_passive_reply_limit(exc: BaseException) -> bool:
    """QQ 官方：同一条用户消息被动回复次数/时间窗口超限。"""
    code = _exception_code(exc)
    text = str(exc)
    return code == 40034128 or "40034128" in text or (
        "被动回复" in text and "超过" in text
    )


def is_bot_not_group_member(exc: BaseException) -> bool:
    """QQ 官方：机器人不在目标群，主动/被动群发均会失败（code=11293）。"""
    # DeliveryError may wrap ActionFailed; walk cause chain without forward-ref.
    cur: BaseException | None = exc
    seen = 0
    while cur is not None and seen < 4:
        code = _exception_code(cur)
        text = str(cur)
        if code == 11293 or "11293" in text or "机器人非群成员" in text or (
            "非群成员" in text and "机器人" in text
        ):
            return True
        cur = cur.__cause__ or getattr(cur, "cause", None)
        seen += 1
    return False


def classify_delivery_error(exc: BaseException) -> tuple[DeliveryErrorKind, bool]:
    name = exc.__class__.__name__.lower()
    code = _exception_code(exc)
    text = str(exc).lower()
    if is_passive_reply_limit(exc):
        # 同一 msg_id 再发也不会好，勿重试
        return "rate_limited", False
    if is_bot_not_group_member(exc):
        # 已退群/未入群：重试无意义，上层应吞掉勿打 Matcher ERROR
        return "unauthorized", False
    if "ratelimit" in name or code == 429 or "rate limit" in text or "频率" in text:
        return "rate_limited", True
    if "network" in name or isinstance(exc, (ConnectionError, TimeoutError)):
        return "retryable", True
    if "unauthorized" in name or code in {401, 403}:
        return "unauthorized", False
    if "apinotavailable" in name or "not supported" in text or "不支持" in text:
        return "unsupported", False
    if code is not None and 400 <= code < 500:
        return "invalid_request", False
    return "unknown", False


@dataclass
class DeliveryError(RuntimeError):
    kind: DeliveryErrorKind
    retryable: bool
    cause: BaseException

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, str(self))

    def __str__(self) -> str:
        return f"消息投递失败[{self.kind}]: {self.cause}"


class MessageSequenceStrategy:
    """按 Bot、场景和目标生成递增 QQ msg_seq。"""

    def __init__(self) -> None:
        self._values: dict[tuple[str, str, str], int] = defaultdict(
            lambda: random.randint(1000, 900000)
        )

    def next(self, bot: Any, scene: str, target_id: str) -> int:
        key = (str(getattr(bot, "self_id", "")), scene, str(target_id))
        value = self._values[key] + random.randint(1, 3)
        if value > 1_000_000:
            value = random.randint(1000, 10000)
        self._values[key] = value
        return value


__all__ = [
    "DeliveryError",
    "DeliveryErrorKind",
    "MessageSequenceStrategy",
    "classify_delivery_error",
    "is_bot_not_group_member",
    "is_msg_seq_conflict",
    "is_passive_reply_limit",
]
