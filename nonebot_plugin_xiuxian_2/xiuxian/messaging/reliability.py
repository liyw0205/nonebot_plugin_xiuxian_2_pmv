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


def classify_delivery_error(exc: BaseException) -> tuple[DeliveryErrorKind, bool]:
    name = exc.__class__.__name__.lower()
    code = _exception_code(exc)
    text = str(exc).lower()
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


@dataclass(frozen=True)
class DeliveryError(RuntimeError):
    kind: DeliveryErrorKind
    retryable: bool
    cause: BaseException

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
    "is_msg_seq_conflict",
]
