from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from ..adapter_message_records import (
    extract_result_message_id,
    extract_result_reference_id,
)


DeliveryScene = Literal["group", "private", "channel_group", "channel_private"]


@dataclass(frozen=True)
class SendResult:
    message_id: str | None
    reference_id: str | None
    raw: Any

    @classmethod
    def from_raw(cls, raw: Any) -> "SendResult":
        return cls(
            message_id=extract_result_message_id(raw) or None,
            reference_id=extract_result_reference_id(raw) or None,
            raw=raw,
        )


@dataclass(frozen=True)
class SendRequest:
    scene: DeliveryScene
    target_id: str
    message: Any
    reference_id: str | None = None
    source_message_id: str | None = None
    revoke_after: int | float = 0


__all__ = ["DeliveryScene", "SendRequest", "SendResult"]
