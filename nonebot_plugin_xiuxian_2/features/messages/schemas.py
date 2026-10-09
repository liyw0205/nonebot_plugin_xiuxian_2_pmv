"""Request-shape contract of the Web message-send owner.

``POST /api/messages/send`` accepts one flat JSON/multipart body and this slice
alone decides what is admissible before anything reaches an adapter: the send
mode, the scene, the media type and the truthy spellings of ``active_send``.
``MessageSendResult`` is the response contract the legacy console keeps parsing,
so it is declared here together with the accepted value sets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ALLOWED_MEDIA_TYPES = frozenset({"image", "video", "audio", "file"})
SCENES = frozenset({"group", "private", "channel_group", "channel_private"})
SEND_MODES = frozenset({"plain", "markdown"})
DEFAULT_SEND_MODE = "plain"
ACTIVE_SEND_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
STICKER_MEDIA_TYPE = "image"


@dataclass(frozen=True)
class MessageSendResult:
    """JSON body returned by the legacy Web message-send endpoint."""

    body: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.body)

    @classmethod
    def failure(cls, error: str) -> "MessageSendResult":
        return cls({"success": False, "error": error})


__all__ = [
    "ACTIVE_SEND_TRUE_VALUES",
    "ALLOWED_MEDIA_TYPES",
    "DEFAULT_SEND_MODE",
    "MessageSendResult",
    "SCENES",
    "SEND_MODES",
    "STICKER_MEDIA_TYPE",
]
