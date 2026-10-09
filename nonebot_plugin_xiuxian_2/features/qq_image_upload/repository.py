"""Persistence-free repository boundary of the QQ image-upload owner.

The real work is one async QQ Open API call chain (upload media, then resolve a
usable URL).  It reaches this slice through an injected coroutine, so the
boundary is declared here as a protocol and the application depends on it
instead of on an untyped callable.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Protocol


class UploadImageAndResolve(Protocol):
    """Upload ``image`` to ``channel_id`` and return a fetchable URL, or ``None``."""

    def __call__(
        self,
        *,
        bot: Any,
        channel_id: str,
        image: bytes,
        mode: str,
    ) -> Awaitable[str | None]: ...


__all__ = ["UploadImageAndResolve"]
