"""Ports and small value objects for the empty-message fallback.

The fallback has no durable state.  Configuration, image lookup, and delivery
are injected by the legacy adapter, so this module only describes those seams.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol


class FallbackConfig(Protocol):
    empty_fallback: bool
    empty_msg: str
    empty_fallback_image: bool


class FallbackImageProvider(Protocol):
    def __call__(self, *, timeout: int) -> Awaitable[str | None]: ...


class FallbackSender(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Any]: ...


FallbackEventClassifier = Callable[[Any], str | None]


__all__ = [
    "FallbackConfig",
    "FallbackEventClassifier",
    "FallbackImageProvider",
    "FallbackSender",
]
