"""Injected ports used by the message fallback application.

No database or filesystem belongs to this feature.  The concrete HTTP and
message adapters remain in the compatibility layer and are passed to the
application at composition time.
"""

from .schemas import (
    FallbackConfig,
    FallbackEventClassifier,
    FallbackImageProvider,
    FallbackSender,
)

__all__ = [
    "FallbackConfig",
    "FallbackEventClassifier",
    "FallbackImageProvider",
    "FallbackSender",
]
