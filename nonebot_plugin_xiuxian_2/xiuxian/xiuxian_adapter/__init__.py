from __future__ import annotations

from .selector import (
    AdapterSelection,
    AdapterSource,
    configure_adapter_paths,
    get_requested_adapter_source,
)


def ensure_vendored_adapters(prefer_vendored: bool = True) -> None:
    """Backward-compatible wrapper for older internal imports."""
    configure_adapter_paths("vendor" if prefer_vendored else "auto")


__all__ = [
    "AdapterSelection",
    "AdapterSource",
    "configure_adapter_paths",
    "ensure_vendored_adapters",
    "get_requested_adapter_source",
]
