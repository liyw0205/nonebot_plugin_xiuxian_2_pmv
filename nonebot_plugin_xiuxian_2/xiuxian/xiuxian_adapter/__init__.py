from __future__ import annotations

from .selector import (
    AdapterSelection,
    AdapterSource,
    configure_adapter_paths,
    get_requested_adapter_source,
)


def ensure_vendored_adapters(prefer_vendored: bool = True) -> None:
    """强制内置 vendor 能力（路径 + intent + 成员事件）。"""
    configure_adapter_paths("vendor" if prefer_vendored else "auto")
    try:
        from .early_inject import force_builtin_qq_adapter

        force_builtin_qq_adapter()
    except Exception:
        pass


__all__ = [
    "AdapterSelection",
    "AdapterSource",
    "configure_adapter_paths",
    "ensure_vendored_adapters",
    "get_requested_adapter_source",
]
