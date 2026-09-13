from __future__ import annotations

from typing import Any


def bank_first_use_enabled(settings: Any) -> bool:
    if settings is None or not hasattr(settings, "get"):
        return False
    return bool(settings.get("bank_first_use_enabled", False))


__all__ = ["bank_first_use_enabled"]
