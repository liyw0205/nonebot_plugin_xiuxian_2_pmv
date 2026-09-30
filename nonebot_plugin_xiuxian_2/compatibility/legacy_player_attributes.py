"""Explicit compatibility adapter for the legacy dynamic-attribute formula."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def get_final_attributes(
    user_id: int | str,
    *,
    ratio: float = 1.0,
    include_current: bool = True,
    **providers: Any,
) -> Mapping[str, Any] | None:
    from ..xiuxian.xiuxian_utils.xiuxian2_handle import (
        get_final_attributes as legacy_get_final_attributes,
    )

    return legacy_get_final_attributes(
        user_id,
        ratio=ratio,
        include_current=include_current,
        **providers,
    )


__all__ = ["get_final_attributes"]
