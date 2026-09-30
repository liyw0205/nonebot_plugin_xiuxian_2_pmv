"""Application boundary for dynamic player attributes.

The calculation still has a compatibility implementation because it spans the
legacy buff, impart, accessory and tianti projections. Keeping that adapter
explicit lets callers depend on one feature-owned read boundary while those
projections are migrated independently.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


AttributeProvider = Callable[..., Mapping[str, Any] | None]


def _legacy_attribute_provider(
    user_id: int | str,
    *,
    ratio: float = 1.0,
    include_current: bool = True,
    **providers: Any,
) -> Mapping[str, Any] | None:
    from ...compatibility.legacy_player_attributes import get_final_attributes

    return get_final_attributes(
        user_id,
        ratio=ratio,
        include_current=include_current,
        **providers,
    )


class PlayerAttributeApplication:
    """Resolve dynamic attributes through an injected read provider."""

    def __init__(self, provider: AttributeProvider | None = None) -> None:
        self.provider = provider or _legacy_attribute_provider

    def get_final_attributes(
        self,
        user_id: int | str,
        *,
        ratio: float = 1.0,
        include_current: bool = True,
        **providers: Any,
    ) -> Mapping[str, Any] | None:
        return self.provider(
            user_id,
            ratio=ratio,
            include_current=include_current,
            **providers,
        )


__all__ = ["AttributeProvider", "PlayerAttributeApplication"]
