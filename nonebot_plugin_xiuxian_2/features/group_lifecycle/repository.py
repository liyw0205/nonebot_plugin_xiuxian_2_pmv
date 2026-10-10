"""Repository boundary for lifecycle state adapters.

The current application delegates persistence to the existing lifecycle adapter;
this module records that boundary without duplicating its storage implementation.
"""

from typing import Any, Protocol


class LifecycleEventApplier(Protocol):
    def __call__(self, bot: Any, event: Any) -> Any: ...


__all__ = ["LifecycleEventApplier"]
