from __future__ import annotations

import warnings
from typing import Any

from .commands import record_compatibility_hit


class FeatureFacade:
    feature = "legacy"

    def _warn(self, name: str) -> None:
        warnings.warn(f"{name} is a compatibility facade; use the feature application", DeprecationWarning, stacklevel=3)
        record_compatibility_hit(self.feature)

    def __getattr__(self, name: str):
        target = getattr(self.application, name)

        def call(*args: Any, **kwargs: Any):
            self._warn(f"{self.__class__.__name__}.{name}")
            if args:
                raise TypeError("compatibility facade methods require keyword arguments")
            return target(**kwargs)

        return call


__all__ = ["FeatureFacade"]
