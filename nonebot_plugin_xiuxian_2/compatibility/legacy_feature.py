"""Generic command facade for a staged legacy feature migration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Type

from ._feature_facade import FeatureFacade
from ..features._legacy_migrated import APPLICATIONS


def build_service(feature: str, application_type: Type[Any] | None = None) -> type[FeatureFacade]:
    app_type = application_type or APPLICATIONS[feature]

    class LegacyService(FeatureFacade):
        def __init__(self, database: str | Path, *databases: str | Path) -> None:
            self.application = app_type(str(database))

    LegacyService.__name__ = f"{feature.title().replace('_', '')}Service"
    LegacyService.feature = feature
    return LegacyService


__all__ = ["build_service"]
