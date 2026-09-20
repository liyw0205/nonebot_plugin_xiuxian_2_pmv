from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import FusionRepository


class FusionApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: FusionRepository | None = None) -> None:
        super().__init__(database, feature="fusion", repository=repository or FusionRepository(database))

    def apply(self, *, operation_id: str, user_id: str, **kwargs):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "apply", **kwargs})

    def apply_result(self, operation_id: str):
        return self.repository.apply_result(operation_id)


__all__ = ["FusionApplication"]
