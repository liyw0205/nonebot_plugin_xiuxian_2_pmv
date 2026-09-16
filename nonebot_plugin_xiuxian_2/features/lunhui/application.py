from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import LunhuiRepository


class LunhuiApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *databases: str | Path, repository: LunhuiRepository | None = None) -> None:
        super().__init__(database, feature="lunhui", repository=repository or LunhuiRepository(database, *databases))

    def reset_result(self, operation_id: str):
        return self.repository.reset_result(operation_id)

    def recall_result(self, operation_id: str):
        return self.repository.recall_result(operation_id)

    def settle_result(self, operation_id: str):
        return self.repository.settle_result(operation_id)


__all__ = ["LunhuiApplication"]
