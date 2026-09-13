from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import ImpartPkRepository


class ImpartPkApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: ImpartPkRepository | None = None) -> None:
        super().__init__(database, feature="impart_pk", repository=repository or ImpartPkRepository(database))


__all__ = ["ImpartPkApplication"]
