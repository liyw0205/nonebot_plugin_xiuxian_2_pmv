"""Compatibility import for the feature-owned fusion transaction boundary.

The historical module remains importable for extensions and tests, while the
implementation is owned by :mod:`nonebot_plugin_xiuxian_2.features.fusion`.
The feature implementation keeps the same ``BEGIN IMMEDIATE`` transaction
boundary and operation tables.
"""

from contextlib import closing

from ...features.fusion.settlement_repository import (
    FusionBatchResult,
    FusionResult,
    FusionService as _FeatureFusionService,
)
from ..xiuxian_utils import db_backend


def _ensure_compat_schema(database) -> None:
    """Keep direct historical callers usable without weakening startup-owned writes."""
    with closing(db_backend.connect(database)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS fusion_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, successful INTEGER NOT NULL, "
            "protected INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS fusion_batch_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, outcomes TEXT NOT NULL, "
            "successful_count INTEGER NOT NULL, failed_count INTEGER NOT NULL, "
            "protected_count INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.commit()


class FusionService(_FeatureFusionService):
    """Historical service identity backed by the feature-owned implementation."""

    def apply(self, *args, **kwargs):
        _ensure_compat_schema(self._database)
        return super().apply(*args, **kwargs)

    def apply_batch(self, *args, **kwargs):
        _ensure_compat_schema(self._database)
        return super().apply_batch(*args, **kwargs)

    def get_result(self, *args, **kwargs):
        _ensure_compat_schema(self._database)
        return super().get_result(*args, **kwargs)

    def get_batch_result(self, *args, **kwargs):
        _ensure_compat_schema(self._database)
        return super().get_batch_result(*args, **kwargs)

__all__ = ["FusionBatchResult", "FusionResult", "FusionService"]
