from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort
from ...infrastructure.clock import SystemClock


class TrainingRepository(ServicePort):
    def __init__(
        self,
        database: str | Path,
        player_database: str | Path | None = None,
        *,
        clock: Any | None = None,
    ) -> None:
        super().__init__("training", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training", handlers={
            "event_apply": self._event_apply,
            "purchase": self._purchase,
            "reset": self._reset,
        })
        self.database = str(database)
        self.player_database = str(player_database) if player_database is not None else None
        self.clock = clock or SystemClock()
        self._event_repository = None
        self._purchase_repository = None
        self._reset_repository = None

    @staticmethod
    def _services():
        # Compatibility is an explicit fallback only when the composition
        # root did not provide a player database (for old maintenance calls).
        from ...xiuxian.xiuxian_training.transaction_service import (
            TrainingEventService,
            TrainingPurchaseService,
            TrainingResetService,
        )

        training_event_service = TrainingEventService
        training_purchase_service = TrainingPurchaseService
        training_reset_service = TrainingResetService
        return training_event_service, training_purchase_service, training_reset_service

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict[str, Any]) -> Any:
        """Keep the migrated event path out of the legacy compatibility counter."""
        if str(action).casefold() == "event_apply":
            values = dict(payload)
            values.pop("user_id", None)
            return self._event_apply(
                operation_id=operation_id,
                user_id=user_id,
                **values,
            )
        if str(action).casefold() == "purchase" and self.player_database is not None:
            values = dict(payload)
            values.pop("user_id", None)
            return self._purchase(
                operation_id=operation_id,
                user_id=user_id,
                **values,
            )
        return super().execute(operation_id, user_id, action, payload)

    def _event_apply(self, **kwargs: Any):
        if self.player_database is None:
            service = self._services()[0](self.database, self.database)
            return service.apply(**kwargs)
        if self._event_repository is None:
            from .event_repository import TrainingEventSqlRepository

            self._event_repository = TrainingEventSqlRepository(
                self.database, self.player_database
            )
        return self._event_repository.apply(**kwargs)

    def _purchase(self, **kwargs: Any):
        if self.player_database is None:
            service = self._services()[1](self.database, self.database)
            return service.purchase(**kwargs)
        if self._purchase_repository is None:
            from .purchase_repository import TrainingPurchaseSqlRepository

            self._purchase_repository = TrainingPurchaseSqlRepository(
                self.database, self.player_database, clock=self.clock
            )
        return self._purchase_repository.purchase(**kwargs)

    def _reset(self, **kwargs: Any):
        if self.player_database is None:
            service = self._services()[2](self.database, self.database)
            return service.reset(**kwargs)
        if self._reset_repository is None:
            from .reset_repository import TrainingResetSqlRepository

            self._reset_repository = TrainingResetSqlRepository(
                self.database, self.player_database, clock=self.clock
            )
        return self._reset_repository.reset(**kwargs)

    def reset_limits(self, **kwargs: Any):
        """Run one resumable reset chunk without the single-operation ledger."""
        return self._reset(**kwargs)


__all__ = ["TrainingRepository"]
