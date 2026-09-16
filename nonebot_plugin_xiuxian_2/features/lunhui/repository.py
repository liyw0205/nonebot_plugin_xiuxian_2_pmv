from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort


class LunhuiRepository(ServicePort):
    def __init__(self, database: str | Path, *databases: str | Path) -> None:
        self.database = str(database)
        self.databases = tuple(str(item) for item in databases)
        self._reset_service = None
        self._recall_service = None
        self._settle_service = None
        super().__init__("lunhui", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui", handlers={
            "reset": self._reset,
            "recall": self._recall,
            "settle": self._settle,
        })

    def _build_services(self):
        from ...xiuxian.xiuxian_lunhui.transaction_service import (
            CultivationResetService,
            LunhuiRecallService,
            LunhuiSettlementService,
        )

        player = self.databases[0] if self.databases else self.database
        impart = self.databases[1] if len(self.databases) > 1 else self.database
        return (
            CultivationResetService(self.database),
            LunhuiRecallService(self.database, player),
            LunhuiSettlementService(self.database, player, impart),
        )

    def _ensure_services(self) -> None:
        if self._reset_service is None:
            self._reset_service, self._recall_service, self._settle_service = self._build_services()

    def _reset(self, **kwargs: Any):
        self._ensure_services()
        return self._reset_service.reset(**kwargs)

    def _recall(self, **kwargs: Any):
        self._ensure_services()
        return self._recall_service.recall(**kwargs)

    def _settle(self, **kwargs: Any):
        self._ensure_services()
        return self._settle_service.settle(**kwargs)

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict[str, Any]) -> Any:
        self._ensure_services()
        service = {"reset": self._reset_service, "recall": self._recall_service, "settle": self._settle_service}.get(str(action).casefold())
        if service is not None:
            return getattr(service, {"reset": "reset", "recall": "recall", "settle": "settle"}[str(action).casefold()])(
                operation_id=operation_id, user_id=user_id, **payload
            )
        return super().execute(operation_id, user_id, action, payload)

    def reset_result(self, operation_id: str) -> Any:
        self._ensure_services()
        return self._reset_service.get_result(operation_id)

    def recall_result(self, operation_id: str) -> Any:
        self._ensure_services()
        return self._recall_service.get_result(operation_id)

    def settle_result(self, operation_id: str) -> Any:
        self._ensure_services()
        return self._settle_service.get_result(operation_id)


__all__ = ["LunhuiRepository"]
