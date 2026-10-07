from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort
from ...infrastructure.database import DatabaseUnitOfWork


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
            CultivationResetService(self.database, player_database=player),
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

    def get_reincarnation_memory(self, user_id: str) -> dict[str, Any] | None:
        player_database = Path(self.databases[0] if self.databases else self.database)
        if not player_database.exists():
            return None

        try:
            with DatabaseUnitOfWork(player_database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM reincarnation_memory WHERE user_id=?",
                    (str(user_id),),
                )
        except Exception as exc:
            if type(exc).__name__ == "OperationalError" and "no such table" in str(exc).casefold():
                return None
            raise
        if row is None:
            return None

        data = row

        def as_int(field: str) -> int:
            try:
                return int(data.get(field, 0) or 0)
            except (TypeError, ValueError):
                return 0

        return {
            "main_buff": as_int("main_buff"),
            "sub_buff": as_int("sub_buff"),
            "sec_buff": as_int("sec_buff"),
            "effect1_buff": as_int("effect1_buff"),
            "effect2_buff": as_int("effect2_buff"),
            "memory_level": data.get("memory_level", ""),
            "retrieved": {
                "main": bool(as_int("retrieved_main")),
                "sub": bool(as_int("retrieved_sub")),
                "sec": bool(as_int("retrieved_sec")),
                "effect1": bool(as_int("retrieved_effect1")),
                "effect2": bool(as_int("retrieved_effect2")),
            },
        }


__all__ = ["LunhuiRepository"]
