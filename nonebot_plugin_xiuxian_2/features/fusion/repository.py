from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort


class FusionRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("fusion", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_fusion")
        self.database = str(database)

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict):
        if str(action).casefold() == "apply_batch":
            from ...xiuxian.xiuxian_fusion.fusion_service import FusionService

            return FusionService(self.database).apply_batch(
                operation_id, user_id, payload["need_stone"], payload["needed_items"],
                payload["equipment_id"], payload["equipment_name"], payload["equipment_type"],
                payload["outcomes"], protection_item_id=payload.get("protection_item_id"),
                reserved_items=payload.get("reserved_items"), max_goods_num=payload["max_goods_num"],
                target_limit=payload.get("target_limit"),
            )
        if str(action).casefold() == "apply":
            from ...xiuxian.xiuxian_fusion.fusion_service import FusionService

            return FusionService(self.database).apply(
                operation_id, user_id, payload["need_stone"], payload["needed_items"],
                payload["equipment_id"], payload["equipment_name"], payload["equipment_type"],
                successful=payload["successful"], protection_item_id=payload.get("protection_item_id"),
                reserved_items=payload.get("reserved_items"), max_goods_num=payload["max_goods_num"],
            )
        return super().execute(operation_id, user_id, action, payload)

    def apply_result(self, operation_id: str):
        from ...xiuxian.xiuxian_fusion.fusion_service import FusionService

        return FusionService(self.database).get_result(operation_id)

    def batch_result(self, operation_id: str):
        from ...xiuxian.xiuxian_fusion.fusion_service import FusionService

        return FusionService(self.database).get_batch_result(operation_id)


__all__ = ["FusionRepository"]
