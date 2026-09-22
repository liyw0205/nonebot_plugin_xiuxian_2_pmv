from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import ItemGrantRequest, StoneAdjustmentRequest
from .repository import AdminItemRepository, AdminStoneRepository, LegacyAdminItemRepository, LegacyAdminStoneRepository
from .schemas import ItemGrantResult, StoneAdjustmentResult
from .stone_repository import AdminStoneSqlRepository
from .item_repository import AdminItemSqlRepository
from .impart_stone_repository import AdminImpartStoneSqlRepository
from .item_destroy_repository import AdminItemDestroySqlRepository
from .exp_repository import AdminExpAdjustmentSqlRepository


class AdminAssetApplication:
    action = "admin.stone_adjust"

    def __init__(self, database: str | Path, *, repository: AdminStoneRepository | None = None,
                 item_repository: AdminItemRepository | None = None,
                 ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.repository = repository
        self.item_repository = item_repository
        self.ledger = ledger or OperationLedger()

    def adjust_stone(
        self,
        *,
        operation_id: str,
        operator_id: str,
        user_id: str,
        expected_stone: int,
        requested_delta: int,
        target_name: str = "",
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = StoneAdjustmentRequest(
                str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip(),
                int(expected_stone), int(requested_delta), str(target_name),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.repository or AdminStoneSqlRepository(self.database)
                raw = repository.adjust(
                    request.operation_id, request.operator_id, request.user_id,
                    request.expected_stone, request.requested_delta,
                    target_name=request.target_name,
                )
                status = str(getattr(raw, "status", None) or (raw.get("status") if isinstance(raw, dict) else "failed"))
                values = {
                    key: getattr(raw, key, None) if not isinstance(raw, dict) else raw.get(key)
                    for key in ("previous_stone", "final_stone", "applied_delta")
                }
                result = StoneAdjustmentResult(
                    status, request.operation_id, request.user_id,
                    int(values["previous_stone"] or 0), int(values["final_stone"] or 0), int(values["applied_delta"] or 0),
                )
                data = result.to_dict()
                if status in {"adjusted", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=data,
                        before={"stone": result.previous_stone},
                        after={"stone": result.final_stone},
                        granted={"stone": result.applied_delta},
                        audit_category="admin_asset",
                    )
                else:
                    messages = {
                        "state_changed": "调整未结算：玩家灵石刚被其他操作改动，请重新执行。",
                        "user_missing": "该玩家已不存在。",
                        "operation_conflict": "本次管理员操作与已记录事件冲突。",
                        "not_ready": "管理员资产服务尚未就绪。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id, self.action, messages.get(status, "灵石调整未结算。"),
                        code=status, data=data, audit_category="admin_asset",
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, request.operation_id, self.action, payload, str(exc))
                raise

    def adjust_impart_stone(
        self, *, operation_id: str, operator_id: str, user_id: str,
        expected_stone: int | None, requested_delta: int, target_name: str = "",
        impart_database: str | Path,
    ):
        raw = AdminImpartStoneSqlRepository(self.database, impart_database).adjust(operation_id, operator_id, user_id, int(expected_stone or 0), requested_delta, target_name=target_name)
        data = asdict(raw) if is_dataclass(raw) else dict(vars(raw))
        return type("AdminImpartStoneOutcome", (), {"data": data, "status": raw.status, "ok": raw.succeeded})()

    def adjust_accessory(
        self, *, operation_id: str, operator_id: str, user_id: str, action: str,
        item_id: int, item_name: str, quantity: int, target_name: str = "",
        player_database: str | Path,
    ):
        from ...xiuxian.xiuxian_admin.transaction_service import AdminAccessoryAdjustmentService
        service = AdminAccessoryAdjustmentService(self.database, player_database)
        equipped, bag = service.snapshot(user_id)
        if action == "grant":
            raw = service.grant(operation_id, operator_id, user_id, item_id, item_name, quantity, equipped, bag, target_name=target_name)
        else:
            raw = service.destroy(operation_id, operator_id, user_id, item_id, item_name, quantity, equipped, bag, target_name=target_name)
        data = asdict(raw) if is_dataclass(raw) else dict(vars(raw))
        return type("AdminAccessoryOutcome", (), {"data": data, "status": raw.status, "ok": raw.succeeded})()

    def grant_item(
        self,
        *,
        operation_id: str,
        operator_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        expected_quantity: int,
        max_goods_num: int,
        target_name: str = "",
    ) -> OperationOutcome[dict[str, Any]]:
        action = "admin.item_grant"
        try:
            request = ItemGrantRequest(
                str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip(),
                int(item_id), str(item_name), str(item_type), int(quantity),
                int(expected_quantity), int(max_goods_num), str(target_name),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.item_repository or AdminItemSqlRepository(self.database)
                raw = repository.grant(
                    request.operation_id, request.operator_id, request.user_id, request.item_id,
                    request.item_name, request.item_type, request.quantity,
                    request.expected_quantity, request.max_goods_num, target_name=request.target_name,
                )
                status = str(getattr(raw, "status", None) or (raw.get("status") if isinstance(raw, dict) else "failed"))
                values = {
                    key: getattr(raw, key, None) if not isinstance(raw, dict) else raw.get(key)
                    for key in ("previous_quantity", "final_quantity", "granted_quantity")
                }
                result = ItemGrantResult(
                    status, request.operation_id, request.user_id, request.item_id,
                    int(values["previous_quantity"] or 0), int(values["final_quantity"] or 0), int(values["granted_quantity"] or 0),
                )
                data = result.to_dict()
                if status in {"granted", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id, action, data=data,
                        before={"item_quantity": result.previous_quantity},
                        after={"item_quantity": result.final_quantity},
                        granted={"item_id": request.item_id, "quantity": result.granted_quantity},
                        audit_category="admin_asset",
                    )
                else:
                    messages = {
                        "inventory_full": "玩家背包已达到容量上限。",
                        "state_changed": "玩家背包数量已更新，请重新执行。",
                        "user_missing": "该玩家已不存在。",
                        "operation_conflict": "本次管理员操作与已记录事件冲突。",
                        "not_ready": "管理员资产服务尚未就绪。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id, action, messages.get(status, "物品发放未结算。"),
                        code=status, data=data, audit_category="admin_asset",
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, request.operation_id, action, payload, str(exc))
                raise

    def destroy_item(self, *, operation_id: str, operator_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, expected_quantity: int, target_name: str = ""):
        raw = AdminItemDestroySqlRepository(self.database).destroy(operation_id, operator_id, user_id, item_id, item_name, item_type, quantity, expected_quantity, target_name=target_name)
        data = asdict(raw)
        return type("AdminItemDestroyOutcome", (), {"data": data, "status": raw.status, "ok": raw.succeeded})()

    def adjust_exp(self, *, operation_id: str, operator_id: str, user_id: str, expected_exp: int, requested_delta: int, target_name: str = ""):
        raw = AdminExpAdjustmentSqlRepository(self.database).adjust(operation_id, operator_id, user_id, expected_exp, requested_delta, target_name=target_name)
        data = asdict(raw)
        return type("AdminExpAdjustmentOutcome", (), {"data": data, "status": raw.status, "ok": raw.succeeded})()

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.adjust_stone(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["AdminAssetApplication"]
