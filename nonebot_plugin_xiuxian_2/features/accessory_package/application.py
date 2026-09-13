from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.observability import trace_context
from .domain import AccessoryReward
from .domain import normalize_accessories
from ..package_reward.domain import PackageReward, normalize_rewards
from .repository import AccessoryPackageGameRepository, AccessoryPackagePlayerRepository
from .schemas import AccessoryPackageRequest


class AccessoryPackageApplication:
    action = "accessory_package.open"

    def __init__(self, game_database: str | Path, player_database: str | Path, *, ledger: OperationLedger | None = None, player_schema_policy: str = "create") -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.game = AccessoryPackageGameRepository()
        self.player = AccessoryPackagePlayerRepository(schema_policy=player_schema_policy)
        self.ledger = ledger or OperationLedger()
        self.outbox = OutboxStore()

    def open_package(
        self,
        *,
        operation_id: str,
        user_id: str,
        package_id: int,
        quantity: int,
        rewards: Iterable[PackageReward | tuple[Any, ...]],
        accessories: Iterable[Mapping[str, Any] | AccessoryReward],
        max_goods_num: int,
        accessory_limit: int,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = AccessoryPackageRequest.build(
                operation_id=operation_id,
                user_id=user_id,
                package_id=package_id,
                quantity=quantity,
                rewards=rewards,
                accessories=accessories,
                max_goods_num=max_goods_num,
                accessory_limit=accessory_limit,
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            stage_committed = False
            try:
                prepared = self._prepare(request, payload)
                if prepared is not None:
                    return prepared
                stage_committed = True
                try:
                    status = self._apply_player(request)
                except Exception as exc:
                    # The game-db phase is already committed.  Compensate it
                    # before exposing the infrastructure failure to callers.
                    return self._compensate(request, f"player_error: {exc}")
                if status != "applied":
                    return self._compensate(request, status)
                return self._finalize(request)
            except DomainError:
                raise
            except Exception as exc:
                if stage_committed:
                    self._record_reconcile_failure(request, payload, exc)
                else:
                    self._record_failure(request, payload, exc)
                raise

    def _prepare(self, request: AccessoryPackageRequest, payload: Mapping[str, Any]) -> OperationOutcome[Any] | None:
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
            if existing is not None:
                outcome = existing.outcome()
                if outcome is not None:
                    return outcome.replay()
                raise ConflictError("操作正在处理中")
            self.game.ensure_schema(uow)
            row = self.game.get(uow, request.operation_id)
            if row is not None and row["status"] == "pending_accessory":
                return None
            if row is not None and row["status"] == "legacy":
                # A historical attached-transaction row is authoritative for
                # the old operation ID; never grant it a second time.
                data = {
                    "user_id": str(row["user_id"]),
                    "package_id": int(row["package_id"]),
                    "quantity": int(row["quantity"]),
                    "accessories": json.loads(row["accessories_json"] or "[]"),
                    "legacy_replay": True,
                }
                outcome = OperationOutcome.applied(
                    request.operation_id,
                    self.action,
                    data=data,
                    audit_category="accessory_package_legacy_replay",
                )
                self.ledger.finish(uow, outcome)
                return outcome
            before = self.game.snapshot(uow, request)
            status, before = self.game.prepare(uow, request)
            if status != "applied":
                outcome = self._rejected(request, status, before)
                self.ledger.finish(uow, outcome)
                return outcome
            self.outbox.append(
                uow,
                event_id=f"{request.operation_id}:{self.action}",
                aggregate_type="accessory_package",
                aggregate_id=request.operation_id,
                event_type=self.action,
                payload=dict(payload),
            )
        return None

    def _apply_player(self, request: AccessoryPackageRequest) -> str:
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            return self.player.apply(
                uow,
                operation_id=request.operation_id,
                user_id=request.user_id,
                accessories=request.accessories,
                limit=request.accessory_limit,
            )

    def reconcile(self, record: Mapping[str, Any], *, operation_id: str | None = None) -> OperationOutcome[Any]:
        """Retry the player phase for a committed operation ledger row."""
        operation_id = str(operation_id or record.get("operation_id", ""))
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = self.game.get(uow, operation_id)
        if row is None:
            raise ValueError("accessory package operation is missing")
        request = AccessoryPackageRequest(
            operation_id=operation_id,
            user_id=str(row["user_id"]),
            package_id=int(row["package_id"]),
            quantity=int(row["quantity"]),
            rewards=normalize_rewards(PackageReward(**item) for item in json.loads(row["rewards_json"])),
            accessories=normalize_accessories(json.loads(row["accessories_json"])),
            max_goods_num=int(row.get("max_goods_num") or 1000000000),
            accessory_limit=int(row.get("accessory_limit") or 1000000000),
        )
        status = self._apply_player(request)
        if status != "applied":
            return self._compensate(request, status)
        return self._finalize(request)

    def _finalize(self, request: AccessoryPackageRequest) -> OperationOutcome[dict[str, Any]]:
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            row = self.game.get(uow, request.operation_id)
            before = json.loads(row["before_json"]) if row else {}
            self.game.finalize(uow, request.operation_id)
            after = self.game.snapshot(uow, request)
            data = {"user_id": request.user_id, "package_id": request.package_id, "quantity": request.quantity, "accessories": [item.to_dict() for item in request.accessories]}
            outcome = OperationOutcome.applied(
                request.operation_id,
                self.action,
                data=data,
                before=before,
                after=after,
                consumed={"package": request.quantity},
                granted={"accessories": data["accessories"]},
                audit_category="accessory_package",
            )
            self.ledger.finish(uow, outcome)
            self.outbox.mark_sent(uow, f"{request.operation_id}:{self.action}")
            return outcome

    def _compensate(self, request: AccessoryPackageRequest, status: str) -> OperationOutcome[Any]:
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            row = self.game.get(uow, request.operation_id)
            before = json.loads(row["before_json"]) if row else {}
            self.game.compensate(uow, request.operation_id, status)
            message = {"accessory_full": "饰品栏已满，礼包未结算。"}.get(status, "饰品礼包未结算。")
            if status == "accessory_full":
                outcome = OperationOutcome.rejected(
                    request.operation_id,
                    self.action,
                    message,
                    code=status,
                    data={"status": status, "compensated": True},
                    before=before,
                    after=before,
                    audit_category="accessory_package",
                )
            else:
                outcome = OperationOutcome.failed(
                    request.operation_id,
                    self.action,
                    message,
                    code="needs_reconcile",
                    data={"status": status, "compensated": True},
                    before=before,
                    after=before,
                    audit_category="accessory_package_compensation",
                )
            self.ledger.finish(uow, outcome)
            self.outbox.mark_sent(uow, f"{request.operation_id}:{self.action}")
            return outcome

    def _rejected(self, request: AccessoryPackageRequest, status: str, before: Mapping[str, Any]) -> OperationOutcome[Any]:
        messages = {
            "user_missing": "未找到修仙数据，礼包未结算。",
            "item_insufficient": "礼包数量不足。",
            "stone_insufficient": "灵石不足，礼包未结算。",
            "inventory_full": "背包已满，礼包未结算。",
            "state_changed": "礼包状态已变化，请刷新后重试。",
        }
        return OperationOutcome.rejected(request.operation_id, self.action, messages.get(status, "礼包未结算。"), code=status, before=before, after=before, audit_category="accessory_package")

    def _record_failure(self, request: AccessoryPackageRequest, payload: Mapping[str, Any], exc: Exception) -> None:
        try:
            self.ledger.record_failure(self.game_database, request.operation_id, self.action, payload, str(exc))
        except Exception:
            pass

    def _record_reconcile_failure(self, request: AccessoryPackageRequest, payload: Mapping[str, Any], exc: Exception) -> None:
        """Keep a committed game phase visible when the final phase fails."""
        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                self.ledger.ensure_schema(uow)
                outcome = OperationOutcome.failed(
                    request.operation_id,
                    self.action,
                    str(exc),
                    code="needs_reconcile",
                    data={"stage": "finalize"},
                    audit_category="cross_database_failure",
                )
                self.ledger.finish(uow, outcome)
        except Exception:
            self._record_failure(request, payload, exc)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.open_package(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["AccessoryPackageApplication"]
