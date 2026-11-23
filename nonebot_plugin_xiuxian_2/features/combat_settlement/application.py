from __future__ import annotations

from ...core.player_reasons import reason_text

from pathlib import Path
from typing import Any, Mapping, Sequence

from ...core.errors import ConflictError, DomainError, OperationConflictError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.database.ledger import request_hash
from ...infrastructure.observability import trace_context
from .domain import CombatSettlementRequest
from .repository import CombatSettlementRepository, CombatSettlementSqlRepository, DaoBattleSqlRepository, DaoBattleWriteRejected
from .schemas import CombatSettlementResult


class CombatSettlementApplication:
    action = "combat.settle"
    dao_action = "combat.dao_battle"

    def __init__(self, game_database: str | Path, player_database: str | Path, *,
                 repository: CombatSettlementRepository | None = None,
                 ledger: OperationLedger | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_daily: Mapping[str, Any],
        snapshot: str,
        daily_limit: int,
        stone: int,
        items: Sequence[Mapping[str, Any]],
        max_goods_num: int,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = CombatSettlementRequest.build(
                operation_id=operation_id,
                user_id=user_id,
                expected_daily=expected_daily,
                snapshot=snapshot,
                daily_limit=daily_limit,
                stone=stone,
                items=items,
                max_goods_num=max_goods_num,
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            reservation = None
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    reservation = self.ledger.get(uow, request.operation_id, self.action)
                repository = self.repository or CombatSettlementSqlRepository(self.game_database, self.player_database)
                raw = repository.settle(
                    request.operation_id,
                    request.user_id,
                    dict(request.expected_daily),
                    request.snapshot,
                    request.daily_limit,
                    request.stone,
                    tuple(dict(item) for item in request.items),
                    request.max_goods_num,
                )
                status = str(getattr(raw, "status", None) or (raw.get("status") if isinstance(raw, dict) else "failed"))
                raw_stone = getattr(raw, "stone", None)
                if raw_stone is None and isinstance(raw, dict):
                    raw_stone = raw.get("stone", 0)
                raw_rewards = getattr(raw, "rewards", None)
                if raw_rewards is None and isinstance(raw, dict):
                    raw_rewards = raw.get("rewards", ())
                rewards = tuple((int(item[0]), int(item[1])) for item in (raw_rewards or ()))
                data = CombatSettlementResult(status, request.operation_id, request.user_id, int(raw_stone or 0), rewards).to_dict()
                diagnostics = raw if isinstance(raw, dict) else vars(raw)
                data.update({key: diagnostics[key] for key in ("reason", "subject") if diagnostics.get(key)})
                retryable_reasons = {
                    "map_combat_settlement_daily_write_rejected",
                    "map_combat_settlement_daily_projection_mismatch",
                    "map_combat_settlement_snapshot_write_rejected",
                    "map_combat_settlement_snapshot_projection_mismatch",
                    "map_combat_settlement_stone_write_rejected",
                    "map_combat_settlement_stone_projection_mismatch",
                    "map_combat_settlement_inventory_write_rejected",
                    "map_combat_settlement_inventory_projection_mismatch",
                    "map_combat_settlement_receipt_write_rejected",
                    "map_combat_settlement_receipt_projection_mismatch",
                }
                if data.get("reason") in retryable_reasons or status == "inventory_full":
                    messages = {
                        "inventory_full": "背包物品已达上限，节点战斗结算尚未领取。",
                    }
                    message = reason_text(data.get("reason")) or messages.get(status, "节点战斗未结算。")
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        message,
                        code=str(data.get("reason") or status),
                        data=data,
                        audit_category="combat_settlement",
                    )
                    if reservation is not None:
                        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                            self.ledger.release_unsettled(uow, reservation)
                    return outcome
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=data,
                        granted={"stone": int(raw_stone or 0), "items": dict(rewards)},
                        audit_category="combat_settlement",
                    )
                else:
                    messages = {
                        "inventory_full": "背包物品已达上限，节点战斗结算尚未领取。",
                        "limit_reached": "今日节点战斗次数已达上限。",
                        "state_changed": "战斗当前状态已更新，请重新执行节点战斗。",
                        "user_missing": "未找到修仙数据，节点战斗未结算。",
                        "not_ready": "战斗结算服务尚未就绪。",
                    }
                    if data.get("reason") == "position_changed" and data.get("subject") == "target":
                        message = "对方已离开当前节点，论道未结算。"
                    else:
                        message = reason_text(data.get("reason")) or messages.get(status, "节点战斗未结算。")
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        message,
                        code=status,
                        data=data,
                        audit_category="combat_settlement",
                    )
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, request.operation_id, self.action, payload, str(exc))
                raise

    @staticmethod
    def _dao_command_payload(challenger_id: str, requested_target: str) -> dict[str, str]:
        return {"challenger_id": str(challenger_id), "requested_target": requested_target}

    def _dao_rejection(self, operation_id: str, data: dict[str, Any]) -> OperationOutcome[dict[str, Any]]:
        status, reason = data["status"], data["reason"]
        message = reason_text(reason)
        if reason == "position_changed" and data["subject"] == "target":
            message = "对方已离开当前节点，论道未结算。"
        return OperationOutcome.rejected(
            operation_id, self.dao_action, message, code="operation_conflict" if reason == "operation_conflict" else status,
            data=data, audit_category="combat_settlement",
        )

    def _dao_conflict(self, operation_id: str) -> OperationOutcome[dict[str, Any]]:
        return self._dao_rejection(
            operation_id, {"status": "state_changed", "reason": "operation_conflict", "subject": "actor"},
        )

    def replay_dao_battle(self, *, operation_id: str, challenger_id: str, requested_target: str) -> OperationOutcome[dict[str, Any]] | None:
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            existing = self.ledger.get(uow, operation_id, self.dao_action)
            if existing is not None:
                if existing.request_hash != request_hash(self._dao_command_payload(challenger_id, requested_target)):
                    return self._dao_conflict(operation_id)
                if existing.status in {"failed", "needs_reconcile"}:
                    return None
                result = existing.outcome()
                if result is not None:
                    return result.replay()
                raise ConflictError("操作正在处理中")
            # Old message IDs ended in :<target>. Their receipts do not distinguish
            # a named command from a random one; never use them to roll again.
            old = uow.query_one(
                "SELECT 1 FROM map_dao_battle_operations WHERE operation_id>=? AND operation_id<? LIMIT 1",
                (operation_id + ":", operation_id + ";"),
            )
            if old is not None:
                return OperationOutcome.applied(
                    operation_id, self.dao_action, data={"status": "duplicate"},
                    message="该条论道消息已结算，可用「论道查看」查看战绩。", audit_category="combat_settlement",
                ).replay()
        return None

    def settle_dao_battle(
        self, *, operation_id: str, challenger_id: str, target_id: str,
        expected_position: Mapping[str, Any], challenger_won: bool,
        requested_target: str | None = None, success_message: str = "",
    ) -> OperationOutcome[dict[str, Any]]:
        if not operation_id or not challenger_id or challenger_id == target_id:
            raise ValidationError("valid operation and distinct players are required")
        operation_id, challenger_id, target_id = str(operation_id), str(challenger_id), str(target_id)
        payload = (
            self._dao_command_payload(challenger_id, requested_target)
            if requested_target is not None else {
                "challenger_id": challenger_id, "target_id": target_id,
                "expected_position": dict(expected_position), "challenger_won": bool(challenger_won),
            }
        )
        with trace_context(operation_id=str(operation_id), user_scope=str(challenger_id)):
            try:
                with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                    try:
                        existing = self.ledger.begin(uow, operation_id, self.dao_action, payload)
                    except OperationConflictError:
                        return self._dao_conflict(operation_id)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    reservation = self.ledger.get(uow, operation_id, self.dao_action)
                    if reservation is None or reservation.status != "started" or reservation.request_hash != request_hash(payload) or reservation.result_json is not None:
                        raise RuntimeError("dao battle ledger reservation rejected")
                    data = DaoBattleSqlRepository(self.player_database, self.game_database).settle_in(
                        uow, operation_id, challenger_id, target_id, dict(expected_position), bool(challenger_won),
                    )
                    if data["status"] in {"applied", "duplicate"}:
                        outcome = OperationOutcome.applied(
                            operation_id, self.dao_action, data=data, message=success_message, audit_category="combat_settlement",
                        )
                    else:
                        outcome = self._dao_rejection(operation_id, data)
                    self.ledger.finish(uow, outcome)
                return outcome
            except DaoBattleWriteRejected as exc:
                with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                    reservation = self.ledger.get(uow, operation_id, self.dao_action)
                    if reservation is not None and reservation.status == "started":
                        self.ledger.release_unsettled(uow, reservation)
                return self._dao_rejection(operation_id, exc.result())
            except DomainError:
                raise
            except Exception as exc:
                try:
                    self.ledger.record_failure(self.player_database, operation_id, self.dao_action, payload, str(exc))
                except Exception as audit_error:
                    raise exc from audit_error
                raise

    def get_dao_record(self, user_id: str) -> dict[str, int]:
        repository: Any = self.repository if self.repository is not None and hasattr(self.repository, "get_record") else DaoBattleSqlRepository(self.player_database, self.game_database)
        return dict(repository.get_record(str(user_id)))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.settle(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["CombatSettlementApplication"]
