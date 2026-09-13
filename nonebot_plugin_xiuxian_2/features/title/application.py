from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, OperationConflictError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import TitleTransactionResult
from .repository import TitleRepository


class TitleApplication:
    """Idempotent title use cases backed by the player database."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: TitleRepository | None = None,
        ledger: OperationLedger | None = None,
        clock: Any | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or TitleRepository()
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()

    def get_result(self, operation_id: str) -> TitleTransactionResult | None:
        with DatabaseUnitOfWork(self.database) as uow:
            return self.repository.get_result(uow, operation_id)

    def execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        request = dict(payload or {})
        action = str(request.pop("action", request.pop("operation", "")) or "").strip().casefold()
        action = action or "unsupported"
        request["user_id"] = user_id
        ledger_action = f"title.{action}"
        ledger_request = self._ledger_payload(action, request)
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, ledger_action, ledger_request)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    result = self._dispatch(uow, action, operation_id, user_id, request)
                    data = result.to_dict()
                    if result.status in {"applied", "duplicate", "already_equipped"}:
                        outcome = OperationOutcome.applied(
                            operation_id,
                            ledger_action,
                            data=data,
                            audit_category="title",
                            clock=self.clock,
                        )
                    elif result.status == "unsupported":
                        outcome = OperationOutcome.rejected(
                            operation_id,
                            ledger_action,
                            f"unsupported title action: {action}",
                            code="unsupported",
                            data=data,
                            audit_category="title",
                            clock=self.clock,
                        )
                    else:
                        messages = {
                            "operation_conflict": "本次称号请求与已处理记录冲突，请重新操作。",
                            "state_changed": "称号操作未结算：称号当前状态已更新，请重新查看。",
                            "title_locked": "你还未解锁该称号！",
                            "already_unlocked": "用户已拥有该称号。",
                            "not_equipped": "你当前没有装备任何称号！",
                        }
                        outcome = OperationOutcome.rejected(
                            operation_id,
                            ledger_action,
                            messages.get(result.status, "称号操作未完成。"),
                            code=result.status,
                            data=data,
                            audit_category="title",
                            clock=self.clock,
                        )
                    self.ledger.finish(uow, outcome)
                    return outcome
            except OperationConflictError:
                return OperationOutcome.rejected(
                    operation_id,
                    ledger_action,
                    "本次称号请求与已处理记录冲突，请重新操作。",
                    code="operation_conflict",
                    data={"status": "operation_conflict"},
                    audit_category="title",
                    clock=self.clock,
                )
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, ledger_action, request, str(exc))
                raise

    def _dispatch(
        self,
        uow: DatabaseUnitOfWork,
        action: str,
        operation_id: str,
        user_id: str,
        request: Mapping[str, Any],
    ) -> TitleTransactionResult:
        if action == "equip":
            return self.repository.equip(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_unlocked=request.get("expected_unlocked", ()),
                expected_equipped=request.get("expected_equipped", ""),
                title_id=str(request.get("title_id", "")),
            )
        if action == "unequip":
            return self.repository.unequip(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_equipped=request.get("expected_equipped", ""),
            )
        if action == "grant":
            return self.repository.grant(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_unlocked=request.get("expected_unlocked", ()),
                title_id=str(request.get("title_id", "")),
            )
        if action == "unlock_batch":
            return self.repository.unlock_batch(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_unlocked=request.get("expected_unlocked", ()),
                title_ids=request.get("title_ids", ()),
            )
        return TitleTransactionResult("unsupported", action)

    @staticmethod
    def _ledger_payload(action: str, request: Mapping[str, Any]) -> dict[str, Any]:
        """Keep optimistic snapshots out of operation identity.

        A retry after a concurrent refresh must replay the original operation;
        the repository still validates the supplied snapshot and rejects stale
        state.  The requested title/action, rather than the mutable snapshot,
        is the idempotency key's payload.
        """
        payload: dict[str, Any] = {"user_id": str(request.get("user_id", "")), "action": action}
        if action in {"equip", "grant"}:
            payload["title_id"] = str(request.get("title_id", ""))
        elif action == "unlock_batch":
            payload["title_ids"] = sorted({str(item) for item in request.get("title_ids", ()) if str(item)})
        return payload

    def equip(self, *, operation_id: str, user_id: str, expected_unlocked: Any, expected_equipped: Any, title_id: str):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "equip", "expected_unlocked": expected_unlocked, "expected_equipped": expected_equipped, "title_id": title_id})

    def unequip(self, *, operation_id: str, user_id: str, expected_equipped: Any):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "unequip", "expected_equipped": expected_equipped})

    def grant(self, *, operation_id: str, user_id: str, expected_unlocked: Any, title_id: str):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "grant", "expected_unlocked": expected_unlocked, "title_id": title_id})

    def unlock_batch(self, *, operation_id: str, user_id: str, expected_unlocked: Any, title_ids: Any):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "unlock_batch", "expected_unlocked": expected_unlocked, "title_ids": title_ids})

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.execute(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["TitleApplication"]
