from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.observability import trace_context
from .._migrated_application import MigratedFeatureApplication
from .bet_repository import DufangBetResult
from .repository import DufangRepository
from .player_stats_repository import DufangPlayerStatsSnapshot


class DufangApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        player_database: str | Path | None = None,
        *,
        repository: DufangRepository | None = None,
    ) -> None:
        super().__init__(
            database,
            feature="dufang",
            repository=repository or DufangRepository(database, player_database),
        )

    def bet(self, *, operation_id: str, user_id: str, **kwargs):
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        if not self.repository.player_projection_ready():
            return OperationOutcome.rejected(
                operation_id,
                f"{self.feature}.bet",
                "鉴石统计数据迁移尚未就绪。",
                code="schema_missing",
                data={"status": "schema_missing"},
                audit_category=self.feature,
            )
        outcome = self._execute_recoverable(
            operation_id=operation_id,
            user_id=user_id,
            action="bet",
            payload={"cost": kwargs["cost"], "placed_at": kwargs["placed_at"], "resolution": kwargs["resolution"]},
            ledger_payload={"cost": kwargs["cost"]},
        )
        if outcome.ok:
            self.repository.reconcile_player_outbox(
                limit=25, priority_event_id=f"{operation_id}:bet"
            )
        return outcome

    def payout(self, *, operation_id: str, user_id: str, **kwargs):
        return self._payout(
            operation_id=operation_id,
            user_id=user_id,
            bet_id=kwargs["bet_id"],
            settled_at=kwargs["settled_at"],
            project=True,
        )

    def _payout(self, *, operation_id: str, user_id: str, bet_id: str, settled_at: str, project: bool):
        outcome = self._execute_recoverable(
            operation_id=operation_id,
            user_id=user_id,
            action="payout",
            payload={"bet_id": bet_id, "settled_at": settled_at},
            ledger_payload={"bet_id": bet_id},
        )
        if project and outcome.ok:
            self.repository.reconcile_player_outbox(
                limit=25, priority_event_id=f"{operation_id}:payout"
            )
        return outcome

    def resolution(self, operation_id: str):
        return self.repository.bet_resolution(operation_id)

    def plan_for_bet(
        self, operation_id: str, *, draw: Callable[[], Mapping[str, Any]]
    ) -> DufangBetResult:
        stored = self.resolution(operation_id)
        if stored.status != "not_found":
            return stored
        if not self.repository.bet_schema_ready() or not self.repository.player_projection_ready():
            return DufangBetResult("schema_missing")
        return DufangBetResult("new", resolution=dict(draw()))

    def player_total_cost(self, user_id: str) -> int:
        return self.repository.player_total_cost(user_id)

    def player_stats_snapshot(self, user_id: str) -> DufangPlayerStatsSnapshot:
        return self.repository.player_stats_snapshot(str(user_id))

    def reconcile_pending(self, *, limit: int = 5, settled_at: str = "") -> Mapping[str, int]:
        limit = max(1, min(int(limit), 5))
        settled = failed = 0
        for record in self.repository.pending_bets(limit=limit):
            bet_id = str(record["operation_id"])
            user_id = str(record["user_id"])
            try:
                payout = self._payout(
                    operation_id=f"dufang-payout:{bet_id}",
                    user_id=user_id,
                    bet_id=bet_id,
                    settled_at=str(settled_at),
                    project=False,
                )
                if payout.ok:
                    settled += 1
                    plan_result = self.resolution(bet_id)
                    plan = dict(plan_result.resolution or {})
                    sharing = plan.get("sharing")
                    if isinstance(sharing, Mapping) and sharing.get("recipients"):
                        self._execute_share(
                            f"dufang-share:{bet_id}",
                            user_id,
                            "share_settle",
                            {
                                "event_type": sharing["event_type"],
                                "title": sharing["title"],
                                "desc": sharing["desc"],
                                "effect_amount": sharing["effect_amount"],
                                "cost_bonus_percent": sharing["bonus_percent"],
                                "recipients": sharing["recipients"],
                                "settled_at": str(settled_at),
                            },
                        )
                else:
                    failed += 1
            except Exception:
                failed += 1
        projection = self.repository.reconcile_player_outbox(limit=limit)
        pending_bets = self.repository.pending_bet_count()
        return {
            "settled": settled,
            "failed": failed,
            "projected": projection.applied,
            "pending": pending_bets + projection.pending,
        }

    def _execute_recoverable(
        self,
        *,
        operation_id: str,
        user_id: str,
        action: str,
        payload: Mapping[str, Any],
        ledger_payload: Mapping[str, Any],
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        request = {"action": action, **dict(payload), "user_id": user_id}
        ledger_request = {**dict(ledger_payload), "user_id": user_id}
        ledger_action = f"{self.feature}.{action}"
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, ledger_action, ledger_request)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        if existing.status != "started":
                            raise ConflictError("操作正在处理中")
                raw = self.repository.execute(operation_id, user_id, action, request)
                if is_dataclass(raw):
                    raw = asdict(raw)
                elif isinstance(raw, Mapping):
                    raw = dict(raw)
                elif raw is not None:
                    raw = dict(vars(raw))
                else:
                    raw = {"status": "applied"}
                status = str(raw.get("status", "failed")).casefold()
                raw.setdefault("status", status)
                if status in self.success_statuses:
                    outcome = OperationOutcome.applied(
                        operation_id, ledger_action, data=raw, audit_category=self.feature
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        ledger_action,
                        str(raw.get("message") or f"{self.feature} 操作未完成。"),
                        code=status or "rejected",
                        data=raw,
                        audit_category=self.feature,
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(
                    self.database, operation_id, ledger_action, ledger_request, str(exc)
                )
                raise

    def payout_result(self, operation_id: str):
        return self.repository.payout_result(operation_id)

    def share_settle(self, *, operation_id: str, user_id: str, **kwargs):
        return self._execute_share(operation_id, user_id, "share_settle", kwargs)

    def resume_share(self, *, operation_id: str, user_id: str, settled_at: str = ""):
        return self._execute_share(
            operation_id, user_id, "share_resume", {"settled_at": str(settled_at)}
        )

    def share_exists(self, operation_id: str) -> bool:
        return bool(self.repository.share_exists(operation_id))

    def _execute_share(self, operation_id: str, user_id: str, action: str, payload: dict):
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        ledger_action = f"{self.feature}.share_settle"
        identity = {"user_id": user_id}
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, ledger_action, identity)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                raw = self.repository.execute(operation_id, user_id, action, payload)
                if is_dataclass(raw):
                    raw = asdict(raw)
                elif isinstance(raw, Mapping):
                    raw = dict(raw)
                elif raw is not None:
                    raw = dict(vars(raw))
                else:
                    raw = {"status": "applied"}
                status = str(raw.get("status", "failed")).casefold()
                raw.setdefault("status", status)
                if status in self.success_statuses:
                    outcome = OperationOutcome.applied(
                        operation_id, ledger_action, data=raw, audit_category=self.feature
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        ledger_action,
                        str(raw.get("message") or f"{self.feature} 操作未完成。"),
                        code=status or "rejected",
                        data=raw,
                        audit_category=self.feature,
                    )
                if str(raw.get("task_status", "")).casefold() != "running":
                    with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                        self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, ledger_action, identity, str(exc))
                raise


__all__ = ["DufangApplication"]
