from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.observability import trace_context
from .._migrated_application import MigratedFeatureApplication
from .claim_domain import TaskClaimResult
from .claim_repository import (
    TaskClaimGameRepository,
    TaskClaimPlayerRepository,
    normalize_claim_tasks,
)
from .repository import TasksRepository
from .progress import TaskProgressEventResult, TasksProgressRepository


class TasksApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: TasksRepository | None = None) -> None:
        super().__init__(database, feature="tasks", repository=repository or TasksRepository(database))


class TaskProgressApplication:
    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: TasksProgressRepository | None = None,
    ) -> None:
        self.repository = repository or TasksProgressRepository(player_database)

    def record(
        self,
        operation_id: str,
        user_id: str,
        events: Iterable[tuple[str, int]],
        periods: Mapping[str, str],
        tasks: Iterable[Mapping[str, Any]],
    ) -> TaskProgressEventResult:
        return self.repository.record(operation_id, user_id, events, periods, tasks)

    def get_states(
        self, user_id: str, periods: Mapping[str, str]
    ) -> dict[str, tuple[dict[str, int], list[str], str]]:
        return self.repository.get_states(user_id, periods)


class TaskClaimApplication:
    action = "tasks.claim_rewards"

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.game = TaskClaimGameRepository()
        self.player = TaskClaimPlayerRepository()
        self.ledger = OperationLedger(clock=self.clock)
        self.outbox = OutboxStore(clock=self.clock)

    @staticmethod
    def _encode(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _result_from_outcome(outcome: OperationOutcome[Any]) -> TaskClaimResult:
        data = outcome.data if isinstance(outcome.data, Mapping) else {}
        status = str(data.get("status") or outcome.code or "duplicate")
        if status == "applied":
            status = "duplicate" if outcome.replayed else "applied"
        tasks = data.get("tasks", ())
        return TaskClaimResult(status, tuple(dict(task) for task in tasks), outcome.message)

    @staticmethod
    def _identity(operation_id: str, cycle: str | None, user_id: str) -> dict[str, Any]:
        return {"cycle": cycle, "user_id": user_id}

    def _begin(
        self,
        operation_id: str,
        identity: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any] | None, TaskClaimResult | None]:
        payload = self._encode(identity)
        request_json = self._encode(context)
        now = self.clock.now().isoformat()
        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                previous = self.ledger.begin(uow, operation_id, self.action, identity)
                if previous is not None and previous.outcome() is not None:
                    return None, self._result_from_outcome(previous.outcome())
                operation = self.game.get(uow, operation_id)
                if operation is None:
                    self.game.create(
                        uow,
                        operation_id=operation_id,
                        payload=payload,
                        request_json=request_json,
                        now=now,
                    )
                    saved_context = dict(context)
                else:
                    stored_payload = str(operation["payload"])
                    legacy_payload = [
                        str(context["user_id"]),
                        list(context["cycles"]),
                    ]
                    try:
                        decoded_payload = json.loads(stored_payload)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        decoded_payload = None
                    if stored_payload != payload and decoded_payload != legacy_payload:
                        return None, TaskClaimResult("operation_conflict")
                    saved_context = json.loads(str(operation["request_json"] or "{}"))
                    if not saved_context:
                        saved_context = dict(context)
                        uow.execute(
                            "UPDATE task_reward_claim_operations SET request_json=? "
                            "WHERE operation_id=?",
                            (request_json, operation_id),
                        )
                self.outbox.append(
                    uow,
                    event_id=f"{operation_id}:{self.action}",
                    aggregate_type="task_reward_claim",
                    aggregate_id=operation_id,
                    event_type=self.action,
                    payload=dict(identity),
                )
                return saved_context, None
        except OperationConflictError:
            return None, TaskClaimResult("operation_conflict")

    def claim_rewards(
        self,
        *,
        operation_id: str,
        user_id: str,
        cycle: str | None,
        periods: Mapping[str, str],
        tasks: Iterable[Mapping[str, Any]],
        max_goods_num: int,
    ) -> TaskClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        cycles = tuple(
            current for current in ("daily", "weekly")
            if cycle is None or current == cycle
        )
        if not cycles or cycle not in {None, "daily", "weekly"}:
            raise ValueError("a valid task cycle is required")
        normalized_periods = {current: str(periods.get(current, "")).strip() for current in cycles}
        if any(not value for value in normalized_periods.values()):
            raise ValueError("current task periods are required")
        max_goods_num = int(max_goods_num)
        if max_goods_num <= 0:
            raise ValueError("max_goods_num must be positive")
        normalized_tasks = normalize_claim_tasks(tasks, cycles)
        identity = self._identity(operation_id, cycle, user_id)
        context = {
            "user_id": user_id,
            "cycle": cycle,
            "cycles": list(cycles),
            "periods": normalized_periods,
            "tasks": list(normalized_tasks),
            "max_goods_num": max_goods_num,
        }
        with trace_context(operation_id=operation_id, user_scope=user_id):
            saved_context, previous = self._begin(operation_id, identity, context)
            if previous is not None:
                return previous
            return self._resume(operation_id, saved_context, finish_ledger=True)

    def _resume(
        self,
        operation_id: str,
        context: Mapping[str, Any] | None,
        *,
        finish_ledger: bool,
    ) -> TaskClaimResult:
        now = self.clock.now().isoformat()
        with DatabaseUnitOfWork(self.game_database) as uow:
            operation = self.game.get(uow, operation_id)
            if operation is None:
                raise RuntimeError("task reward operation is missing")
            status = str(operation["status"])
            if status == "applied":
                result = TaskClaimResult("applied", tuple(json.loads(str(operation["result_json"]))))
            elif status == "rejected":
                result = TaskClaimResult(str(operation["result_status"]), ())
            elif status == "granted":
                result = TaskClaimResult("granted", tuple(json.loads(str(operation["result_json"]))))
            else:
                stored = json.loads(str(operation["request_json"] or "{}"))
                context = dict(context or stored)
                result = TaskClaimResult("started")

        if result.status == "started":
            with DatabaseUnitOfWork(self.game_database) as uow:
                user = uow.query_one(
                    "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?",
                    (str(context["user_id"]),),
                )
            if user is None:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.game.reject(uow, operation_id, "user_missing", now)
                result = TaskClaimResult("user_missing")
            else:
                identity = self._identity(
                    operation_id, context.get("cycle"), str(context["user_id"])
                )
                with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                    prepared_result = self.player.prepare(
                        uow,
                        operation_id=operation_id,
                        payload=self._encode(identity),
                        context=context,
                    )
                if prepared_result.status in {"operation_conflict", "claim_in_progress"}:
                    with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                        self.game.reject(uow, operation_id, prepared_result.status, now)
                    result = prepared_result
                else:
                    prepared = self._prepared_context(context, prepared_result.tasks)
                    with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                        self.game.save_prepared(uow, operation_id, prepared, now)
                        result = self.game.grant(uow, operation_id, now)

        if result.status == "granted":
            with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                result = self.player.finalize(uow, operation_id)
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                self.game.finalize(uow, operation_id, self.clock.now().isoformat())
            result = TaskClaimResult("applied", result.tasks)
        elif result.status not in {"applied", "duplicate"}:
            with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                if self.player.has_operation(uow, operation_id):
                    self.player.reject(uow, operation_id, result.status)

        outcome = self._outcome(operation_id, result)
        if finish_ledger:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                self.ledger.finish(uow, outcome)
                self.outbox.mark_sent(uow, f"{operation_id}:{self.action}")
        return result

    @staticmethod
    def _prepared_context(context: Mapping[str, Any], tasks: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        eligible = [dict(task) for task in tasks]
        totals: dict[int, dict[str, Any]] = {}
        for task in eligible:
            for item in task.get("items", ()):
                item_id = int(item["id"])
                total = totals.get(item_id)
                if total is None:
                    total = dict(item)
                    total["bound_amount"] = int(item["amount"]) if int(item["bind_flag"]) else 0
                    totals[item_id] = total
                else:
                    if (total["name"], total["type"]) != (item["name"], item["type"]):
                        raise ValueError("conflicting task item metadata")
                    total["amount"] += int(item["amount"])
                    if int(item["bind_flag"]):
                        total["bound_amount"] += int(item["amount"])
        return {
            "user_id": str(context["user_id"]),
            "cycles": list(context["cycles"]),
            "periods": dict(context["periods"]),
            "tasks": eligible,
            "totals": list(totals.values()),
            "max_goods_num": int(context["max_goods_num"]),
        }

    @staticmethod
    def _outcome(operation_id: str, result: TaskClaimResult) -> OperationOutcome[Any]:
        data = {"status": result.status, "tasks": list(result.tasks)}
        if result.status == "applied":
            return OperationOutcome.applied(
                operation_id,
                TaskClaimApplication.action,
                data=data,
                granted={
                    "items": [item for task in result.tasks for item in task.get("items", ())]
                },
                audit_category="task_reward_claim",
            )
        messages = {
            "inventory_full": "背包容量不足，任务奖励尚未领取。",
            "user_missing": "未找到角色信息，无法领取任务奖励。",
            "claim_in_progress": "任务奖励正在处理中，请稍后再试。",
        }
        return OperationOutcome.rejected(
            operation_id,
            TaskClaimApplication.action,
            messages.get(result.status, "任务奖励领取失败，请稍后再试。"),
            code=result.status,
            data=data,
            audit_category="task_reward_claim",
        )

    def reconcile(self, record: Mapping[str, Any]) -> OperationOutcome[Any]:
        operation_id = str(record.get("operation_id", "")).strip()
        if str(record.get("action", "")) != self.action or not operation_id:
            raise ValueError("invalid task reward reconcile record")
        with DatabaseUnitOfWork(self.game_database) as uow:
            operation = self.game.get(uow, operation_id)
            context = json.loads(str(operation["request_json"] or "{}")) if operation else None
        result = self._resume(operation_id, context, finish_ledger=False)
        return self._outcome(operation_id, result)


__all__ = ["TasksApplication", "TaskProgressApplication", "TaskClaimApplication"]
