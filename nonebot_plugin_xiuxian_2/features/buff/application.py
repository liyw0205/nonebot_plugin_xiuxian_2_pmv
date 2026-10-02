from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, ValidationError
from ...core.result import OperationOutcome
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from .._legacy_application import LegacyApplication
from .repository import BuffRepository, LegacyBuffRepository
from .rename_repository import BlessedSpotRenameSqlRepository
from .upgrade_repository import BlessedSpotUpgradeSqlRepository
from .training_start_repository import NormalTrainingStartSqlRepository
from .training_complete_repository import NormalTrainingCompleteSqlRepository
from .closing_repository import ClosingSettlementSqlRepository
from .stone_training_repository import StoneTrainingSqlRepository
from .pvp_repository import NormalPvpSqlRepository


class _TrainingSchemaMissing(RuntimeError):
    pass


class BuffApplication(LegacyApplication):
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: BuffRepository | None = None,
        closing_effects: Any | None = None,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.closing_effects = closing_effects
        self.outbox = OutboxStore(clock=self.clock)
        self.ledger = OperationLedger(clock=self.clock)
        self._explicit_repository = repository
        super().__init__(
            game_database,
            repository=repository or LegacyBuffRepository(game_database, player_database),
            feature="buff",
            ledger=self.ledger,
        )

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"buff.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def open(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("open", operation_id=operation_id, user_id=user_id, **kwargs)
    def upgrade_field(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            if "expected_level" not in kwargs or "stone_cost" not in kwargs:
                raise ValidationError("expected_level and stone_cost are required")
            repository = BlessedSpotUpgradeSqlRepository(self.game_database, self.player_database)
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.upgrade_field", payload={"user_id": user_id, **kwargs}, call=lambda: repository.upgrade(operation_id, user_id, kwargs["expected_level"], kwargs["stone_cost"], kwargs.get("max_level", 10)))
        return self._action("upgrade_field", operation_id=operation_id, user_id=user_id, **kwargs)
    def rename(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            if "expected_name" not in kwargs or "new_name" not in kwargs:
                raise ValidationError("expected_name and new_name are required")
            repository = BlessedSpotRenameSqlRepository(self.game_database)
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.rename", payload={"user_id": user_id, **kwargs}, call=lambda: repository.rename(operation_id, user_id, kwargs["expected_name"], kwargs["new_name"]))
        return self._action("rename", operation_id=operation_id, user_id=user_id, **kwargs)
    def training_start(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            payload = {"user_id": user_id, **kwargs}
            return self._training_lifecycle_execute(
                operation_id, user_id, "buff.training_start", payload,
                lambda uow: NormalTrainingStartSqlRepository(self.game_database).start_in_uow(
                    uow, operation_id, user_id, kwargs["kind"], kwargs["expected_exp"],
                    kwargs["expected_stone"], kwargs["reward"], kwargs["exp_cap"],
                    kwargs["power_multiplier"], kwargs.get("duration_seconds", 60),
                    kwargs.get("now") or self.clock.now(),
                ),
                (self.game_database,),
            )
        return self._action("training_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def training_complete(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            payload = {"user_id": user_id}
            return self._training_lifecycle_execute(
                operation_id, user_id, "buff.training_complete", payload,
                lambda uow: NormalTrainingCompleteSqlRepository(
                    self.game_database, self.player_database
                ).complete_in_uow(
                    uow, operation_id, kwargs["task_period"], user_id
                ),
                (self.game_database, self.player_database),
            )
        return self._action("training_complete", operation_id=operation_id, user_id=user_id, **kwargs)

    @staticmethod
    def _training_result_data(result: Any) -> dict[str, Any]:
        return {
            name: getattr(result, name)
            for name in (
                "status", "kind", "create_time", "scheduled_time",
                "exp_gain", "stone_gain", "hp_gain", "mp_gain",
            )
            if hasattr(result, name)
        }

    def _training_lifecycle_execute(
        self,
        operation_id: str,
        user_id: str,
        action: str,
        payload: dict[str, Any],
        call: Any,
        required_databases: tuple[str, ...],
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        if any(not Path(database).is_file() for database in required_databases):
            return OperationOutcome.rejected(
                operation_id, action, "修炼数据结构尚未就绪。",
                code="schema_missing", data={"status": "schema_missing"},
                audit_category="buff", clock=self.clock,
            )
        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                existing = self.ledger.begin(uow, operation_id, action, payload)
                if existing is not None:
                    previous = existing.outcome()
                    if previous is not None:
                        return previous.replay()
                    raise ConflictError("修炼操作正在处理中")
                result = call(uow)
                data = self._training_result_data(result)
                if str(data.get("status")) == "schema_missing":
                    raise _TrainingSchemaMissing("normal training schema is not ready")
                if str(data.get("status")) in {"started", "applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        operation_id, action, data=data, audit_category="buff",
                        clock=self.clock,
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id, action, "修炼操作未完成：状态或数据结构已更新。",
                        code=str(data.get("status") or "rejected"), data=data,
                        audit_category="buff", clock=self.clock,
                    )
                self.ledger.finish(uow, outcome)
                return outcome
        except _TrainingSchemaMissing:
            return OperationOutcome.rejected(
                operation_id, action, "修炼数据结构尚未就绪。",
                code="schema_missing", data={"status": "schema_missing"},
                audit_category="buff", clock=self.clock,
            )
        except Exception as exc:
            self.ledger.record_failure(
                self.game_database, operation_id, action, payload, str(exc)
            )
            raise
    def stone_training(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.stone_training", payload={"user_id": user_id, **kwargs}, call=lambda: StoneTrainingSqlRepository(self.game_database, self.player_database).settle(operation_id, user_id, requested_stone=kwargs["requested_stone"], expected_exp=kwargs["expected_exp"], expected_stone=kwargs["expected_stone"], exp_cap=kwargs["exp_cap"], power_multiplier=kwargs["power_multiplier"]))
        return self._action("stone_training", operation_id=operation_id, user_id=user_id, **kwargs)
    def closing_settle(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._closing_settle(operation_id=operation_id, user_id=user_id, **kwargs)
        return self._action("closing_settle", operation_id=operation_id, user_id=user_id, **kwargs)

    @staticmethod
    def _closing_data(result: Any) -> dict[str, Any]:
        return {
            "status": str(result.status),
            "exp_gain": int(result.exp_gain),
            "stone_cost": int(result.stone_cost),
            "hp": int(result.hp),
            "mp": int(result.mp),
            "atk": int(result.atk),
            "power": int(result.power),
            "exp_time": int(result.exp_time),
            "effects_event_id": result.effects_event_id,
            "occurred_at": result.occurred_at,
        }

    def _closing_settle(self, *, operation_id: str, user_id: str, **kwargs: Any):
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        action = "buff.closing_settle"
        payload = {"user_id": user_id, **kwargs}
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            existing = self.ledger.begin(uow, operation_id, action, payload)
            if existing is not None:
                previous = existing.outcome()
                if previous is not None:
                    replayed = previous.replay()
                    uow.commit()
                    return self._apply_closing_effects(replayed)

        result = ClosingSettlementSqlRepository(
            self.game_database, clock=self.clock, outbox=self.outbox
        ).settle(
            operation_id,
            user_id,
            kwargs["expected_create_time"],
            kwargs["exp_gain"],
            kwargs["stone_cost"],
            kwargs["new_hp"],
            kwargs["new_mp"],
            kwargs["new_atk"],
            kwargs["new_power"],
            kwargs.get("exp_time", 0),
        )
        data = self._closing_data(result)
        if result.status in {"applied", "duplicate"}:
            outcome = OperationOutcome.applied(
                operation_id,
                action,
                data=data,
                audit_category="buff",
                occurred_at=result.occurred_at or self.clock.now().isoformat(),
            )
        else:
            outcome = OperationOutcome.rejected(
                operation_id,
                action,
                "闭关操作未完成：闭关状态或资源已更新，请重新查看。",
                code=result.status,
                data=data,
                audit_category="buff",
                clock=self.clock,
            )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            self.ledger.finish(uow, outcome)
        return self._apply_closing_effects(outcome)

    def _apply_closing_effects(self, outcome: OperationOutcome[Any]):
        if not outcome.ok or not isinstance(outcome.data, dict):
            return outcome
        event_id = outcome.data.get("effects_event_id")
        if not event_id:
            # Historical receipts predate the outbox; their projection state is unknowable.
            return outcome
        try:
            with DatabaseUnitOfWork(self.game_database) as uow:
                row = self.outbox.get(uow, str(event_id))
            if row is None:
                raise RuntimeError("closing effect outbox receipt is missing")
            if str(row["status"]) == "sent":
                return outcome
            payload = json.loads(str(row["payload_json"]))
            if self.closing_effects is None:
                return outcome
            self.closing_effects.on_closing_settled(
                payload=payload,
                event_id=str(event_id),
            )
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                self.outbox.mark_sent(uow, str(event_id))
            return outcome
        except Exception:
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.outbox.mark_failed(uow, str(event_id))
            except Exception:
                pass
            return replace(
                outcome,
                message="闭关结算已完成，统计和进度正在补偿。",
            )

    def closing_replay(self, operation_id: str):
        """Replay a completed close or recover its ledger after the core commit."""
        repository = ClosingSettlementSqlRepository(
            self.game_database, clock=self.clock, outbox=self.outbox
        )
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            record = self.ledger.get(uow, str(operation_id), "buff.closing_settle")
        if record is None:
            return None
        previous = record.outcome()
        if previous is not None:
            if record.status not in {"applied", "rejected"}:
                return None
            return self._apply_closing_effects(previous.replay())
        if record.status != "started":
            return None
        settled = repository.get_result(str(operation_id))
        if settled is None or not settled.effects_event_id:
            return None
        recovered = OperationOutcome.applied(
            str(operation_id),
            "buff.closing_settle",
            data=self._closing_data(settled),
            audit_category="buff",
            occurred_at=settled.occurred_at or self.clock.now().isoformat(),
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            self.ledger.finish(uow, recovered)
        return self._apply_closing_effects(recovered.replay())

    def reconcile_outbox_event(self, record: dict[str, Any]) -> None:
        if self.closing_effects is None:
            raise RuntimeError("buff closing effects are not configured")
        payload = record.get("payload") or {}
        self.closing_effects.on_closing_settled(
            payload=payload,
            event_id=str(record["event_id"]),
        )
    def pvp_replay(self, *, operation_id: str, challenger_id: str, opponent_id: str):
        if self._explicit_repository is not None:
            getter = getattr(self.repository, "get_result", None)
            return getter(operation_id, challenger_id, opponent_id) if callable(getter) else None
        return NormalPvpSqlRepository(self.game_database, self.player_database).get_result(
            operation_id, challenger_id, opponent_id
        )

    def pvp_settle(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            if not kwargs.get("opponent_id"):
                raise ValidationError("opponent_id is required")
            repository = NormalPvpSqlRepository(self.game_database, self.player_database)
            payload = {
                "user_id": str(user_id),
                "opponent_id": str(kwargs["opponent_id"]),
                "stamina_cost": int(kwargs.get("stamina_cost", 1)),
            }
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="buff.pvp_settle",
                payload=payload,
                call=lambda: repository.settle(operation_id, user_id, **kwargs),
            )
        return self._action("pvp_settle", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["BuffApplication"]
