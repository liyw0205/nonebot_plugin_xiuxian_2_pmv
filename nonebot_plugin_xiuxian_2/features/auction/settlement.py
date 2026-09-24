"""Application boundary for auction session settlement.

The feature-owned repository is the production default.  The legacy adapter
remains available only for explicit compatibility injection.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.database.ledger import OperationRecord
from ...infrastructure.observability import trace_context
from .settlement_effects import AuctionSettlementEffects, NullAuctionSettlementEffects
from .settlement_repository import AuctionSettlementSqlRepository


class AuctionSettlementRepository(Protocol):
    def settle_active(
        self,
        operation_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: Mapping[int, str],
    ) -> Any: ...


class LegacyAuctionSettlementRepository:
    """Lazy bridge to the historical auction session service."""

    def __init__(self, database: str | Path, trade_database: str | Path) -> None:
        self.database = str(database)
        self.trade_database = str(trade_database)

    def settle_active(
        self,
        operation_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: Mapping[int, str],
    ) -> Any:
        # Import only when a real settlement is requested.  Health/manifest
        # commands must remain usable without importing the legacy matcher.
        try:
            from ...xiuxian.xiuxian_trade.transaction_service import _auction_dependencies

            _items, _sql, _trade, repository, sessions = _auction_dependencies()
        except (ImportError, RuntimeError, ValueError, OSError):
            # The isolated web factory intentionally does not load legacy
            # matchers.  Importing the legacy package can also touch its
            # configured JSON data directory and raise FileNotFoundError when
            # a test or maintenance context uses an empty isolated directory.
            # Expose a deterministic not-ready result instead of turning an
            # otherwise valid admin request into an opaque 500.
            return {"status": "not_ready", "results": ()}
        active = sessions.get_active_session()
        if active is None:
            return {"status": "empty", "results": ()}
        current = repository.get_current_auction() or []
        resolved_types = dict(item_types)
        if not resolved_types:
            for item in current:
                info = _items.get_data_by_item_id(item.get("item_id"))
                if info:
                    resolved_types[int(item["item_id"])] = str(info["type"])
        return sessions.finish(
            operation_id,
            str(active["session_id"]),
            end_time=float(end_time),
            fee_rate=float(fee_rate),
            item_types=resolved_types,
        )


class AuctionSettlementApplication:
    action = "auction.settle"
    effects_event = "auction.settlement.effects"

    def __init__(
        self,
        database: str | Path,
        *,
        repository: AuctionSettlementRepository | None = None,
        ledger: OperationLedger | None = None,
        outbox: OutboxStore | None = None,
        effects: AuctionSettlementEffects | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or AuctionSettlementSqlRepository(self.database)
        self.ledger = ledger or OperationLedger()
        self.outbox = outbox or OutboxStore()
        self.effects = effects or NullAuctionSettlementEffects()

    @staticmethod
    def _event_id(operation_id: str, settlement: Mapping[str, Any], event_key: str) -> str:
        auction_id = str(settlement.get("auction_id", ""))
        return f"{operation_id}:auction.settlement:{auction_id}:{event_key}"

    @staticmethod
    def _effect_keys(settlement: Mapping[str, Any]) -> tuple[str, ...]:
        seller_id = str(settlement.get("seller_id", "0"))
        if settlement.get("final_price") is not None:
            return ("winner", "seller") if seller_id != "0" else ("winner",)
        return ("seller_miss",) if seller_id != "0" else ()

    def _append_effect_events(self, uow: DatabaseUnitOfWork, outcome: OperationOutcome[Any]) -> None:
        if not outcome.ok:
            return
        data = outcome.data if isinstance(outcome.data, Mapping) else {}
        occurred_at = outcome.occurred_at or self.ledger.clock.now().isoformat()
        for settlement in data.get("results", ()):
            record = dict(settlement)
            for event_key in self._effect_keys(record):
                event_id = self._event_id(outcome.operation_id, record, event_key)
                self.outbox.append(
                    uow,
                    event_id=event_id,
                    aggregate_type="auction",
                    aggregate_id=str(record.get("auction_id", "")),
                    event_type=self.effects_event,
                    payload={
                        "event_id": event_id,
                        "operation_id": outcome.operation_id,
                        "event_key": event_key,
                        "settlement": record,
                        "occurred_at": occurred_at,
                    },
                )

    def _apply_effects(self, outcome: OperationOutcome[dict[str, Any]]) -> OperationOutcome[dict[str, Any]]:
        if not outcome.ok:
            return outcome
        data = outcome.data or {}
        event_id: str | None = None
        try:
            for settlement in data.get("results", ()):
                record = dict(settlement)
                for event_key in self._effect_keys(record):
                    event_id = self._event_id(outcome.operation_id, record, event_key)
                    with DatabaseUnitOfWork(self.database) as uow:
                        row = self.outbox.get(uow, event_id)
                    if row is None or str(row["status"]) == "sent":
                        continue
                    payload = json.loads(str(row["payload_json"]))
                    self.reconcile_outbox_event({"payload": payload, "replayed": outcome.replayed})
                    with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                        self.outbox.mark_sent(uow, event_id)
        except Exception:
            if event_id is not None:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.outbox.mark_failed(uow, event_id)
            return replace(outcome, message="拍卖结算已完成，日志与统计稍后补偿。")
        return outcome

    def reconcile_outbox_event(self, record: Mapping[str, Any]) -> None:
        payload = record.get("payload") or {}
        self.effects.on_settlement(
            event_id=str(payload["event_id"]),
            event_key=str(payload["event_key"]),
            operation_id=str(payload["operation_id"]),
            settlement=dict(payload["settlement"]),
            occurred_at=str(payload["occurred_at"]),
            replayed=bool(record.get("replayed", True)),
        )

    def lookup(self, operation_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database) as uow:
            try:
                row = uow.query_one(
                    "SELECT operation_id, action, request_hash, status, result_json, created_at, updated_at "
                    "FROM operation_ledger WHERE operation_id = ? AND action = ?",
                    (str(operation_id).strip(), self.action),
                )
            except Exception as exc:
                if "no such table" in str(exc).lower():
                    return None
                raise
            record = OperationRecord(**row) if row else None
        return record.outcome().to_dict() if record and record.outcome() else None

    def settle_active(
        self,
        *,
        operation_id: str,
        end_time: float,
        fee_rate: float,
        item_types: Mapping[int, str] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(operation_id).strip()
        try:
            end_time = float(end_time)
            fee_rate = float(fee_rate)
        except (TypeError, ValueError) as exc:
            raise ValidationError("end_time and fee_rate must be numeric") from exc
        if not operation_id:
            raise ValidationError("operation_id is required")
        if end_time <= 0 or fee_rate < 0 or fee_rate > 1:
            raise ValidationError("invalid settlement parameters")
        payload = {
            "end_time": end_time,
            "fee_rate": fee_rate,
            "item_types": {str(int(key)): str(value) for key, value in (item_types or {}).items()},
        }
        with trace_context(operation_id=operation_id, user_scope="auction"):
            try:
                replayed_outcome: OperationOutcome[dict[str, Any]] | None = None
                recover_started_operation = False
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    before_begin = self.ledger.get(uow, operation_id, self.action)
                    existing = self.ledger.begin(uow, operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            replayed_outcome = previous.replay()
                        elif existing.status != "started":
                            raise ConflictError("操作正在处理中")
                        else:
                            recover_started_operation = True
                    elif before_begin is not None:
                        recover_started_operation = before_begin.status in {
                            "started", "failed", "needs_reconcile"
                        }
                if replayed_outcome is not None:
                    return self._apply_effects(replayed_outcome)
                raw = self.repository.settle_active(
                    operation_id,
                    end_time=end_time,
                    fee_rate=fee_rate,
                    item_types=item_types or {},
                )
                status = str(getattr(raw, "status", None) or (raw.get("status") if isinstance(raw, dict) else "failed"))
                raw_results = getattr(raw, "results", None)
                if raw_results is None and isinstance(raw, dict):
                    raw_results = raw.get("results", ())
                results = [dict(item) for item in (raw_results or ())]
                data = {"status": status, "results": results}
                if status in {"settled", "duplicate", "empty"}:
                    outcome = OperationOutcome.applied(
                        operation_id,
                        self.action,
                        data=data,
                        granted={"settled_items": len(results)},
                        audit_category="auction_settlement",
                        clock=self.ledger.clock,
                    )
                    if status == "duplicate":
                        outcome = replace(outcome, replayed=True)
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        self.action,
                        f"拍卖结算未完成：{status}",
                        code=status,
                        data=data,
                        audit_category="auction_settlement",
                        clock=self.ledger.clock,
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                    if status != "duplicate" or recover_started_operation:
                        self._append_effect_events(uow, outcome)
                return self._apply_effects(outcome)
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, self.action, payload, str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.settle_active(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = [
    "AuctionSettlementApplication",
    "AuctionSettlementRepository",
    "LegacyAuctionSettlementRepository",
]
