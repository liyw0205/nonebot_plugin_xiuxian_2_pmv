"""Application boundary for auction session settlement.

The legacy repository still owns the detailed SQLite settlement algorithm.  It
is injected behind a small protocol so the operation ledger, audit record and
manual/job adapters are independent of that implementation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.database.ledger import OperationRecord
from ...infrastructure.observability import trace_context


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

    def __init__(
        self,
        database: str | Path,
        *,
        repository: AuctionSettlementRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

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
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.repository or LegacyAuctionSettlementRepository(self.database, self.database)
                raw = repository.settle_active(
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
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        self.action,
                        f"拍卖结算未完成：{status}",
                        code=status,
                        data=data,
                        audit_category="auction_settlement",
                    )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
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
