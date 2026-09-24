from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.observability import trace_context
from .domain import AuctionBidRequest
from .bid_repository import AuctionBidSqlRepository
from .bid_effects import AuctionBidEffects, NullAuctionBidEffects
from .repository import AuctionBidRepository
from .schemas import AuctionBidResult


class AuctionBidApplication:
    action = "auction.bid"
    effects_event = "auction.bid.effects"

    def __init__(
        self,
        database: str | Path,
        *,
        repository: AuctionBidRepository | None = None,
        ledger: OperationLedger | None = None,
        outbox: OutboxStore | None = None,
        effects: AuctionBidEffects | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or AuctionBidSqlRepository(self.database)
        self.ledger = ledger or OperationLedger()
        self.outbox = outbox or OutboxStore()
        self.effects = effects or NullAuctionBidEffects()

    def _apply_effects(self, outcome: OperationOutcome[dict[str, Any]]) -> OperationOutcome[dict[str, Any]]:
        if not outcome.ok:
            return outcome
        event_id = f"{outcome.operation_id}:{self.action}"
        try:
            with DatabaseUnitOfWork(self.database) as uow:
                row = self.outbox.get(uow, event_id)
            if row is None:
                # Applied rows created before the outbox cutover cannot reveal
                # whether their legacy projection already ran.
                return outcome
            if str(row["status"]) != "sent":
                payload = json.loads(str(row["payload_json"]))
                self.reconcile_outbox_event({"payload": payload, "replayed": outcome.replayed})
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.outbox.mark_sent(uow, event_id)
        except Exception:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                self.outbox.mark_failed(uow, event_id)
            # The bid and its ledger are already durable.  A projection/log
            # failure must not make a retry debit the bidder a second time.
            return replace(outcome, message="竞价资产已结算，统计稍后补偿。")
        return outcome

    def reconcile_outbox_event(self, record: Mapping[str, Any]) -> None:
        payload = record.get("payload") or {}
        self.effects.on_bid(
            operation_id=str(payload["operation_id"]),
            auction_id=str(payload["auction_id"]),
            bidder_id=str(payload["bidder_id"]),
            item_name=str(payload.get("item_name", "")),
            bid_price=int(payload["bid_price"]),
            replayed=bool(record.get("replayed", True)),
            occurred_at=str(payload["occurred_at"]),
        )

    def _append_effect_event(self, uow: DatabaseUnitOfWork, outcome: OperationOutcome[Any], request: AuctionBidRequest, item_name: str) -> None:
        if not outcome.ok:
            return
        event_id = f"{outcome.operation_id}:{self.action}"
        event_name = str(item_name)
        if not event_name and uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='auction_current'"
        ):
            row = uow.query_one("SELECT name FROM auction_current WHERE id=?", (request.auction_id,))
            event_name = str(row["name"]) if row else request.auction_id
        self.outbox.append(
            uow,
            event_id=event_id,
            aggregate_type="auction",
            aggregate_id=request.auction_id,
            event_type=self.effects_event,
            payload={
                "operation_id": outcome.operation_id,
                "auction_id": request.auction_id,
                "bidder_id": request.bidder_id,
                "item_name": event_name,
                "bid_price": int((outcome.data or {}).get("bid_price", request.bid_price)),
                "occurred_at": outcome.occurred_at or self.ledger.clock.now().isoformat(),
            },
        )

    def place_bid(
        self,
        *,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        bid_price: int,
        expected_price: int,
        expected_bids: Mapping[str, int],
        bid_time: float,
        item_name: str = "",
    ) -> OperationOutcome[dict[str, Any]]:
        request = AuctionBidRequest(
            str(operation_id).strip(), str(auction_id).strip(), str(bidder_id).strip(),
            int(bid_price), int(expected_price), {str(key): int(value) for key, value in expected_bids.items()}, float(bid_time),
        )
        try:
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        replayed_outcome: OperationOutcome[dict[str, Any]] | None = None
        with trace_context(operation_id=request.operation_id, user_scope=request.bidder_id):
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                if existing is not None:
                    previous = existing.outcome()
                    if previous is not None:
                        replayed_outcome = previous.replay()
                    elif existing.status != "started":
                        raise ConflictError("操作正在处理中")
            if replayed_outcome is None:
                try:
                    result = self.repository.place_auction_bid(
                        request.operation_id,
                        request.auction_id,
                        request.bidder_id,
                        request.bid_price,
                        request.expected_price,
                        request.expected_bids,
                        request.bid_time,
                    )
                except Exception as exc:
                    self.ledger.record_failure(self.database, request.operation_id, self.action, payload, str(exc))
                    raise
                outcome = self._to_outcome(request, result)
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                    self._append_effect_event(uow, outcome, request, str(item_name))
            else:
                outcome = replayed_outcome
        return self._apply_effects(outcome)

    def _to_outcome(self, request: AuctionBidRequest, result: Any) -> OperationOutcome[dict[str, Any]]:
        status = str(getattr(result, "status", "failed"))
        data = AuctionBidResult(
            status=status,
            operation_id=request.operation_id,
            auction_id=request.auction_id,
            bidder_id=request.bidder_id,
            bid_price=int(getattr(result, "bid_price", 0) or 0),
            debit=int(getattr(result, "debit", 0) or 0),
            refunded_bidder=str(getattr(result, "refunded_bidder", "") or ""),
            refunded_amount=int(getattr(result, "refunded_amount", 0) or 0),
        ).to_dict()
        if status in {"bid", "duplicate"}:
            outcome = OperationOutcome.applied(
                request.operation_id,
                self.action,
                data=data,
                consumed={"stone": data["debit"]},
                granted={"refund": data["refunded_amount"]},
                audit_category="auction_bid",
                clock=self.ledger.clock,
            )
            return replace(outcome, replayed=True) if status == "duplicate" else outcome
        return OperationOutcome.rejected(request.operation_id, self.action, f"竞拍未结算：{status}", code=status, data=data, audit_category="auction_bid", clock=self.ledger.clock)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.place_bid(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["AuctionBidApplication"]
