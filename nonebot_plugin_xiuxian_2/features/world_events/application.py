from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import DemonClaimRequest, normalize_items
from .repository import LegacyWorldEventClaimRepository, WorldEventClaimRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class DemonClaimApplication:
    action = "world_events.demon_claim"

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: WorldEventClaimRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def claim(
        self,
        *,
        operation_id: str,
        event_key: str,
        event_id: str,
        user_id: str,
        expected_claimed: Mapping[str, Any],
        stone: int,
        exp: int,
        items: Any,
        max_goods_num: int,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = DemonClaimRequest(
                str(operation_id).strip(),
                str(event_key).strip(),
                str(event_id).strip(),
                str(user_id).strip(),
                dict(expected_claimed or {}),
                int(stone),
                int(exp),
                normalize_items(items or ()),
                int(max_goods_num),
            )
            request.validate()
        except (TypeError, ValueError, KeyError) as exc:
            raise ValidationError(str(exc)) from exc

        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")

                repository = self.repository or LegacyWorldEventClaimRepository(self.game_database, self.player_database)
                raw = repository.claim(
                    request.operation_id,
                    request.event_key,
                    request.event_id,
                    request.user_id,
                    request.expected_claimed,
                    request.stone,
                    request.exp,
                    request.items,
                    request.max_goods_num,
                )
                service_data = _data(raw)
                status = str(service_data.get("status", "failed"))
                data = {
                    "status": status,
                    "operation_id": request.operation_id,
                    "event_id": request.event_id,
                    "user_id": request.user_id,
                    "stone": int(service_data.get("stone", request.stone) or 0),
                    "exp": int(service_data.get("exp", request.exp) or 0),
                    "items": [dict(item) for item in request.items],
                }
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=data,
                        granted={"stone": data["stone"], "exp": data["exp"], "items": data["items"]},
                        after={"claimed": True},
                        audit_category="world_events",
                    )
                else:
                    messages = {
                        "already_claimed": "你已经领取过本期魔修入侵奖励了。",
                        "inventory_full": "背包物品已达上限，本期奖励尚未领取。",
                        "state_changed": "领奖未完成：贡献或领奖状态已更新，请重新领取。",
                        "user_missing": "未找到修仙数据。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        messages.get(status, "魔修入侵奖励暂时无法领取。"),
                        code=status,
                        data=data,
                        audit_category="world_events",
                    )
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, request.operation_id, self.action, payload, str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.claim(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["DemonClaimApplication"]
