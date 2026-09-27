from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ...core.errors import ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.clock import SystemClock
from ...infrastructure.observability import trace_context
from .claim_repository import SectFairylandSqlRepository
from .domain import FairylandClaimRequest
from .repository import SectFairylandRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class SectFairylandApplication:
    action = "sect.fairyland_claim"

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: SectFairylandRepository | None = None,
        clock: Any | None = None,
        spirit_vein_multiplier: Callable[[], float] | None = None,
    ) -> None:
        self.player_database = str(player_database)
        self.repository = repository
        self.clock = clock or SystemClock()
        self.spirit_vein_multiplier = spirit_vein_multiplier or (lambda: 1.0)

    def _repository(self) -> SectFairylandRepository:
        if self.repository is None:
            self.repository = SectFairylandSqlRepository(
                self.player_database,
                clock=self.clock,
                spirit_vein_multiplier=self.spirit_vein_multiplier,
            )
        return self.repository

    def claim(
        self,
        *,
        operation_id: str,
        user_id: str,
        sect_id: str,
        day: str,
        level: int,
        minutes: int,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = FairylandClaimRequest(
                str(operation_id).strip(), str(user_id).strip(), str(sect_id).strip(),
                str(day).strip(), int(level), int(minutes),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            raw = self._repository().claim(
                request.operation_id,
                request.user_id,
                request.sect_id,
                request.day,
                request.level,
                request.minutes,
            )
            data = _data(raw)
            status = str(data.get("status", "failed"))
            if status in {"claimed", "duplicate"}:
                return OperationOutcome.applied(
                    request.operation_id,
                    self.action,
                    data=data,
                    after={"tianti_hp": int(data.get("detail", {}).get("new_hp", 0) or 0)},
                    granted={"tianti_hp": int(data.get("detail", {}).get("real_gain", 0) or 0)},
                    audit_category="sect_fairyland",
                    replayed=status == "duplicate",
                    clock=self.clock,
                )
            messages = {
                "already_claimed": "今日已经完成过宗门淬体修行。",
                "state_changed": "宗门淬体状态已更新，请重新领取。",
                "user_missing": "未找到修仙数据。",
                "schema_missing": "宗门炼体堂数据尚未就绪，请稍后重试。",
            }
            return OperationOutcome.rejected(
                request.operation_id,
                self.action,
                messages.get(status, "宗门淬体修行未完成。"),
                code=status,
                data=data,
                audit_category="sect_fairyland",
                clock=self.clock,
            )

    def get_last_claim_day(self, user_id: str, sect_id: str) -> str:
        user_id, sect_id = str(user_id).strip(), str(sect_id).strip()
        if not user_id or not sect_id:
            return ""
        return self._repository().get_last_claim_day(user_id, sect_id)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.claim(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["SectFairylandApplication"]
