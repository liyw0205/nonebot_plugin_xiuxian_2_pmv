from __future__ import annotations

from ...core.numeric import as_int_like
from datetime import date, datetime, time
from dataclasses import replace
from calendar import monthrange
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import INTENT_MARKDOWN, OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.observability import trace_context
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from .daily_reset_repository import SignInDailyResetSqlRepository
from .calendar import render_sign_in_calendar
from .domain import SignInRecord, validate_limits
from .effects import NullSignInEffects, SignInEffects
from .repository import SignInRepository
from .schemas import SignInMakeupRequest, SignInRequest


class SignInApplication:
    action = "sign_in.claim"
    effects_event = "sign_in.effects"

    def __init__(
        self,
        database: str | Path,
        *,
        repository: SignInRepository | None = None,
        ledger: OperationLedger | None = None,
        random_source: Any | None = None,
        clock: Any | None = None,
        lower_limit: int = 100000,
        upper_limit: int = 500000,
        makeup_cost: int = 100000,
        effects: SignInEffects | None = None,
        outbox: OutboxStore | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or SignInRepository()
        self.ledger = ledger or OperationLedger()
        self.random = random_source or SystemRandom()
        self.clock = clock or SystemClock()
        self.lower_limit, self.upper_limit = validate_limits(lower_limit, upper_limit)
        self.makeup_cost = max(0, int(makeup_cost))
        self.effects = effects or NullSignInEffects()
        self.outbox = outbox or OutboxStore(clock=self.clock)

    def _now(self) -> datetime:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if isinstance(value, datetime):
            return value
        raise TypeError("clock.now() must return a datetime")

    def _append_effect_event(
        self,
        uow: DatabaseUnitOfWork,
        outcome: OperationOutcome[dict[str, Any]],
        *,
        user_id: str,
        stone: int,
    ) -> None:
        # CLI and isolated application callers intentionally use no-op effects;
        # do not leave an unhandled outbox event in those contexts.
        if isinstance(self.effects, NullSignInEffects) or not outcome.ok:
            return
        self.outbox.append(
            uow,
            event_id=f"{outcome.operation_id}:{self.action}",
            aggregate_type="sign_in",
            aggregate_id=str(user_id),
            event_type=self.effects_event,
            payload={
                "operation_id": outcome.operation_id,
                "user_id": str(user_id),
                "stone": int(stone),
                "occurred_at": outcome.occurred_at or self._now().isoformat(),
            },
        )

    def reconcile_outbox_event(self, record: dict[str, Any]) -> str | None:
        payload = record.get("payload") or {}
        return self.effects.on_signed(
            user_id=str(payload["user_id"]),
            operation_id=str(payload["operation_id"]),
            stone=int(payload["stone"]),
            replayed=bool(record.get("replayed", True)),
        )

    def _apply_effects(self, outcome: OperationOutcome[dict[str, Any]], *, user_id: str, operation_id: str, stone: int, replayed: bool) -> OperationOutcome[dict[str, Any]]:
        if isinstance(self.effects, NullSignInEffects) or not outcome.ok:
            return outcome
        event_id = f"{operation_id}:{self.action}"
        try:
            with DatabaseUnitOfWork(self.database) as uow:
                row = self.outbox.get(uow, event_id)
            if row is None or (str(row["status"]) == "sent" and not replayed):
                # Applied rows created before the effects outbox was added do
                # not reveal whether their projections already ran.
                return outcome
            import json

            payload = json.loads(str(row["payload_json"]))
            message = self.reconcile_outbox_event({"payload": payload, "replayed": replayed})
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                self.outbox.mark_sent(uow, event_id)
        except Exception:
            # Asset mutation is already committed; side-effect failures must not
            # turn a successful operation into a retry that can duplicate assets.
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.outbox.mark_failed(uow, event_id)
            except Exception:
                pass
            message = "签到资产已结算，但附加奖励稍后补偿。"
        return replace(outcome, message=message or outcome.message)

    def lookup(self, operation_id: str) -> SignInRecord | None:
        with DatabaseUnitOfWork(self.database) as uow:
            return self.repository.operation(uow, str(operation_id).strip())

    def reset_daily_flags(self, business_date: str | date):
        try:
            normalized_date = date.fromisoformat(str(business_date)).isoformat()
        except (TypeError, ValueError) as exc:
            raise ValidationError("invalid business_date") from exc
        operation_id = f"sign-in.daily-reset:{normalized_date}"
        return SignInDailyResetSqlRepository(
            self.database, ledger=self.ledger
        ).reset(operation_id, normalized_date)

    def claim(
        self,
        *,
        user_id: str,
        operation_id: str,
        lower_limit: int | None = None,
        upper_limit: int | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            moment = self._now()
            business_date = moment.date().isoformat()
            request = SignInRequest(
                str(user_id),
                str(operation_id),
                self.lower_limit if lower_limit is None else lower_limit,
                self.upper_limit if upper_limit is None else upper_limit,
            )
            try:
                request.validate()
                lower, upper = validate_limits(request.lower_limit, request.upper_limit)
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc
            payload = {"user_id": request.user_id, "lower_limit": lower, "upper_limit": upper}
            try:
                with DatabaseUnitOfWork(self.database) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            replayed = previous.replay()
                            uow.commit()
                            return self._apply_effects(replayed, user_id=request.user_id, operation_id=request.operation_id, stone=as_int_like((replayed.data or {}).get("sign_in", {}).get("stone", 0)), replayed=True)
                        raise ConflictError("操作正在处理中", reason="operation_pending")
                    legacy_operation = self.repository.operation(uow, request.operation_id)
                    if legacy_operation is not None:
                        snapshot = self.repository.user_snapshot(uow, request.user_id)
                        outcome = OperationOutcome.applied(
                            request.operation_id,
                            self.action,
                            data={"sign_in": legacy_operation.to_dict()},
                            before=snapshot,
                            after=snapshot,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        self._append_effect_event(uow, outcome, user_id=request.user_id, stone=legacy_operation.stone)
                        uow.commit()
                        replayed = outcome.replay()
                        return self._apply_effects(replayed, user_id=request.user_id, operation_id=request.operation_id, stone=legacy_operation.stone, replayed=True)
                    before = self.repository.user_snapshot(uow, request.user_id)
                    states = self.repository.user_sign_states(uow, request.user_id)
                    if not states:
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "角色不存在",
                            code="user_missing",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    if all(state == 1 for state in states):
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "今日已经签到",
                            code="already_signed",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    if self.repository.daily_signed(
                        uow, request.user_id, business_date
                    ):
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "今日已经签到",
                            code="already_signed",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    stone = int(self.random.randint(lower, upper))
                    changed = self.repository.apply(uow, request.user_id, stone)
                    if changed < 1:
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "今日已经签到",
                            code="already_signed",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    record = SignInRecord(request.user_id, request.operation_id, stone)
                    self.repository.insert_operation(uow, record)
                    self.repository.record_daily(
                        uow,
                        request.user_id,
                        business_date,
                        stone,
                        request.operation_id,
                    )
                    after = self.repository.user_snapshot(uow, request.user_id)
                    now = moment.isoformat()
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data={"sign_in": record.to_dict()},
                        before=before,
                        after=after,
                        granted={"stone": stone},
                        audit_category="sign_in",
                        occurred_at=now,
                    )
                    self.ledger.finish(uow, outcome)
                    self._append_effect_event(uow, outcome, user_id=request.user_id, stone=stone)
                    uow.commit()
                    return self._apply_effects(outcome, user_id=request.user_id, operation_id=request.operation_id, stone=stone, replayed=False)
            except DomainError:
                raise
            except Exception as exc:
                try:
                    self.ledger.record_failure(self.database, request.operation_id, self.action, payload, str(exc))
                except Exception:
                    pass
                raise

    def makeup(
        self,
        *,
        user_id: str,
        operation_id: str,
        business_date: str,
        cost: int | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        """补签本月内、早于今天且没有回执的一天；只扣成本，不发放签到灵石。"""

        price = self.makeup_cost if cost is None else int(cost)
        action = "sign_in.makeup"
        request = SignInMakeupRequest(
            str(user_id), str(operation_id), str(business_date).strip(), price
        )
        try:
            request.validate()
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        payload = {
            "user_id": request.user_id,
            "business_date": request.business_date,
            "cost": request.cost,
        }
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            moment = self._now()
            today = moment.date()
            try:
                with DatabaseUnitOfWork(self.database) as uow:
                    existing = self.ledger.begin(
                        uow, request.operation_id, action, payload
                    )
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            replayed = previous.replay()
                            uow.commit()
                            return self._apply_makeup_effects(
                                replayed, request=request, target=None, replayed=True
                            )
                        raise ConflictError("操作正在处理中", reason="operation_pending")

                    before = self.repository.user_snapshot(uow, request.user_id)

                    def reject(
                        message: str, code: str
                    ) -> OperationOutcome[dict[str, Any]]:
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            action,
                            message,
                            code=code,
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome

                    try:
                        target = date.fromisoformat(request.business_date)
                    except (TypeError, ValueError):
                        return reject(
                            "日期格式不正确，请发送【修仙补签 2026-11-01】或【修仙补签 1】。",
                            "invalid_date",
                        )
                    if target == today:
                        return reject(
                            "今天请直接发送【修仙签到】，补签只用于已过去的日子。",
                            "today_not_allowed",
                        )
                    if target > today:
                        return reject("不能补签未来的日期。", "future_date")
                    if (target.year, target.month) != (today.year, today.month):
                        return reject("只能补签本月内的日期。", "out_of_month")
                    if not self.repository.makeup_schema_ready(uow):
                        return reject(
                            "补签需要完成数据库迁移后才能使用。", "schema_missing"
                        )
                    if self.repository.daily_signed(
                        uow, request.user_id, target.isoformat()
                    ):
                        return reject(
                            f"{target.isoformat()} 已经签到，无需补签。",
                            "already_signed",
                        )
                    balance = self.repository.stone_balance(uow, request.user_id)
                    if balance is None:
                        return reject("角色不存在", "user_missing")
                    if balance < request.cost:
                        return reject(
                            f"补签需要{request.cost}灵石，当前灵石不足。",
                            "stone_insufficient",
                        )
                    if not self.repository.record_makeup_daily(
                        uow, request.user_id, target.isoformat(), request.operation_id
                    ):
                        raise ConflictError("该日期已有签到回执，请刷新后重试", reason="sign_receipt_exists")
                    if (
                        self.repository.charge_stone(
                            uow, request.user_id, request.cost
                        )
                        != 1
                    ):
                        raise ConflictError("灵石余额已变化，请重试", reason="stone_changed")

                    after = self.repository.user_snapshot(uow, request.user_id)
                    month_start = today.replace(day=1)
                    month_end = today.replace(
                        day=monthrange(today.year, today.month)[1]
                    )
                    dates = self.repository.daily_dates(
                        uow,
                        request.user_id,
                        month_start.isoformat(),
                        month_end.isoformat(),
                    ) or []
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        action,
                        data={
                            "makeup": {
                                "business_date": target.isoformat(),
                                "cost": request.cost,
                                "stone_before": as_int_like(before.get("stone", 0)),
                                "stone_after": as_int_like(after.get("stone", 0)),
                            },
                            "calendar": list(render_sign_in_calendar(today, dates)),
                        },
                        before=before,
                        after=after,
                        consumed={"stone": request.cost},
                        audit_category="sign_in",
                        occurred_at=moment.isoformat(),
                    )
                    self.ledger.finish(uow, outcome)
                    uow.commit()
                    return self._apply_makeup_effects(
                        outcome, request=request, target=target, replayed=False
                    )
            except DomainError:
                raise
            except Exception as exc:
                try:
                    self.ledger.record_failure(
                        self.database,
                        request.operation_id,
                        action,
                        payload,
                        str(exc),
                    )
                except Exception:
                    pass
                raise

    def _apply_makeup_effects(
        self,
        outcome: OperationOutcome[dict[str, Any]],
        *,
        request: SignInMakeupRequest,
        target: date | None,
        replayed: bool,
    ) -> OperationOutcome[dict[str, Any]]:
        if isinstance(self.effects, NullSignInEffects) or not outcome.ok:
            return outcome
        try:
            day = target or date.fromisoformat(request.business_date)
            message = self.effects.on_makeup(
                user_id=request.user_id,
                operation_id=request.operation_id,
                business_date=request.business_date,
                occurred_at=datetime.combine(day, time(12, 0)),
                replayed=replayed,
            )
        except Exception:
            return replace(outcome, message="补签已记账，但签到统计稍后补偿。")
        return replace(outcome, message=message or outcome.message)

    def reply_makeup(self, **kwargs: Any) -> ReplyPlan:
        result = self.makeup(**kwargs)
        data = result.data if isinstance(result.data, dict) else {}
        makeup = data.get("makeup", {}) if isinstance(data, dict) else {}
        if not result.ok:
            return ReplyPlan(result.message or "补签未完成，请稍后重试。", reference=True)
        lines = [
            "**修仙补签**",
            "",
            "---",
            "",
            f"✅ 补签成功：{makeup.get('business_date', '')}",
            f"消耗：{int(makeup.get('cost', 0) or 0)} 灵石"
            f"｜余额：{int(makeup.get('stone_after', 0) or 0)} 灵石",
        ]
        calendar_lines = data.get("calendar") or []
        if calendar_lines:
            lines.append("")
            lines.extend(str(line) for line in calendar_lines)
        if result.replayed:
            lines.append("")
            lines.append("该补签请求已经处理，无需重复提交。")
        return ReplyPlan("\n".join(lines), reference=True, intent=INTENT_MARKDOWN)

    def calendar_text(self, user_id: str) -> str:
        """Render the current-month calendar, or an empty string when unavailable."""

        try:
            today = self._now().date()
            start = today.replace(day=1)
            end = today.replace(day=monthrange(today.year, today.month)[1])
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                dates = self.repository.daily_dates(
                    uow, str(user_id), start.isoformat(), end.isoformat()
                )
        except Exception:
            return ""
        if dates is None:
            return ""
        return "\n".join(render_sign_in_calendar(today, dates))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        result = self.claim(**kwargs)
        if result.message:
            content = result.message
        else:
            sign_in = (result.data or {}).get("sign_in", {}) if isinstance(result.data, dict) else {}
            stone = sign_in.get("stone", result.granted.get("stone", 0))
            content = f"**修仙签到**\n\n---\n\n✅ 签到成功，获取{stone}块灵石!"
        user_id = str(kwargs.get("user_id", ""))
        if user_id and (result.ok or result.code == "already_signed"):
            calendar_text = self.calendar_text(user_id)
            if calendar_text:
                content = f"{content}\n{calendar_text}"
        return ReplyPlan(content, reference=True, intent=INTENT_MARKDOWN)


__all__ = ["SignInApplication"]
