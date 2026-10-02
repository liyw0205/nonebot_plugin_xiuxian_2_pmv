from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

from .._legacy_application import LegacyApplication
from .contest_repository import BaseStoneContestSqlRepository
from .breakthrough_repository import BaseDirectBreakthroughSqlRepository
from .repository import BaseRepository
from .rename_repository import BaseRenameSqlRepository
from .root_reroll_repository import BaseRootRerollSqlRepository
from .robbery_repository import BaseStoneRobberySqlRepository
from .theft_repository import BaseStoneTheftSqlRepository
from .xiangyuan_application import XiangyuanApplication


class BaseApplication(LegacyApplication):
    _DIRECT_BREAKTHROUGH_RETRY_BASE_SECONDS = 5
    _DIRECT_BREAKTHROUGH_RETRY_MAX_SECONDS = 300

    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: BaseRepository | None = None, clock: Any | None = None, direct_breakthrough_effects: Any | None = None) -> None:
        super().__init__(game_database, repository=repository, feature="base")
        self._stone_contest_repository = BaseStoneContestSqlRepository(game_database)
        self._direct_breakthrough_repository = BaseDirectBreakthroughSqlRepository(game_database, clock=clock)
        self.direct_breakthrough_effects = direct_breakthrough_effects
        self._xiangyuan_application = XiangyuanApplication(game_database, player_database)

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"base.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def breakthrough(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._action("breakthrough", operation_id=operation_id, user_id=user_id, **kwargs)

    def settle_direct_breakthrough(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._direct_breakthrough_repository.apply(
            operation_id, user_id, **kwargs,
        )

    def direct_breakthrough_replay(self, operation_id: str, user_id: str):
        result = self._direct_breakthrough_repository.get_result(operation_id, user_id)
        return self._apply_direct_breakthrough_effects(result) if result is not None else None

    def plan_direct_breakthrough_relations(self, game, user, new_level, occurred_at):
        if self.direct_breakthrough_effects is None:
            raise RuntimeError("direct breakthrough effects are not configured")
        return self.direct_breakthrough_effects.plan_relations(game, user, new_level, occurred_at)

    def resolve_direct_breakthrough(self, *, operation_id, user_id, expected, plan_factory):
        result = self._direct_breakthrough_repository.resolve(
            operation_id, user_id, expected=expected, plan_factory=plan_factory,
        )
        return self._apply_direct_breakthrough_effects(result)

    def _apply_direct_breakthrough_effects(self, result):
        if not result.effects_event_id:
            return result
        repo = self._direct_breakthrough_repository
        event_id = result.effects_event_id
        try:
            with DatabaseUnitOfWork(repo.database, read_only=True) as uow:
                row = repo.outbox.get(uow, event_id)
            if row is None:
                raise RuntimeError("direct breakthrough outbox is missing")
            payload = json.loads(row["payload_json"])
            if row["status"] == "sent":
                return result
            if row["status"] == "dead":
                raise RuntimeError("direct breakthrough effects require reconciliation")
            if self.direct_breakthrough_effects is None:
                raise RuntimeError("direct breakthrough effects are not configured")
            message = self.direct_breakthrough_effects.on_settled(payload=payload, event_id=event_id)
            with DatabaseUnitOfWork(repo.database, immediate=True) as uow:
                uow.execute(
                    "UPDATE direct_breakthrough_plans SET effects_message=? WHERE operation_id=?",
                    (message, payload["operation_id"]),
                )
                repo.outbox.mark_sent(uow, event_id)
            return replace(result, message=payload["message"] + message)
        except Exception:
            # Keep the committed core and durable reservation available to reconcile.
            try:
                if repo.database.is_file():
                    with DatabaseUnitOfWork(repo.database, immediate=True) as uow:
                        current = repo.outbox.get(uow, event_id)
                        if current is not None and current["status"] == "pending":
                            repo.outbox.mark_failed(
                                uow,
                                event_id,
                                next_attempt_at=self._direct_breakthrough_next_attempt_at(current),
                            )
            except Exception:
                pass
            return replace(result, message=result.message + "\n统计或关系奖励尚未完成，已保留恢复记录。")

    def _direct_breakthrough_next_attempt_at(self, outbox_row) -> str:
        """Back off failed projections so a capped scan can rotate to newer events."""
        attempts = max(int(outbox_row["attempts"] or 0) + 1, 1)
        delay = min(
            self._DIRECT_BREAKTHROUGH_RETRY_MAX_SECONDS,
            self._DIRECT_BREAKTHROUGH_RETRY_BASE_SECONDS * (2 ** min(attempts - 1, 6)),
        )
        now = self._direct_breakthrough_repository.clock.now()
        return (now + timedelta(seconds=delay)).isoformat()

    def resume_pending_direct_breakthroughs(self, *, limit=5):
        repo = self._direct_breakthrough_repository
        if not repo.database.is_file():
            return
        with DatabaseUnitOfWork(repo.database, read_only=True) as uow:
            if not repo._columns(uow, "domain_outbox"):
                return
            now = repo.clock.now().isoformat()
            pending = uow.query_all(
                "SELECT payload_json FROM domain_outbox WHERE event_type=? AND status='pending' "
                "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
                "ORDER BY attempts,created_at,event_id LIMIT ?",
                (repo.EVENT_TYPE, now, max(1, min(int(limit), 5))),
            )
        for row in pending:
            payload = json.loads(row["payload_json"])
            self.direct_breakthrough_replay(payload["operation_id"], payload["user_id"])

    def reconcile_direct_breakthrough_event(self, record):
        if self.direct_breakthrough_effects is None:
            raise RuntimeError("direct breakthrough effects are not configured")
        payload = record["payload"]
        message = self.direct_breakthrough_effects.on_settled(payload=payload, event_id=str(record["event_id"]))
        with DatabaseUnitOfWork(self._direct_breakthrough_repository.database, immediate=True) as uow:
            uow.execute(
                "UPDATE direct_breakthrough_plans SET effects_message=? WHERE operation_id=?",
                (message, payload["operation_id"]),
            )
    def tribulation(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("tribulation", operation_id=operation_id, user_id=user_id, **kwargs)
    def get_rename_result(self, operation_id: str):
        return BaseRenameSqlRepository(self.database).get_result(operation_id)
    def get_root_reroll_result(self, operation_id: str, user_id: str | None = None):
        return BaseRootRerollSqlRepository(self.database).get_result(operation_id, user_id)
    def reroll_root(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return BaseRootRerollSqlRepository(self.database).reroll(
            operation_id,
            user_id,
            kwargs.get("expected_snapshot", {}),
            kwargs.get("root", ""),
            kwargs.get("root_type", ""),
            kwargs.get("stone_cost", 0),
            kwargs.get("root_rate", 0),
            kwargs.get("level_spend", 0),
        )
    def rename(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="base.rename", payload={"user_id": user_id, **kwargs}, call=lambda: BaseRenameSqlRepository(self.database).rename(operation_id, user_id, kwargs.get("rename_kind", "user"), kwargs.get("new_name", ""), item_id=kwargs.get("item_id"), stone_cost=int(kwargs.get("stone_cost", 0) or 0)))
        return self._action("rename", operation_id=operation_id, user_id=user_id, **kwargs)
    def get_stone_theft_result(self, operation_id: str, thief_id: str, victim_id: str):
        return BaseStoneTheftSqlRepository(self.database).get_result(operation_id, thief_id, victim_id)
    def settle_stone_theft(self, *, operation_id: str, thief_id: str, victim_id: str, **kwargs: Any):
        return BaseStoneTheftSqlRepository(self.database).settle(operation_id, thief_id, victim_id, **kwargs)
    def get_stone_robbery_result(self, operation_id: str, robber_id: str, victim_id: str):
        return BaseStoneRobberySqlRepository(self.database, self.player_database).get_result(operation_id, robber_id, victim_id)
    def settle_stone_robbery(self, *, operation_id: str, robber_id: str, victim_id: str, **kwargs: Any):
        return BaseStoneRobberySqlRepository(self.database, self.player_database).settle(operation_id, robber_id, victim_id, **kwargs)
    def get_stone_contest_result(self, operation_id: str, payer_id: str, receiver_id: str, requested_amount: int | None = None):
        return self._stone_contest_repository.get_result(operation_id, payer_id, receiver_id, requested_amount)
    def stone_contest(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is not None:
            return self._action("stone_contest", operation_id=operation_id, user_id=user_id, **kwargs)
        payer_id = kwargs.get("payer_id", user_id)
        receiver_id = kwargs.get("receiver_id", kwargs.get("recipient_id", ""))
        requested_amount = kwargs.get("requested_amount", kwargs.get("amount", 0))
        return self._stone_contest_repository.transfer(
            operation_id, payer_id, receiver_id, requested_amount
        )
    def stone_robbery(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("stone_robbery", operation_id=operation_id, user_id=user_id, **kwargs)
    def sign(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("sign", operation_id=operation_id, user_id=user_id, **kwargs)
    def xiangyuan_create(self, *, operation_id: str, user_id: str, group_id: str, giver_name: str, stone: int, items: Any, receiver_count: int, send_limit: int = 3, legacy_data: Any = None):
        return self._xiangyuan_application.create(
            operation_id, group_id, user_id, giver_name, stone, items, receiver_count, send_limit,
            legacy_data=legacy_data,
        )
    def xiangyuan_claim(self, *, operation_id: str, user_id: str, group_id: str, gift_id: int, stone_reward: int, item_ids: Any, receive_limit: int, max_goods_num: int, legacy_data: Any = None):
        return self._xiangyuan_application.claim(
            operation_id, group_id, gift_id, user_id, stone_reward, item_ids,
            receive_limit, max_goods_num, legacy_data=legacy_data,
        )
    def xiangyuan_group(self, *, group_id: str, **kwargs: Any):
        return self._xiangyuan_application.get_group(group_id, **kwargs)
    def xiangyuan_clear(self, *, max_goods_num: int, **kwargs: Any):
        return self._xiangyuan_application.clear_all(max_goods_num, **kwargs)


__all__ = ["BaseApplication"]
