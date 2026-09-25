from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .demon_token_repository import RiftDemonTokenBattleSqlRepository
from .generation_repository import RiftGenerationSqlRepository
from .key_event_repository import RiftKeyEventSqlRepository
from .repository import LegacyRiftRepository, RiftRepository
from .speedup_repository import RiftSpeedupSqlRepository
from .termination_repository import RiftTerminationSqlRepository


class RiftApplication(LegacyApplication):
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: RiftRepository | None = None,
        demon_token_repository: Any | None = None,
        key_event_repository: Any | None = None,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        super().__init__(game_database, repository=repository, feature="rift")
        self.legacy_repository = (
            repository
            if repository is not None
            else LegacyRiftRepository(game_database, player_database)
        )
        self.demon_token_repository = demon_token_repository or RiftDemonTokenBattleSqlRepository(
            game_database, player_database, clock=clock
        )
        self.key_event_repository = key_event_repository or RiftKeyEventSqlRepository(
            game_database, player_database, clock=clock
        )

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        repository = self.repository or self.legacy_repository
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action=f"rift.{action}",
            payload={"user_id": user_id, **kwargs},
            call=lambda: repository.invoke(action, operation_id, user_id, **kwargs),
        )

    def generate(
        self,
        *,
        operation_id: str,
        rift_key: str,
        rift_plan: dict[str, Any],
        user_id: str = "rift-world",
    ):
        if self.repository is not None:
            return self._action(
                "generate",
                operation_id=operation_id,
                user_id=user_id,
                rift_key=rift_key,
                rift_plan=rift_plan,
            )
        repository = RiftGenerationSqlRepository(self.database)
        normalized_plan, _ = repository.normalize_plan(rift_plan)
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="rift.generate",
            payload={
                "user_id": user_id,
                "rift_key": rift_key,
                "rift_plan": rift_plan,
            },
            call=lambda: repository.generate(
                operation_id, rift_key, normalized_plan
            ),
        )

    def current_world(self, *, rift_key: str):
        return RiftGenerationSqlRepository(self.database).get_current(rift_key)

    def bootstrap_world(self, *, rift_key: str, legacy_snapshot: dict[str, Any]):
        return RiftGenerationSqlRepository(self.database).bootstrap(
            rift_key, legacy_snapshot
        )

    def enter(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("enter", operation_id=operation_id, user_id=user_id, **kwargs)
    def terminate(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is not None:
            return self._action("terminate", operation_id=operation_id, user_id=user_id, **kwargs)
        repository = RiftTerminationSqlRepository(self.database)
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="rift.terminate",
            payload={"user_id": user_id, **kwargs},
            call=lambda: repository.terminate(
                operation_id, user_id, kwargs["rift_data"]
            ),
        )

    def replay_termination(self, *, operation_id: str, user_id: str):
        return RiftTerminationSqlRepository(self.database).replay(operation_id, user_id)

    def event_settle(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is not None:
            return self._action("event_settle", operation_id=operation_id, user_id=user_id, **kwargs)
        return self.key_event_repository.settle(
            operation_id,
            user_id,
            kwargs["item_id"],
            kwargs["expected_rift"],
            kwargs["expected_user"],
            kwargs["expected_explore_count"],
            kwargs["outcome"],
            kwargs["max_goods_num"],
        )

    def replay_key_event(self, *, operation_id: str):
        return self.key_event_repository.replay(operation_id)
    def speedup(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="rift.speedup", payload={"user_id": user_id, **kwargs}, call=lambda: RiftSpeedupSqlRepository(self.database).apply(operation_id, user_id, kwargs["item_id"], kwargs.get("expected_rift"), kwargs.get("expected_cd"), kwargs["remaining_ratio"]))
        return self._action("speedup", operation_id=operation_id, user_id=user_id, **kwargs)
    def settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("settle", operation_id=operation_id, user_id=user_id, **kwargs)

    def replay_demon_token_battle(self, *, operation_id: str):
        return self.demon_token_repository.replay(operation_id)

    def settle_demon_token_battle(
        self,
        *,
        operation_id: str,
        user_id: str,
        item_id: int,
        expected_rift: dict[str, Any],
        expected_user: dict[str, Any],
        expected_explore_count: int,
        outcome: dict[str, Any],
        max_goods_num: int,
    ):
        return self.demon_token_repository.settle(
            operation_id,
            user_id,
            item_id,
            expected_rift,
            expected_user,
            expected_explore_count,
            outcome,
            max_goods_num,
        )


__all__ = ["RiftApplication"]
