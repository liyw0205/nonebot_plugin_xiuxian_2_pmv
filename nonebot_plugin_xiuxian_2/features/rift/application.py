from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .demon_token_repository import RiftDemonTokenBattleSqlRepository
from .repository import LegacyRiftRepository, RiftRepository
from .speedup_repository import RiftSpeedupSqlRepository


class RiftApplication(LegacyApplication):
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: RiftRepository | None = None,
        demon_token_repository: Any | None = None,
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

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        repository = self.repository or self.legacy_repository
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action=f"rift.{action}",
            payload={"user_id": user_id, **kwargs},
            call=lambda: repository.invoke(action, operation_id, user_id, **kwargs),
        )

    def generate(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("generate", operation_id=operation_id, user_id=user_id, **kwargs)
    def enter(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("enter", operation_id=operation_id, user_id=user_id, **kwargs)
    def terminate(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("terminate", operation_id=operation_id, user_id=user_id, **kwargs)
    def event_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("event_settle", operation_id=operation_id, user_id=user_id, **kwargs)
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
