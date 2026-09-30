from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import BaseRepository
from .rename_repository import BaseRenameSqlRepository
from .robbery_repository import BaseStoneRobberySqlRepository
from .theft_repository import BaseStoneTheftSqlRepository
from .xiangyuan_application import XiangyuanApplication


class BaseApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: BaseRepository | None = None) -> None:
        super().__init__(game_database, repository=repository, feature="base")
        self._xiangyuan_application = XiangyuanApplication(game_database, player_database)

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"base.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def breakthrough(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("breakthrough", operation_id=operation_id, user_id=user_id, **kwargs)
    def tribulation(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("tribulation", operation_id=operation_id, user_id=user_id, **kwargs)
    def get_rename_result(self, operation_id: str):
        return BaseRenameSqlRepository(self.database).get_result(operation_id)
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
    def stone_contest(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("stone_contest", operation_id=operation_id, user_id=user_id, **kwargs)
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
