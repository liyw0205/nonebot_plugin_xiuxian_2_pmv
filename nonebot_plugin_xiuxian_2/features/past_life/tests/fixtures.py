"""测试夹具：按启动迁移形状建 past_life 的表。

`past_life.002` 建 player 库的 `past_life` 与 `statistics` 列，
`past_life.003` 建 game 库的五张回执表；测试不再依赖仓储在请求期惰性建表。
"""

from __future__ import annotations

from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_past_life_game, apply_past_life_player


def migrate_player(player: Path) -> None:
    with DatabaseUnitOfWork(str(player), immediate=True) as uow:
        apply_past_life_player(uow)


def migrate_game(game: Path) -> None:
    with DatabaseUnitOfWork(str(game), immediate=True) as uow:
        apply_past_life_game(uow)


def migrate_pair(game: Path, player: Path) -> None:
    migrate_player(player)
    migrate_game(game)


__all__ = ["migrate_game", "migrate_pair", "migrate_player"]
