"""测试夹具：按启动迁移形状建宠物回执表。

`pet.005` 建 player 库的三张回执表（出战切换/派遣出发/融合突破），
`pet.006` 建 game 库的三张（喂养/放生/技能洗练）。
"""

from __future__ import annotations

from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_pet_game_operations, apply_pet_player_operations


def migrate_player(database: Path) -> None:
    with DatabaseUnitOfWork(str(database), immediate=True) as uow:
        apply_pet_player_operations(uow)


def migrate_game(database: Path) -> None:
    with DatabaseUnitOfWork(str(database), immediate=True) as uow:
        apply_pet_game_operations(uow)


__all__ = ["migrate_game", "migrate_player"]
