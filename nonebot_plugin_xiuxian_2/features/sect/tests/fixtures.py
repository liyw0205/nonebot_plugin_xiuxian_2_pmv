"""测试夹具：按启动迁移形状建宗门回执表。

`sect.019` 是这 12 张回执表的唯一创建者；测试不再依赖仓储在请求期惰性建表。
"""

from __future__ import annotations

from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_sect_operations


def migrate_sect(database: Path | str) -> None:
    with DatabaseUnitOfWork(str(database), immediate=True) as uow:
        apply_sect_operations(uow)


__all__ = ["migrate_sect"]
