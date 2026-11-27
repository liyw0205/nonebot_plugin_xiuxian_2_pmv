from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.numeric import normalize_sect_row
from ...infrastructure.database import DatabaseUnitOfWork
from ...xiuxian.xiuxian_utils.shared_read_cache import process_read_cache, shared_snapshot_key


class SectDirectorySqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def list_with_member_count(self) -> list[tuple[Any, ...]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT s.sect_id,s.sect_name,s.sect_scale,"
                "(SELECT user_name FROM user_xiuxian WHERE user_id=s.sect_owner) AS user_name,"
                "COUNT(ux.user_id) AS member_count "
                "FROM sects s LEFT JOIN user_xiuxian ux ON s.sect_id=ux.sect_id "
                "GROUP BY s.sect_id"
            )
            return [
                (
                    row["sect_id"],
                    row["sect_name"],
                    row["sect_scale"],
                    row["user_name"],
                    row["member_count"],
                )
                for row in rows
            ]

    def list_active_sect_names(self) -> list[str | None]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT sect_name FROM sects WHERE sect_owner IS NOT NULL"
            )
            return [row["sect_name"] for row in rows]

    def list_join_display(self) -> list[dict[str, Any]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            # Legacy schemas may lack the sect primary key; choose one stable row per ID.
            rows = uow.query_all(
                "WITH first_sects AS ("
                "SELECT MIN(rowid) AS first_rowid FROM sects GROUP BY sect_id) "
                "SELECT s.sect_id,s.sect_name,s.sect_scale,s.closed,s.join_open,"
                "(SELECT user_name FROM user_xiuxian WHERE user_id=s.sect_owner "
                "ORDER BY rowid ASC LIMIT 1) AS owner_name,"
                "(SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=s.sect_id) AS member_count "
                "FROM first_sects f JOIN sects s ON s.rowid=f.first_rowid ORDER BY s.sect_id"
            )
            return [normalize_sect_row(row) for row in rows]

    def list_scale_rank(self) -> list[tuple[Any, ...]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT sect_id,sect_name,sect_scale FROM sects "
                "WHERE sect_owner IS NOT NULL ORDER BY xiuxian_int_text(sect_scale) COLLATE xiuxian_integer DESC"
            )
        return [(row["sect_id"], row["sect_name"], row["sect_scale"]) for row in rows]

    def list_combat_power_rank(self) -> list[tuple[Any, ...]]:
        key = shared_snapshot_key(self.database, "sect-combat-power-rank-v1", 50)
        return process_read_cache.get_or_load(
            key,
            database_path=self.database,
            ttl=60 * 60,
            loader=self._load_combat_power_rank,
            snapshot=True,
        )

    def _load_combat_power_rank(self) -> list[tuple[Any, ...]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT sect_id,sect_name,combat_power FROM sects "
                "WHERE sect_owner IS NOT NULL ORDER BY xiuxian_int_text(combat_power) COLLATE xiuxian_integer DESC LIMIT 50"
            )
        return [
            (row["sect_id"], row["sect_name"], row["combat_power"]) for row in rows
        ]


__all__ = ["SectDirectorySqlRepository"]
