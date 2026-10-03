from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


CANDIDATE_PAGE_SIZE = 256
_CANDIDATE_FIELDS = (
    "built", "plant_slots", "plot_count", "planting", "plant_seed_id",
    "plant_start", "plant_finish", "intrude_date", "intrude_count",
)


class DongfuCandidateReadError(RuntimeError):
    """A partial candidate scan must not be used as a successful sample."""


class DongfuRandomTargetSqlQueryRepository:
    """Short read-only transactions; no connection survives a returned page."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    def upper_rowid(self) -> int | None:
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                row = uow.query_one("SELECT MAX(rowid) AS upper_rowid FROM dongfu_status")
            return int(row["upper_rowid"]) if row["upper_rowid"] is not None else None
        except Exception as exc:
            raise DongfuCandidateReadError("candidate boundary unavailable") from exc

    def page(self, after_rowid: int | None, upper_rowid: int) -> list[dict[str, Any]]:
        where = "rowid<=?" if after_rowid is None else "rowid>? AND rowid<=?"
        bounds = (upper_rowid,) if after_rowid is None else (after_rowid, upper_rowid)
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                # Bound raw rows, not matching rows: sparse eligible sets still advance.
                rows = uow.query_all(
                    "SELECT rowid AS cursor,user_id,built=? AS is_built FROM dongfu_status "
                    f"WHERE {where} ORDER BY rowid ASC LIMIT ?",
                    ("1", *bounds, CANDIDATE_PAGE_SIZE),
                )
            return rows
        except Exception as exc:
            raise DongfuCandidateReadError("candidate page unavailable") from exc

    def candidate(self, user_id: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                columns = {
                    str(row["name"])
                    for row in uow.query_all("PRAGMA table_info(dongfu_status)")
                }
                if not {"user_id", "built"}.issubset(columns):
                    raise DongfuCandidateReadError("candidate schema unavailable")
                # Optional legacy fields use the normalizer's existing defaults.
                selected = ",".join(
                    f"cave.{field}" for field in _CANDIDATE_FIELDS if field in columns
                )
                uow.attach_database(
                    f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data"
                )
                row = uow.query_one(
                    f"SELECT {selected},profile.user_id,profile.user_name "
                    "FROM dongfu_status AS cave JOIN game_data.user_xiuxian AS profile "
                    "ON profile.rowid=(SELECT rowid FROM game_data.user_xiuxian "
                    "WHERE user_id=? ORDER BY rowid ASC LIMIT 1) "
                    "WHERE cave.user_id=? ORDER BY cave.rowid ASC LIMIT 1",
                    (str(user_id), str(user_id)),
                )
            return row
        except Exception as exc:
            raise DongfuCandidateReadError("candidate state unavailable") from exc


__all__ = ["DongfuCandidateReadError", "DongfuRandomTargetSqlQueryRepository"]
