from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class PlayerStateResult:
    """Result of initializing a player's empty vital state."""

    status: str
    user_id: str
    hp: Any = None
    mp: Any = None
    atk: Any = None
    exp: Any = None

    @property
    def changed(self) -> bool:
        return self.status == "applied"


class PlayerStateRepository:
    """Persist the legacy empty-HP initialization through one SQLite boundary."""

    REQUIRED_COLUMNS = {"user_id", "hp", "mp", "atk", "exp"}
    VITAL_COLUMNS = {"user_id", "hp", "mp"}

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = Path(player_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA table_info("user_xiuxian")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='user_xiuxian'"
        )
        return table is not None and cls.REQUIRED_COLUMNS.issubset(cls._columns(uow))

    @classmethod
    def _vital_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='user_xiuxian'"
        )
        return table is not None and cls.VITAL_COLUMNS.issubset(cls._columns(uow))

    @staticmethod
    def _result(status: str, user_id: str, row: dict[str, Any] | None = None) -> PlayerStateResult:
        values = row or {}
        return PlayerStateResult(
            status=status,
            user_id=user_id,
            hp=values.get("hp"),
            mp=values.get("mp"),
            atk=values.get("atk"),
            exp=values.get("exp"),
        )

    def initialize_if_empty(
        self,
        user_id: str,
        *,
        fallback: Callable[[str], Any] | None = None,
    ) -> PlayerStateResult:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        if not self.player_database.is_file():
            return self._fallback("schema_missing", user_id, fallback)

        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            schema_ready = self._schema_ready(uow)
        if not schema_ready:
            return self._fallback("schema_missing", user_id, fallback)

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing", user_id)
            row = uow.query_one(
                "SELECT rowid AS _rowid,user_id,hp,mp,atk,exp "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing", user_id)
            current = dict(row)
            if current.get("hp") not in (None, 0):
                return self._result("already_initialized", user_id, current)

            changed = uow.execute(
                "UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 "
                "WHERE rowid=? AND user_id=? AND (hp IS NULL OR hp=0) AND exp IS ?",
                (current["_rowid"], user_id, current.get("exp")),
            )
            if changed.rowcount != 1:
                latest = uow.query_one(
                    "SELECT hp,mp,atk,exp FROM user_xiuxian WHERE rowid=?",
                    (current["_rowid"],),
                )
                if latest is not None and latest.get("hp") not in (None, 0):
                    return self._result("already_initialized", user_id, dict(latest))
                return self._result("state_changed", user_id, dict(latest or current))

            updated = uow.query_one(
                "SELECT hp,mp,atk,exp FROM user_xiuxian WHERE rowid=?",
                (current["_rowid"],),
            )
            return self._result("applied", user_id, dict(updated or current))

    def update_vitals(
        self,
        user_id: str,
        hp: int,
        mp: int,
        *,
        expected_hp: int | None = None,
        expected_mp: int | None = None,
        fallback: Callable[[str, int, int], Any] | None = None,
    ) -> PlayerStateResult:
        """Persist combat HP/MP on the first matching player row.

        This projection needs no operation table. Optional expected values turn
        the write into a CAS; legacy callers without a pre-battle snapshot keep
        their overwrite semantics while duplicate user rows remain bounded.
        """
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        hp, mp = int(hp), int(mp)
        if not self.player_database.is_file():
            return self._vital_fallback("schema_missing", user_id, hp, mp, fallback)

        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            schema_ready = self._vital_schema_ready(uow)
        if not schema_ready:
            return self._vital_fallback("schema_missing", user_id, hp, mp, fallback)

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._vital_schema_ready(uow):
                return self._result("schema_missing", user_id)
            row = uow.query_one(
                "SELECT rowid AS _rowid,hp,mp FROM user_xiuxian "
                "WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing", user_id)
            current_hp, current_mp = row.get("hp"), row.get("mp")
            if expected_hp is not None and int(current_hp or 0) != int(expected_hp):
                return self._result("state_changed", user_id, dict(row))
            if expected_mp is not None and int(current_mp or 0) != int(expected_mp):
                return self._result("state_changed", user_id, dict(row))
            predicates = ["rowid=?", "user_id=?"]
            params: list[Any] = [hp, mp, row["_rowid"], user_id]
            if expected_hp is not None:
                predicates.append("COALESCE(hp,0)=?")
                params.append(int(expected_hp))
            if expected_mp is not None:
                predicates.append("COALESCE(mp,0)=?")
                params.append(int(expected_mp))
            changed = uow.execute(
                "UPDATE user_xiuxian SET hp=?,mp=? WHERE "
                + " AND ".join(predicates),
                tuple(params),
            )
            if changed.rowcount != 1:
                latest = uow.query_one(
                    "SELECT hp,mp FROM user_xiuxian WHERE rowid=?",
                    (row["_rowid"],),
                )
                return self._result("state_changed", user_id, dict(latest or row))
            return self._result("applied", user_id, {"hp": hp, "mp": mp})

    def _vital_fallback(
        self,
        status: str,
        user_id: str,
        hp: int,
        mp: int,
        fallback: Callable[[str, int, int], Any] | None,
    ) -> PlayerStateResult:
        if fallback is None:
            return self._result(status, user_id)
        fallback(user_id, hp, mp)
        return self._result("legacy_fallback", user_id, {"hp": hp, "mp": mp})

    def _fallback(
        self,
        status: str,
        user_id: str,
        fallback: Callable[[str], Any] | None,
    ) -> PlayerStateResult:
        if fallback is None:
            return self._result(status, user_id)
        fallback(user_id)
        return self._result("legacy_fallback", user_id)


__all__ = ["PlayerStateRepository", "PlayerStateResult"]
