"""Read-only inputs used to build a dungeon exploration intent."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from ...features.info.profile_application import PlayerProfileApplication
from ...infrastructure.database import DatabaseUnitOfWork


class DungeonExploreSnapshotError(RuntimeError):
    """The durable inputs for an exploration snapshot are unavailable."""


class DungeonExploreSnapshotRepositoryProtocol(Protocol):
    def read(self, member_ids: Sequence[str]) -> Mapping[str, Any]: ...


class DungeonExploreSnapshotRepository:
    """Batch-read mutable game inputs without opening a writer connection."""

    def __init__(self, game_database: str | Path) -> None:
        self.game_database = Path(game_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _require_schema(
        cls, uow: DatabaseUnitOfWork, table: str, required: set[str]
    ) -> None:
        columns = cls._columns(uow, table)
        if not columns or not required.issubset(columns):
            missing = sorted(required - columns)
            suffix = f" missing columns: {', '.join(missing)}" if missing else ""
            raise DungeonExploreSnapshotError(
                f"dungeon exploration snapshot schema is unavailable: {table}{suffix}"
            )

    @staticmethod
    def _ids(member_ids: Sequence[str]) -> tuple[str, ...]:
        result: list[str] = []
        seen: set[str] = set()
        for member_id in member_ids:
            normalized = str(member_id).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
        return tuple(result)

    def read(self, member_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        ids = self._ids(member_ids)
        if not ids:
            return {"cd_types": {}, "inventory": {}}
        if not self.game_database.is_file():
            raise DungeonExploreSnapshotError(
                f"dungeon exploration snapshot database is unavailable: {self.game_database}"
            )

        placeholders = ",".join("?" for _ in ids)
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                self._require_schema(uow, "user_cd", {"user_id", "type"})
                self._require_schema(
                    uow, "back", {"user_id", "goods_id", "goods_num", "bind_num"}
                )
                cd_types = {member_id: 0 for member_id in ids}
                seen_cd: set[str] = set()
                # The legacy reader uses the newest row when duplicate user_cd
                # rows exist; preserve that rule while doing one batch query.
                rows = uow.query_all(
                    "SELECT user_id,COALESCE(type,0) AS type "
                    f"FROM user_cd WHERE user_id IN ({placeholders}) "
                    "ORDER BY user_id ASC,rowid DESC",
                    ids,
                )
                for row in rows:
                    member_id = str(row["user_id"])
                    if member_id in cd_types and member_id not in seen_cd:
                        cd_types[member_id] = int(row["type"] or 0)
                        seen_cd.add(member_id)

                inventory: dict[str, dict[str, dict[str, int]]] = {
                    member_id: {} for member_id in ids
                }
                rows = uow.query_all(
                    "SELECT user_id,goods_id,COALESCE(goods_num,0) AS goods_num,"
                    "COALESCE(bind_num,0) AS bind_num "
                    f"FROM back WHERE user_id IN ({placeholders}) "
                    "ORDER BY user_id ASC,rowid ASC",
                    ids,
                )
                for row in rows:
                    member_id = str(row["user_id"])
                    if member_id not in inventory:
                        continue
                    inventory[member_id][str(row["goods_id"])] = {
                        "goods_num": int(row["goods_num"] or 0),
                        "bind_num": int(row["bind_num"] or 0),
                    }
                return {"cd_types": cd_types, "inventory": inventory}
        except DungeonExploreSnapshotError:
            raise
        except Exception as exc:
            raise DungeonExploreSnapshotError(
                "dungeon exploration snapshot read failed"
            ) from exc


class DungeonExploreSnapshotApplication:
    """Compose profile, cooldown and inventory inputs for an exploration intent."""

    def __init__(
        self,
        game_database: str | Path,
        *,
        profile_reader: Any | None = None,
        repository: DungeonExploreSnapshotRepositoryProtocol | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.profile_reader = profile_reader or PlayerProfileApplication(game_database)
        self.repository = repository or DungeonExploreSnapshotRepository(game_database)

    @staticmethod
    def _ids(member_ids: Sequence[str]) -> tuple[str, ...]:
        return DungeonExploreSnapshotRepository._ids(member_ids)

    def _profile(self, member_id: str) -> dict[str, Any] | None:
        reader = self.profile_reader
        if callable(reader):
            return reader(member_id)
        method = getattr(reader, "get_user_profile", None)
        if not callable(method):
            raise TypeError("profile_reader must provide get_user_profile")
        return method(member_id)

    def _state(self, member_ids: tuple[str, ...]) -> Mapping[str, Any]:
        repository = self.repository
        method = getattr(repository, "read", None)
        if not callable(method):
            method = getattr(repository, "read_state", None)
        if not callable(method):
            raise TypeError("repository must provide read or read_state")
        return method(member_ids)

    def read(
        self,
        member_ids: Sequence[str],
        supplied_profiles: Mapping[str, Mapping[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        ids = self._ids(member_ids)
        if not ids:
            return {"profiles": {}, "cd_types": {}, "inventory": {}}
        supplied = supplied_profiles or {}
        profiles: dict[str, dict[str, Any] | None] = {}
        for member_id in ids:
            if member_id in supplied:
                profile = supplied[member_id]
            else:
                profile = self._profile(member_id)
            profiles[member_id] = None if profile is None else dict(profile)
        state = self._state(ids)
        if not isinstance(state, Mapping):
            raise DungeonExploreSnapshotError("invalid dungeon exploration snapshot state")
        return {
            "profiles": profiles,
            "cd_types": dict(state.get("cd_types", {})),
            "inventory": dict(state.get("inventory", {})),
        }


__all__ = [
    "DungeonExploreSnapshotApplication",
    "DungeonExploreSnapshotError",
    "DungeonExploreSnapshotRepository",
]
