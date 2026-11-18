from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import ELIXIR_ROOM_COLUMNS, SCHEMA_MISSING, table_ready


class SectElixirRoomSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _reject(step: str, subject: str | int) -> None:
        reason = f"sect_elixir_room_{step}_write_rejected"
        raise DomainError(reason, f"{reason}: {subject}", {"reason": reason, "subject": str(subject)})

    def upgrade(
        self,
        operation_id,
        actor_id,
        sect_id,
        expected_level,
        next_level,
        stone_cost,
        scale_cost,
        *,
        owner_position=0,
    ) -> dict[str, Any]:
        operation_id, actor_id = str(operation_id).strip(), str(actor_id)
        sect_id, expected_level, next_level, stone_cost, scale_cost = (
            int(sect_id), int(expected_level), int(next_level), max(int(stone_cost), 0), max(int(scale_cost), 0)
        )
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        try:
            return self._upgrade(
                operation_id, actor_id, sect_id, expected_level, next_level,
                stone_cost, scale_cost, int(owner_position),
            )
        except DomainError as exc:
            if exc.code.startswith("sect_elixir_room_") and exc.code.endswith("_write_rejected"):
                return {
                    "status": "state_changed",
                    "actor_id": actor_id,
                    "sect_id": sect_id,
                    "from_level": expected_level,
                    "to_level": next_level,
                    "stone_cost": stone_cost,
                    "scale_cost": scale_cost,
                    "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or operation_id),
                }
            raise

    def _upgrade(
        self,
        operation_id: str,
        actor_id: str,
        sect_id: int,
        expected_level: int,
        next_level: int,
        stone_cost: int,
        scale_cost: int,
        owner_position: int,
    ) -> dict[str, Any]:
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_elixir_room_operations", ELIXIR_ROOM_COLUMNS):
                return {"status": SCHEMA_MISSING}
            old = uow.query_one(
                "SELECT actor_id,sect_id,from_level,to_level,stone_cost,scale_cost "
                "FROM sect_elixir_room_operations WHERE operation_id=?", (operation_id,)
            )
            if old:
                if str(old["actor_id"]) != actor_id or int(old["sect_id"]) != sect_id:
                    return {
                        "status": "state_changed", "actor_id": actor_id, "sect_id": sect_id,
                        "from_level": expected_level, "to_level": next_level,
                        "stone_cost": stone_cost, "scale_cost": scale_cost,
                        "reason": "operation_conflict", "subject": "actor",
                    }
                return {
                    "status": "duplicate", "actor_id": actor_id, "sect_id": sect_id,
                    "from_level": int(old["from_level"]), "to_level": int(old["to_level"]),
                    "stone_cost": int(old["stone_cost"]), "scale_cost": int(old["scale_cost"]),
                }

            user = uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?", (actor_id,))
            sect = uow.query_one(
                "SELECT sect_owner,COALESCE(elixir_room_level,0) AS level,"
                "COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_scale,0) AS scale "
                "FROM sects WHERE sect_id=?", (sect_id,)
            )
            if user is None:
                return {"status": "actor_missing", "actor_id": actor_id, "sect_id": sect_id}
            if sect is None:
                return {"status": "sect_missing", "actor_id": actor_id, "sect_id": sect_id}
            base = {
                "actor_id": actor_id, "sect_id": sect_id, "from_level": int(sect["level"]),
                "to_level": next_level, "stone_cost": stone_cost, "scale_cost": scale_cost,
            }
            if int(user["sect_id"] or 0) != sect_id:
                return {"status": "membership_changed", **base}
            if str(sect["sect_owner"]) != actor_id or int(user["sect_position"] or 0) != owner_position:
                return {"status": "not_owner", "reason": "sect_owner_changed" if str(sect["sect_owner"]) != actor_id else "sect_position_changed", **base}
            if int(sect["level"]) != expected_level:
                return {"status": "level_changed", "reason": "sect_elixir_level_changed", **base}
            if as_int_like(sect["stones"]) < stone_cost:
                return {"status": "stone_insufficient", "reason": "sect_stone_insufficient", **base}
            if as_int_like(sect["scale"]) < scale_cost:
                return {"status": "scale_insufficient", **base}

            changed = uow.execute(
                "UPDATE sects SET elixir_room_level=?,"
                "sect_used_stone=xiuxian_int_store(xiuxian_int_sub(sect_used_stone,?,'sect_used_stone','actor')),"
                "sect_scale=xiuxian_int_sub(sect_scale,?,'sect_scale','actor') "
                "WHERE sect_id=? AND sect_owner=? AND COALESCE(elixir_room_level,0)=? "
                "AND xiuxian_int_cmp(COALESCE(sect_used_stone,0),?,1,'sect_used_stone','actor')>=0 "
                "AND xiuxian_int_cmp(COALESCE(sect_scale,0),?,1,'sect_scale','actor')>=0",
                (next_level, stone_cost, scale_cost, sect_id, actor_id, expected_level, stone_cost, scale_cost),
            )
            if changed.rowcount != 1:
                self._reject("state", sect_id)
            saved_sect = uow.query_one(
                "SELECT sect_owner,COALESCE(elixir_room_level,0) AS level,"
                "COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_scale,0) AS scale "
                "FROM sects WHERE sect_id=?", (sect_id,)
            )
            if (
                saved_sect is None or str(saved_sect["sect_owner"]) != actor_id
                or as_int_like(saved_sect["level"]) != next_level
                or as_int_like(saved_sect["stones"]) != as_int_like(sect["stones"]) - stone_cost
                or as_int_like(saved_sect["scale"]) != as_int_like(sect["scale"]) - scale_cost
            ):
                self._reject("state", sect_id)

            recorded = uow.execute(
                "INSERT INTO sect_elixir_room_operations(operation_id,actor_id,sect_id,from_level,to_level,stone_cost,scale_cost) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, actor_id, sect_id, expected_level, next_level, stone_cost, scale_cost),
            )
            if recorded.rowcount != 1:
                self._reject("receipt", operation_id)
            saved_receipt = uow.query_one(
                "SELECT actor_id,sect_id,from_level,to_level,stone_cost,scale_cost "
                "FROM sect_elixir_room_operations WHERE operation_id=?", (operation_id,)
            )
            if (
                saved_receipt is None or str(saved_receipt["actor_id"]) != actor_id
                or int(saved_receipt["sect_id"]) != sect_id
                or int(saved_receipt["from_level"]) != expected_level
                or int(saved_receipt["to_level"]) != next_level
                or int(saved_receipt["stone_cost"]) != stone_cost
                or int(saved_receipt["scale_cost"]) != scale_cost
            ):
                self._reject("receipt", operation_id)
            final_sect = uow.query_one(
                "SELECT sect_owner,COALESCE(elixir_room_level,0) AS level,"
                "COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_scale,0) AS scale "
                "FROM sects WHERE sect_id=?", (sect_id,)
            )
            if (
                final_sect is None or str(final_sect["sect_owner"]) != actor_id
                or as_int_like(final_sect["level"]) != next_level
                or as_int_like(final_sect["stones"]) != as_int_like(sect["stones"]) - stone_cost
                or as_int_like(final_sect["scale"]) != as_int_like(sect["scale"]) - scale_cost
            ):
                self._reject("state", sect_id)
            return {"status": "upgraded", **base}


__all__ = ["SectElixirRoomSqlRepository"]
