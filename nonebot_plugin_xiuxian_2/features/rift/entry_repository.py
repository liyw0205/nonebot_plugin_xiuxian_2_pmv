from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .generation_repository import RiftGenerationSqlRepository


@dataclass(frozen=True)
class RiftEntryResult:
    status: str
    entries: int = 0
    rift_data: dict[str, Any] = field(default_factory=dict)
    world: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftEntrySqlRepository:
    """Persist normal and ticket-based entry in the generation-owned game DB."""

    world_table = "rift_world_state"
    entry_table = "rift_entries"
    count_table = "rift_entry_counts"
    operation_table = "rift_entry_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _tables(uow: DatabaseUnitOfWork) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }

    @staticmethod
    def _canonical(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _read_world(cls, uow: DatabaseUnitOfWork, rift_key: str) -> dict[str, Any] | None:
        return RiftGenerationSqlRepository._read_world(uow, rift_key)

    def enter(
        self,
        operation_id: str,
        user_id: str,
        rift_key: str,
        rift_data: dict[str, Any],
        duration: int,
        ticket_id: int = 0,
        *,
        expected_generation_id: str,
        expected_revision: int,
        stamina_cost: int = 0,
        expected_stamina: int | None = None,
    ) -> RiftEntryResult:
        operation_id, user_id, rift_key = str(operation_id).strip(), str(user_id).strip(), str(rift_key).strip()
        expected_generation_id = str(expected_generation_id).strip()
        expected_revision, duration, ticket_id, stamina_cost = int(expected_revision), int(duration), int(ticket_id), int(stamina_cost)
        expected_stamina = None if expected_stamina is None else int(expected_stamina)
        expected_rift, _ = RiftGenerationSqlRepository.normalize_plan(rift_data)
        if (
            not operation_id
            or not user_id
            or not rift_key
            or not expected_generation_id
            or expected_revision <= 0
            or duration <= 0
            or ticket_id < 0
            or stamina_cost < 0
            or (stamina_cost > 0 and expected_stamina is None)
        ):
            raise ValueError("valid operation, user, generation and duration are required")
        payload = self._canonical([
            user_id,
            rift_key,
            expected_generation_id,
            expected_rift,
            duration,
            ticket_id,
            stamina_cost,
        ])
        if not self.database.is_file():
            return RiftEntryResult("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            tables = self._tables(uow)
            required = {
                self.world_table,
                self.entry_table,
                self.count_table,
                self.operation_table,
                "user_xiuxian",
                "user_cd",
                "back",
            }
            if not required.issubset(tables):
                return RiftEntryResult("schema_missing")
            user_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(user_xiuxian)")}
            if stamina_cost and "user_stamina" not in user_columns:
                return RiftEntryResult("schema_missing")

            previous = uow.query_one(
                f"SELECT payload,entry_count,generation_id,rift_data FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if previous["payload"] != payload:
                    return RiftEntryResult("state_changed")
                current = self._read_world(uow, rift_key)
                return RiftEntryResult(
                    "duplicate",
                    int(previous["entry_count"]),
                    json.loads(str(previous["rift_data"])),
                    current,
                )

            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return RiftEntryResult("user_missing")
            world = self._read_world(uow, rift_key)
            if (
                world is None
                or world["generation_id"] != expected_generation_id
                or self._canonical(world["rift_data"]) != self._canonical(expected_rift)
                or int(world["rift_data"].get("time", 0)) != duration
                or int(world["revision"]) != expected_revision
            ):
                return RiftEntryResult("rift_changed", world=world)
            if user_id in world["participants"] and not ticket_id:
                return RiftEntryResult("already_joined", world=world)
            if uow.query_one(
                f"SELECT 1 FROM {self.entry_table} WHERE user_id=? AND status='active'", (user_id,)
            ) is not None:
                return RiftEntryResult("already_active", world=world)
            cd = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?", (user_id,))
            if cd is None or int(cd["type"]) != 0:
                return RiftEntryResult("busy", world=world)

            if stamina_cost:
                stamina = uow.query_one("SELECT COALESCE(user_stamina,0) AS user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
                if stamina is None or int(stamina["user_stamina"]) != expected_stamina:
                    return RiftEntryResult("state_changed", world=world)
                if expected_stamina < stamina_cost:
                    return RiftEntryResult("stamina_missing", world=world)

            if ticket_id:
                back_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
                bind_update = ",bind_num=MIN(MAX(COALESCE(bind_num,0)-1,0),goods_num-1)" if "bind_num" in back_columns else ""
                consumed = uow.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update +
                    " WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)>=1",
                    (user_id, ticket_id),
                )
                if consumed.rowcount != 1:
                    return RiftEntryResult("ticket_missing", world=world)
            if stamina_cost:
                updated = uow.execute(
                    "UPDATE user_xiuxian SET user_stamina=user_stamina-? WHERE user_id=? "
                    "AND COALESCE(user_stamina,0)=? AND COALESCE(user_stamina,0)>=?",
                    (stamina_cost, user_id, expected_stamina, stamina_cost),
                )
                if updated.rowcount != 1:
                    return RiftEntryResult("state_changed", world=world)

            snapshot = self._canonical(expected_rift)
            uow.execute(
                f"INSERT INTO {self.entry_table}(user_id,rift_key,rift_data,status,duration,created_at,generation_id) "
                "VALUES(?,?,?,'active',?,CURRENT_TIMESTAMP,?) "
                "ON CONFLICT(user_id) DO UPDATE SET rift_key=excluded.rift_key,rift_data=excluded.rift_data,"
                "status=excluded.status,duration=excluded.duration,created_at=excluded.created_at,generation_id=excluded.generation_id",
                (user_id, rift_key, snapshot, duration, expected_generation_id),
            )
            if uow.execute(
                "UPDATE user_cd SET type=3,create_time=CURRENT_TIMESTAMP,scheduled_time=? "
                "WHERE user_id=? AND COALESCE(type,0)=0",
                (duration, user_id),
            ).rowcount != 1:
                return RiftEntryResult("state_changed", world=world)

            participants = tuple((*world["participants"], user_id))
            next_revision = int(world["revision"]) + 1
            if uow.execute(
                f"UPDATE {self.world_table} SET participants=?,revision=?,updated_at=CURRENT_TIMESTAMP "
                "WHERE rift_key=? AND generation_id=? AND revision=?",
                (self._canonical(participants), next_revision, rift_key, expected_generation_id, expected_revision),
            ).rowcount != 1:
                return RiftEntryResult("rift_changed", world=world)
            uow.execute(
                f"INSERT INTO {self.count_table}(user_id,entry_count) VALUES(?,1) "
                f"ON CONFLICT(user_id) DO UPDATE SET entry_count={self.count_table}.entry_count+1",
                (user_id,),
            )
            count = int(uow.query_one(f"SELECT entry_count FROM {self.count_table} WHERE user_id=?", (user_id,))["entry_count"])
            uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload,entry_count,generation_id,rift_data,global_revision) VALUES(?,?,?,?,?,?)",
                (operation_id, payload, count, expected_generation_id, snapshot, next_revision),
            )
            return RiftEntryResult(
                "applied",
                count,
                expected_rift,
                {
                    "rift_key": rift_key,
                    "generation_id": expected_generation_id,
                    "rift_data": world["rift_data"],
                    "participants": participants,
                    "revision": next_revision,
                },
            )


__all__ = ["RiftEntryResult", "RiftEntrySqlRepository"]
