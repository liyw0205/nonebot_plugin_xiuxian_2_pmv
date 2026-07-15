from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field, replace
from pathlib import Path
from threading import RLock
from datetime import datetime
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.json_store import safe_json_dumps, safe_json_loads
from ..xiuxian_tianti.tianti_data import TiantiDataManager
from ..xiuxian_tianti.transaction_service import grant_tianti_settle_minutes
from .sect_fairyland import SECT_FAIRYLAND_CLAIM_TABLE, _fairyland_claim_key
from datetime import date, datetime
from typing import Any, Iterable

@dataclass(frozen=True)
class SectOwnerTransfer:
    status: str
    actor_id: str
    target_id: str
    sect_id: int | None = None
    actor_name: str = ""
    target_name: str = ""
    sect_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}

@dataclass(frozen=True)
class SectFairylandUpgrade:
    status: str
    actor_id: str
    sect_id: int
    from_level: int = 0
    to_level: int = 0
    stone_cost: int = 0
    materials_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "upgraded"

@dataclass(frozen=True)
class SectElixirRoomUpgrade:
    status: str
    actor_id: str
    sect_id: int
    from_level: int = 0
    to_level: int = 0
    stone_cost: int = 0
    scale_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "upgraded"

@dataclass(frozen=True)
class SectBuffSearch:
    status: str
    actor_id: str
    sect_id: int
    buff_type: str
    previous_value: str = ""
    new_value: str = ""
    stone_cost: int = 0
    materials_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "applied"

@dataclass(frozen=True)
class SectPracticeUpgrade:
    status: str
    user_id: str
    sect_id: int
    practice_type: str
    from_level: int = 0
    to_level: int = 0
    stone_cost: int = 0
    materials_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "upgraded"

@dataclass(frozen=True)
class SectScheduledMaterialGrant:
    status: str
    grant_key: str
    sect_id: int
    materials: int = 0
    combat_power: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "granted"

@dataclass(frozen=True)
class SectElixirRoomMaintenance:
    status: str
    maintenance_key: str
    sect_id: int
    sect_name: str = ""
    room_level: int = 0
    materials_cost: int = 0
    duplicate: bool = False

    @property
    def charged(self) -> bool:
        return self.status == "charged" and not self.duplicate

@dataclass(frozen=True)
class SectDonation:
    status: str
    user_id: str
    sect_id: int
    stone: int = 0
    materials: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"donated", "duplicate"}

@dataclass(frozen=True)
class SectTaskSettlement:
    status: str
    user_id: str
    sect_id: int
    period: str
    cost_type: str
    cost: int = 0
    exp_reward: int = 0
    sect_reward: int = 0
    materials_reward: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"settled", "duplicate"}

@dataclass(frozen=True)
class SectTaskClaim:
    status: str
    user_id: str
    sect_id: int
    period: str
    task_key: str = ""
    task_data: dict | None = None

    @property
    def applied(self) -> bool:
        return self.status in {"claimed", "duplicate"}

@dataclass(frozen=True)
class SectCreation:
    status: str
    user_id: str
    sect_name: str
    stone_cost: int
    sect_id: int | None = None

    @property
    def applied(self) -> bool:
        return self.status in {"created", "duplicate"}

@dataclass(frozen=True)
class SectNameRefresh:
    status: str
    user_id: str
    stone_cost: int

    @property
    def applied(self) -> bool:
        return self.status in {"charged", "duplicate"}

@dataclass(frozen=True)
class SectRename:
    status: str
    actor_id: str
    sect_id: int
    previous_name: str
    new_name: str
    stone_cost: int
    rename_card_id: int

    @property
    def applied(self) -> bool:
        return self.status in {"renamed", "duplicate"}

@dataclass(frozen=True)
class SectMemberRemoval:
    status: str
    actor_id: str
    target_id: str
    sect_id: int | None = None
    sect_name: str = ""
    actor_name: str = ""
    target_name: str = ""
    actor_position: int | None = None
    target_position: int | None = None

    @property
    def applied(self) -> bool:
        return self.status in {"left", "kicked", "duplicate"}

@dataclass(frozen=True)
class SectPositionChange:
    status: str
    actor_id: str
    target_id: str
    sect_id: int | None = None
    actor_name: str = ""
    target_name: str = ""
    old_position: int | None = None
    new_position: int | None = None

    @property
    def applied(self) -> bool:
        return self.status in {"changed", "duplicate", "unchanged"}

class SectMembershipService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_operations (
                operation_id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_fairyland_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_fairyland_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                from_level INTEGER NOT NULL,
                to_level INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                materials_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_elixir_room_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_elixir_room_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                from_level INTEGER NOT NULL,
                to_level INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                scale_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_buff_search_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_buff_search_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                buff_type TEXT NOT NULL,
                previous_value TEXT NOT NULL,
                new_value TEXT NOT NULL,
                stone_cost INTEGER NOT NULL,
                materials_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_practice_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_practice_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                practice_type TEXT NOT NULL,
                from_level INTEGER NOT NULL,
                to_level INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                materials_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_scheduled_material_grants(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_scheduled_material_grants (
                grant_key TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                materials INTEGER NOT NULL,
                combat_power INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (grant_key, sect_id)
            )
            """
        )

    @staticmethod
    def _ensure_elixir_room_maintenance(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_elixir_room_maintenance (
                maintenance_key TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                sect_name TEXT NOT NULL,
                room_level INTEGER NOT NULL,
                materials_cost INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (maintenance_key, sect_id)
            )
            """
        )

    @staticmethod
    def _ensure_donation_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_donation_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                stone INTEGER NOT NULL,
                materials INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_task_settlement_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_task_settlement_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                period TEXT NOT NULL,
                cost_type TEXT NOT NULL,
                cost INTEGER NOT NULL,
                exp_reward INTEGER NOT NULL,
                sect_reward INTEGER NOT NULL,
                materials_reward INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_task_claim_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_task_claim_operations (
                operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL, period TEXT NOT NULL,
                task_key TEXT NOT NULL, task_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_creation_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_creation_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                sect_name TEXT NOT NULL,
                stone_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_name_refresh_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                stone_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_rename_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_rename_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                previous_name TEXT NOT NULL,
                new_name TEXT NOT NULL,
                stone_cost INTEGER NOT NULL,
                rename_card_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_member_removal_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_member_removal_operations (
                operation_id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                sect_name TEXT NOT NULL,
                actor_name TEXT NOT NULL,
                target_name TEXT NOT NULL,
                actor_position INTEGER,
                target_position INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_position_change_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_position_change_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                actor_name TEXT NOT NULL,
                target_name TEXT NOT NULL,
                old_position INTEGER NOT NULL,
                new_position INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def rename_sect(
        self,
        operation_id,
        actor_id,
        sect_id,
        new_name,
        stone_cost,
        rename_card_id,
        *,
        owner_position: int = 0,
    ) -> SectRename:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        sect_id = int(sect_id)
        new_name = str(new_name).strip()
        stone_cost = int(stone_cost)
        rename_card_id = int(rename_card_id)

        def result(status: str, previous_name: str = "") -> SectRename:
            return SectRename(
                status,
                actor_id,
                sect_id,
                previous_name,
                new_name,
                stone_cost,
                rename_card_id,
            )

        if not new_name:
            return result("invalid_name")
        if stone_cost < 0:
            return result("invalid_cost")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_rename_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, previous_name, new_name, stone_cost, rename_card_id "
                    "FROM sect_rename_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectRename(
                        "duplicate",
                        actor_id,
                        int(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                        int(previous[4]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return result("actor_missing")
                if actor[0] is None or int(actor[0]) != sect_id:
                    conn.rollback()
                    return result("sect_changed")

                sect = conn.execute(
                    "SELECT sect_name, sect_owner, COALESCE(sect_used_stone, 0) "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return result("sect_missing")
                current_name = str(sect[0] or "")
                if str(sect[1]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return result("not_owner", current_name)
                if current_name == new_name:
                    conn.rollback()
                    return result("name_exists", current_name)
                conflict = conn.execute(
                    "SELECT sect_id FROM sects WHERE sect_name=%s AND sect_id<>%s",
                    (new_name, sect_id),
                ).fetchone()
                if conflict is not None:
                    conn.rollback()
                    return result("name_exists", current_name)
                if int(sect[2]) < stone_cost:
                    conn.rollback()
                    return result("stone_insufficient", current_name)

                card = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (actor_id, rename_card_id),
                ).fetchone()
                if card is None or int(card[0]) < 1:
                    conn.rollback()
                    return result("card_insufficient", current_name)

                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1, "
                    "bind_num=CASE WHEN COALESCE(bind_num, 0)>=1 "
                    "THEN COALESCE(bind_num, 0)-1 ELSE 0 END "
                    "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num, 0)>=1",
                    (actor_id, rename_card_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("card_insufficient", current_name)
                renamed = conn.execute(
                    "UPDATE sects SET sect_name=%s, sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)-CAST(%s AS REAL) "
                    "WHERE sect_id=%s AND sect_owner=%s AND sect_name=%s "
                    "AND COALESCE(sect_used_stone, 0)>=%s",
                    (new_name, stone_cost, sect_id, actor_id, current_name, stone_cost),
                )
                if renamed.rowcount != 1:
                    raise db_backend.IntegrityError("sect rename state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_rename_operations "
                    "(operation_id, actor_id, sect_id, previous_name, new_name, stone_cost, rename_card_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        actor_id,
                        sect_id,
                        current_name,
                        new_name,
                        stone_cost,
                        rename_card_id,
                    ),
                )
                conn.commit()
                return result("renamed", current_name)
            except Exception:
                conn.rollback()
                raise

    def leave_sect(self, operation_id, user_id, *, owner_position: int = 0) -> SectMemberRemoval:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_member_removal_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, sect_name, actor_name, target_name, actor_position, target_position "
                    "FROM sect_member_removal_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectMemberRemoval(
                        "duplicate",
                        user_id,
                        user_id,
                        int(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        str(previous[3]),
                        None if previous[4] is None else int(previous[4]),
                        None if previous[5] is None else int(previous[5]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectMemberRemoval("user_not_found", user_id, user_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectMemberRemoval("not_in_sect", user_id, user_id)
                if int(actor[1]) == int(owner_position):
                    conn.rollback()
                    return SectMemberRemoval(
                        "owner_cannot_leave",
                        user_id,
                        user_id,
                        int(actor[0]),
                        actor_name=str(actor[2] or ""),
                        target_name=str(actor[2] or ""),
                        actor_position=int(actor[1]),
                        target_position=int(actor[1]),
                    )

                sect = conn.execute(
                    "SELECT sect_name FROM sects WHERE sect_id=%s",
                    (actor[0],),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectMemberRemoval("sect_not_found", user_id, user_id, int(actor[0]))

                updated = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=NULL, sect_position=NULL, sect_contribution=0 "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (user_id, actor[0], actor[1]),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect leave state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_member_removal_operations "
                    "(operation_id, operation_type, actor_id, target_id, sect_id, sect_name, actor_name, target_name, actor_position, target_position) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        "leave",
                        user_id,
                        user_id,
                        actor[0],
                        str(sect[0] or ""),
                        str(actor[2] or ""),
                        str(actor[2] or ""),
                        actor[1],
                        actor[1],
                    ),
                )
                conn.commit()
                return SectMemberRemoval(
                    "left",
                    user_id,
                    user_id,
                    int(actor[0]),
                    str(sect[0] or ""),
                    str(actor[2] or ""),
                    str(actor[2] or ""),
                    int(actor[1]),
                    int(actor[1]),
                )
            except Exception:
                conn.rollback()
                raise

    def kick_member(
        self,
        operation_id,
        actor_id,
        target_id,
        *,
        manager_max_position: int,
    ) -> SectMemberRemoval:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        target_id = str(target_id)
        manager_max_position = int(manager_max_position)

        if actor_id == target_id:
            return SectMemberRemoval("self_target", actor_id, target_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_member_removal_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, sect_name, actor_name, target_name, actor_position, target_position "
                    "FROM sect_member_removal_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectMemberRemoval(
                        "duplicate",
                        actor_id,
                        target_id,
                        int(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        str(previous[3]),
                        None if previous[4] is None else int(previous[4]),
                        None if previous[5] is None else int(previous[5]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectMemberRemoval("actor_not_found", actor_id, target_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectMemberRemoval("actor_not_in_sect", actor_id, target_id)
                target = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian WHERE user_id=%s",
                    (target_id,),
                ).fetchone()
                if target is None:
                    conn.rollback()
                    return SectMemberRemoval(
                        "target_not_found",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        actor_name=str(actor[2] or ""),
                        actor_position=int(actor[1]),
                    )
                if int(actor[1]) > manager_max_position:
                    conn.rollback()
                    return SectMemberRemoval(
                        "insufficient_rank",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        actor_name=str(actor[2] or ""),
                        target_name=str(target[2] or ""),
                        actor_position=int(actor[1]),
                        target_position=int(target[1]) if target[1] is not None else None,
                    )
                if target[0] != actor[0]:
                    conn.rollback()
                    return SectMemberRemoval(
                        "different_sect",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        actor_name=str(actor[2] or ""),
                        target_name=str(target[2] or ""),
                        actor_position=int(actor[1]),
                        target_position=int(target[1]) if target[1] is not None else None,
                    )
                if int(target[1]) <= int(actor[1]):
                    conn.rollback()
                    return SectMemberRemoval(
                        "target_not_lower",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        actor_name=str(actor[2] or ""),
                        target_name=str(target[2] or ""),
                        actor_position=int(actor[1]),
                        target_position=int(target[1]),
                    )
                sect = conn.execute(
                    "SELECT sect_name FROM sects WHERE sect_id=%s",
                    (actor[0],),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectMemberRemoval("sect_not_found", actor_id, target_id, int(actor[0]))

                updated = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=NULL, sect_position=NULL, sect_contribution=0 "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (target_id, target[0], target[1]),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect kick state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_member_removal_operations "
                    "(operation_id, operation_type, actor_id, target_id, sect_id, sect_name, actor_name, target_name, actor_position, target_position) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        "kick",
                        actor_id,
                        target_id,
                        actor[0],
                        str(sect[0] or ""),
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        actor[1],
                        target[1],
                    ),
                )
                conn.commit()
                return SectMemberRemoval(
                    "kicked",
                    actor_id,
                    target_id,
                    int(actor[0]),
                    str(sect[0] or ""),
                    str(actor[2] or ""),
                    str(target[2] or ""),
                    int(actor[1]),
                    int(target[1]),
                )
            except Exception:
                conn.rollback()
                raise

    def change_position(
        self,
        operation_id,
        actor_id,
        target_id,
        requested_position,
        position_limits,
        *,
        manager_max_position: int,
    ) -> SectPositionChange:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        target_id = str(target_id)
        requested_position = int(requested_position)
        manager_max_position = int(manager_max_position)
        normalized_limits = {
            int(position): max(0, int(limit))
            for position, limit in position_limits.items()
        }
        if requested_position not in normalized_limits:
            return SectPositionChange("invalid_position", actor_id, target_id)
        if actor_id == target_id:
            return SectPositionChange("self_target", actor_id, target_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_position_change_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, actor_name, target_name, old_position, new_position "
                    "FROM sect_position_change_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectPositionChange(
                        "duplicate",
                        actor_id,
                        target_id,
                        int(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                        int(previous[4]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectPositionChange("actor_missing", actor_id, target_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectPositionChange("actor_without_sect", actor_id, target_id)
                target = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian WHERE user_id=%s",
                    (target_id,),
                ).fetchone()
                if target is None:
                    conn.rollback()
                    return SectPositionChange(
                        "target_missing",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        actor_name=str(actor[2] or ""),
                    )
                if int(actor[1]) > manager_max_position:
                    conn.rollback()
                    return SectPositionChange(
                        "actor_not_manager",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        int(target[1]) if target[1] is not None else None,
                        requested_position,
                    )
                if target[0] != actor[0]:
                    conn.rollback()
                    return SectPositionChange(
                        "target_not_member",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        int(target[1]) if target[1] is not None else None,
                        requested_position,
                    )
                if int(target[1]) <= int(actor[1]):
                    conn.rollback()
                    return SectPositionChange(
                        "target_not_below_actor",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        int(target[1]),
                        requested_position,
                    )
                if requested_position <= int(actor[1]):
                    conn.rollback()
                    return SectPositionChange(
                        "position_not_below_actor",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        int(target[1]),
                        requested_position,
                    )
                current_position = int(target[1])
                if current_position == requested_position:
                    conn.rollback()
                    return SectPositionChange(
                        "unchanged",
                        actor_id,
                        target_id,
                        int(actor[0]),
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        current_position,
                        requested_position,
                    )
                limit = normalized_limits[requested_position]
                if limit > 0:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=%s AND sect_position=%s AND user_id<>%s",
                        (actor[0], requested_position, target_id),
                    ).fetchone()
                    if int(count[0] or 0) >= limit:
                        conn.rollback()
                        return SectPositionChange(
                            "position_full",
                            actor_id,
                            target_id,
                            int(actor[0]),
                            str(actor[2] or ""),
                            str(target[2] or ""),
                            current_position,
                            requested_position,
                        )

                updated = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (requested_position, target_id, actor[0], current_position),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect position changed concurrently")
                conn.execute(
                    "INSERT INTO sect_position_change_operations "
                    "(operation_id, actor_id, target_id, sect_id, actor_name, target_name, old_position, new_position) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        actor_id,
                        target_id,
                        actor[0],
                        str(actor[2] or ""),
                        str(target[2] or ""),
                        current_position,
                        requested_position,
                    ),
                )
                conn.commit()
                return SectPositionChange(
                    "changed",
                    actor_id,
                    target_id,
                    int(actor[0]),
                    str(actor[2] or ""),
                    str(target[2] or ""),
                    current_position,
                    requested_position,
                )
            except Exception:
                conn.rollback()
                raise

    def charge_name_refresh(self, operation_id, user_id, stone_cost) -> SectNameRefresh:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        stone_cost = int(stone_cost)
        if stone_cost < 0:
            return SectNameRefresh("invalid_cost", user_id, stone_cost)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_creation_operations(conn)
                previous = conn.execute(
                    "SELECT stone_cost FROM sect_name_refresh_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectNameRefresh("duplicate", user_id, int(previous[0]))
                user = conn.execute(
                    "SELECT sect_id, stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return SectNameRefresh("user_missing", user_id, stone_cost)
                if user[0] is not None:
                    conn.rollback()
                    return SectNameRefresh("already_member", user_id, stone_cost)
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) WHERE user_id=%s AND sect_id IS NULL AND stone >= %s",
                    (stone_cost, user_id, stone_cost),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return SectNameRefresh("stone_insufficient", user_id, stone_cost)
                conn.execute(
                    "INSERT INTO sect_name_refresh_operations (operation_id, user_id, stone_cost) VALUES (%s, %s, %s)",
                    (operation_id, user_id, stone_cost),
                )
                conn.commit()
                return SectNameRefresh("charged", user_id, stone_cost)
            except Exception:
                conn.rollback()
                raise

    def create_sect(
        self, operation_id, user_id, sect_name, stone_cost, owner_position
    ) -> SectCreation:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_name = str(sect_name).strip()
        stone_cost = int(stone_cost)
        owner_position = int(owner_position)
        if not sect_name:
            return SectCreation("invalid_name", user_id, sect_name, stone_cost)
        if stone_cost < 0:
            return SectCreation("invalid_cost", user_id, sect_name, stone_cost)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_creation_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, sect_name, stone_cost FROM sect_creation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectCreation("duplicate", user_id, str(previous[1]), int(previous[2]), int(previous[0]))
                user = conn.execute(
                    "SELECT sect_id, stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return SectCreation("user_missing", user_id, sect_name, stone_cost)
                if user[0] is not None:
                    conn.rollback()
                    return SectCreation("already_member", user_id, sect_name, stone_cost)
                if int(user[1] or 0) < stone_cost:
                    conn.rollback()
                    return SectCreation("stone_insufficient", user_id, sect_name, stone_cost)
                if conn.execute("SELECT 1 FROM sects WHERE sect_name=%s", (sect_name,)).fetchone():
                    conn.rollback()
                    return SectCreation("name_exists", user_id, sect_name, stone_cost)

                conn.execute(
                    """
                    INSERT INTO sects (
                        sect_name, sect_owner, sect_scale, sect_used_stone,
                        join_open, closed, combat_power
                    ) VALUES (%s, %s, 0, 0, 1, 0, 0)
                    """,
                    (sect_name, user_id),
                )
                created = conn.execute(
                    "SELECT sect_id FROM sects WHERE sect_owner=%s AND sect_name=%s",
                    (user_id, sect_name),
                ).fetchone()
                if created is None:
                    raise RuntimeError("created sect could not be read back")
                sect_id = int(created[0])
                user_update = conn.execute(
                    """
                    UPDATE user_xiuxian
                    SET sect_id=%s, sect_position=%s, stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL)
                    WHERE user_id=%s AND sect_id IS NULL AND stone >= %s
                    """,
                    (sect_id, owner_position, stone_cost, user_id, stone_cost),
                )
                if user_update.rowcount != 1:
                    conn.rollback()
                    return SectCreation("user_changed", user_id, sect_name, stone_cost)
                conn.execute(
                    """
                    INSERT INTO sect_creation_operations (
                        operation_id, user_id, sect_id, sect_name, stone_cost
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (operation_id, user_id, sect_id, sect_name, stone_cost),
                )
                conn.commit()
                return SectCreation("created", user_id, sect_name, stone_cost, sect_id)
            except Exception:
                conn.rollback()
                raise

    def refresh_task(self, operation_id, user_id, sect_id, period, expected_task_key,
                     expected_task_data, task_key, task_data, daily_limit) -> SectTaskClaim:
        operation_id = str(operation_id).strip()
        user_id, sect_id, period = str(user_id), int(sect_id), str(period).strip()
        expected_json = safe_json_dumps(dict(expected_task_data))
        task_key, task_data = str(task_key).strip(), dict(task_data)
        task_json = safe_json_dumps(task_data)
        if not operation_id or not period or not task_key:
            raise ValueError("operation, period and task are required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_task_claim_operations(conn)
                previous = conn.execute("SELECT task_key,task_data FROM sect_task_claim_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return SectTaskClaim("duplicate", user_id, sect_id, period, str(previous[0]), safe_json_loads(previous[1], {}, dict))
                user = conn.execute("SELECT sect_id,sect_task FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or user[0] is None or int(user[0]) != sect_id:
                    conn.rollback(); return SectTaskClaim("sect_changed", user_id, sect_id, period)
                if int(user[1] or 0) >= int(daily_limit):
                    conn.rollback(); return SectTaskClaim("daily_limit", user_id, sect_id, period)
                current = conn.execute("SELECT task_key,task_data,status FROM sect_task_state WHERE user_id=%s AND period=%s", (user_id, period)).fetchone()
                if current is None or str(current[2]) != "accepted":
                    conn.rollback(); return SectTaskClaim("task_missing", user_id, sect_id, period)
                if str(current[0]) != str(expected_task_key) or safe_json_dumps(safe_json_loads(current[1], {}, dict)) != expected_json:
                    conn.rollback(); return SectTaskClaim("state_changed", user_id, sect_id, period)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute("UPDATE sect_task_state SET task_key=%s,task_data=%s,progress=0,target=1,accepted_at=%s,updated_at=%s,completed_at=NULL WHERE user_id=%s AND period=%s", (task_key,task_json,now,now,user_id,period))
                conn.execute("INSERT INTO sect_task_claim_operations (operation_id,user_id,sect_id,period,task_key,task_data) VALUES (%s,%s,%s,%s,%s,%s)", (operation_id,user_id,sect_id,period,task_key,task_json))
                conn.commit()
                return SectTaskClaim("claimed", user_id, sect_id, period, task_key, task_data)
            except Exception:
                conn.rollback()
                raise

    def claim_task(self, operation_id, user_id, sect_id, period, task_key,
                   task_data, daily_limit, replace_existing=False) -> SectTaskClaim:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id, sect_id = str(user_id), int(sect_id)
        period, task_key = str(period).strip(), str(task_key).strip()
        task_data, daily_limit = dict(task_data), int(daily_limit)
        if not period or not task_key:
            raise ValueError("period and task_key must not be empty")
        task_json = safe_json_dumps(task_data)

        def result(status, key=task_key, data=task_data):
            return SectTaskClaim(status, user_id, sect_id, period, key, dict(data))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_task_claim_operations(conn)
                previous = conn.execute("SELECT task_key, task_data FROM sect_task_claim_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", str(previous[0]), safe_json_loads(previous[1], {}, dict))
                user = conn.execute("SELECT sect_id, sect_task FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback(); return result("user_missing")
                if user[0] is None or int(user[0]) != sect_id:
                    conn.rollback(); return result("sect_changed")
                if int(user[1] or 0) >= daily_limit:
                    conn.rollback(); return result("daily_limit")
                if conn.execute("SELECT 1 FROM sects WHERE sect_id=%s", (sect_id,)).fetchone() is None:
                    conn.rollback(); return result("sect_missing")
                existing = conn.execute("SELECT status FROM sect_task_state WHERE user_id=%s AND period=%s", (user_id, period)).fetchone()
                if existing is not None and str(existing[0]) == "accepted" and not replace_existing:
                    conn.rollback(); return result("task_exists")
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute("""INSERT INTO sect_task_state
                    (user_id,sect_id,task_key,task_data,period,status,progress,target,accepted_at,updated_at,completed_at)
                    VALUES (%s,%s,%s,%s,%s,'accepted',0,1,%s,%s,NULL)
                    ON CONFLICT(user_id,period) DO UPDATE SET sect_id=EXCLUDED.sect_id,
                    task_key=EXCLUDED.task_key,task_data=EXCLUDED.task_data,status='accepted',progress=0,target=1,
                    accepted_at=EXCLUDED.accepted_at,updated_at=EXCLUDED.updated_at,completed_at=NULL""",
                    (user_id, sect_id, task_key, task_json, period, now, now))
                conn.execute("INSERT INTO sect_task_claim_operations (operation_id,user_id,sect_id,period,task_key,task_data) VALUES (%s,%s,%s,%s,%s,%s)", (operation_id,user_id,sect_id,period,task_key,task_json))
                conn.commit()
                return result("claimed")
            except Exception:
                conn.rollback()
                raise

    def settle_task(
        self,
        operation_id,
        user_id,
        sect_id,
        period,
        cost_type,
        cost,
        exp_reward,
        sect_reward,
        expected_task_key=None,
        expected_task_data=None,
    ) -> SectTaskSettlement:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)
        period = str(period).strip()
        cost_type = str(cost_type).strip().lower()
        cost = int(cost)
        exp_reward = int(exp_reward)
        sect_reward = int(sect_reward)
        materials_reward = sect_reward * 10
        if not period:
            raise ValueError("period must not be empty")
        if cost_type not in {"hp", "stone"}:
            raise ValueError("cost_type must be hp or stone")
        if min(cost, exp_reward, sect_reward) < 0:
            return SectTaskSettlement(
                "invalid_amount", user_id, sect_id, period, cost_type,
                cost, exp_reward, sect_reward, materials_reward,
            )

        def result(status, values=None):
            values = values or (cost_type, cost, exp_reward, sect_reward, materials_reward)
            return SectTaskSettlement(
                status, user_id, sect_id, period, str(values[0]),
                int(values[1]), int(values[2]), int(values[3]), int(values[4]),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_task_settlement_operations(conn)
                previous = conn.execute(
                    """
                    SELECT cost_type, cost, exp_reward, sect_reward, materials_reward
                    FROM sect_task_settlement_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)

                task = conn.execute(
                    """
                    SELECT sect_id, status, task_key, task_data FROM sect_task_state
                    WHERE user_id=%s AND period=%s
                    """,
                    (user_id, period),
                ).fetchone()
                if task is None or str(task[1]) != "accepted":
                    conn.rollback()
                    return result("task_missing")
                if int(task[0]) != sect_id:
                    conn.rollback()
                    return result("task_sect_changed")
                if expected_task_key is not None and str(task[2]) != str(expected_task_key):
                    conn.rollback()
                    return result("task_snapshot_changed")
                if expected_task_data is not None and safe_json_loads(task[3], {}, dict) != dict(expected_task_data):
                    conn.rollback()
                    return result("task_snapshot_changed")

                user = conn.execute(
                    "SELECT sect_id, stone, hp FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return result("sect_changed")
                balance = int((user[2] if cost_type == "hp" else user[1]) or 0)
                if balance < cost:
                    conn.rollback()
                    return result(f"{cost_type}_insufficient")
                if conn.execute("SELECT 1 FROM sects WHERE sect_id=%s", (sect_id,)).fetchone() is None:
                    conn.rollback()
                    return result("sect_missing")

                cost_column = "hp" if cost_type == "hp" else "stone"
                user_update = conn.execute(
                    f"""
                    UPDATE user_xiuxian
                    SET {cost_column}={cost_column}-%s,
                        exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),
                        sect_task=COALESCE(sect_task, 0)+1,
                        sect_contribution=CAST(COALESCE(sect_contribution,0) AS REAL)+CAST(%s AS REAL)
                    WHERE user_id=%s AND sect_id=%s AND {cost_column} >= %s
                    """,
                    (cost, exp_reward, sect_reward, user_id, sect_id, cost),
                )
                sect_update = conn.execute(
                    """
                    UPDATE sects
                    SET sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)+CAST(%s AS REAL),
                        sect_scale=CAST(COALESCE(sect_scale,0) AS REAL)+CAST(%s AS REAL),
                        sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+CAST(%s AS REAL)
                    WHERE sect_id=%s
                    """,
                    (sect_reward, sect_reward, materials_reward, sect_id),
                )
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                task_update = conn.execute(
                    """
                    UPDATE sect_task_state
                    SET status='completed', progress=target,
                        updated_at=%s, completed_at=%s
                    WHERE user_id=%s AND period=%s AND sect_id=%s AND status='accepted'
                    """,
                    (now, now, user_id, period, sect_id),
                )
                if user_update.rowcount != 1 or sect_update.rowcount != 1 or task_update.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                conn.execute(
                    """
                    INSERT INTO sect_task_settlement_operations (
                        operation_id, user_id, sect_id, period, cost_type, cost,
                        exp_reward, sect_reward, materials_reward
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (operation_id, user_id, sect_id, period, cost_type, cost,
                     exp_reward, sect_reward, materials_reward),
                )
                conn.commit()
                return result("settled")
            except Exception:
                conn.rollback()
                raise

    def donate(self, operation_id, user_id, sect_id, stone, materials) -> SectDonation:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)
        stone = int(stone)
        materials = int(materials)
        if stone <= 0:
            return SectDonation("invalid_amount", user_id, sect_id, stone, materials)
        if materials < 0:
            return SectDonation("invalid_materials", user_id, sect_id, stone, materials)

        def result(status, result_stone=stone, result_materials=materials):
            return SectDonation(status, user_id, sect_id, int(result_stone), int(result_materials))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_donation_operations(conn)
                previous = conn.execute(
                    "SELECT stone, materials FROM sect_donation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], previous[1])

                user = conn.execute(
                    "SELECT sect_id, stone FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return result("sect_changed")
                if int(user[1] or 0) < stone:
                    conn.rollback()
                    return result("stone_insufficient")
                if conn.execute(
                    "SELECT 1 FROM sects WHERE sect_id=%s", (sect_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("sect_missing")

                user_update = conn.execute(
                    """
                    UPDATE user_xiuxian
                    SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL),
                        sect_contribution=CAST(COALESCE(sect_contribution,0) AS REAL)+CAST(%s AS REAL)
                    WHERE user_id=%s AND sect_id=%s AND stone >= %s
                    """,
                    (stone, stone, user_id, sect_id, stone),
                )
                sect_update = conn.execute(
                    """
                    UPDATE sects
                    SET sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)+CAST(%s AS REAL),
                        sect_scale=CAST(COALESCE(sect_scale,0) AS REAL)+CAST(%s AS REAL),
                        sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+CAST(%s AS REAL)
                    WHERE sect_id=%s
                    """,
                    (stone, stone, materials, sect_id),
                )
                if user_update.rowcount != 1:
                    conn.rollback()
                    return result("user_changed")
                if sect_update.rowcount != 1:
                    conn.rollback()
                    return result("sect_changed")

                conn.execute(
                    """
                    INSERT INTO sect_donation_operations (
                        operation_id, user_id, sect_id, stone, materials
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (operation_id, user_id, sect_id, stone, materials),
                )
                conn.commit()
                return result("donated")
            except Exception:
                conn.rollback()
                raise

    def charge_elixir_room_maintenance(
        self,
        maintenance_key,
        sect_id,
        costs_by_level,
    ) -> SectElixirRoomMaintenance:
        maintenance_key = str(maintenance_key).strip()
        if not maintenance_key:
            raise ValueError("maintenance_key must not be empty")
        sect_id = int(sect_id)
        normalized_costs = {
            int(level): max(int(cost), 0) for level, cost in costs_by_level.items()
        }

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                self._ensure_elixir_room_maintenance(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    """
                    SELECT sect_name, room_level, materials_cost, outcome
                    FROM sect_elixir_room_maintenance
                    WHERE maintenance_key=%s AND sect_id=%s
                    """,
                    (maintenance_key, sect_id),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectElixirRoomMaintenance(
                        str(previous[3]),
                        maintenance_key,
                        sect_id,
                        str(previous[0]),
                        int(previous[1]),
                        int(previous[2]),
                        True,
                    )

                sect = conn.execute(
                    """
                    SELECT sect_name, sect_owner, elixir_room_level,
                           COALESCE(sect_materials, 0)
                    FROM sects WHERE sect_id=%s
                    """,
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectElixirRoomMaintenance(
                        "sect_missing", maintenance_key, sect_id
                    )
                if sect[1] is None:
                    conn.rollback()
                    return SectElixirRoomMaintenance(
                        "sect_inactive", maintenance_key, sect_id, str(sect[0] or "")
                    )

                sect_name = str(sect[0] or "")
                room_level = int(sect[2] or 0)
                materials_cost = normalized_costs.get(room_level, 0)
                if room_level <= 0:
                    outcome = "no_room"
                elif room_level not in normalized_costs:
                    outcome = "level_unsupported"
                elif int(sect[3]) < materials_cost:
                    outcome = "insufficient"
                else:
                    outcome = "charged"
                    conn.execute(
                        """
                        UPDATE sects
                        SET sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)-CAST(%s AS REAL)
                        WHERE sect_id=%s
                        """,
                        (materials_cost, sect_id),
                    )

                conn.execute(
                    """
                    INSERT INTO sect_elixir_room_maintenance (
                        maintenance_key, sect_id, sect_name, room_level,
                        materials_cost, outcome
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        maintenance_key,
                        sect_id,
                        sect_name,
                        room_level,
                        materials_cost,
                        outcome,
                    ),
                )
                conn.commit()
                return SectElixirRoomMaintenance(
                    outcome,
                    maintenance_key,
                    sect_id,
                    sect_name,
                    room_level,
                    materials_cost,
                )
            except Exception:
                conn.rollback()
                raise

    def grant_scheduled_materials(
        self,
        grant_key,
        sect_id,
        multiplier,
    ) -> SectScheduledMaterialGrant:
        grant_key = str(grant_key).strip()
        if not grant_key:
            raise ValueError("grant_key must not be empty")
        sect_id = int(sect_id)
        multiplier = max(int(multiplier), 0)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                self._ensure_scheduled_material_grants(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    """
                    SELECT materials, combat_power
                    FROM sect_scheduled_material_grants
                    WHERE grant_key=%s AND sect_id=%s
                    """,
                    (grant_key, sect_id),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectScheduledMaterialGrant(
                        "duplicate",
                        grant_key,
                        sect_id,
                        int(previous[0]),
                        int(previous[1]),
                    )

                sect = conn.execute(
                    "SELECT sect_scale, sect_owner FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectScheduledMaterialGrant("sect_missing", grant_key, sect_id)
                if sect[1] is None:
                    conn.rollback()
                    return SectScheduledMaterialGrant("sect_inactive", grant_key, sect_id)

                materials = max(int(sect[0] or 0), 0) * multiplier
                power_row = conn.execute(
                    "SELECT COALESCE(SUM(power), 0) FROM user_xiuxian WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                combat_power = int(power_row[0] or 0)
                conn.execute(
                    """
                    UPDATE sects
                    SET sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+CAST(%s AS REAL),
                        combat_power=%s
                    WHERE sect_id=%s
                    """,
                    (materials, combat_power, sect_id),
                )
                conn.execute(
                    """
                    INSERT INTO sect_scheduled_material_grants (
                        grant_key, sect_id, materials, combat_power
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (grant_key, sect_id, materials, combat_power),
                )
                conn.commit()
                return SectScheduledMaterialGrant(
                    "granted", grant_key, sect_id, materials, combat_power
                )
            except Exception:
                conn.rollback()
                raise

    def transfer_owner(
        self,
        operation_id,
        actor_id,
        target_id,
        *,
        owner_position: int = 0,
        former_owner_position: int | None = None,
    ) -> SectOwnerTransfer:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        target_id = str(target_id)
        former_owner_position = (
            int(owner_position) + 1
            if former_owner_position is None
            else int(former_owner_position)
        )

        if actor_id == target_id:
            return SectOwnerTransfer("self_transfer", actor_id, target_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, a.user_name, t.user_name, s.sect_name "
                    "FROM sect_operations o "
                    "LEFT JOIN user_xiuxian a ON a.user_id=o.actor_id "
                    "LEFT JOIN user_xiuxian t ON t.user_id=o.target_id "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectOwnerTransfer(
                        "duplicate",
                        actor_id,
                        target_id,
                        int(previous[0]),
                        str(previous[1] or ""),
                        str(previous[2] or ""),
                        str(previous[3] or ""),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectOwnerTransfer("actor_missing", actor_id, target_id)
                target = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (target_id,),
                ).fetchone()
                if target is None:
                    conn.rollback()
                    return SectOwnerTransfer("target_missing", actor_id, target_id)

                actor_sect_id = actor[0]
                if actor_sect_id is None:
                    conn.rollback()
                    return SectOwnerTransfer("actor_without_sect", actor_id, target_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name FROM sects WHERE sect_id=%s",
                    (actor_sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectOwnerTransfer("sect_missing", actor_id, target_id)

                result = SectOwnerTransfer(
                    "",
                    actor_id,
                    target_id,
                    int(actor_sect_id),
                    str(actor[2] or ""),
                    str(target[2] or ""),
                    str(sect[1] or ""),
                )
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return replace(result, status="not_owner")
                if target[0] != actor_sect_id:
                    conn.rollback()
                    return replace(result, status="target_not_member")
                if int(target[1]) == int(owner_position):
                    conn.rollback()
                    return replace(result, status="target_already_owner")

                former_owner = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (former_owner_position, actor_id, actor_sect_id, owner_position),
                )
                new_owner = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s",
                    (owner_position, target_id, actor_sect_id),
                )
                if former_owner.rowcount != 1 or new_owner.rowcount != 1:
                    raise db_backend.IntegrityError("sect membership changed concurrently")
                updated = conn.execute(
                    "UPDATE sects SET sect_owner=%s WHERE sect_id=%s AND sect_owner=%s",
                    (target_id, actor_sect_id, actor_id),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect owner changed concurrently")
                conn.execute(
                    "INSERT INTO sect_operations "
                    "(operation_id, operation_type, actor_id, target_id, sect_id) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, "transfer_owner", actor_id, target_id, actor_sect_id),
                )
                conn.commit()
                return replace(result, status="transferred")
            except Exception:
                conn.rollback()
                raise

    def upgrade_fairyland(
        self,
        operation_id,
        actor_id,
        sect_id,
        expected_level,
        next_level,
        stone_cost,
        materials_cost,
        *,
        owner_position: int = 0,
    ) -> SectFairylandUpgrade:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        sect_id = int(sect_id)
        expected_level = int(expected_level)
        next_level = int(next_level)
        stone_cost = max(int(stone_cost), 0)
        materials_cost = max(int(materials_cost), 0)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_fairyland_operations(conn)
                previous = conn.execute(
                    "SELECT from_level, to_level, stone_cost, materials_cost "
                    "FROM sect_fairyland_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectFairylandUpgrade(
                        "duplicate",
                        actor_id,
                        sect_id,
                        int(previous[0]),
                        int(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectFairylandUpgrade("actor_missing", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, COALESCE(sect_fairyland, 0), "
                    "COALESCE(sect_used_stone, 0), COALESCE(sect_materials, 0) "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectFairylandUpgrade("sect_missing", actor_id, sect_id)
                if (
                    actor[0] != sect_id
                    or int(actor[1]) != int(owner_position)
                    or str(sect[0]) != actor_id
                ):
                    conn.rollback()
                    return SectFairylandUpgrade("not_owner", actor_id, sect_id)
                current_level = int(sect[1] or 0)
                if current_level != expected_level or next_level != current_level + 1:
                    conn.rollback()
                    return SectFairylandUpgrade(
                        "level_changed", actor_id, sect_id, current_level, next_level
                    )
                if int(sect[2] or 0) < stone_cost:
                    conn.rollback()
                    return SectFairylandUpgrade(
                        "stone_insufficient",
                        actor_id,
                        sect_id,
                        current_level,
                        next_level,
                        stone_cost,
                        materials_cost,
                    )
                if int(sect[3] or 0) < materials_cost:
                    conn.rollback()
                    return SectFairylandUpgrade(
                        "materials_insufficient",
                        actor_id,
                        sect_id,
                        current_level,
                        next_level,
                        stone_cost,
                        materials_cost,
                    )

                updated = conn.execute(
                    "UPDATE sects SET sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)-CAST(%s AS REAL), "
                    "sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)-CAST(%s AS REAL), sect_fairyland=%s "
                    "WHERE sect_id=%s AND sect_owner=%s "
                    "AND COALESCE(sect_fairyland, 0)=%s "
                    "AND COALESCE(sect_used_stone, 0)>=%s "
                    "AND COALESCE(sect_materials, 0)>=%s",
                    (
                        stone_cost,
                        materials_cost,
                        next_level,
                        sect_id,
                        actor_id,
                        current_level,
                        stone_cost,
                        materials_cost,
                    ),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect fairyland changed concurrently")
                conn.execute(
                    "INSERT INTO sect_fairyland_operations "
                    "(operation_id, actor_id, sect_id, from_level, to_level, "
                    "stone_cost, materials_cost) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        actor_id,
                        sect_id,
                        current_level,
                        next_level,
                        stone_cost,
                        materials_cost,
                    ),
                )
                conn.commit()
                return SectFairylandUpgrade(
                    "upgraded",
                    actor_id,
                    sect_id,
                    current_level,
                    next_level,
                    stone_cost,
                    materials_cost,
                )
            except Exception:
                conn.rollback()
                raise

    def upgrade_elixir_room(
        self,
        operation_id,
        actor_id,
        sect_id,
        expected_level,
        next_level,
        stone_cost,
        scale_cost,
        *,
        owner_position: int = 0,
    ) -> SectElixirRoomUpgrade:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        sect_id = int(sect_id)
        expected_level = int(expected_level)
        next_level = int(next_level)
        stone_cost = max(int(stone_cost), 0)
        scale_cost = max(int(scale_cost), 0)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_elixir_room_operations(conn)
                previous = conn.execute(
                    "SELECT from_level, to_level, stone_cost, scale_cost "
                    "FROM sect_elixir_room_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectElixirRoomUpgrade(
                        "duplicate",
                        actor_id,
                        sect_id,
                        int(previous[0]),
                        int(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectElixirRoomUpgrade("actor_missing", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, COALESCE(elixir_room_level, 0), "
                    "COALESCE(sect_used_stone, 0), COALESCE(sect_scale, 0) "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectElixirRoomUpgrade("sect_missing", actor_id, sect_id)
                if (
                    actor[0] is None
                    or int(actor[0]) != sect_id
                    or int(actor[1]) != int(owner_position)
                    or str(sect[0]) != actor_id
                ):
                    conn.rollback()
                    return SectElixirRoomUpgrade("not_owner", actor_id, sect_id)
                current_level = int(sect[1] or 0)
                if current_level != expected_level or next_level != current_level + 1:
                    conn.rollback()
                    return SectElixirRoomUpgrade(
                        "level_changed", actor_id, sect_id, current_level, next_level
                    )
                if int(sect[2] or 0) < stone_cost:
                    conn.rollback()
                    return SectElixirRoomUpgrade(
                        "stone_insufficient",
                        actor_id,
                        sect_id,
                        current_level,
                        next_level,
                        stone_cost,
                        scale_cost,
                    )
                if int(sect[3] or 0) < scale_cost:
                    conn.rollback()
                    return SectElixirRoomUpgrade(
                        "scale_insufficient",
                        actor_id,
                        sect_id,
                        current_level,
                        next_level,
                        stone_cost,
                        scale_cost,
                    )

                updated = conn.execute(
                    "UPDATE sects SET sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)-CAST(%s AS REAL), "
                    "sect_scale=CAST(COALESCE(sect_scale,0) AS REAL)-CAST(%s AS REAL), elixir_room_level=%s "
                    "WHERE sect_id=%s AND sect_owner=%s "
                    "AND COALESCE(elixir_room_level, 0)=%s "
                    "AND COALESCE(sect_used_stone, 0)>=%s "
                    "AND COALESCE(sect_scale, 0)>=%s",
                    (
                        stone_cost,
                        scale_cost,
                        next_level,
                        sect_id,
                        actor_id,
                        current_level,
                        stone_cost,
                        scale_cost,
                    ),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect elixir room changed concurrently")
                conn.execute(
                    "INSERT INTO sect_elixir_room_operations "
                    "(operation_id, actor_id, sect_id, from_level, to_level, "
                    "stone_cost, scale_cost) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        actor_id,
                        sect_id,
                        current_level,
                        next_level,
                        stone_cost,
                        scale_cost,
                    ),
                )
                conn.commit()
                return SectElixirRoomUpgrade(
                    "upgraded",
                    actor_id,
                    sect_id,
                    current_level,
                    next_level,
                    stone_cost,
                    scale_cost,
                )
            except Exception:
                conn.rollback()
                raise

    def apply_buff_search(
        self,
        operation_id,
        actor_id,
        sect_id,
        buff_type,
        expected_value,
        new_value,
        stone_cost,
        materials_cost,
        *,
        owner_position: int = 0,
    ) -> SectBuffSearch:
        columns = {"main": "mainbuff", "secondary": "secbuff"}
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        sect_id = int(sect_id)
        buff_type = str(buff_type).strip().lower()
        try:
            column = columns[buff_type]
        except KeyError as exc:
            raise ValueError(f"unsupported buff type: {buff_type}") from exc
        expected_value = str(expected_value or "")
        new_value = str(new_value or "")
        stone_cost = max(int(stone_cost), 0)
        materials_cost = max(int(materials_cost), 0)

        def result(status, previous=expected_value, current=new_value):
            return SectBuffSearch(
                status,
                actor_id,
                sect_id,
                buff_type,
                previous,
                current,
                stone_cost,
                materials_cost,
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_buff_search_operations(conn)
                previous = conn.execute(
                    "SELECT buff_type, previous_value, new_value, stone_cost, "
                    "materials_cost FROM sect_buff_search_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectBuffSearch(
                        "duplicate",
                        actor_id,
                        sect_id,
                        str(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                        int(previous[4]),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return result("actor_missing")
                sect = conn.execute(
                    f"SELECT sect_owner, COALESCE(sect_used_stone, 0), "
                    f"COALESCE(sect_materials, 0), COALESCE({column}, '') "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return result("sect_missing")
                if (
                    actor[0] is None
                    or int(actor[0]) != sect_id
                    or int(actor[1]) != int(owner_position)
                    or str(sect[0]) != actor_id
                ):
                    conn.rollback()
                    return result("not_owner")
                current_value = str(sect[3] or "")
                if current_value != expected_value:
                    conn.rollback()
                    return result("buff_changed", current_value, current_value)
                if int(sect[1] or 0) < stone_cost:
                    conn.rollback()
                    return result("stone_insufficient")
                if int(sect[2] or 0) < materials_cost:
                    conn.rollback()
                    return result("materials_insufficient")

                updated = conn.execute(
                    f"UPDATE sects SET sect_used_stone=CAST(COALESCE(sect_used_stone,0) AS REAL)-CAST(%s AS REAL), "
                    f"sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)-CAST(%s AS REAL), {column}=%s "
                    "WHERE sect_id=%s AND sect_owner=%s "
                    f"AND COALESCE({column}, '')=%s "
                    "AND COALESCE(sect_used_stone, 0)>=%s "
                    "AND COALESCE(sect_materials, 0)>=%s",
                    (
                        stone_cost,
                        materials_cost,
                        new_value,
                        sect_id,
                        actor_id,
                        expected_value,
                        stone_cost,
                        materials_cost,
                    ),
                )
                if updated.rowcount != 1:
                    raise db_backend.IntegrityError("sect buff search changed concurrently")
                conn.execute(
                    "INSERT INTO sect_buff_search_operations "
                    "(operation_id, actor_id, sect_id, buff_type, previous_value, "
                    "new_value, stone_cost, materials_cost) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        actor_id,
                        sect_id,
                        buff_type,
                        expected_value,
                        new_value,
                        stone_cost,
                        materials_cost,
                    ),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

    def upgrade_practice(
        self,
        operation_id,
        user_id,
        sect_id,
        practice_type,
        expected_level,
        next_level,
        stone_cost,
        materials_cost,
    ) -> SectPracticeUpgrade:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)
        practice_type = str(practice_type)
        column = {
            "attack": "atkpractice",
            "health": "hppractice",
            "mana": "mppractice",
        }.get(practice_type)
        if column is None:
            raise ValueError("unsupported practice_type")
        expected_level = int(expected_level)
        next_level = int(next_level)
        stone_cost = max(int(stone_cost), 0)
        materials_cost = max(int(materials_cost), 0)

        def result(status, from_level=expected_level, to_level=next_level):
            return SectPracticeUpgrade(
                status,
                user_id,
                sect_id,
                practice_type,
                from_level,
                to_level,
                stone_cost,
                materials_cost,
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_practice_operations(conn)
                previous = conn.execute(
                    """
                    SELECT practice_type, from_level, to_level, stone_cost,
                           materials_cost
                    FROM sect_practice_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectPracticeUpgrade(
                        "duplicate",
                        user_id,
                        sect_id,
                        str(previous[0]),
                        int(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                        int(previous[4]),
                    )

                user = conn.execute(
                    f"SELECT sect_id, stone, {column} FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return result("sect_changed")
                current_level = int(user[2] or 0)
                if current_level != expected_level:
                    conn.rollback()
                    return result("level_changed", current_level, current_level)
                if int(user[1] or 0) < stone_cost:
                    conn.rollback()
                    return result("stone_insufficient", current_level, next_level)

                sect = conn.execute(
                    "SELECT sect_materials FROM sects WHERE sect_id=%s", (sect_id,)
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return result("sect_missing", current_level, next_level)
                if int(sect[0] or 0) < materials_cost:
                    conn.rollback()
                    return result("materials_insufficient", current_level, next_level)

                conn.execute(
                    f"UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL), {column}=%s WHERE user_id=%s",
                    (stone_cost, next_level, user_id),
                )
                conn.execute(
                    "UPDATE sects SET sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)-CAST(%s AS REAL) WHERE sect_id=%s",
                    (materials_cost, sect_id),
                )
                conn.execute(
                    """
                    INSERT INTO sect_practice_operations (
                        operation_id, user_id, sect_id, practice_type,
                        from_level, to_level, stone_cost, materials_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        user_id,
                        sect_id,
                        practice_type,
                        current_level,
                        next_level,
                        stone_cost,
                        materials_cost,
                    ),
                )
                conn.commit()
                return result("upgraded", current_level, next_level)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class FairylandClaimResult:
    status: str
    user_id: str
    sect_id: str
    day: str
    level: int
    minutes: int
    detail: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"claimed", "duplicate"}

class FairylandClaimService:
    """Grant daily sect fairyland tianti gain and mark the claim atomically."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields, claim_field) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_fairyland_claim_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_id TEXT NOT NULL, "
            "claim_day TEXT NOT NULL, level INTEGER NOT NULL, minutes INTEGER NOT NULL, "
            "detail_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        tianti_columns = set(conn.column_names("tianti_info"))
        for field in fields:
            if field not in tianti_columns:
                conn.execute(
                    f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} "
            "(user_id TEXT PRIMARY KEY)"
        )
        claim_columns = set(conn.column_names(SECT_FAIRYLAND_CLAIM_TABLE))
        if claim_field not in claim_columns:
            conn.execute(
                f"ALTER TABLE {db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} "
                f"ADD COLUMN {db_backend.quote_ident(claim_field)} TEXT"
            )

    def claim(self, operation_id, user_id, sect_id, day, level, minutes):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        sect_id = str(sect_id)
        day = str(day).strip()
        level = int(level)
        minutes = int(minutes)
        if not operation_id or not day or level <= 0 or minutes <= 0:
            raise ValueError("operation_id, day, level and minutes must be valid")

        def result(status, detail=None, result_level=level, result_minutes=minutes):
            return FairylandClaimResult(
                status, user_id, sect_id, day, int(result_level), int(result_minutes),
                detail or {},
            )

        fields = tuple(self._manager._default().keys())
        claim_field = _fairyland_claim_key(sect_id)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields, claim_field)
                previous = conn.execute(
                    "SELECT user_id, sect_id, claim_day, level, minutes, detail_json "
                    "FROM sect_fairyland_claim_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id or str(previous[1]) != sect_id
                        or str(previous[2]) != day or int(previous[3]) != level
                        or int(previous[4]) != minutes
                    ):
                        return result("state_changed")
                    return result("duplicate", json.loads(previous[5]), previous[3], previous[4])

                prior_claim = conn.execute(
                    f"SELECT {db_backend.quote_ident(claim_field)} FROM "
                    f"{db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if prior_claim is not None and str(prior_claim[0] or "") == day:
                    conn.rollback()
                    return result("already_claimed")

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                detail = grant_tianti_settle_minutes(
                    data, minutes, sect_fairyland_level=level
                )
                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}",
                    (user_id, *values),
                )
                conn.execute(
                    f"INSERT INTO {db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} "
                    f"(user_id, {db_backend.quote_ident(claim_field)}) VALUES (%s, %s) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {db_backend.quote_ident(claim_field)}=EXCLUDED.{db_backend.quote_ident(claim_field)}",
                    (user_id, day),
                )
                conn.execute(
                    "INSERT INTO sect_fairyland_claim_operations "
                    "(operation_id, user_id, sect_id, claim_day, level, minutes, detail_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, day, level, minutes,
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return result("claimed", detail)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectCloseMountainResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"closed", "duplicate"}

class SectCloseMountainService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_close_mountain_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def close(
        self,
        operation_id,
        actor_id,
        *,
        owner_position: int = 0,
        former_owner_position: int = 2,
        expected_sect_id: int | None = None,
    ) -> SectCloseMountainResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, s.sect_name "
                    "FROM sect_close_mountain_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectCloseMountainResult(
                        "duplicate", actor_id, int(previous[0]), str(previous[1] or "")
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectCloseMountainResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectCloseMountainResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectCloseMountainResult("sect_changed", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name, closed FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectCloseMountainResult("sect_missing", actor_id, sect_id)

                result = SectCloseMountainResult(
                    "", actor_id, sect_id, str(sect[1] or "")
                )
                if int(sect[2] or 0) == 1:
                    conn.rollback()
                    return SectCloseMountainResult(
                        "already_closed", actor_id, sect_id, result.sect_name
                    )
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectCloseMountainResult(
                        "not_owner", actor_id, sect_id, result.sect_name
                    )

                member = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (former_owner_position, actor_id, sect_id, owner_position),
                )
                sect_update = conn.execute(
                    "UPDATE sects SET join_open=0, closed=1, sect_owner=NULL "
                    "WHERE sect_id=%s AND sect_owner=%s AND COALESCE(closed, 0)=0",
                    (sect_id, actor_id),
                )
                if member.rowcount != 1 or sect_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect owner changed concurrently")
                conn.execute(
                    "INSERT INTO sect_close_mountain_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectCloseMountainResult(
                    "closed", actor_id, sect_id, result.sect_name
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectOwnerInheritResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    actor_name: str = ""
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"inherited", "duplicate"}

class SectOwnerInheritService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_owner_inherit_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def inherit(
        self,
        operation_id,
        actor_id,
        *,
        expected_sect_id: int | None = None,
        eligible_positions: tuple[int, ...] = (1, 2, 6, 7),
        eligible_user_ids: tuple[str, ...] | list[str] | None = None,
        owner_position: int = 0,
    ) -> SectOwnerInheritResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)
        positions = tuple(int(value) for value in eligible_positions)
        if not positions:
            raise ValueError("eligible_positions must not be empty")
        allowed_ids = (
            None
            if eligible_user_ids is None
            else tuple(str(value) for value in eligible_user_ids)
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, u.user_name, s.sect_name "
                    "FROM sect_owner_inherit_operations o "
                    "LEFT JOIN user_xiuxian u ON u.user_id=o.actor_id "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "duplicate",
                        actor_id,
                        int(previous[0]),
                        str(previous[1] or ""),
                        str(previous[2] or ""),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position, user_name FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectOwnerInheritResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectOwnerInheritResult("actor_without_sect", actor_id)
                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectOwnerInheritResult("sect_changed", actor_id, sect_id)

                sect = conn.execute(
                    "SELECT sect_owner, sect_name, closed FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectOwnerInheritResult("sect_missing", actor_id, sect_id)
                result = SectOwnerInheritResult(
                    "", actor_id, sect_id, str(actor[2] or ""), str(sect[1] or "")
                )
                if int(sect[2] or 0) != 1 or sect[0] is not None:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "not_closed", actor_id, sect_id, result.actor_name, result.sect_name
                    )
                if int(actor[1]) not in positions:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "ineligible", actor_id, sect_id, result.actor_name, result.sect_name
                    )
                if allowed_ids is not None and actor_id not in allowed_ids:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "ineligible", actor_id, sect_id, result.actor_name, result.sect_name
                    )

                position_marks = ", ".join("%s" for _ in positions)
                params: list[object] = [sect_id, *positions]
                candidate_sql = (
                    "SELECT user_id FROM user_xiuxian WHERE sect_id=%s "
                    f"AND sect_position IN ({position_marks})"
                )
                if allowed_ids is not None:
                    if not allowed_ids:
                        conn.rollback()
                        return SectOwnerInheritResult(
                            "ineligible", actor_id, sect_id, result.actor_name, result.sect_name
                        )
                    id_marks = ", ".join("%s" for _ in allowed_ids)
                    candidate_sql += f" AND user_id IN ({id_marks})"
                    params.extend(allowed_ids)
                candidate_sql += (
                    " ORDER BY sect_position ASC, "
                    "COALESCE(sect_contribution, 0) DESC, user_id ASC LIMIT 1"
                )
                candidate = conn.execute(candidate_sql, tuple(params)).fetchone()
                if candidate is None or str(candidate[0]) != actor_id:
                    conn.rollback()
                    return SectOwnerInheritResult(
                        "higher_priority", actor_id, sect_id, result.actor_name, result.sect_name
                    )

                member = conn.execute(
                    "UPDATE user_xiuxian SET sect_position=%s "
                    "WHERE user_id=%s AND sect_id=%s AND sect_position=%s",
                    (owner_position, actor_id, sect_id, int(actor[1])),
                )
                sect_update = conn.execute(
                    "UPDATE sects SET sect_owner=%s, closed=0, join_open=1 "
                    "WHERE sect_id=%s AND sect_owner IS NULL AND closed=1",
                    (actor_id, sect_id),
                )
                if member.rowcount != 1 or sect_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect inheritance changed concurrently")
                conn.execute(
                    "INSERT INTO sect_owner_inherit_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectOwnerInheritResult(
                    "inherited", actor_id, sect_id, result.actor_name, result.sect_name
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectShopPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    contribution: int = 0
    materials: int = 0
    purchased: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class SectShopPurchaseService:
    """Atomically exchange sect contribution for a bound shop item."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def purchase(
        self,
        operation_id,
        user_id,
        sect_id,
        item_id,
        item_name,
        item_type,
        quantity,
        unit_cost,
        weekly_limit,
        legacy_purchased,
        max_goods_num,
        week_key=None,
    ) -> SectShopPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id, sect_id = str(user_id), int(sect_id)
        item_id, quantity = int(item_id), int(quantity)
        unit_cost, weekly_limit = int(unit_cost), int(weekly_limit)
        legacy_purchased, max_goods_num = int(legacy_purchased), int(max_goods_num)
        week_key = str(week_key or date.today().strftime("%G-W%V"))
        if not operation_id or quantity <= 0 or min(item_id, sect_id, unit_cost, weekly_limit, legacy_purchased, max_goods_num) < 0:
            raise ValueError("valid operation and purchase parameters are required")
        payload = json.dumps(
            [user_id, sect_id, item_id, str(item_name), str(item_type), quantity, unit_cost, weekly_limit, week_key],
            ensure_ascii=True,
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sect_shop_weekly_purchases ("
                    "user_id TEXT NOT NULL, week_key TEXT NOT NULL, item_id INTEGER NOT NULL, quantity INTEGER NOT NULL, "
                    "PRIMARY KEY (user_id, week_key, item_id))"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sect_shop_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, cost INTEGER NOT NULL, "
                    "contribution INTEGER NOT NULL, materials INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, contribution, materials, purchased "
                    "FROM sect_shop_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return SectShopPurchaseResult("state_changed")
                    return SectShopPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                user = conn.execute(
                    "SELECT sect_id, COALESCE(sect_contribution, 0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                sect = conn.execute(
                    "SELECT COALESCE(sect_materials, 0), COALESCE(closed, 0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if user is None or sect is None or int(user[0] or 0) != sect_id:
                    conn.rollback()
                    return SectShopPurchaseResult("membership_changed")
                # Historical sect_materials can exceed SQLite INTEGER; clamp for safe compare/write.
                sqlite_max = 2**63 - 1

                def _safe_int(value, default=0) -> int:
                    try:
                        number = int(float(value))
                    except (TypeError, ValueError):
                        number = int(default)
                    if number < 0:
                        return 0
                    return min(number, sqlite_max)

                contribution, materials = _safe_int(user[1]), _safe_int(sect[0])
                if int(sect[1]):
                    conn.rollback()
                    return SectShopPurchaseResult("sect_closed", contribution=contribution, materials=materials)

                row = conn.execute(
                    "SELECT quantity FROM sect_shop_weekly_purchases WHERE user_id=%s AND week_key=%s AND item_id=%s",
                    (user_id, week_key, item_id),
                ).fetchone()
                purchased = int(row[0]) if row else legacy_purchased
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return SectShopPurchaseResult("limit_reached", contribution=contribution, materials=materials, purchased=purchased)
                cost = quantity * unit_cost
                if contribution < cost:
                    conn.rollback()
                    return SectShopPurchaseResult("contribution_insufficient", contribution=contribution, materials=materials, purchased=purchased)
                if materials < cost:
                    conn.rollback()
                    return SectShopPurchaseResult("materials_insufficient", contribution=contribution, materials=materials, purchased=purchased)

                inventory_row = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory = int(inventory_row[0]) if inventory_row else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return SectShopPurchaseResult("inventory_full", contribution=contribution, materials=materials, purchased=purchased)

                contribution = _safe_int(contribution - cost)
                materials = _safe_int(materials - cost)
                purchased += quantity
                conn.execute("UPDATE user_xiuxian SET sect_contribution=%s WHERE user_id=%s", (contribution, user_id))
                conn.execute("UPDATE sects SET sect_materials=%s WHERE sect_id=%s", (materials, sect_id))
                now = datetime.now()
                conn.execute(
                    "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                    "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.goods_num",
                    (user_id, item_id, str(item_name), str(item_type), quantity, now, now, quantity),
                )
                conn.execute(
                    "INSERT INTO sect_shop_weekly_purchases (user_id, week_key, item_id, quantity) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (user_id, week_key, item_id) DO UPDATE SET quantity=EXCLUDED.quantity",
                    (user_id, week_key, item_id, purchased),
                )
                conn.execute(
                    "INSERT INTO sect_shop_purchase_operations "
                    "(operation_id, payload, quantity, cost, contribution, materials, purchased) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, payload, quantity, cost, contribution, materials, purchased),
                )
                conn.commit()
                return SectShopPurchaseResult("applied", quantity, cost, contribution, materials, purchased)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectElixirClaimResult:
    status: str
    rewards: tuple[tuple[int, str, str, int], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class SectElixirClaimService:
    """Grant the complete daily elixir-room reward and mark it claimed."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def claim(self, operation_id, user_id, sect_id, contribution_required, materials_required, rewards, max_goods_num):
        operation_id = str(operation_id).strip()
        user_id, sect_id = str(user_id), int(sect_id)
        contribution_required, materials_required = int(contribution_required), int(materials_required)
        max_goods_num = int(max_goods_num)
        normalized = tuple((int(item_id), str(name), str(item_type), int(quantity)) for item_id, name, item_type, quantity in rewards)
        if not operation_id or not normalized or any(item_id < 0 or quantity <= 0 for item_id, _, _, quantity in normalized):
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps([user_id, sect_id, contribution_required, materials_required, normalized], ensure_ascii=True)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sect_elixir_claim_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, rewards TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, rewards FROM sect_elixir_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectElixirClaimResult("duplicate", tuple(tuple(item) for item in json.loads(previous[1])))

                user = conn.execute(
                    "SELECT sect_id, sect_position, COALESCE(sect_contribution, 0), COALESCE(sect_elixir_get, 0) "
                    "FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                sect = conn.execute(
                    "SELECT COALESCE(elixir_room_level, 0), COALESCE(sect_materials, 0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if user is None or sect is None or int(user[0] or 0) != sect_id:
                    conn.rollback()
                    return SectElixirClaimResult("membership_changed")
                if int(user[1] if user[1] is not None else 15) == 15:
                    conn.rollback()
                    return SectElixirClaimResult("position_ineligible")
                if int(sect[0]) <= 0:
                    conn.rollback()
                    return SectElixirClaimResult("room_missing")
                if int(user[2]) < contribution_required:
                    conn.rollback()
                    return SectElixirClaimResult("contribution_insufficient")
                if int(sect[1]) < materials_required:
                    conn.rollback()
                    return SectElixirClaimResult("materials_insufficient")
                if int(user[3]) == 1:
                    conn.rollback()
                    return SectElixirClaimResult("already_claimed")

                for item_id, _, _, quantity in normalized:
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(row[0]) if row else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return SectElixirClaimResult("inventory_full")

                now = datetime.now()
                for item_id, name, item_type, quantity in normalized:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.goods_num",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                if conn.execute(
                    "UPDATE user_xiuxian SET sect_elixir_get=1 WHERE user_id=%s AND COALESCE(sect_elixir_get, 0)=0", (user_id,)
                ).rowcount != 1:
                    conn.rollback()
                    return SectElixirClaimResult("already_claimed")
                conn.execute(
                    "INSERT INTO sect_elixir_claim_operations (operation_id, payload, rewards) VALUES (%s, %s, %s)",
                    (operation_id, payload, json.dumps(normalized, ensure_ascii=True)),
                )
                conn.commit()
                return SectElixirClaimResult("applied", normalized)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectOpenJoinResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"opened", "duplicate"}

class SectOpenJoinService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_open_join_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def open(
        self,
        operation_id,
        actor_id,
        *,
        owner_position: int = 0,
        expected_sect_id: int | None = None,
    ) -> SectOpenJoinResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, s.sect_name "
                    "FROM sect_open_join_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectOpenJoinResult(
                        "duplicate", actor_id, int(previous[0]), str(previous[1] or "")
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectOpenJoinResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectOpenJoinResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectOpenJoinResult("sect_changed", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name, join_open, closed "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectOpenJoinResult("sect_missing", actor_id, sect_id)

                sect_name = str(sect[1] or "")
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectOpenJoinResult("not_owner", actor_id, sect_id, sect_name)
                if int(sect[3] or 0) == 1:
                    conn.rollback()
                    return SectOpenJoinResult("sect_closed", actor_id, sect_id, sect_name)
                if int(sect[2] or 0) == 1:
                    conn.rollback()
                    return SectOpenJoinResult("already_open", actor_id, sect_id, sect_name)

                changed = conn.execute(
                    "UPDATE sects SET join_open=1 "
                    "WHERE sect_id=%s AND sect_owner=%s "
                    "AND COALESCE(closed, 0)=0 AND COALESCE(join_open, 0)=0",
                    (sect_id, actor_id),
                )
                if changed.rowcount != 1:
                    raise db_backend.IntegrityError("sect join state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_open_join_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectOpenJoinResult("opened", actor_id, sect_id, sect_name)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectCloseJoinResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"closed", "duplicate"}

class SectCloseJoinService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_close_join_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def close(
        self,
        operation_id,
        actor_id,
        *,
        owner_position: int = 0,
        expected_sect_id: int | None = None,
    ) -> SectCloseJoinResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.sect_id, s.sect_name "
                    "FROM sect_close_join_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectCloseJoinResult(
                        "duplicate", actor_id, int(previous[0]), str(previous[1] or "")
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectCloseJoinResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectCloseJoinResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectCloseJoinResult("sect_changed", actor_id, sect_id)
                sect = conn.execute(
                    "SELECT sect_owner, sect_name, join_open, closed "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectCloseJoinResult("sect_missing", actor_id, sect_id)

                sect_name = str(sect[1] or "")
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectCloseJoinResult("not_owner", actor_id, sect_id, sect_name)
                if int(sect[3] or 0) == 1:
                    conn.rollback()
                    return SectCloseJoinResult("sect_closed", actor_id, sect_id, sect_name)
                if int(sect[2] or 0) == 0:
                    conn.rollback()
                    return SectCloseJoinResult("already_closed", actor_id, sect_id, sect_name)

                changed = conn.execute(
                    "UPDATE sects SET join_open=0 "
                    "WHERE sect_id=%s AND sect_owner=%s "
                    "AND COALESCE(closed, 0)=0 AND COALESCE(join_open, 0)=1",
                    (sect_id, actor_id),
                )
                if changed.rowcount != 1:
                    raise db_backend.IntegrityError("sect join state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_close_join_operations "
                    "(operation_id, actor_id, sect_id) VALUES (%s, %s, %s)",
                    (operation_id, actor_id, sect_id),
                )
                conn.commit()
                return SectCloseJoinResult("closed", actor_id, sect_id, sect_name)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectMemberJoinResult:
    status: str
    user_id: str
    sect_id: int
    sect_name: str = ""
    member_count: int = 0
    member_limit: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"joined", "duplicate"}

class SectMemberJoinService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _member_limit(sect_scale: int) -> int:
        return min(20 + max(0, int(sect_scale)) // 50_000_000, 100)

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_member_join_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                member_count INTEGER NOT NULL,
                member_limit INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def join(
        self,
        operation_id,
        user_id,
        sect_id,
        *,
        member_position: int = 12,
    ) -> SectMemberJoinResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT o.user_id, o.sect_id, s.sect_name, "
                    "o.member_count, o.member_limit "
                    "FROM sect_member_join_operations o "
                    "LEFT JOIN sects s ON s.sect_id=o.sect_id "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != sect_id:
                        return SectMemberJoinResult("operation_conflict", user_id, sect_id)
                    return SectMemberJoinResult(
                        "duplicate",
                        user_id,
                        sect_id,
                        str(previous[2] or ""),
                        int(previous[3]),
                        int(previous[4]),
                    )

                user = conn.execute(
                    "SELECT sect_id FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return SectMemberJoinResult("user_missing", user_id, sect_id)
                if user[0] is not None:
                    conn.rollback()
                    return SectMemberJoinResult("already_in_sect", user_id, sect_id)

                sect = conn.execute(
                    "SELECT sect_name, sect_scale, join_open, closed "
                    "FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectMemberJoinResult("sect_missing", user_id, sect_id)
                sect_name = str(sect[0] or "")
                if int(sect[3] or 0) == 1:
                    conn.rollback()
                    return SectMemberJoinResult("sect_closed", user_id, sect_id, sect_name)
                if int(sect[2] or 0) != 1:
                    conn.rollback()
                    return SectMemberJoinResult("join_closed", user_id, sect_id, sect_name)

                member_limit = self._member_limit(int(sect[1] or 0))
                member_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=%s", (sect_id,)
                    ).fetchone()[0]
                )
                if member_count >= member_limit:
                    conn.rollback()
                    return SectMemberJoinResult(
                        "sect_full", user_id, sect_id, sect_name, member_count, member_limit
                    )

                changed = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=%s, sect_position=%s "
                    "WHERE user_id=%s AND sect_id IS NULL",
                    (sect_id, member_position, user_id),
                )
                if changed.rowcount != 1:
                    raise db_backend.IntegrityError("user sect changed concurrently")
                member_count += 1
                conn.execute(
                    "INSERT INTO sect_member_join_operations "
                    "(operation_id, user_id, sect_id, member_count, member_limit) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, member_count, member_limit),
                )
                conn.commit()
                return SectMemberJoinResult(
                    "joined", user_id, sect_id, sect_name, member_count, member_limit
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectMainBuffLearnResult:
    status: str
    user_id: str
    sect_id: int
    buff_id: int
    materials_cost: int = 0
    materials_left: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "learned"

class SectMainBuffLearnService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_mainbuff_learn_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_id INTEGER NOT NULL, "
            "buff_id INTEGER NOT NULL, materials_cost INTEGER NOT NULL, materials_left INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def learn(
        self,
        operation_id,
        user_id,
        sect_id,
        buff_id,
        materials_cost,
        *,
        expected_catalog,
        forbidden_positions=(12, 14, 15),
    ) -> SectMainBuffLearnResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)
        buff_id = int(buff_id)
        materials_cost = int(materials_cost)
        if materials_cost < 0:
            raise ValueError("materials_cost must not be negative")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT user_id, sect_id, buff_id, materials_cost, materials_left "
                    "FROM sect_mainbuff_learn_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if (str(previous[0]), int(previous[1]), int(previous[2])) != (
                        user_id,
                        sect_id,
                        buff_id,
                    ):
                        return SectMainBuffLearnResult("state_changed", user_id, sect_id, buff_id)
                    return SectMainBuffLearnResult(
                        "duplicate", user_id, sect_id, buff_id, int(previous[3]), int(previous[4])
                    )

                user = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return SectMainBuffLearnResult("membership_changed", user_id, sect_id, buff_id)
                if int(user[1]) in {int(value) for value in forbidden_positions}:
                    conn.rollback()
                    return SectMainBuffLearnResult("position_forbidden", user_id, sect_id, buff_id)

                sect = conn.execute(
                    "SELECT mainbuff, COALESCE(sect_materials, 0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None or str(sect[0]) != str(expected_catalog):
                    conn.rollback()
                    return SectMainBuffLearnResult("catalog_changed", user_id, sect_id, buff_id)
                materials = int(sect[1])
                if materials < materials_cost:
                    conn.rollback()
                    return SectMainBuffLearnResult(
                        "materials_insufficient", user_id, sect_id, buff_id, materials_cost, materials
                    )

                buff = conn.execute(
                    "SELECT main_buff FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return SectMainBuffLearnResult("buff_missing", user_id, sect_id, buff_id)
                if int(buff[0] or 0) == buff_id:
                    conn.rollback()
                    return SectMainBuffLearnResult("already_learned", user_id, sect_id, buff_id)

                materials_left = materials - materials_cost
                sect_update = conn.execute(
                    "UPDATE sects SET sect_materials=%s WHERE sect_id=%s AND sect_materials=%s AND mainbuff=%s",
                    (materials_left, sect_id, materials, str(expected_catalog)),
                )
                buff_update = conn.execute(
                    "UPDATE BuffInfo SET main_buff=%s WHERE user_id=%s AND COALESCE(main_buff, 0)<>%s",
                    (buff_id, user_id, buff_id),
                )
                if sect_update.rowcount != 1 or buff_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect main buff learning state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_mainbuff_learn_operations "
                    "(operation_id, user_id, sect_id, buff_id, materials_cost, materials_left) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, buff_id, materials_cost, materials_left),
                )
                conn.commit()
                return SectMainBuffLearnResult(
                    "learned", user_id, sect_id, buff_id, materials_cost, materials_left
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectSecBuffLearnResult:
    status: str
    user_id: str
    sect_id: int
    buff_id: int
    materials_cost: int = 0
    materials_left: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "learned"

class SectSecBuffLearnService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_secbuff_learn_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_id INTEGER NOT NULL, "
            "buff_id INTEGER NOT NULL, materials_cost INTEGER NOT NULL, materials_left INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def learn(
        self,
        operation_id,
        user_id,
        sect_id,
        buff_id,
        materials_cost,
        *,
        expected_catalog,
        forbidden_positions=(12, 14, 15),
    ) -> SectSecBuffLearnResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        sect_id = int(sect_id)
        buff_id = int(buff_id)
        materials_cost = int(materials_cost)
        if materials_cost < 0:
            raise ValueError("materials_cost must not be negative")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT user_id, sect_id, buff_id, materials_cost, materials_left "
                    "FROM sect_secbuff_learn_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if (str(previous[0]), int(previous[1]), int(previous[2])) != (
                        user_id,
                        sect_id,
                        buff_id,
                    ):
                        return SectSecBuffLearnResult("state_changed", user_id, sect_id, buff_id)
                    return SectSecBuffLearnResult(
                        "duplicate", user_id, sect_id, buff_id, int(previous[3]), int(previous[4])
                    )

                user = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return SectSecBuffLearnResult("membership_changed", user_id, sect_id, buff_id)
                if int(user[1]) in {int(value) for value in forbidden_positions}:
                    conn.rollback()
                    return SectSecBuffLearnResult("position_forbidden", user_id, sect_id, buff_id)

                sect = conn.execute(
                    "SELECT secbuff, COALESCE(sect_materials, 0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None or str(sect[0]) != str(expected_catalog):
                    conn.rollback()
                    return SectSecBuffLearnResult("catalog_changed", user_id, sect_id, buff_id)
                materials = int(sect[1])
                if materials < materials_cost:
                    conn.rollback()
                    return SectSecBuffLearnResult(
                        "materials_insufficient", user_id, sect_id, buff_id, materials_cost, materials
                    )

                buff = conn.execute(
                    "SELECT sec_buff FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return SectSecBuffLearnResult("buff_missing", user_id, sect_id, buff_id)
                if int(buff[0] or 0) == buff_id:
                    conn.rollback()
                    return SectSecBuffLearnResult("already_learned", user_id, sect_id, buff_id)

                materials_left = materials - materials_cost
                sect_update = conn.execute(
                    "UPDATE sects SET sect_materials=%s WHERE sect_id=%s AND sect_materials=%s AND secbuff=%s",
                    (materials_left, sect_id, materials, str(expected_catalog)),
                )
                buff_update = conn.execute(
                    "UPDATE BuffInfo SET sec_buff=%s WHERE user_id=%s AND COALESCE(sec_buff, 0)<>%s",
                    (buff_id, user_id, buff_id),
                )
                if sect_update.rowcount != 1 or buff_update.rowcount != 1:
                    raise db_backend.IntegrityError("sect secondary buff learning state changed concurrently")
                conn.execute(
                    "INSERT INTO sect_secbuff_learn_operations "
                    "(operation_id, user_id, sect_id, buff_id, materials_cost, materials_left) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, buff_id, materials_cost, materials_left),
                )
                conn.commit()
                return SectSecBuffLearnResult(
                    "learned", user_id, sect_id, buff_id, materials_cost, materials_left
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectDisbandResult:
    status: str
    actor_id: str
    sect_id: int | None = None
    sect_name: str = ""
    member_count: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"disbanded", "duplicate"}

@dataclass(frozen=True)
class SectInactiveDisbandResult:
    status: str
    sect_id: int
    sect_name: str = ""
    reason: str = ""
    member_count: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"disbanded", "duplicate"}

class SectDisbandService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_disband_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                sect_id INTEGER NOT NULL,
                sect_name TEXT NOT NULL DEFAULT '',
                member_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_inactive_operations(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_inactive_disband_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sect_id INTEGER NOT NULL,"
            "sect_name TEXT NOT NULL,reason TEXT NOT NULL,member_count INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _parse_timestamp(value) -> datetime | None:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    def disband(
        self,
        operation_id,
        actor_id,
        *,
        expected_sect_id: int | None = None,
        owner_position: int = 0,
    ) -> SectDisbandResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        actor_id = str(actor_id)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT sect_id, sect_name, member_count "
                    "FROM sect_disband_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectDisbandResult(
                        "duplicate",
                        actor_id,
                        int(previous[0]),
                        str(previous[1] or ""),
                        int(previous[2] or 0),
                    )

                actor = conn.execute(
                    "SELECT sect_id, sect_position FROM user_xiuxian WHERE user_id=%s",
                    (actor_id,),
                ).fetchone()
                if actor is None:
                    conn.rollback()
                    return SectDisbandResult("actor_missing", actor_id)
                if actor[0] is None:
                    conn.rollback()
                    return SectDisbandResult("actor_without_sect", actor_id)

                sect_id = int(actor[0])
                if expected_sect_id is not None and sect_id != int(expected_sect_id):
                    conn.rollback()
                    return SectDisbandResult("sect_changed", actor_id, sect_id)

                sect = conn.execute(
                    "SELECT sect_owner, sect_name FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectDisbandResult("sect_missing", actor_id, sect_id)

                sect_name = str(sect[1] or "")
                if str(sect[0]) != actor_id or int(actor[1]) != int(owner_position):
                    conn.rollback()
                    return SectDisbandResult("not_owner", actor_id, sect_id, sect_name)

                member_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=%s", (sect_id,)
                    ).fetchone()[0]
                )
                members = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=NULL, sect_position=NULL, "
                    "sect_contribution=0 WHERE sect_id=%s",
                    (sect_id,),
                )
                deleted = conn.execute(
                    "DELETE FROM sects WHERE sect_id=%s AND sect_owner=%s",
                    (sect_id, actor_id),
                )
                if members.rowcount != member_count or deleted.rowcount != 1:
                    raise db_backend.IntegrityError("sect ownership or membership changed")

                conn.execute(
                    "INSERT INTO sect_disband_operations "
                    "(operation_id, actor_id, sect_id, sect_name, member_count) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, actor_id, sect_id, sect_name, member_count),
                )
                conn.commit()
                return SectDisbandResult(
                    "disbanded", actor_id, sect_id, sect_name, member_count
                )
            except Exception:
                conn.rollback()
                raise

    def disband_inactive(
        self,
        operation_id,
        sect_id,
        reason,
        *,
        expected_sect_name,
        expected_owner_id,
        expected_closed,
        expected_member_ids,
        expected_active_candidate_ids,
        checked_at,
        inactivity_days,
    ) -> SectInactiveDisbandResult:
        operation_id = str(operation_id).strip()
        sect_id = int(sect_id)
        reason = str(reason).strip()
        expected_sect_name = str(expected_sect_name)
        expected_owner_id = (
            None if expected_owner_id in (None, "") else str(expected_owner_id)
        )
        expected_closed = bool(expected_closed)
        member_ids = tuple(sorted({str(user_id) for user_id in expected_member_ids}))
        active_candidate_ids = tuple(
            sorted({str(user_id) for user_id in expected_active_candidate_ids})
        )
        checked_at_value = self._parse_timestamp(checked_at)
        inactivity_days = int(inactivity_days)
        if (
            not operation_id
            or sect_id <= 0
            or reason not in {"empty", "no_active_successor", "inactive_sole_owner"}
            or checked_at_value is None
            or inactivity_days <= 0
        ):
            raise ValueError("invalid inactive sect disband request")
        checked_at_text = checked_at_value.isoformat(sep=" ")
        payload = json.dumps(
            {
                "reason": reason,
                "sect": [
                    sect_id,
                    expected_sect_name,
                    expected_owner_id,
                    int(expected_closed),
                ],
                "member_ids": member_ids,
                "active_candidate_ids": active_candidate_ids,
                "checked_at": checked_at_text,
                "inactivity_days": inactivity_days,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_inactive_operations(conn)
                previous = conn.execute(
                    "SELECT payload,sect_name,reason,member_count "
                    "FROM sect_inactive_disband_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return SectInactiveDisbandResult(
                            "operation_conflict",
                            sect_id,
                            str(previous[1]),
                            str(previous[2]),
                            int(previous[3]),
                        )
                    return SectInactiveDisbandResult(
                        "duplicate",
                        sect_id,
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                    )

                sect = conn.execute(
                    "SELECT sect_name,sect_owner,COALESCE(closed,0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if sect is None:
                    conn.rollback()
                    return SectInactiveDisbandResult("sect_missing", sect_id, reason=reason)
                sect_name = str(sect[0] or "")
                owner_id = None if sect[1] in (None, "") else str(sect[1])
                closed = bool(int(sect[2] or 0))
                if (
                    sect_name != expected_sect_name
                    or owner_id != expected_owner_id
                    or closed != expected_closed
                ):
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "sect_changed", sect_id, sect_name, reason
                    )

                members = conn.execute(
                    "SELECT user_id,sect_position FROM user_xiuxian "
                    "WHERE sect_id=%s ORDER BY user_id",
                    (sect_id,),
                ).fetchall()
                current_member_ids = tuple(str(row[0]) for row in members)
                if current_member_ids != member_ids:
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "members_changed", sect_id, sect_name, reason, len(members)
                    )

                last_active_by_user = {}
                if conn.table_exists("user_cd"):
                    last_active_by_user = {
                        str(row[0]): self._parse_timestamp(row[1])
                        for row in conn.execute(
                            "SELECT user_id,last_check_info_time FROM user_cd "
                            "WHERE user_id IN (SELECT user_id FROM user_xiuxian WHERE sect_id=%s)",
                            (sect_id,),
                        ).fetchall()
                    }
                current_active_candidates = tuple(
                    sorted(
                        str(row[0])
                        for row in members
                        if row[1] is not None
                        and int(row[1]) != 0
                        and last_active_by_user.get(str(row[0])) is not None
                        and (
                            checked_at_value - last_active_by_user[str(row[0])]
                        ).days
                        <= inactivity_days
                    )
                )
                if current_active_candidates != active_candidate_ids:
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "candidates_changed", sect_id, sect_name, reason, len(members)
                    )

                if reason == "empty":
                    valid_reason = closed and not members
                elif reason == "no_active_successor":
                    valid_reason = closed and bool(members) and not current_active_candidates
                else:
                    sole_owner = (
                        not closed
                        and owner_id is not None
                        and len(members) == 1
                        and str(members[0][0]) == owner_id
                        and int(members[0][1]) == 0
                    )
                    owner_last_active = last_active_by_user.get(owner_id or "")
                    valid_reason = (
                        sole_owner
                        and owner_last_active is not None
                        and (checked_at_value - owner_last_active).days
                        >= inactivity_days
                    )
                if not valid_reason:
                    conn.rollback()
                    return SectInactiveDisbandResult(
                        "condition_changed", sect_id, sect_name, reason, len(members)
                    )

                cleared = conn.execute(
                    "UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 "
                    "WHERE sect_id=%s",
                    (sect_id,),
                )
                deleted = conn.execute("DELETE FROM sects WHERE sect_id=%s", (sect_id,))
                if cleared.rowcount != len(members) or deleted.rowcount != 1:
                    raise db_backend.IntegrityError("inactive sect snapshot changed")
                conn.execute(
                    "INSERT INTO sect_inactive_disband_operations("
                    "operation_id,payload,sect_id,sect_name,reason,member_count) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, sect_id, sect_name, reason, len(members)),
                )
                conn.commit()
                return SectInactiveDisbandResult(
                    "disbanded", sect_id, sect_name, reason, len(members)
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectMaintenanceOutcome:
    sect_id: int
    sect_name: str
    status: str
    from_level: int
    to_level: int
    materials_cost: int

@dataclass(frozen=True)
class SectDailyResetResult:
    status: str
    business_date: str
    user_count: int = 0
    outcomes: tuple[SectMaintenanceOutcome, ...] = ()

    @property
    def applied(self) -> bool:
        return self.status in {"applied", "duplicate"}

class SectDailyResetMaintenanceService:
    """Reset daily sect counters and settle all elixir-room maintenance."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_daily_reset_operations (
                business_date TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                user_count INTEGER NOT NULL,
                outcomes TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _normalize_costs(costs_by_level) -> dict[int, int]:
        costs = {
            int(level): max(int(cost), 0)
            for level, cost in dict(costs_by_level).items()
        }
        if not costs or any(level <= 0 for level in costs):
            raise ValueError("positive elixir-room levels are required")
        return costs

    @staticmethod
    def _decode_outcomes(raw: str) -> tuple[SectMaintenanceOutcome, ...]:
        return tuple(SectMaintenanceOutcome(**item) for item in json.loads(raw))

    def settle(self, business_date, costs_by_level) -> SectDailyResetResult:
        business_date = str(business_date).strip()
        if not business_date:
            raise ValueError("business_date must not be empty")
        costs = self._normalize_costs(costs_by_level)
        payload = json.dumps(costs, ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT payload, user_count, outcomes "
                    "FROM sect_daily_reset_operations WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if previous:
                    if str(previous[0]) != payload:
                        conn.rollback()
                        return SectDailyResetResult("operation_conflict", business_date)
                    conn.rollback()
                    return SectDailyResetResult(
                        "duplicate",
                        business_date,
                        int(previous[1]),
                        self._decode_outcomes(str(previous[2])),
                    )

                user_count = int(
                    conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]
                )
                conn.execute(
                    "UPDATE user_xiuxian SET sect_task=0, sect_elixir_get=0"
                )

                sects = conn.execute(
                    "SELECT sect_id, COALESCE(sect_name, ''), sect_owner, "
                    "COALESCE(elixir_room_level, 0), COALESCE(sect_materials, 0) "
                    "FROM sects ORDER BY sect_id"
                ).fetchall()
                outcomes: list[SectMaintenanceOutcome] = []
                for row in sects:
                    sect_id = int(row[0])
                    sect_name = str(row[1])
                    owner = row[2]
                    room_level = int(row[3])
                    materials = int(row[4])
                    cost = costs.get(room_level, 0)
                    to_level = room_level

                    if owner is None:
                        status = "inactive"
                    elif room_level <= 0:
                        status = "no_room"
                    elif room_level not in costs:
                        status = "level_unsupported"
                    elif materials >= cost:
                        status = "charged"
                        conn.execute(
                            "UPDATE sects SET sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)-CAST(%s AS REAL) "
                            "WHERE sect_id=%s AND elixir_room_level=%s",
                            (cost, sect_id, room_level),
                        )
                    else:
                        to_level = max(room_level - 1, 0)
                        status = "disabled" if to_level == 0 else "downgraded"
                        conn.execute(
                            "UPDATE sects SET elixir_room_level=%s "
                            "WHERE sect_id=%s AND elixir_room_level=%s",
                            (to_level, sect_id, room_level),
                        )

                    outcomes.append(
                        SectMaintenanceOutcome(
                            sect_id,
                            sect_name,
                            status,
                            room_level,
                            to_level,
                            cost,
                        )
                    )

                outcomes_json = json.dumps(
                    [outcome.__dict__ for outcome in outcomes],
                    ensure_ascii=True,
                    sort_keys=True,
                )
                conn.execute(
                    "INSERT INTO sect_daily_reset_operations "
                    "(business_date, payload, user_count, outcomes) "
                    "VALUES (%s, %s, %s, %s)",
                    (business_date, payload, user_count, outcomes_json),
                )
                conn.commit()
                return SectDailyResetResult(
                    "applied", business_date, user_count, tuple(outcomes)
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class SectWeeklyRewardClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class SectWeeklyRewardClaimService:
    """Claim one or more sect weekly goals in one cross-database transaction."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_weekly_reward_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.boss_limit ("
            "user_id TEXT PRIMARY KEY,integral INTEGER DEFAULT 0)"
        )

    @staticmethod
    def _normalize_goals(goals: Iterable[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
        normalized = []
        goal_keys = set()
        for goal in goals:
            goal_key = str(goal["key"])
            if goal_key in goal_keys:
                raise ValueError(f"duplicate weekly goal: {goal_key}")
            goal_keys.add(goal_key)
            reward = dict(goal.get("rewards") or {})
            items = []
            for item in reward.get("items", ()) or ():
                amount = int(item.get("amount", item.get("num", 1)) or 0)
                if amount <= 0:
                    continue
                items.append(
                    {
                        "id": int(item.get("id") or item.get("goods_id")),
                        "name": str(item.get("name") or ""),
                        "type": str(item.get("type") or "道具"),
                        "amount": amount,
                        "bind_flag": int(item.get("bind_flag", item.get("bind", 1)) or 0),
                    }
                )
            normalized.append(
                {
                    "key": goal_key,
                    "name": str(goal.get("name") or goal["key"]),
                    "target": int(goal["target"]),
                    "rewards": {
                        "items": sorted(items, key=lambda row: row["id"]),
                        "stone": max(0, int(reward.get("stone", 0) or 0)),
                        "exp": max(0, int(reward.get("exp", 0) or 0)),
                        "sect_contribution": max(0, int(reward.get("sect_contribution", 0) or 0)),
                        "sect_scale": max(0, int(reward.get("sect_scale", 0) or 0)),
                        "sect_materials": max(0, int(reward.get("sect_materials", 0) or 0)),
                        "boss_integral": max(0, int(reward.get("boss_integral", 0) or 0)),
                    },
                }
            )
        return tuple(sorted(normalized, key=lambda row: row["key"]))

    @staticmethod
    def _format_reward(reward: dict[str, Any]) -> str:
        parts = [f"{item['name']}x{item['amount']}" for item in reward["items"]]
        labels = (
            ("stone", "灵石"),
            ("exp", "修为"),
            ("sect_contribution", "宗门贡献"),
            ("sect_scale", "宗门建设度"),
            ("sect_materials", "宗门资材"),
            ("boss_integral", "BOSS积分"),
        )
        parts.extend(f"{label}{reward[key]}" for key, label in labels if reward[key] > 0)
        return "、".join(parts) if parts else "无"

    @staticmethod
    def _grant_items(conn, user_id: str, items: Iterable[dict[str, Any]], max_goods_num: int) -> bool:
        totals: dict[int, dict[str, Any]] = {}
        for item in items:
            item_id = int(item["id"])
            current = totals.get(item_id)
            if current is None:
                totals[item_id] = dict(item)
                continue
            if current["name"] != item["name"] or current["type"] != item["type"]:
                raise ValueError(f"conflicting metadata for item {item_id}")
            current["amount"] += int(item["amount"])
            current["bind_flag"] = min(int(current["bind_flag"]), int(item["bind_flag"]))

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item_id, item in totals.items():
            row = conn.execute(
                "SELECT goods_num,COALESCE(bind_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                (user_id, item_id),
            ).fetchone()
            current_num = int(row[0] or 0) if row else 0
            if current_num + int(item["amount"]) > max_goods_num:
                return False
            bind_delta = int(item["amount"]) if int(item["bind_flag"]) == 1 else 0
            if row:
                conn.execute(
                    "UPDATE back SET goods_name=%s,goods_type=%s,goods_num=COALESCE(goods_num,0)+%s,"
                    "bind_num=COALESCE(bind_num,0)+%s,update_time=%s WHERE user_id=%s AND goods_id=%s",
                    (item["name"], item["type"], item["amount"], bind_delta, now, user_id, item_id),
                )
            else:
                conn.execute(
                    "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (user_id, item_id, item["name"], item["type"], item["amount"], now, now, bind_delta),
                )
        return True

    def claim(
        self,
        operation_id: str,
        user_id: str | int,
        sect_id: str | int,
        week_key: str,
        goals: Iterable[dict[str, Any]],
        max_goods_num: int,
    ) -> SectWeeklyRewardClaimResult:
        operation_id = str(operation_id).strip()
        user_id, sect_id, week_key = str(user_id), int(sect_id), str(week_key).strip()
        max_goods_num = int(max_goods_num)
        goals = self._normalize_goals(goals)
        if not operation_id or not week_key or not goals or max_goods_num < 0:
            raise ValueError("valid operation, week, goals and inventory limit are required")
        payload = self._json([user_id, sect_id, week_key, goals, max_goods_num])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM sect_weekly_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    if str(previous[0]) != payload:
                        conn.rollback()
                        return SectWeeklyRewardClaimResult("operation_conflict")
                    result = json.loads(str(previous[1]))
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("duplicate", tuple(tuple(row) for row in result))

                user = conn.execute(
                    "SELECT sect_id FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if not user:
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("user_missing")
                if user[0] is None or int(user[0]) != sect_id:
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("sect_changed")
                if not conn.execute("SELECT 1 FROM sects WHERE sect_id=%s", (sect_id,)).fetchone():
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("sect_missing")

                rows = {}
                for goal in goals:
                    row = conn.execute(
                        "SELECT progress,target,claimed_users FROM sect_weekly_goal "
                        "WHERE sect_id=%s AND week_key=%s AND goal_key=%s",
                        (sect_id, week_key, goal["key"]),
                    ).fetchone()
                    if not row or int(row[0] or 0) < int(goal["target"]) or int(row[1]) != int(goal["target"]):
                        conn.rollback()
                        return SectWeeklyRewardClaimResult("not_completed")
                    claimed = [str(value) for value in json.loads(str(row[2] or "[]"))]
                    if user_id in claimed:
                        conn.rollback()
                        return SectWeeklyRewardClaimResult("already_claimed")
                    rows[goal["key"]] = claimed

                all_items = [item for goal in goals for item in goal["rewards"]["items"]]
                if not self._grant_items(conn, user_id, all_items, max_goods_num):
                    conn.rollback()
                    return SectWeeklyRewardClaimResult("inventory_full")

                totals = {
                    key: sum(goal["rewards"][key] for goal in goals)
                    for key in ("stone", "exp", "sect_contribution", "sect_scale", "sect_materials", "boss_integral")
                }
                conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL),exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),"
                    "sect_contribution=CAST(COALESCE(sect_contribution,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                    (totals["stone"], totals["exp"], totals["sect_contribution"], user_id),
                )
                conn.execute(
                    "UPDATE sects SET sect_scale=CAST(COALESCE(sect_scale,0) AS REAL)+CAST(%s AS REAL),"
                    "sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+CAST(%s AS REAL) WHERE sect_id=%s",
                    (totals["sect_scale"], totals["sect_materials"], sect_id),
                )
                conn.execute(
                    "INSERT INTO player_data.boss_limit(user_id,integral) VALUES(%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET integral=COALESCE(integral,0)+EXCLUDED.integral",
                    (user_id, totals["boss_integral"]),
                )

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for goal in goals:
                    claimed = rows[goal["key"]]
                    claimed.append(user_id)
                    conn.execute(
                        "UPDATE sect_weekly_goal SET claimed_users=%s,updated_at=%s "
                        "WHERE sect_id=%s AND week_key=%s AND goal_key=%s",
                        (self._json(claimed), now, sect_id, week_key, goal["key"]),
                    )
                result = tuple((goal["name"], self._format_reward(goal["rewards"])) for goal in goals)
                conn.execute(
                    "INSERT INTO sect_weekly_reward_operations(operation_id,payload,result_json,created_at) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, self._json(result), now),
                )
                conn.commit()
                return SectWeeklyRewardClaimResult("applied", result)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "SectOwnerTransfer",
    "SectFairylandUpgrade",
    "SectElixirRoomUpgrade",
    "SectBuffSearch",
    "SectPracticeUpgrade",
    "SectScheduledMaterialGrant",
    "SectElixirRoomMaintenance",
    "SectDonation",
    "SectTaskSettlement",
    "SectTaskClaim",
    "SectCreation",
    "SectNameRefresh",
    "SectRename",
    "SectMemberRemoval",
    "SectPositionChange",
    "SectMembershipService",
    "FairylandClaimResult",
    "FairylandClaimService",
    "SectCloseMountainResult",
    "SectCloseMountainService",
    "SectOwnerInheritResult",
    "SectOwnerInheritService",
    "SectShopPurchaseResult",
    "SectShopPurchaseService",
    "SectElixirClaimResult",
    "SectElixirClaimService",
    "SectOpenJoinResult",
    "SectOpenJoinService",
    "SectCloseJoinResult",
    "SectCloseJoinService",
    "SectMemberJoinResult",
    "SectMemberJoinService",
    "SectMainBuffLearnResult",
    "SectMainBuffLearnService",
    "SectSecBuffLearnResult",
    "SectSecBuffLearnService",
    "SectDisbandResult",
    "SectInactiveDisbandResult",
    "SectDisbandService",
    "SectMaintenanceOutcome",
    "SectDailyResetResult",
    "SectDailyResetMaintenanceService",
    "SectWeeklyRewardClaimResult",
    "SectWeeklyRewardClaimService",
]
