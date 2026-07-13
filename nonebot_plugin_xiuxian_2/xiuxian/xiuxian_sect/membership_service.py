from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.json_store import safe_json_dumps, safe_json_loads


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
                    "UPDATE sects SET sect_name=%s, sect_used_stone=sect_used_stone-%s "
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
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND sect_id IS NULL AND stone >= %s",
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
                    SET sect_id=%s, sect_position=%s, stone=stone-%s
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
                        exp=COALESCE(exp, 0)+%s,
                        sect_task=COALESCE(sect_task, 0)+1,
                        sect_contribution=COALESCE(sect_contribution, 0)+%s
                    WHERE user_id=%s AND sect_id=%s AND {cost_column} >= %s
                    """,
                    (cost, exp_reward, sect_reward, user_id, sect_id, cost),
                )
                sect_update = conn.execute(
                    """
                    UPDATE sects
                    SET sect_used_stone=COALESCE(sect_used_stone, 0)+%s,
                        sect_scale=COALESCE(sect_scale, 0)+%s,
                        sect_materials=COALESCE(sect_materials, 0)+%s
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
                    SET stone=stone-%s,
                        sect_contribution=COALESCE(sect_contribution, 0)+%s
                    WHERE user_id=%s AND sect_id=%s AND stone >= %s
                    """,
                    (stone, stone, user_id, sect_id, stone),
                )
                sect_update = conn.execute(
                    """
                    UPDATE sects
                    SET sect_used_stone=COALESCE(sect_used_stone, 0)+%s,
                        sect_scale=COALESCE(sect_scale, 0)+%s,
                        sect_materials=COALESCE(sect_materials, 0)+%s
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
                        SET sect_materials=sect_materials-%s
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
                    SET sect_materials=COALESCE(sect_materials, 0)+%s,
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
                    "UPDATE sects SET sect_used_stone=sect_used_stone-%s, "
                    "sect_materials=sect_materials-%s, sect_fairyland=%s "
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
                    "UPDATE sects SET sect_used_stone=sect_used_stone-%s, "
                    "sect_scale=sect_scale-%s, elixir_room_level=%s "
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
                    f"UPDATE sects SET sect_used_stone=sect_used_stone-%s, "
                    f"sect_materials=sect_materials-%s, {column}=%s "
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
                    f"UPDATE user_xiuxian SET stone=stone-%s, {column}=%s WHERE user_id=%s",
                    (stone_cost, next_level, user_id),
                )
                conn.execute(
                    "UPDATE sects SET sect_materials=sect_materials-%s WHERE sect_id=%s",
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


__all__ = [
    "SectBuffSearch",
    "SectCreation",
    "SectDonation",
    "SectElixirRoomMaintenance",
    "SectElixirRoomUpgrade",
    "SectFairylandUpgrade",
    "SectMemberRemoval",
    "SectMembershipService",
    "SectNameRefresh",
    "SectPositionChange",
    "SectRename",
    "SectOwnerTransfer",
    "SectPracticeUpgrade",
    "SectScheduledMaterialGrant",
    "SectTaskSettlement",
]
