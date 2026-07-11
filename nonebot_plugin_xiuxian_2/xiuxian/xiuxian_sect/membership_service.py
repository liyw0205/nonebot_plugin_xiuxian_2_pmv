from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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
                    SELECT sect_id, status FROM sect_task_state
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
    "SectDonation",
    "SectElixirRoomMaintenance",
    "SectElixirRoomUpgrade",
    "SectFairylandUpgrade",
    "SectMembershipService",
    "SectOwnerTransfer",
    "SectPracticeUpgrade",
    "SectScheduledMaterialGrant",
    "SectTaskSettlement",
]
