from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from datetime import datetime
from typing import Callable

@dataclass(frozen=True)
class AdminLevelChangeResult:
    status: str
    level: str = ""
    exp: int = 0
    hp: int = 0
    mp: int = 0
    atk: int = 0
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AdminLevelChangeService:
    """Replace a player's realm and derived combat state in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_level_change_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _snapshot(row) -> tuple:
        return (
            str(row[0] or ""),
            int(row[1] or 0),
            int(row[2] or 0),
            int(row[3] or 0),
            int(row[4] or 0),
            int(row[5] or 0),
            str(row[6] or ""),
            int(row[7] or 0),
        )

    def change(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_snapshot,
        new_level,
        new_exp,
        level_spend,
        root_rate,
    ) -> AdminLevelChangeResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        new_level = str(new_level).strip()
        expected = tuple(expected_snapshot)
        new_exp = int(new_exp)
        level_spend = float(level_spend)
        root_rate = float(root_rate)
        if not operation_id or not operator_id or not user_id or not new_level:
            raise ValueError("operation, operator, user and level are required")
        if len(expected) != 8 or new_exp < 0 or level_spend <= 0 or root_rate <= 0:
            raise ValueError("invalid realm change snapshot or configuration")
        expected = (
            str(expected[0] or ""), int(expected[1] or 0), int(expected[2] or 0),
            int(expected[3] or 0), int(expected[4] or 0), int(expected[5] or 0),
            str(expected[6] or ""), int(expected[7] or 0),
        )
        payload = json.dumps(
            [operator_id, user_id, new_level, new_exp, level_spend, root_rate],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM admin_level_change_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminLevelChangeResult("operation_conflict")
                    return AdminLevelChangeResult("duplicate", **json.loads(str(previous[1])))

                row = conn.execute(
                    "SELECT level,COALESCE(exp,0),COALESCE(hp,0),COALESCE(mp,0),"
                    "COALESCE(atk,0),COALESCE(power,0),COALESCE(root_type,''),"
                    "COALESCE(root_level,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminLevelChangeResult("user_missing")
                if self._snapshot(row) != expected:
                    conn.rollback()
                    return AdminLevelChangeResult("state_changed")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET level=%s,exp=%s,hp=%s/2,mp=%s,atk=%s/10,"
                    "power=ROUND(%s*%s*%s,0) WHERE user_id=%s AND level=%s AND COALESCE(exp,0)=%s "
                    "AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s AND COALESCE(atk,0)=%s "
                    "AND COALESCE(power,0)=%s AND COALESCE(root_type,'')=%s "
                    "AND COALESCE(root_level,0)=%s",
                    (
                        new_level, new_exp, new_exp, new_exp, new_exp, new_exp, root_rate,
                        level_spend, user_id, *expected,
                    ),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminLevelChangeResult("state_changed")
                result_row = conn.execute(
                    "SELECT level,exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                result_data = {
                    "level": str(result_row[0]), "exp": int(result_row[1]),
                    "hp": int(result_row[2]), "mp": int(result_row[3]),
                    "atk": int(result_row[4]), "power": int(result_row[5]),
                }
                conn.execute(
                    "INSERT INTO admin_level_change_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(result_data, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
                return AdminLevelChangeResult("applied", **result_data)
            except Exception:
                conn.rollback()
                raise

ROOT_CHANGES = {
    1: ("全属性灵根", "混沌灵根"),
    2: ("融合万物灵根", "融合灵根"),
    3: ("月灵根", "超灵根"),
    4: ("言灵灵根", "龙灵根"),
    5: ("金灵根", "天灵根"),
    6: ("轮回千次不灭，只为臻至巅峰", "轮回道果"),
    7: ("轮回万次不灭，只为超越巅峰", "真·轮回道果"),
    8: ("轮回无尽不灭，只为触及永恒之境", "永恒道果"),
}

@dataclass(frozen=True)
class AdminRootChangeResult:
    status: str
    root: str = ""
    root_type: str = ""
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AdminRootChangeService:
    """Change a player's root and recompute power atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_root_change_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def root_values(root_id: int, user_name: str) -> tuple[str, str]:
        root_id = int(root_id)
        if root_id == 9:
            return f"轮回命主·{user_name}", "命运道果"
        try:
            return ROOT_CHANGES[root_id]
        except KeyError as exc:
            raise ValueError("root_id must be between 1 and 9") from exc

    def change(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_snapshot,
        root_id,
        level_spend,
        new_root_rate,
    ) -> AdminRootChangeResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        root_id = int(root_id)
        expected = tuple(expected_snapshot)
        level_spend = float(level_spend)
        new_root_rate = float(new_root_rate)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if len(expected) != 7 or level_spend <= 0 or new_root_rate <= 0:
            raise ValueError("invalid root change snapshot or configuration")
        expected = (
            str(expected[0] or ""), str(expected[1] or ""), int(expected[2] or 0),
            str(expected[3] or ""), int(expected[4] or 0), int(expected[5] or 0),
            str(expected[6] or ""),
        )
        new_root, new_root_type = self.root_values(root_id, expected[6])
        payload = json.dumps(
            [operator_id, user_id, root_id, level_spend, new_root_rate],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM admin_root_change_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminRootChangeResult("operation_conflict")
                    return AdminRootChangeResult("duplicate", **json.loads(str(previous[1])))

                row = conn.execute(
                    "SELECT COALESCE(root,''),COALESCE(root_type,''),COALESCE(root_level,0),"
                    "COALESCE(level,''),COALESCE(exp,0),COALESCE(power,0),COALESCE(user_name,'') "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminRootChangeResult("user_missing")
                actual = (
                    str(row[0]), str(row[1]), int(row[2]), str(row[3]), int(row[4]),
                    int(row[5]), str(row[6]),
                )
                if actual != expected:
                    conn.rollback()
                    return AdminRootChangeResult("state_changed")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET root=%s,root_type=%s,power=ROUND(%s*%s*%s,0) "
                    "WHERE user_id=%s AND COALESCE(root,'')=%s AND COALESCE(root_type,'')=%s "
                    "AND COALESCE(root_level,0)=%s AND COALESCE(level,'')=%s "
                    "AND COALESCE(exp,0)=%s AND COALESCE(power,0)=%s AND COALESCE(user_name,'')=%s",
                    (new_root, new_root_type, expected[4], new_root_rate, level_spend, user_id, *expected),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminRootChangeResult("state_changed")
                power = int(conn.execute(
                    "SELECT power FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()[0])
                result_data = {"root": new_root, "root_type": new_root_type, "power": power}
                conn.execute(
                    "INSERT INTO admin_root_change_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(result_data, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
                return AdminRootChangeResult("applied", **result_data)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminExpAdjustmentResult:
    status: str
    previous_exp: int = 0
    final_exp: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}

class AdminExpAdjustmentService:
    """Apply one administrator experience adjustment with its audit record."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_exp_adjustment_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_exp INTEGER NOT NULL,"
            "final_exp INTEGER NOT NULL,applied_delta INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        if not conn.column_exists("economy_log", "trace_id"):
            conn.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")

    def adjust(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_exp,
        requested_delta,
        *,
        target_name="",
    ) -> AdminExpAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_exp = int(expected_exp)
        requested_delta = int(requested_delta)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_exp < 0 or requested_delta == 0:
            raise ValueError("valid experience snapshot and non-zero adjustment are required")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_exp,final_exp,applied_delta "
                    "FROM admin_exp_adjustment_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminExpAdjustmentResult("operation_conflict")
                    return AdminExpAdjustmentResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                row = conn.execute(
                    "SELECT COALESCE(exp,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminExpAdjustmentResult("user_missing")
                actual_exp = int(row[0])
                if actual_exp != expected_exp:
                    conn.rollback()
                    return AdminExpAdjustmentResult("state_changed", actual_exp, actual_exp)

                final_exp = max(0, expected_exp + requested_delta)
                applied_delta = final_exp - expected_exp
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s AND COALESCE(exp,0)=%s",
                    (final_exp, user_id, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminExpAdjustmentResult("state_changed")

                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "requested_delta": requested_delta,
                        "previous_exp": expected_exp,
                        "final_exp": final_exp,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log(user_id,source,action,exp_delta,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin',%s,%s,'[]',%s,%s,CURRENT_TIMESTAMP)",
                    (
                        user_id,
                        "admin_exp_add" if applied_delta > 0 else "admin_exp_cost",
                        applied_delta,
                        detail,
                        operation_id,
                    ),
                )
                conn.execute(
                    "INSERT INTO admin_exp_adjustment_operations("
                    "operation_id,payload,previous_exp,final_exp,applied_delta) VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, expected_exp, final_exp, applied_delta),
                )
                conn.commit()
                return AdminExpAdjustmentResult(
                    "adjusted", expected_exp, final_exp, applied_delta
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminStoneAdjustmentResult:
    status: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}

class AdminStoneAdjustmentService:
    """Apply one administrator stone adjustment and economy audit atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_stone_adjustment_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_stone INTEGER NOT NULL,"
            "final_stone INTEGER NOT NULL,applied_delta INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        if not conn.column_exists("economy_log", "trace_id"):
            conn.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")

    def adjust(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_stone,
        requested_delta,
        *,
        target_name="",
    ) -> AdminStoneAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = int(expected_stone)
        requested_delta = int(requested_delta)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_stone < 0 or requested_delta == 0:
            raise ValueError("valid stone snapshot and non-zero adjustment are required")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_stone,final_stone,applied_delta "
                    "FROM admin_stone_adjustment_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminStoneAdjustmentResult("operation_conflict")
                    return AdminStoneAdjustmentResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                row = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminStoneAdjustmentResult("user_missing")
                actual_stone = int(row[0])
                if actual_stone != expected_stone:
                    conn.rollback()
                    return AdminStoneAdjustmentResult(
                        "state_changed", actual_stone, actual_stone
                    )

                final_stone = max(0, expected_stone + requested_delta)
                applied_delta = final_stone - expected_stone
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s AND COALESCE(stone,0)=%s",
                    (final_stone, user_id, expected_stone),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminStoneAdjustmentResult("state_changed")

                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "requested_delta": requested_delta,
                        "previous_stone": expected_stone,
                        "final_stone": final_stone,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log(user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin',%s,%s,'[]',%s,%s,CURRENT_TIMESTAMP)",
                    (
                        user_id,
                        "admin_stone_add" if applied_delta > 0 else "admin_stone_cost",
                        applied_delta,
                        detail,
                        operation_id,
                    ),
                )
                conn.execute(
                    "INSERT INTO admin_stone_adjustment_operations("
                    "operation_id,payload,previous_stone,final_stone,applied_delta) VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, expected_stone, final_stone, applied_delta),
                )
                conn.commit()
                return AdminStoneAdjustmentResult(
                    "adjusted", expected_stone, final_stone, applied_delta
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminItemGrantResult:
    status: str
    previous_quantity: int = 0
    final_quantity: int = 0
    granted_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"granted", "duplicate"}

class AdminItemGrantService:
    """Grant one ordinary item and persist its economy audit atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_grant_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_quantity INTEGER NOT NULL,final_quantity INTEGER NOT NULL,"
            "granted_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )

    def grant(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        expected_quantity,
        max_goods_num,
        *,
        target_name="",
    ) -> AdminItemGrantResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        expected_quantity = int(expected_quantity)
        max_goods_num = int(max_goods_num)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id or item_id <= 0:
            raise ValueError("operation, operator, user and item are required")
        if quantity <= 0 or expected_quantity < 0 or max_goods_num <= 0:
            raise ValueError("valid quantity snapshot and inventory limit are required")

        payload = json.dumps(
            [
                operator_id,
                user_id,
                item_id,
                item_name,
                item_type,
                quantity,
                max_goods_num,
                target_name,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_quantity,final_quantity,granted_quantity "
                    "FROM admin_item_grant_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminItemGrantResult("operation_conflict")
                    return AdminItemGrantResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return AdminItemGrantResult("user_missing")
                row = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                actual_quantity = int(row[0]) if row else 0
                if actual_quantity != expected_quantity:
                    conn.rollback()
                    return AdminItemGrantResult(
                        "state_changed", actual_quantity, actual_quantity
                    )
                final_quantity = expected_quantity + quantity
                if final_quantity > max_goods_num:
                    conn.rollback()
                    return AdminItemGrantResult(
                        "inventory_full", expected_quantity, expected_quantity
                    )

                now = datetime.now()
                columns = set(conn.column_names("back"))
                if row is None:
                    names = [
                        "user_id", "goods_id", "goods_name", "goods_type",
                        "goods_num", "create_time", "update_time",
                    ]
                    values = [user_id, item_id, item_name, item_type, quantity, now, now]
                    if "bind_num" in columns:
                        names.append("bind_num")
                        values.append(0)
                    placeholders = ",".join(["%s"] * len(values))
                    conn.execute(
                        f"INSERT INTO back({','.join(names)}) VALUES({placeholders})",
                        tuple(values),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE back SET goods_name=%s,goods_type=%s,goods_num=%s,update_time=%s "
                        "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num,0)=%s",
                        (
                            item_name, item_type, final_quantity, now,
                            user_id, item_id, expected_quantity,
                        ),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return AdminItemGrantResult("state_changed")

                item_delta = json.dumps(
                    [{"id": item_id, "name": item_name, "type": item_type, "amount": quantity}],
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "previous_quantity": expected_quantity,
                        "final_quantity": final_quantity,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin','admin_item_add',%s,%s,%s,CURRENT_TIMESTAMP)",
                    (user_id, item_delta, detail, operation_id),
                )
                conn.execute(
                    "INSERT INTO admin_item_grant_operations("
                    "operation_id,payload,previous_quantity,final_quantity,granted_quantity) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, expected_quantity, final_quantity, quantity),
                )
                conn.commit()
                return AdminItemGrantResult(
                    "granted", expected_quantity, final_quantity, quantity
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminItemDestroyResult:
    status: str
    previous_quantity: int = 0
    final_quantity: int = 0
    removed_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"destroyed", "duplicate"}

class AdminItemDestroyService:
    """Remove one ordinary item and persist its economy audit atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_destroy_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_quantity INTEGER NOT NULL,final_quantity INTEGER NOT NULL,"
            "removed_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )

    def destroy(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        expected_quantity,
        *,
        target_name="",
    ) -> AdminItemDestroyResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        expected_quantity = int(expected_quantity)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id or item_id <= 0:
            raise ValueError("operation, operator, user and item are required")
        if quantity <= 0 or expected_quantity < 0:
            raise ValueError("valid quantity and inventory snapshot are required")

        payload = json.dumps(
            [
                operator_id, user_id, item_id, item_name, item_type,
                quantity, target_name,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_quantity,final_quantity,removed_quantity "
                    "FROM admin_item_destroy_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminItemDestroyResult("operation_conflict")
                    return AdminItemDestroyResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return AdminItemDestroyResult("user_missing")
                columns = set(conn.column_names("back"))
                bind_select = ",COALESCE(bind_num,0)" if "bind_num" in columns else ""
                row = conn.execute(
                    "SELECT COALESCE(goods_num,0)" + bind_select +
                    " FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                actual_quantity = int(row[0]) if row else 0
                actual_bind_quantity = int(row[1]) if row and bind_select else 0
                if actual_quantity != expected_quantity:
                    conn.rollback()
                    return AdminItemDestroyResult(
                        "state_changed", actual_quantity, actual_quantity
                    )
                if actual_quantity <= 0:
                    conn.rollback()
                    return AdminItemDestroyResult("item_missing")

                removed_quantity = min(quantity, expected_quantity)
                final_quantity = expected_quantity - removed_quantity
                now = datetime.now()
                bind_update = ""
                if "bind_num" in columns:
                    final_bind_quantity = (
                        max(actual_bind_quantity - removed_quantity, 0)
                        if actual_bind_quantity >= removed_quantity
                        else min(actual_bind_quantity, final_quantity)
                    )
                    bind_update = ",bind_num=%s"
                params = [final_quantity, now]
                if bind_update:
                    params.append(final_bind_quantity)
                params.extend([user_id, item_id, expected_quantity])
                changed = conn.execute(
                    "UPDATE back SET goods_num=%s,update_time=%s" + bind_update +
                    " WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num,0)=%s",
                    tuple(params),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminItemDestroyResult("state_changed")

                item_delta = json.dumps(
                    [{
                        "id": item_id,
                        "name": item_name,
                        "type": item_type,
                        "amount": -removed_quantity,
                    }],
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "requested_quantity": quantity,
                        "previous_quantity": expected_quantity,
                        "final_quantity": final_quantity,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin','admin_item_cost',%s,%s,%s,CURRENT_TIMESTAMP)",
                    (user_id, item_delta, detail, operation_id),
                )
                conn.execute(
                    "INSERT INTO admin_item_destroy_operations("
                    "operation_id,payload,previous_quantity,final_quantity,removed_quantity) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (
                        operation_id, payload, expected_quantity,
                        final_quantity, removed_quantity,
                    ),
                )
                conn.commit()
                return AdminItemDestroyResult(
                    "destroyed", expected_quantity, final_quantity, removed_quantity
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminItemBatchGrantResult:
    status: str
    total: int = 0
    completed: int = 0
    added: int = 0
    granted_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AdminItemBatchGrantService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(operator_id, user_ids, item_id, item_name, item_type, quantity, max_goods_num):
        return json.dumps(
            {
                "request": [
                    str(operator_id),
                    int(item_id),
                    str(item_name),
                    str(item_type),
                    int(quantity),
                    int(max_goods_num),
                ],
                "users": list(user_ids),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _ensure_tables(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_batch_grant_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,added INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_item_batch_grant_progress ("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,added INTEGER NOT NULL,"
            "PRIMARY KEY(operation_id,user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )

    @staticmethod
    def _result(conn, operation_id: str, status: str) -> AdminItemBatchGrantResult:
        row = conn.execute(
            "SELECT total,completed,added FROM admin_item_batch_grant_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        granted_users = conn.execute(
            "SELECT COUNT(*) FROM admin_item_batch_grant_progress WHERE operation_id=%s AND added>0",
            (operation_id,),
        ).fetchone()
        return AdminItemBatchGrantResult(
            status,
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(granted_users[0]),
        )

    def grant(
        self,
        operation_id,
        operator_id,
        user_ids,
        item_id,
        item_name,
        item_type,
        quantity,
        max_goods_num,
        *,
        chunk_size=100,
    ):
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        normalized_users = tuple(sorted({str(user_id) for user_id in user_ids}))
        item_id, quantity, max_goods_num = int(item_id), int(quantity), int(max_goods_num)
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not operator_id
            or not normalized_users
            or item_id <= 0
            or quantity <= 0
            or max_goods_num <= 0
        ):
            raise ValueError("invalid batch grant arguments")
        payload = self._payload(
            operator_id,
            normalized_users,
            item_id,
            item_name,
            item_type,
            quantity,
            max_goods_num,
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                previous = conn.execute(
                    "SELECT payload,total,completed,added,status FROM admin_item_batch_grant_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if previous is None:
                    conn.execute(
                        "INSERT INTO admin_item_batch_grant_operations VALUES (%s,%s,%s,0,0,'running',%s,%s)",
                        (operation_id, payload, len(normalized_users), now, now),
                    )
                else:
                    previous_payload = json.loads(str(previous[0]))
                    current_payload = json.loads(payload)
                    if previous_payload.get("request") != current_payload.get("request"):
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result
                    normalized_users = tuple(str(user_id) for user_id in previous_payload["users"])
                if previous is not None and str(previous[4]) == "completed":
                    result = self._result(conn, operation_id, "duplicate")
                    conn.rollback()
                    return result
                completed_users = {
                    str(row[0]) for row in conn.execute(
                        "SELECT user_id FROM admin_item_batch_grant_progress WHERE operation_id=%s", (operation_id,)
                    ).fetchall()
                }
                pending = [user_id for user_id in normalized_users if user_id not in completed_users][:chunk_size]
                added_total = 0
                columns = set(conn.column_names("back"))
                for user_id in pending:
                    actual_add = 0
                    if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone():
                        row = conn.execute(
                            "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                        ).fetchone()
                        current = int(row[0]) if row else 0
                        actual_add = quantity if current + quantity <= max_goods_num else 0
                        if actual_add:
                            bind_columns = ",bind_num" if "bind_num" in columns else ""
                            bind_values = ",0" if "bind_num" in columns else ""
                            conn.execute(
                                f"INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time{bind_columns}) "
                                f"VALUES (%s,%s,%s,%s,%s,%s,%s{bind_values}) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                                f"goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                                "update_time=excluded.update_time",
                                (user_id, item_id, str(item_name), str(item_type), actual_add, now, now),
                            )
                            item_delta = json.dumps(
                                [{
                                    "id": item_id,
                                    "name": str(item_name),
                                    "type": str(item_type),
                                    "amount": actual_add,
                                }],
                                ensure_ascii=True,
                                separators=(",", ":"),
                            )
                            detail = json.dumps(
                                {
                                    "operator_id": operator_id,
                                    "requested_quantity": quantity,
                                    "previous_quantity": current,
                                    "final_quantity": current + actual_add,
                                    "target": "all",
                                },
                                ensure_ascii=True,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            conn.execute(
                                "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
                                "VALUES(%s,'admin','admin_item_add_all',%s,%s,%s,%s)",
                                (user_id, item_delta, detail, operation_id, now),
                            )
                    conn.execute("INSERT INTO admin_item_batch_grant_progress VALUES (%s,%s,%s)", (operation_id, user_id, actual_add))
                    added_total += actual_add
                completed = len(completed_users) + len(pending)
                status = "completed" if completed >= len(normalized_users) else "running"
                conn.execute(
                    "UPDATE admin_item_batch_grant_operations SET completed=%s,added=added+%s,status=%s,updated_at=%s WHERE operation_id=%s",
                    (completed, added_total, status, now, operation_id),
                )
                result = self._result(conn, operation_id, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminAccessoryAdjustmentResult:
    status: str
    action: str
    user_id: str
    requested_quantity: int = 0
    affected_quantity: int = 0
    accessories: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"granted", "destroyed", "duplicate"}

class AdminAccessoryAdjustmentService:
    """Atomically adjust one player's accessory bag and admin audit."""

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
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_accessory_operations("
            "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.player_accessory("
            "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(player_accessory)"
            ).fetchall()
        }
        for field_name in ("equipped", "bag"):
            if field_name not in columns:
                conn.execute(
                    "ALTER TABLE player_data.player_accessory "
                    f"ADD COLUMN {field_name} TEXT"
                )

    @staticmethod
    def _json(value) -> str:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _load_dict(value) -> dict:
        if not value:
            return {}
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _load_list(value) -> list:
        if not value:
            return []
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, list) else []

    @classmethod
    def _result_from_json(cls, status: str, value: str):
        result = json.loads(value)
        return AdminAccessoryAdjustmentResult(
            status,
            str(result["action"]),
            str(result["user_id"]),
            int(result["requested_quantity"]),
            int(result["affected_quantity"]),
            tuple(result.get("accessories", [])),
        )

    @staticmethod
    def _load_accessories(conn, user_id: str) -> tuple[dict, list]:
        row = conn.execute(
            "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            return {}, []
        return (
            AdminAccessoryAdjustmentService._load_dict(row[0]),
            AdminAccessoryAdjustmentService._load_list(row[1]),
        )

    @staticmethod
    def _save_accessories(conn, user_id: str, equipped: dict, bag: list) -> None:
        conn.execute(
            "INSERT INTO player_data.player_accessory(user_id,equipped,bag) "
            "VALUES(%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
            "equipped=EXCLUDED.equipped,bag=EXCLUDED.bag",
            (
                user_id,
                json.dumps(equipped, ensure_ascii=False),
                json.dumps(bag, ensure_ascii=False),
            ),
        )

    @staticmethod
    def _owned_count(equipped: dict, bag: list) -> int:
        return len(bag) + sum(1 for item in equipped.values() if item)

    @staticmethod
    def _owned_uids(equipped: dict, bag: list) -> set[str] | None:
        accessories = list(bag) + [item for item in equipped.values() if item]
        if any(not isinstance(item, dict) for item in accessories):
            return None
        try:
            if any(
                int(item.get("item_id", 0)) <= 0
                or int(item.get("quality", 0)) not in {1, 2, 3, 4, 5}
                or not str(item.get("name", "")).strip()
                for item in accessories
            ):
                return None
        except (TypeError, ValueError):
            return None
        uids = [str(item.get("uid", "")).strip() for item in accessories]
        if any(not uid for uid in uids) or len(set(uids)) != len(uids):
            return None
        return set(uids)

    @staticmethod
    def _audit(
        conn,
        operation_id: str,
        action: str,
        operator_id: str,
        user_id: str,
        target_name: str,
        item_id: int,
        item_name: str,
        requested_quantity: int,
        accessories: list[dict],
    ) -> None:
        amount = len(accessories) if action == "grant" else -len(accessories)
        item_delta = json.dumps(
            [{"id": item_id, "name": item_name, "type": "饰品", "amount": amount}],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        detail = json.dumps(
            {
                "operator_id": operator_id,
                "target_name": target_name,
                "requested_quantity": requested_quantity,
                "accessory_uids": [str(item.get("uid", "")) for item in accessories],
                "qualities": [int(item.get("quality", 1)) for item in accessories],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn.execute(
            "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
            "VALUES(%s,'admin',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (
                user_id,
                "admin_accessory_add" if action == "grant" else "admin_accessory_cost",
                item_delta,
                detail,
                operation_id,
            ),
        )

    def snapshot(self, user_id) -> tuple[dict, list]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not conn.table_exists("player_accessory"):
                return {}, []
            columns = set(conn.column_names("player_accessory"))
            if not {"equipped", "bag"}.issubset(columns):
                return {}, []
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            if row is None:
                return {}, []
            return self._load_dict(row[0]), self._load_list(row[1])

    def replay(
        self, operation_id, action
    ) -> AdminAccessoryAdjustmentResult | None:
        operation_id = str(operation_id).strip()
        action = str(action)
        if not operation_id:
            raise ValueError("operation id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("admin_accessory_operations"):
                return None
            row = conn.execute(
                "SELECT action,result_json FROM admin_accessory_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None or str(row[0]) != action:
                return None
            return self._result_from_json("duplicate", str(row[1]))

    def _run(self, operation_id, action, payload, apply):
        operation_id = str(operation_id).strip()
        payload_json = self._json(payload)
        if not operation_id:
            raise ValueError("operation id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT action,payload,result_json FROM admin_accessory_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != action or str(previous[1]) != payload_json:
                        return AdminAccessoryAdjustmentResult(
                            "operation_conflict", action, str(payload.get("user_id", ""))
                        )
                    return self._result_from_json("duplicate", str(previous[2]))

                result = apply(conn)
                if result.status not in {"granted", "destroyed"}:
                    conn.rollback()
                    return result
                result_json = self._json(
                    {
                        "action": result.action,
                        "user_id": result.user_id,
                        "requested_quantity": result.requested_quantity,
                        "affected_quantity": result.affected_quantity,
                        "accessories": result.accessories,
                    }
                )
                conn.execute(
                    "INSERT INTO admin_accessory_operations("
                    "operation_id,action,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (operation_id, action, payload_json, result_json),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def grant(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        quality,
        quantity,
        expected_equipped,
        expected_bag,
        max_accessories,
        create_accessory: Callable[[], dict],
        *,
        target_name="",
    ) -> AdminAccessoryAdjustmentResult:
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        quality = int(quality)
        quantity = int(quantity)
        max_accessories = int(max_accessories)
        target_name = str(target_name)
        if not operator_id or not user_id or item_id <= 0:
            raise ValueError("operator, user and item are required")
        if quality not in {1, 2, 3, 4, 5} or quantity <= 0 or max_accessories <= 0:
            raise ValueError("valid quality, quantity and inventory limit are required")
        expected_equipped = json.loads(self._json(expected_equipped))
        expected_bag = json.loads(self._json(expected_bag))
        payload = {
            "operator_id": operator_id,
            "user_id": user_id,
            "item_id": item_id,
            "item_name": item_name,
            "quality": quality,
            "quantity": quantity,
            "max_accessories": max_accessories,
            "target_name": target_name,
        }

        def apply(conn):
            if conn.execute(
                "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
            ).fetchone() is None:
                return AdminAccessoryAdjustmentResult(
                    "user_missing", "grant", user_id, quantity
                )
            equipped, bag = self._load_accessories(conn, user_id)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(bag) != self._json(expected_bag)
            ):
                return AdminAccessoryAdjustmentResult(
                    "state_changed", "grant", user_id, quantity
                )
            known_uids = self._owned_uids(equipped, bag)
            if known_uids is None:
                return AdminAccessoryAdjustmentResult(
                    "invalid_state", "grant", user_id, quantity
                )
            if self._owned_count(equipped, bag) + quantity > max_accessories:
                return AdminAccessoryAdjustmentResult(
                    "inventory_full", "grant", user_id, quantity
                )

            generated = []
            for _ in range(quantity):
                accessory = create_accessory()
                if not isinstance(accessory, dict):
                    return AdminAccessoryAdjustmentResult(
                        "invalid_plan", "grant", user_id, quantity
                    )
                uid = str(accessory.get("uid", "")).strip()
                try:
                    matches_plan = (
                        uid
                        and uid not in known_uids
                        and int(accessory.get("item_id", 0)) == item_id
                        and int(accessory.get("quality", 0)) == quality
                        and str(accessory.get("name", "")) == item_name
                    )
                except (TypeError, ValueError):
                    matches_plan = False
                if not matches_plan:
                    return AdminAccessoryAdjustmentResult(
                        "invalid_plan", "grant", user_id, quantity
                    )
                known_uids.add(uid)
                generated.append(json.loads(self._json(accessory)))

            bag.extend(generated)
            self._save_accessories(conn, user_id, equipped, bag)
            self._audit(
                conn,
                str(operation_id),
                "grant",
                operator_id,
                user_id,
                target_name,
                item_id,
                item_name,
                quantity,
                generated,
            )
            return AdminAccessoryAdjustmentResult(
                "granted", "grant", user_id, quantity, quantity, tuple(generated)
            )

        return self._run(operation_id, "grant", payload, apply)

    def destroy(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        quantity,
        expected_equipped,
        expected_bag,
        *,
        target_name="",
    ) -> AdminAccessoryAdjustmentResult:
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        quantity = int(quantity)
        target_name = str(target_name)
        if not operator_id or not user_id or item_id <= 0 or quantity <= 0:
            raise ValueError("operator, user, item and positive quantity are required")
        expected_equipped = json.loads(self._json(expected_equipped))
        expected_bag = json.loads(self._json(expected_bag))
        payload = {
            "operator_id": operator_id,
            "user_id": user_id,
            "item_id": item_id,
            "item_name": item_name,
            "quantity": quantity,
            "target_name": target_name,
        }

        def apply(conn):
            if conn.execute(
                "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
            ).fetchone() is None:
                return AdminAccessoryAdjustmentResult(
                    "user_missing", "destroy", user_id, quantity
                )
            equipped, bag = self._load_accessories(conn, user_id)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(bag) != self._json(expected_bag)
            ):
                return AdminAccessoryAdjustmentResult(
                    "state_changed", "destroy", user_id, quantity
                )
            if self._owned_uids(equipped, bag) is None:
                return AdminAccessoryAdjustmentResult(
                    "invalid_state", "destroy", user_id, quantity
                )

            removed = []
            kept = []
            for accessory in bag:
                if (
                    len(removed) < quantity
                    and int(accessory.get("item_id", 0)) == item_id
                ):
                    removed.append(accessory)
                else:
                    kept.append(accessory)
            if not removed:
                return AdminAccessoryAdjustmentResult(
                    "item_missing", "destroy", user_id, quantity
                )

            self._save_accessories(conn, user_id, equipped, kept)
            self._audit(
                conn,
                str(operation_id),
                "destroy",
                operator_id,
                user_id,
                target_name,
                item_id,
                item_name,
                quantity,
                removed,
            )
            return AdminAccessoryAdjustmentResult(
                "destroyed",
                "destroy",
                user_id,
                quantity,
                len(removed),
                tuple(removed),
            )

        return self._run(operation_id, "destroy", payload, apply)

@dataclass(frozen=True)
class AdminAccessoryBatchAdjustmentResult:
    status: str
    action: str
    total: int = 0
    completed: int = 0
    affected_quantity: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AdminAccessoryBatchAdjustmentService:
    """Persist and resume full-server accessory adjustment plans."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        adjustment_service: AdminAccessoryAdjustmentService | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._adjustment_service = adjustment_service or AdminAccessoryAdjustmentService(
            game_database, player_database
        )
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_accessory_batch_operations("
            "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
            "total INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_accessory_batch_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "affected_quantity INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _request(
        action,
        operator_id,
        item_id,
        item_name,
        quality,
        quantity,
        max_accessories,
    ) -> dict:
        return {
            "action": str(action),
            "operator_id": str(operator_id),
            "item_id": int(item_id),
            "item_name": str(item_name),
            "quality": int(quality),
            "quantity": int(quantity),
            "max_accessories": int(max_accessories),
        }

    @staticmethod
    def _payload(request: dict, user_ids: tuple[str, ...]) -> str:
        return json.dumps(
            {"request": request, "users": list(user_ids)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(
        conn, operation_id: str, status: str
    ) -> AdminAccessoryBatchAdjustmentResult:
        operation = conn.execute(
            "SELECT action,total FROM admin_accessory_batch_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        counts = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(affected_quantity),0),"
            "COALESCE(SUM(CASE WHEN affected_quantity>0 THEN 1 ELSE 0 END),0) "
            "FROM admin_accessory_batch_progress WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        completed = int(counts[0])
        affected_users = int(counts[2])
        return AdminAccessoryBatchAdjustmentResult(
            status,
            str(operation[0]),
            int(operation[1]),
            completed,
            int(counts[1]),
            affected_users,
            completed - affected_users,
        )

    def find_running(
        self,
        action,
        operator_id,
        item_id,
        item_name,
        quality,
        quantity,
        max_accessories,
    ) -> str | None:
        request = self._request(
            action,
            operator_id,
            item_id,
            item_name,
            quality,
            quantity,
            max_accessories,
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("admin_accessory_batch_operations"):
                return None
            rows = conn.execute(
                "SELECT operation_id,payload FROM admin_accessory_batch_operations "
                "WHERE action=%s AND status='running' ORDER BY created_at DESC,rowid DESC",
                (str(action),),
            ).fetchall()
            for row in rows:
                try:
                    if json.loads(str(row[1])).get("request") == request:
                        return str(row[0])
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
        return None

    def _begin(
        self,
        operation_id: str,
        request: dict,
        user_ids: tuple[str, ...],
        chunk_size: int,
    ) -> tuple[AdminAccessoryBatchAdjustmentResult | None, tuple[str, ...]]:
        payload = self._payload(request, user_ids)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,status FROM admin_accessory_batch_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    conn.execute(
                        "INSERT INTO admin_accessory_batch_operations("
                        "operation_id,action,payload,total) VALUES(%s,%s,%s,%s)",
                        (operation_id, request["action"], payload, len(user_ids)),
                    )
                    frozen_users = user_ids
                else:
                    previous_payload = json.loads(str(previous[0]))
                    if previous_payload.get("request") != request:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result, ()
                    frozen_users = tuple(
                        str(user_id) for user_id in previous_payload.get("users", [])
                    )
                    if str(previous[1]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result, ()

                completed_users = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT user_id FROM admin_accessory_batch_progress "
                        "WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchall()
                }
                pending = tuple(
                    user_id
                    for user_id in frozen_users
                    if user_id not in completed_users
                )[:chunk_size]
                conn.commit()
                return None, pending
            except Exception:
                conn.rollback()
                raise

    def _record(self, operation_id: str, user_id: str, result) -> None:
        result_json = json.dumps(
            {
                "status": result.status,
                "requested_quantity": result.requested_quantity,
                "affected_quantity": result.affected_quantity,
                "accessories": result.accessories,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute(
                    "INSERT INTO admin_accessory_batch_progress("
                    "operation_id,user_id,status,affected_quantity,result_json) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(operation_id,user_id) "
                    "DO UPDATE SET status=EXCLUDED.status,"
                    "affected_quantity=EXCLUDED.affected_quantity,"
                    "result_json=EXCLUDED.result_json "
                    "WHERE EXCLUDED.affected_quantity>"
                    "admin_accessory_batch_progress.affected_quantity",
                    (
                        operation_id,
                        user_id,
                        result.status,
                        result.affected_quantity,
                        result_json,
                    ),
                )
                counts = conn.execute(
                    "SELECT o.total,COUNT(p.user_id) "
                    "FROM admin_accessory_batch_operations o "
                    "LEFT JOIN admin_accessory_batch_progress p "
                    "ON p.operation_id=o.operation_id WHERE o.operation_id=%s "
                    "GROUP BY o.total",
                    (operation_id,),
                ).fetchone()
                status = "completed" if int(counts[1]) >= int(counts[0]) else "running"
                conn.execute(
                    "UPDATE admin_accessory_batch_operations "
                    "SET status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (status, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _advance(
        self,
        operation_id,
        request: dict,
        user_ids,
        *,
        chunk_size,
        create_accessory: Callable[[str], dict] | None = None,
    ) -> AdminAccessoryBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        normalized_users = tuple(
            sorted({str(user_id).strip() for user_id in user_ids if str(user_id).strip()})
        )
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"].strip()
            or not normalized_users
            or request["action"] not in {"grant", "destroy"}
            or request["item_id"] <= 0
            or request["quantity"] <= 0
        ):
            raise ValueError("invalid accessory batch arguments")
        if request["action"] == "grant" and (
            request["quality"] not in {1, 2, 3, 4, 5}
            or request["max_accessories"] <= 0
            or create_accessory is None
        ):
            raise ValueError("grant requires quality, capacity and instance factory")

        previous, pending = self._begin(
            operation_id, request, normalized_users, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                equipped, bag = self._adjustment_service.snapshot(user_id)
                child_operation = (
                    f"admin-accessory-batch:{operation_id}:{request['action']}:{user_id}"
                )
                if request["action"] == "grant":
                    result = self._adjustment_service.grant(
                        child_operation,
                        request["operator_id"],
                        user_id,
                        request["item_id"],
                        request["item_name"],
                        request["quality"],
                        request["quantity"],
                        equipped,
                        bag,
                        request["max_accessories"],
                        lambda user_id=user_id: create_accessory(user_id),
                        target_name="all",
                    )
                else:
                    result = self._adjustment_service.destroy(
                        child_operation,
                        request["operator_id"],
                        user_id,
                        request["item_id"],
                        request["item_name"],
                        request["quantity"],
                        equipped,
                        bag,
                        target_name="all",
                    )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(
                    f"accessory batch child operation conflict: {user_id}"
                )
            self._record(operation_id, user_id, result)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            return self._result(conn, operation_id, "applied")

    def grant(
        self,
        operation_id,
        operator_id,
        user_ids,
        item_id,
        item_name,
        quality,
        quantity,
        max_accessories,
        create_accessory: Callable[[str], dict],
        *,
        chunk_size=100,
    ) -> AdminAccessoryBatchAdjustmentResult:
        request = self._request(
            "grant",
            operator_id,
            item_id,
            item_name,
            quality,
            quantity,
            max_accessories,
        )
        return self._advance(
            operation_id,
            request,
            user_ids,
            chunk_size=chunk_size,
            create_accessory=create_accessory,
        )

    def destroy(
        self,
        operation_id,
        operator_id,
        user_ids,
        item_id,
        item_name,
        quantity,
        *,
        chunk_size=100,
    ) -> AdminAccessoryBatchAdjustmentResult:
        request = self._request(
            "destroy", operator_id, item_id, item_name, 0, quantity, 0
        )
        return self._advance(
            operation_id, request, user_ids, chunk_size=chunk_size
        )

@dataclass(frozen=True)
class AdminImpartStoneAdjustmentResult:
    status: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}

class AdminImpartStoneAdjustmentService:
    """Atomically adjust one player's impart stones and admin audit."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_impart_stone_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_stone INTEGER NOT NULL,final_stone INTEGER NOT NULL,"
            "applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        if not conn.column_exists("economy_log", "trace_id"):
            conn.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS impart_data.xiuxian_impart("
            "user_id TEXT,stone_num INTEGER DEFAULT 0)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA impart_data.table_info(xiuxian_impart)"
            ).fetchall()
        }
        if "user_id" not in columns:
            conn.execute(
                "ALTER TABLE impart_data.xiuxian_impart ADD COLUMN user_id TEXT"
            )
        if "stone_num" not in columns:
            conn.execute(
                "ALTER TABLE impart_data.xiuxian_impart "
                "ADD COLUMN stone_num INTEGER DEFAULT 0"
            )

    def snapshot(self, user_id) -> int | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._impart_database)) as conn:
            if not conn.table_exists("xiuxian_impart"):
                return None
            if not {"user_id", "stone_num"}.issubset(
                set(conn.column_names("xiuxian_impart"))
            ):
                return None
            rows = conn.execute(
                "SELECT COALESCE(stone_num,0) FROM xiuxian_impart WHERE user_id=%s",
                (user_id,),
            ).fetchall()
            return int(rows[0][0]) if rows else None

    def adjust(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_stone,
        requested_delta,
        *,
        target_name="",
    ) -> AdminImpartStoneAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = (
            None if expected_stone is None else int(expected_stone)
        )
        requested_delta = int(requested_delta)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_stone is not None and expected_stone < 0:
            raise ValueError("stone snapshot cannot be negative")
        if requested_delta == 0:
            raise ValueError("adjustment cannot be zero")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS impart_data", (str(self._impart_database),)
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_stone,final_stone,applied_delta "
                    "FROM admin_impart_stone_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminImpartStoneAdjustmentResult(
                            "operation_conflict"
                        )
                    return AdminImpartStoneAdjustmentResult(
                        "duplicate",
                        int(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return AdminImpartStoneAdjustmentResult("user_missing")
                rows = conn.execute(
                    "SELECT COALESCE(stone_num,0) "
                    "FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchall()
                if len(rows) > 1:
                    conn.rollback()
                    return AdminImpartStoneAdjustmentResult("invalid_state")
                actual_stone = int(rows[0][0]) if rows else None
                if actual_stone != expected_stone:
                    conn.rollback()
                    current = int(actual_stone or 0)
                    return AdminImpartStoneAdjustmentResult(
                        "state_changed", current, current
                    )

                previous_stone = int(actual_stone or 0)
                final_stone = max(0, previous_stone + requested_delta)
                applied_delta = final_stone - previous_stone
                if actual_stone is None:
                    conn.execute(
                        "INSERT INTO impart_data.xiuxian_impart(user_id,stone_num) "
                        "VALUES(%s,%s)",
                        (user_id, final_stone),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE impart_data.xiuxian_impart SET stone_num=%s "
                        "WHERE user_id=%s AND COALESCE(stone_num,0)=%s",
                        (final_stone, user_id, previous_stone),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return AdminImpartStoneAdjustmentResult("state_changed")

                item_delta = json.dumps(
                    [{
                        "id": "impart_stone",
                        "name": "思恋结晶",
                        "type": "传承货币",
                        "amount": applied_delta,
                    }],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                detail = json.dumps(
                    {
                        "operator_id": operator_id,
                        "target_name": target_name,
                        "requested_delta": requested_delta,
                        "previous_stone": previous_stone,
                        "final_stone": final_stone,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO economy_log("
                    "user_id,source,action,item_delta,detail,trace_id,created_at) "
                    "VALUES(%s,'admin',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (
                        user_id,
                        "admin_impart_stone_add"
                        if requested_delta > 0
                        else "admin_impart_stone_cost",
                        item_delta,
                        detail,
                        operation_id,
                    ),
                )
                conn.execute(
                    "INSERT INTO admin_impart_stone_operations("
                    "operation_id,payload,previous_stone,final_stone,applied_delta) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        previous_stone,
                        final_stone,
                        applied_delta,
                    ),
                )
                conn.commit()
                return AdminImpartStoneAdjustmentResult(
                    "adjusted",
                    previous_stone,
                    final_stone,
                    applied_delta,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE impart_data")

@dataclass(frozen=True)
class AdminImpartStoneBatchAdjustmentResult:
    status: str
    total: int = 0
    completed: int = 0
    applied_delta: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AdminImpartStoneBatchAdjustmentService:
    """Persist and resume full-server impart stone adjustment plans."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        adjustment_service: AdminImpartStoneAdjustmentService | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._adjustment_service = adjustment_service or AdminImpartStoneAdjustmentService(
            game_database, impart_database
        )
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "applied_delta INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _request(operator_id, requested_delta) -> dict:
        return {
            "operator_id": str(operator_id),
            "requested_delta": int(requested_delta),
        }

    @staticmethod
    def _payload(request: dict, user_ids: tuple[str, ...]) -> str:
        return json.dumps(
            {"request": request, "users": list(user_ids)},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(
        conn, operation_id: str, status: str
    ) -> AdminImpartStoneBatchAdjustmentResult:
        operation = conn.execute(
            "SELECT total FROM admin_impart_stone_batch_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        counts = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(applied_delta),0),"
            "COALESCE(SUM(CASE WHEN applied_delta!=0 THEN 1 ELSE 0 END),0) "
            "FROM admin_impart_stone_batch_progress WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        completed = int(counts[0])
        affected_users = int(counts[2])
        return AdminImpartStoneBatchAdjustmentResult(
            status,
            int(operation[0]),
            completed,
            int(counts[1]),
            affected_users,
            completed - affected_users,
        )

    def find_running(self, operator_id, requested_delta) -> str | None:
        request = self._request(operator_id, requested_delta)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("admin_impart_stone_batch_operations"):
                return None
            rows = conn.execute(
                "SELECT operation_id,payload FROM admin_impart_stone_batch_operations "
                "WHERE status='running' ORDER BY created_at DESC,rowid DESC"
            ).fetchall()
            for row in rows:
                try:
                    if json.loads(str(row[1])).get("request") == request:
                        return str(row[0])
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
        return None

    def _begin(
        self,
        operation_id: str,
        request: dict,
        user_ids: tuple[str, ...],
        chunk_size: int,
    ) -> tuple[AdminImpartStoneBatchAdjustmentResult | None, tuple[str, ...]]:
        payload = self._payload(request, user_ids)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,status FROM admin_impart_stone_batch_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    conn.execute(
                        "INSERT INTO admin_impart_stone_batch_operations("
                        "operation_id,payload,total) VALUES(%s,%s,%s)",
                        (operation_id, payload, len(user_ids)),
                    )
                    frozen_users = user_ids
                else:
                    previous_payload = json.loads(str(previous[0]))
                    if previous_payload.get("request") != request:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result, ()
                    frozen_users = tuple(
                        str(user_id) for user_id in previous_payload.get("users", [])
                    )
                    if str(previous[1]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result, ()

                completed_users = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT user_id FROM admin_impart_stone_batch_progress "
                        "WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchall()
                }
                pending = tuple(
                    user_id
                    for user_id in frozen_users
                    if user_id not in completed_users
                )[:chunk_size]
                conn.commit()
                return None, pending
            except Exception:
                conn.rollback()
                raise

    def _record(self, operation_id: str, user_id: str, result) -> None:
        result_json = json.dumps(
            {
                "status": result.status,
                "previous_stone": result.previous_stone,
                "final_stone": result.final_stone,
                "applied_delta": result.applied_delta,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute(
                    "INSERT INTO admin_impart_stone_batch_progress("
                    "operation_id,user_id,status,applied_delta,result_json) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(operation_id,user_id) "
                    "DO UPDATE SET status=EXCLUDED.status,"
                    "applied_delta=EXCLUDED.applied_delta,"
                    "result_json=EXCLUDED.result_json "
                    "WHERE ABS(EXCLUDED.applied_delta)>"
                    "ABS(admin_impart_stone_batch_progress.applied_delta)",
                    (
                        operation_id,
                        user_id,
                        result.status,
                        result.applied_delta,
                        result_json,
                    ),
                )
                counts = conn.execute(
                    "SELECT o.total,COUNT(p.user_id) "
                    "FROM admin_impart_stone_batch_operations o "
                    "LEFT JOIN admin_impart_stone_batch_progress p "
                    "ON p.operation_id=o.operation_id WHERE o.operation_id=%s "
                    "GROUP BY o.total",
                    (operation_id,),
                ).fetchone()
                status = "completed" if int(counts[1]) >= int(counts[0]) else "running"
                conn.execute(
                    "UPDATE admin_impart_stone_batch_operations "
                    "SET status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (status, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def adjust(
        self,
        operation_id,
        operator_id,
        user_ids,
        requested_delta,
        *,
        chunk_size=100,
    ) -> AdminImpartStoneBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, requested_delta)
        normalized_users = tuple(
            sorted({str(user_id).strip() for user_id in user_ids if str(user_id).strip()})
        )
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"].strip()
            or not normalized_users
            or request["requested_delta"] == 0
        ):
            raise ValueError("invalid impart stone batch arguments")

        previous, pending = self._begin(
            operation_id, request, normalized_users, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                expected_stone = self._adjustment_service.snapshot(user_id)
                result = self._adjustment_service.adjust(
                    f"admin-impart-stone-batch:{operation_id}:{user_id}",
                    request["operator_id"],
                    user_id,
                    expected_stone,
                    request["requested_delta"],
                    target_name="all",
                )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(
                    f"impart stone batch child operation conflict: {user_id}"
                )
            self._record(operation_id, user_id, result)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            return self._result(conn, operation_id, "applied")

@dataclass(frozen=True)
class AdminPlayerStatusResetResult:
    status: str
    previous_state: tuple[int, int, int, int, int] = ()
    final_state: tuple[int, int, int, int, int] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"reset", "duplicate"}

class AdminPlayerStatusResetService:
    """Atomically recompute one player's combat state and stamina."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_player_status_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_state TEXT NOT NULL,final_state TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _state(row) -> tuple[int, int, int, int, int]:
        return tuple(int(value or 0) for value in row)

    def snapshot(self, user_id) -> tuple[int, int, int, int, int] | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            row = conn.execute(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                "WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            return self._state(row) if row else None

    def reset(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_state,
        max_stamina,
        *,
        target_name="",
    ) -> AdminPlayerStatusResetResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        max_stamina = int(max_stamina)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if max_stamina <= 0:
            raise ValueError("stamina limit must be positive")
        if expected_state is None:
            normalized_expected = None
        else:
            normalized_expected = tuple(int(value or 0) for value in expected_state)
            if len(normalized_expected) != 5:
                raise ValueError("complete status snapshot is required")

        payload = json.dumps(
            [operator_id, user_id, max_stamina],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_state,final_state "
                    "FROM admin_player_status_reset_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminPlayerStatusResetResult("operation_conflict")
                    return AdminPlayerStatusResetResult(
                        "duplicate",
                        tuple(json.loads(str(previous[1]))),
                        tuple(json.loads(str(previous[2]))),
                    )

                row = conn.execute(
                    "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminPlayerStatusResetResult("user_missing")
                actual_state = self._state(row)
                if actual_state != normalized_expected:
                    conn.rollback()
                    return AdminPlayerStatusResetResult(
                        "state_changed", actual_state, actual_state
                    )

                exp = actual_state[0]
                final_state = (
                    exp,
                    exp // 2,
                    exp,
                    exp // 10,
                    max_stamina,
                )
                changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,atk=%s,user_stamina=%s "
                    "WHERE user_id=%s AND COALESCE(exp,0)=%s AND COALESCE(hp,0)=%s "
                    "AND COALESCE(mp,0)=%s AND COALESCE(atk,0)=%s "
                    "AND COALESCE(user_stamina,0)=%s",
                    (
                        final_state[1],
                        final_state[2],
                        final_state[3],
                        final_state[4],
                        user_id,
                        *actual_state,
                    ),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminPlayerStatusResetResult("state_changed")

                conn.execute(
                    "INSERT INTO admin_player_status_reset_operations("
                    "operation_id,payload,previous_state,final_state) "
                    "VALUES(%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        json.dumps(actual_state, separators=(",", ":")),
                        json.dumps(final_state, separators=(",", ":")),
                    ),
                )
                conn.commit()
                return AdminPlayerStatusResetResult(
                    "reset", actual_state, final_state
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class AdminPlayerStatusBatchResetResult:
    status: str
    total: int = 0
    completed: int = 0
    reset_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class AdminPlayerStatusBatchResetService:
    """Persist and resume full-server player status reset plans."""

    def __init__(
        self,
        database: str | Path,
        reset_service: AdminPlayerStatusResetService | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._database = Path(database)
        self._reset_service = reset_service or AdminPlayerStatusResetService(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "reset_applied INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _request(operator_id, max_stamina) -> dict:
        return {
            "operator_id": str(operator_id),
            "max_stamina": int(max_stamina),
        }

    @staticmethod
    def _payload(request: dict, user_ids: tuple[str, ...]) -> str:
        return json.dumps(
            {"request": request, "users": list(user_ids)},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(
        conn, operation_id: str, status: str
    ) -> AdminPlayerStatusBatchResetResult:
        operation = conn.execute(
            "SELECT total FROM admin_player_status_batch_reset_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        counts = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(reset_applied),0) "
            "FROM admin_player_status_batch_reset_progress WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        completed = int(counts[0])
        reset_users = int(counts[1])
        return AdminPlayerStatusBatchResetResult(
            status,
            int(operation[0]),
            completed,
            reset_users,
            completed - reset_users,
        )

    def find_running(self, operator_id, max_stamina) -> str | None:
        request = self._request(operator_id, max_stamina)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            if not conn.table_exists("admin_player_status_batch_reset_operations"):
                return None
            rows = conn.execute(
                "SELECT operation_id,payload "
                "FROM admin_player_status_batch_reset_operations "
                "WHERE status='running' ORDER BY created_at DESC,rowid DESC"
            ).fetchall()
            for row in rows:
                try:
                    if json.loads(str(row[1])).get("request") == request:
                        return str(row[0])
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
        return None

    def _begin(
        self,
        operation_id: str,
        request: dict,
        user_ids: tuple[str, ...],
        chunk_size: int,
    ) -> tuple[AdminPlayerStatusBatchResetResult | None, tuple[str, ...]]:
        payload = self._payload(request, user_ids)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,status "
                    "FROM admin_player_status_batch_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    if not user_ids:
                        conn.rollback()
                        raise ValueError("new player status reset plan requires users")
                    conn.execute(
                        "INSERT INTO admin_player_status_batch_reset_operations("
                        "operation_id,payload,total) VALUES(%s,%s,%s)",
                        (operation_id, payload, len(user_ids)),
                    )
                    frozen_users = user_ids
                else:
                    previous_payload = json.loads(str(previous[0]))
                    if previous_payload.get("request") != request:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result, ()
                    frozen_users = tuple(
                        str(user_id) for user_id in previous_payload.get("users", [])
                    )
                    if str(previous[1]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result, ()

                completed_users = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT user_id FROM admin_player_status_batch_reset_progress "
                        "WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchall()
                }
                pending = tuple(
                    user_id
                    for user_id in frozen_users
                    if user_id not in completed_users
                )[:chunk_size]
                conn.commit()
                return None, pending
            except Exception:
                conn.rollback()
                raise

    def _record(self, operation_id: str, user_id: str, result) -> None:
        result_json = json.dumps(
            {
                "status": result.status,
                "previous_state": result.previous_state,
                "final_state": result.final_state,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute(
                    "INSERT INTO admin_player_status_batch_reset_progress("
                    "operation_id,user_id,status,reset_applied,result_json) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(operation_id,user_id) "
                    "DO NOTHING",
                    (
                        operation_id,
                        user_id,
                        result.status,
                        int(result.succeeded),
                        result_json,
                    ),
                )
                counts = conn.execute(
                    "SELECT o.total,COUNT(p.user_id) "
                    "FROM admin_player_status_batch_reset_operations o "
                    "LEFT JOIN admin_player_status_batch_reset_progress p "
                    "ON p.operation_id=o.operation_id WHERE o.operation_id=%s "
                    "GROUP BY o.total",
                    (operation_id,),
                ).fetchone()
                status = "completed" if int(counts[1]) >= int(counts[0]) else "running"
                conn.execute(
                    "UPDATE admin_player_status_batch_reset_operations "
                    "SET status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (status, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def reset(
        self,
        operation_id,
        operator_id,
        user_ids,
        max_stamina,
        *,
        chunk_size=100,
    ) -> AdminPlayerStatusBatchResetResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, max_stamina)
        normalized_users = tuple(
            sorted({str(user_id).strip() for user_id in user_ids if str(user_id).strip()})
        )
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"].strip()
            or request["max_stamina"] <= 0
        ):
            raise ValueError("invalid player status batch reset arguments")

        previous, pending = self._begin(
            operation_id, request, normalized_users, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                expected_state = self._reset_service.snapshot(user_id)
                result = self._reset_service.reset(
                    f"admin-player-status-reset-batch:{operation_id}:{user_id}",
                    request["operator_id"],
                    user_id,
                    expected_state,
                    request["max_stamina"],
                    target_name="all",
                )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(
                    f"player status batch child operation conflict: {user_id}"
                )
            if result.status == "state_changed":
                raise RuntimeError(
                    f"player status remained unstable during batch reset: {user_id}"
                )
            self._record(operation_id, user_id, result)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            return self._result(conn, operation_id, "applied")

@dataclass(frozen=True)
class AdminBlackhouseStatusResult:
    status: str
    action: str = ""
    previous_banned: bool = False
    final_banned: bool = False
    changed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.status in {"changed", "unchanged", "duplicate"}

class AdminBlackhouseStatusService:
    """Atomically change one player's blackhouse status."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_blackhouse_status_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def snapshot(self, user_id) -> bool | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            row = conn.execute(
                "SELECT COALESCE(is_ban,0) FROM user_xiuxian WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            return bool(int(row[0])) if row else None

    def set_banned(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_banned,
        banned,
    ) -> AdminBlackhouseStatusResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        banned = bool(banned)
        action = "ban" if banned else "unban"
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        normalized_expected = (
            None if expected_banned is None else bool(expected_banned)
        )
        payload = json.dumps(
            [operator_id, user_id, action],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json "
                    "FROM admin_blackhouse_status_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminBlackhouseStatusResult("operation_conflict")
                    saved = json.loads(str(previous[1]))
                    return AdminBlackhouseStatusResult(
                        "duplicate",
                        str(saved["action"]),
                        bool(saved["previous_banned"]),
                        bool(saved["final_banned"]),
                        bool(saved["changed"]),
                    )

                row = conn.execute(
                    "SELECT COALESCE(is_ban,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminBlackhouseStatusResult("user_missing", action=action)
                actual_banned = bool(int(row[0]))
                if actual_banned != normalized_expected:
                    conn.rollback()
                    return AdminBlackhouseStatusResult(
                        "state_changed",
                        action,
                        actual_banned,
                        actual_banned,
                    )

                changed = actual_banned != banned
                if changed:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET is_ban=%s "
                        "WHERE user_id=%s AND COALESCE(is_ban,0)=%s",
                        (int(banned), user_id, int(actual_banned)),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return AdminBlackhouseStatusResult("state_changed", action=action)

                result_json = json.dumps(
                    {
                        "action": action,
                        "previous_banned": actual_banned,
                        "final_banned": banned,
                        "changed": changed,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO admin_blackhouse_status_operations("
                    "operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return AdminBlackhouseStatusResult(
                    "changed" if changed else "unchanged",
                    action,
                    actual_banned,
                    banned,
                    changed,
                )
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "AdminLevelChangeResult",
    "AdminLevelChangeService",
    "AdminRootChangeResult",
    "AdminRootChangeService",
    "AdminExpAdjustmentResult",
    "AdminExpAdjustmentService",
    "AdminStoneAdjustmentResult",
    "AdminStoneAdjustmentService",
    "AdminItemGrantResult",
    "AdminItemGrantService",
    "AdminItemDestroyResult",
    "AdminItemDestroyService",
    "AdminItemBatchGrantResult",
    "AdminItemBatchGrantService",
    "AdminAccessoryAdjustmentResult",
    "AdminAccessoryAdjustmentService",
    "AdminAccessoryBatchAdjustmentResult",
    "AdminAccessoryBatchAdjustmentService",
    "AdminImpartStoneAdjustmentResult",
    "AdminImpartStoneAdjustmentService",
    "AdminImpartStoneBatchAdjustmentResult",
    "AdminImpartStoneBatchAdjustmentService",
    "AdminPlayerStatusResetResult",
    "AdminPlayerStatusResetService",
    "AdminPlayerStatusBatchResetResult",
    "AdminPlayerStatusBatchResetService",
    "AdminBlackhouseStatusResult",
    "AdminBlackhouseStatusService",
    "ROOT_CHANGES",
]
