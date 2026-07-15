from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class FusionResult:
    status: str
    successful: bool
    protected: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class FusionBatchResult:
    status: str
    successful_count: int
    failed_count: int
    protected_count: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class FusionService:
    """Apply fusion costs, material consumption and reward in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _apply_payload(user_id, stone_cost, materials, target_id, protection_item_id) -> str:
        # Request identity only — roll outcome (successful/protected) is stored separately.
        return json.dumps(
            [str(user_id), int(stone_cost), sorted((int(k), int(v)) for k, v in materials.items()),
             int(target_id), None if protection_item_id is None else int(protection_item_id)],
            ensure_ascii=True, separators=(",", ":"),
        )

    @staticmethod
    def _batch_payload(user_id, stone_cost, materials, target_id, attempt_count, protection_item_id) -> str:
        return json.dumps(
            [str(user_id), int(stone_cost), sorted((int(k), int(v)) for k, v in materials.items()),
             int(target_id), int(attempt_count),
             None if protection_item_id is None else int(protection_item_id)],
            ensure_ascii=True, separators=(",", ":"),
        )

    def get_result(self, operation_id: str) -> FusionResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS fusion_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, successful INTEGER NOT NULL, "
                "protected INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, successful, protected FROM fusion_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return FusionResult("duplicate", bool(previous[1]), bool(previous[2]))

    def get_batch_result(self, operation_id: str) -> FusionBatchResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS fusion_batch_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, outcomes TEXT NOT NULL, "
                "successful_count INTEGER NOT NULL, failed_count INTEGER NOT NULL, "
                "protected_count INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, successful_count, failed_count, protected_count "
                "FROM fusion_batch_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return FusionBatchResult("duplicate", int(previous[1]), int(previous[2]), int(previous[3]))

    def apply(
        self,
        operation_id,
        user_id,
        stone_cost,
        materials,
        target_id,
        target_name,
        target_type,
        *,
        successful,
        protection_item_id=None,
        reserved_items=None,
        max_goods_num,
    ) -> FusionResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone_cost = int(stone_cost)
        target_id = int(target_id)
        successful = bool(successful)
        max_goods_num = int(max_goods_num)
        materials = {int(key): int(value) for key, value in materials.items() if int(value) > 0}
        reserved = {int(key): int(value) for key, value in (reserved_items or {}).items()}
        protection_item_id = int(protection_item_id) if protection_item_id is not None else None
        if not operation_id or stone_cost < 0 or max_goods_num <= 0:
            raise ValueError("valid operation, non-negative cost and positive capacity are required")

        payload = self._apply_payload(user_id, stone_cost, materials, target_id, protection_item_id)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS fusion_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, successful INTEGER NOT NULL, "
                    "protected INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, successful, protected FROM fusion_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return FusionResult("state_changed", successful, False)
                    return FusionResult("duplicate", bool(previous[1]), bool(previous[2]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return FusionResult("user_missing", successful, False)

                protected = False
                if not successful and protection_item_id is not None:
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, protection_item_id),
                    ).fetchone()
                    protected = row is not None and int(row[0]) > 0

                if protected:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s AND goods_num>=1",
                        (user_id, protection_item_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return FusionResult("state_changed", successful, False)
                else:
                    if int(user[0]) < stone_cost:
                        conn.rollback()
                        return FusionResult("stone_insufficient", successful, False)
                    columns = set(conn.column_names("back"))
                    for item_id, quantity in materials.items():
                        row = conn.execute(
                            "SELECT COALESCE(goods_num, 0)" +
                            (", COALESCE(bind_num, 0)" if "bind_num" in columns else "") +
                            " FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id),
                        ).fetchone()
                        available = 0 if row is None else int(row[0]) - (int(row[1]) if len(row) > 1 else 0)
                        if available - max(0, reserved.get(item_id, 0)) < quantity:
                            conn.rollback()
                            return FusionResult("item_insufficient", successful, False)
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                        (stone_cost, user_id, stone_cost),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return FusionResult("state_changed", successful, False)
                    for item_id, quantity in materials.items():
                        consumed = conn.execute(
                            "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s",
                            (quantity, user_id, item_id),
                        )
                        if consumed.rowcount != 1:
                            conn.rollback()
                            return FusionResult("state_changed", successful, False)

                if successful:
                    if "bind_num" in set(conn.column_names("back")):
                        conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                            "VALUES (%s, %s, %s, %s, 1, 1) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=MIN(COALESCE(back.goods_num, 0)+1, %s), "
                            "bind_num=MIN(COALESCE(back.bind_num, 0)+1, %s)",
                            (user_id, target_id, str(target_name), str(target_type), max_goods_num, max_goods_num),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num) "
                            "VALUES (%s, %s, %s, %s, 1) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=MIN(COALESCE(back.goods_num, 0)+1, %s)",
                            (user_id, target_id, str(target_name), str(target_type), max_goods_num),
                        )
                conn.execute(
                    "INSERT INTO fusion_operations (operation_id, payload, successful, protected) "
                    "VALUES (%s, %s, %s, %s)",
                    (operation_id, payload, int(successful), int(protected)),
                )
                conn.commit()
                return FusionResult("applied", successful, protected)
            except Exception:
                conn.rollback()
                raise

    def apply_batch(
        self,
        operation_id,
        user_id,
        stone_cost,
        materials,
        target_id,
        target_name,
        target_type,
        outcomes,
        *,
        protection_item_id=None,
        reserved_items=None,
        max_goods_num,
        target_limit=None,
    ) -> FusionBatchResult:
        """Settle pre-rolled fusion attempts with one database commit."""
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone_cost = int(stone_cost)
        target_id = int(target_id)
        target_name = str(target_name)
        target_type = str(target_type)
        outcomes = tuple(bool(value) for value in outcomes)
        max_goods_num = int(max_goods_num)
        materials = {int(key): int(value) for key, value in materials.items() if int(value) > 0}
        reserved = {int(key): max(0, int(value)) for key, value in (reserved_items or {}).items()}
        protection_item_id = int(protection_item_id) if protection_item_id is not None else None
        target_limit = int(target_limit) if target_limit is not None else None
        if not operation_id or not outcomes or stone_cost < 0 or max_goods_num <= 0:
            raise ValueError("valid operation, outcomes, non-negative cost and positive capacity are required")
        if target_limit is not None and target_limit <= 0:
            raise ValueError("target_limit must be positive")

        payload = self._batch_payload(user_id, stone_cost, materials, target_id, len(outcomes), protection_item_id)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS fusion_batch_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, outcomes TEXT NOT NULL, "
                    "successful_count INTEGER NOT NULL, failed_count INTEGER NOT NULL, "
                    "protected_count INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, successful_count, failed_count, protected_count "
                    "FROM fusion_batch_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return FusionBatchResult("state_changed", 0, 0, 0)
                    return FusionBatchResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
                    )

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return FusionBatchResult("user_missing", 0, 0, 0)

                successful_count = sum(outcomes)
                failed_count = len(outcomes) - successful_count
                protection_available = 0
                if protection_item_id is not None and failed_count:
                    protection = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, protection_item_id),
                    ).fetchone()
                    protection_available = 0 if protection is None else max(0, int(protection[0]))
                protected_count = min(failed_count, protection_available)
                charged_attempts = len(outcomes) - protected_count
                total_stone = stone_cost * charged_attempts
                total_materials = {
                    item_id: quantity * charged_attempts for item_id, quantity in materials.items()
                }

                if int(user[0]) < total_stone:
                    conn.rollback()
                    return FusionBatchResult("stone_insufficient", 0, 0, 0)
                columns = set(conn.column_names("back"))
                for item_id, quantity in total_materials.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0)" +
                        (", COALESCE(bind_num, 0)" if "bind_num" in columns else "") +
                        " FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id),
                    ).fetchone()
                    available = 0 if row is None else int(row[0]) - (int(row[1]) if len(row) > 1 else 0)
                    if available - reserved.get(item_id, 0) < quantity:
                        conn.rollback()
                        return FusionBatchResult("item_insufficient", 0, 0, 0)

                target = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, target_id),
                ).fetchone()
                current_target = 0 if target is None else int(target[0])
                capacity = min(max_goods_num, target_limit or max_goods_num)
                if current_target + successful_count > capacity:
                    conn.rollback()
                    return FusionBatchResult("inventory_full", 0, 0, 0)

                if total_stone:
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                        (total_stone, user_id, total_stone),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return FusionBatchResult("state_changed", 0, 0, 0)
                for item_id, quantity in total_materials.items():
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s",
                        (quantity, user_id, item_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return FusionBatchResult("state_changed", 0, 0, 0)
                if protected_count:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s "
                        "AND goods_num>=%s",
                        (protected_count, user_id, protection_item_id, protected_count),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return FusionBatchResult("state_changed", 0, 0, 0)

                if successful_count:
                    if "bind_num" in columns:
                        conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, "
                            "bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.bind_num",
                            (user_id, target_id, target_name, target_type, successful_count, successful_count),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num) "
                            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num",
                            (user_id, target_id, target_name, target_type, successful_count),
                        )
                conn.execute(
                    "INSERT INTO fusion_batch_operations "
                    "(operation_id, payload, outcomes, successful_count, failed_count, protected_count) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, payload, json.dumps(outcomes), successful_count, failed_count, protected_count),
                )
                conn.commit()
                return FusionBatchResult("applied", successful_count, failed_count, protected_count)
            except Exception:
                conn.rollback()
                raise


__all__ = ["FusionBatchResult", "FusionResult", "FusionService"]
