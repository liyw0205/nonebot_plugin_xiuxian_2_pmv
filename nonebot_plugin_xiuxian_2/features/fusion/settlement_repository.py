from __future__ import annotations

from ...core.numeric import as_int_like

from ...core.errors import DomainError

import json
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ...infrastructure.database import DatabaseUnitOfWork


class _FusionUnitOfWork(DatabaseUnitOfWork):
    """Adapt the historical SQL placeholders to the shared UoW boundary."""

    def execute(self, sql, params=()):
        return super().execute(str(sql).replace("%s", "?").replace("LEAST(", "MIN("), params)

    def column_names(self, table: str) -> list[str]:
        return [str(row[1]) for row in self.execute(f"PRAGMA table_info({table})").fetchall()]

    def rollback(self) -> None:
        self._conn().rollback()


@dataclass(frozen=True)
class FusionResult:
    status: str
    successful: bool
    protected: bool
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class FusionBatchResult:
    status: str
    successful_count: int
    failed_count: int
    protected_count: int
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class FusionService:
    """Apply fusion costs, material consumption and reward in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _back_projection(conn, user_id: str, item_id: int, has_bind: bool):
        columns = "COALESCE(goods_num, 0), goods_name, goods_type"
        if has_bind:
            columns = "COALESCE(goods_num, 0), COALESCE(bind_num, 0), goods_name, goods_type"
        row = conn.execute(
            f"SELECT {columns} FROM back WHERE user_id=%s AND goods_id=%s",
            (user_id, item_id),
        ).fetchone()
        if row is None:
            return None
        if has_bind:
            return (
                int(as_int_like(row[0])), int(as_int_like(row[1])),
                str(row[2] or ""), str(row[3] or ""),
            )
        return int(as_int_like(row[0])), None, str(row[1] or ""), str(row[2] or "")

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
        with self._lock, _FusionUnitOfWork(self._database) as conn:
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
        with self._lock, _FusionUnitOfWork(self._database) as conn:
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
        with self._lock, _FusionUnitOfWork(self._database, immediate=True) as conn:
            try:
                previous = conn.execute(
                    "SELECT payload, successful, protected FROM fusion_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return FusionResult("state_changed", successful, False, reason="operation_conflict")
                    return FusionResult("duplicate", bool(previous[1]), bool(previous[2]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return FusionResult("user_missing", successful, False)

                stone_before = as_int_like(user[0])
                columns = set(conn.column_names("back"))
                has_bind = "bind_num" in columns
                target_before = (
                    self._back_projection(conn, user_id, target_id, has_bind)
                    if successful else None
                )

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
                        raise DomainError(
                            "fusion_protection_write_rejected", "fusion write count mismatch",
                            {"subject": f"item:{protection_item_id}"},
                        )
                    saved_protection = self._back_projection(conn, user_id, protection_item_id, has_bind)
                    if saved_protection is None or saved_protection[0] != int(row[0]) - 1:
                        raise DomainError(
                            "fusion_protection_projection_mismatch", "fusion protection projection mismatch",
                            {"subject": f"item:{protection_item_id}"},
                        )
                else:
                    # 0 灵石配方（如追捕令）不扣灵石；负灵石也不应拦零成本合成
                    current_stone = as_int_like(user[0])
                    if stone_cost > 0 and current_stone < stone_cost:
                        conn.rollback()
                        return FusionResult("stone_insufficient", successful, False)
                    for item_id, quantity in materials.items():
                        row = conn.execute(
                            "SELECT COALESCE(goods_num, 0)" + (", COALESCE(bind_num, 0)" if has_bind else "")
                            + " FROM back WHERE user_id=%s AND goods_id=%s",
                            (user_id, item_id),
                        ).fetchone()
                        total = 0 if row is None else int(row[0] or 0)
                        # 合成可用绑定材料；仅预留已装备件数（reserved）
                        reserve = max(0, int(reserved.get(item_id, 0)))
                        if total - reserve < quantity:
                            conn.rollback()
                            return FusionResult("item_insufficient", successful, False)
                    if stone_cost > 0:
                        charged = conn.execute(
                            "UPDATE user_xiuxian "
                            "SET stone=xiuxian_int_store(xiuxian_int_sub(COALESCE(stone,0),%s,'stone','actor')) "
                            "WHERE user_id=%s AND xiuxian_int_cmp(COALESCE(stone,0),%s,1,'stone','actor')>=0",
                            (stone_cost, user_id, stone_cost),
                        )
                        if charged.rowcount != 1:
                            conn.rollback()
                            raise DomainError(
                                "fusion_stone_write_rejected", "fusion write count mismatch",
                                {"subject": "stone_wallet"},
                            )
                        saved_stone = conn.execute(
                            "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,),
                        ).fetchone()
                        if saved_stone is None or as_int_like(saved_stone[0]) != stone_before - stone_cost:
                            raise DomainError(
                                "fusion_stone_projection_mismatch", "fusion stone projection mismatch",
                                {"subject": "stone_wallet"},
                            )
                    for item_id, quantity in materials.items():
                        reserve = max(0, int(reserved.get(item_id, 0)))
                        if has_bind:
                            # 允许消耗绑定材料；扣完后 bind_num 不超过剩余 goods_num
                            consumed = conn.execute(
                                "UPDATE back SET goods_num=goods_num-%s, "
                                "bind_num=LEAST(COALESCE(bind_num,0), goods_num-%s) "
                                "WHERE user_id=%s AND goods_id=%s "
                                "AND CAST(COALESCE(goods_num,0) AS REAL)-%s>=%s",
                                (quantity, quantity, user_id, item_id, reserve, quantity),
                            )
                        else:
                            consumed = conn.execute(
                                "UPDATE back SET goods_num=goods_num-%s "
                                "WHERE user_id=%s AND goods_id=%s "
                                "AND CAST(COALESCE(goods_num,0) AS REAL)-%s>=%s",
                                (quantity, user_id, item_id, reserve, quantity),
                            )
                        if consumed.rowcount != 1:
                            conn.rollback()
                            raise DomainError(
                                "fusion_material_write_rejected", "fusion write count mismatch",
                                {"subject": f"item:{item_id}"},
                            )
                        saved_material = self._back_projection(conn, user_id, item_id, has_bind)
                        expected_goods = total - quantity
                        expected_bind = min(int(row[1]), expected_goods) if has_bind else None
                        if saved_material is None or saved_material[0] != expected_goods or (
                            has_bind and saved_material[1] != expected_bind
                        ):
                            raise DomainError(
                                "fusion_material_projection_mismatch", "fusion material projection mismatch",
                                {"subject": f"item:{item_id}"},
                            )

                if successful:
                    if has_bind:
                        granted = conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                            "VALUES (%s, %s, %s, %s, 1, 1) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=MIN(COALESCE(back.goods_num, 0)+1, %s), "
                            "bind_num=MIN(COALESCE(back.bind_num, 0)+1, %s)",
                            (user_id, target_id, str(target_name), str(target_type), max_goods_num, max_goods_num),
                        )
                    else:
                        granted = conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num) "
                            "VALUES (%s, %s, %s, %s, 1) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=MIN(COALESCE(back.goods_num, 0)+1, %s)",
                            (user_id, target_id, str(target_name), str(target_type), max_goods_num),
                        )
                    if granted.rowcount != 1:
                        raise DomainError(
                            "fusion_reward_write_rejected", "fusion reward write count mismatch",
                            {"subject": f"item:{target_id}"},
                        )
                    previous_goods = target_before[0] if target_before else 0
                    consumed_from_target = materials.get(target_id, 0) if not protected else 0
                    expected_goods = min(max_goods_num, previous_goods - consumed_from_target + 1)
                    expected_bind = None
                    if has_bind:
                        previous_bind = target_before[1] if target_before else 0
                        base_bind = min(previous_bind, previous_goods - consumed_from_target)
                        expected_bind = min(max_goods_num, base_bind + 1)
                    saved_reward = self._back_projection(conn, user_id, target_id, has_bind)
                    if saved_reward != (expected_goods, expected_bind, str(target_name), str(target_type)):
                        raise DomainError(
                            "fusion_reward_projection_mismatch", "fusion reward projection mismatch",
                            {"subject": f"item:{target_id}"},
                        )
                receipt = conn.execute(
                    "INSERT INTO fusion_operations (operation_id, payload, successful, protected) "
                    "VALUES (%s, %s, %s, %s)",
                    (operation_id, payload, int(successful), int(protected)),
                )
                if receipt.rowcount != 1:
                    raise DomainError(
                        "fusion_receipt_write_rejected", "fusion receipt write count mismatch",
                        {"subject": "receipt"},
                    )
                saved_receipt = conn.execute(
                    "SELECT payload, successful, protected FROM fusion_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if saved_receipt is None or (
                    str(saved_receipt[0]), int(saved_receipt[1]), int(saved_receipt[2])
                ) != (payload, int(successful), int(protected)):
                    raise DomainError(
                        "fusion_receipt_projection_mismatch", "fusion receipt projection mismatch",
                        {"subject": "receipt"},
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
        with self._lock, _FusionUnitOfWork(self._database, immediate=True) as conn:
            try:
                previous = conn.execute(
                    "SELECT payload, successful_count, failed_count, protected_count "
                    "FROM fusion_batch_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return FusionBatchResult("state_changed", 0, 0, 0, reason="operation_conflict")
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
                stone_before = as_int_like(user[0])
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

                current_stone = as_int_like(user[0])
                # 0 灵石总成本不校验/不扣费（负灵石也可合成追捕令这类配方）
                if total_stone > 0 and current_stone < total_stone:
                    conn.rollback()
                    return FusionBatchResult("stone_insufficient", 0, 0, 0)
                columns = set(conn.column_names("back"))
                has_bind = "bind_num" in columns
                material_before = {}
                for item_id, quantity in total_materials.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0)" + (", COALESCE(bind_num, 0)" if has_bind else "")
                        + " FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    total = 0 if row is None else int(row[0] or 0)
                    material_before[item_id] = (total, int(row[1]) if has_bind and row is not None else None)
                    # 合成可用绑定材料；仅预留已装备件数
                    reserve = max(0, int(reserved.get(item_id, 0)))
                    if total - reserve < quantity:
                        conn.rollback()
                        return FusionBatchResult("item_insufficient", 0, 0, 0)

                target_before = self._back_projection(conn, user_id, target_id, has_bind)
                current_target = target_before[0] if target_before else 0
                capacity = min(max_goods_num, target_limit or max_goods_num)
                if current_target + successful_count > capacity:
                    conn.rollback()
                    return FusionBatchResult("inventory_full", 0, 0, 0)

                if total_stone > 0:
                    charged = conn.execute(
                        "UPDATE user_xiuxian "
                        "SET stone=xiuxian_int_store(xiuxian_int_sub(COALESCE(stone,0),%s,'stone','actor')) "
                        "WHERE user_id=%s AND xiuxian_int_cmp(COALESCE(stone,0),%s,1,'stone','actor')>=0",
                        (total_stone, user_id, total_stone),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        raise DomainError(
                            "fusion_stone_write_rejected", "fusion write count mismatch",
                            {"subject": "stone_wallet"},
                        )
                    saved_stone = conn.execute(
                        "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,),
                    ).fetchone()
                    if saved_stone is None or as_int_like(saved_stone[0]) != stone_before - total_stone:
                        raise DomainError(
                            "fusion_stone_projection_mismatch", "fusion stone projection mismatch",
                            {"subject": "stone_wallet"},
                        )
                for item_id, quantity in total_materials.items():
                    reserve = max(0, int(reserved.get(item_id, 0)))
                    if has_bind:
                        consumed = conn.execute(
                            "UPDATE back SET goods_num=goods_num-%s, "
                            "bind_num=LEAST(COALESCE(bind_num,0), goods_num-%s) "
                            "WHERE user_id=%s AND goods_id=%s "
                            "AND CAST(COALESCE(goods_num,0) AS REAL)-%s>=%s",
                            (quantity, quantity, user_id, item_id, reserve, quantity),
                        )
                    else:
                        consumed = conn.execute(
                            "UPDATE back SET goods_num=goods_num-%s "
                            "WHERE user_id=%s AND goods_id=%s "
                            "AND CAST(COALESCE(goods_num,0) AS REAL)-%s>=%s",
                            (quantity, user_id, item_id, reserve, quantity),
                        )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        raise DomainError(
                            "fusion_material_write_rejected", "fusion write count mismatch",
                            {"subject": f"item:{item_id}"},
                        )
                    before_goods, before_bind = material_before[item_id]
                    expected_goods = before_goods - quantity
                    expected_bind = min(before_bind, expected_goods) if has_bind else None
                    saved_material = self._back_projection(conn, user_id, item_id, has_bind)
                    if saved_material is None or saved_material[0] != expected_goods or (
                        has_bind and saved_material[1] != expected_bind
                    ):
                        raise DomainError(
                            "fusion_material_projection_mismatch", "fusion material projection mismatch",
                            {"subject": f"item:{item_id}"},
                        )
                if protected_count:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s "
                        "AND goods_num>=%s",
                        (protected_count, user_id, protection_item_id, protected_count),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        raise DomainError(
                            "fusion_protection_write_rejected", "fusion write count mismatch",
                            {"subject": f"item:{protection_item_id}"},
                        )
                    saved_protection = self._back_projection(conn, user_id, protection_item_id, has_bind)
                    if saved_protection is None or saved_protection[0] != protection_available - protected_count:
                        raise DomainError(
                            "fusion_protection_projection_mismatch", "fusion protection projection mismatch",
                            {"subject": f"item:{protection_item_id}"},
                        )

                if successful_count:
                    if "bind_num" in columns:
                        granted = conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, "
                            "bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.bind_num",
                            (user_id, target_id, target_name, target_type, successful_count, successful_count),
                        )
                    else:
                        granted = conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num) "
                            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num",
                            (user_id, target_id, target_name, target_type, successful_count),
                        )
                    if granted.rowcount != 1:
                        raise DomainError(
                            "fusion_reward_write_rejected", "fusion reward write count mismatch",
                            {"subject": f"item:{target_id}"},
                        )
                    consumed_target_material = total_materials.get(target_id, 0)
                    consumed_target_protection = protected_count if target_id == protection_item_id else 0
                    expected_goods = (
                        current_target - consumed_target_material - consumed_target_protection + successful_count
                    )
                    expected_bind = None
                    if has_bind:
                        previous_bind = target_before[1] if target_before else 0
                        base_bind = min(previous_bind, current_target - consumed_target_material)
                        expected_bind = min(max_goods_num, base_bind + successful_count)
                    saved_reward = self._back_projection(conn, user_id, target_id, has_bind)
                    if saved_reward != (expected_goods, expected_bind, target_name, target_type):
                        raise DomainError(
                            "fusion_reward_projection_mismatch", "fusion reward projection mismatch",
                            {"subject": f"item:{target_id}"},
                        )
                receipt = conn.execute(
                    "INSERT INTO fusion_batch_operations "
                    "(operation_id, payload, outcomes, successful_count, failed_count, protected_count) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, payload, json.dumps(outcomes), successful_count, failed_count, protected_count),
                )
                if receipt.rowcount != 1:
                    raise DomainError(
                        "fusion_receipt_write_rejected", "fusion batch receipt write count mismatch",
                        {"subject": "receipt"},
                    )
                saved_receipt = conn.execute(
                    "SELECT payload, outcomes, successful_count, failed_count, protected_count "
                    "FROM fusion_batch_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                expected_receipt = (
                    payload, json.dumps(outcomes), successful_count, failed_count, protected_count,
                )
                if saved_receipt is None or tuple(saved_receipt) != expected_receipt:
                    raise DomainError(
                        "fusion_receipt_projection_mismatch", "fusion batch receipt projection mismatch",
                        {"subject": "receipt"},
                    )
                conn.commit()
                return FusionBatchResult("applied", successful_count, failed_count, protected_count)
            except Exception:
                conn.rollback()
                raise


__all__ = ["FusionBatchResult", "FusionResult", "FusionService"]
