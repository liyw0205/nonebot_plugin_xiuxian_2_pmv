from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


__all__ = ["SectShopPurchaseResult", "SectShopPurchaseService"]
