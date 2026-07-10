from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from threading import RLock
from typing import Literal

from ..xiuxian_utils import db_backend


PurchaseStatus = Literal[
    "purchased",
    "duplicate",
    "listing_missing",
    "self_purchase",
    "stock_insufficient",
    "buyer_missing",
    "seller_missing",
    "stone_insufficient",
    "inventory_full",
]


@dataclass(frozen=True)
class XianshiPurchase:
    status: PurchaseStatus
    listing_id: str
    buyer_id: str
    seller_id: str = ""
    goods_id: int = 0
    name: str = ""
    goods_type: str = ""
    quantity: int = 0
    total_cost: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"purchased", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "purchased"


class TradeRepository:
    """Atomic marketplace operations stored with player economy data."""

    def __init__(
        self,
        database: str | Path,
        *,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._database = Path(database)
        self._max_goods_num = max(1, int(max_goods_num))
        self._lock = lock or RLock()

    def initialize(self, legacy_database: str | Path | None = None) -> None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self.ensure_schema(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_migrations (
                    migration_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            if legacy_database is not None and Path(legacy_database) != self._database:
                migration_id = "xianshi_item_from_trade_db_v1"
                if conn.execute(
                    "SELECT 1 FROM trade_migrations WHERE migration_id=%s",
                    (migration_id,),
                ).fetchone():
                    conn.commit()
                    return
                conn.execute("ATTACH DATABASE %s AS legacy_trade", (str(legacy_database),))
                try:
                    if conn.execute(
                        "SELECT 1 FROM legacy_trade.sqlite_master "
                        "WHERE type='table' AND name='xianshi_item'"
                    ).fetchone():
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO xianshi_item (
                                id, user_id, goods_id, name, type, price, quantity
                            )
                            SELECT id, user_id, goods_id, name, type, price, quantity
                            FROM legacy_trade.xianshi_item
                            """
                        )
                    conn.execute(
                        "INSERT INTO trade_migrations (migration_id) VALUES (%s)",
                        (migration_id,),
                    )
                finally:
                    conn.commit()
                    conn.execute("DETACH DATABASE legacy_trade")
            conn.commit()

    def add_xianshi_item(self, user_id, goods_id, name, goods_type, price, quantity) -> str:
        import secrets

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self.ensure_schema(conn)
            for _ in range(20):
                listing_id = str(secrets.randbelow(9_000_000_000_000) + 1_000_000_000_000)
                try:
                    conn.execute(
                        """
                        INSERT INTO xianshi_item (
                            id, user_id, goods_id, name, type, price, quantity
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            listing_id,
                            str(user_id),
                            int(goods_id),
                            str(name),
                            str(goods_type),
                            int(price),
                            int(quantity),
                        ),
                    )
                    conn.commit()
                    return listing_id
                except db_backend.IntegrityError:
                    conn.rollback()
            raise RuntimeError("failed to allocate xianshi listing id")

    def get_xianshi_items(self, *, user_id=None, type=None, id=None, name=None):
        conditions = []
        params = []
        for column, value in (
            ("user_id", user_id),
            ("type", type),
            ("id", id),
            ("name", name),
        ):
            if value is not None:
                conditions.append(f"{column}=%s")
                params.append(str(value))
        sql = "SELECT * FROM xianshi_item"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self.ensure_schema(conn)
            conn.row_factory = db_backend.Row
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows] or None

    def remove_xianshi_item(self, listing_id, quantity=1) -> bool:
        quantity = abs(int(quantity))
        if quantity <= 0:
            return True
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self.ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT quantity FROM xianshi_item WHERE id=%s", (str(listing_id),)
            ).fetchone()
            if row is None or (int(row[0]) != -1 and int(row[0]) < quantity):
                conn.rollback()
                return False
            stock = int(row[0])
            if stock != -1:
                if stock == quantity:
                    conn.execute("DELETE FROM xianshi_item WHERE id=%s", (str(listing_id),))
                else:
                    conn.execute(
                        "UPDATE xianshi_item SET quantity=quantity-%s WHERE id=%s",
                        (quantity, str(listing_id)),
                    )
            conn.commit()
            return True

    def remove_xianshi_all_item(self, listing_id) -> None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self.ensure_schema(conn)
            conn.execute("DELETE FROM xianshi_item WHERE id=%s", (str(listing_id),))
            conn.commit()

    @staticmethod
    def ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS xianshi_item (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                goods_id INTEGER,
                name TEXT,
                type TEXT,
                price INTEGER,
                quantity INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS xianshi_operations (
                operation_id TEXT PRIMARY KEY,
                listing_id TEXT NOT NULL,
                buyer_id TEXT NOT NULL,
                seller_id TEXT NOT NULL,
                goods_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                goods_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                total_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _row_purchase(status: PurchaseStatus, row) -> XianshiPurchase:
        return XianshiPurchase(
            status=status,
            listing_id=str(row[0]),
            buyer_id=str(row[1]),
            seller_id=str(row[2]),
            goods_id=int(row[3]),
            name=str(row[4]),
            goods_type=str(row[5]),
            quantity=int(row[6]),
            total_cost=int(row[7]),
        )

    def purchase_xianshi_item(
        self,
        operation_id: str,
        buyer_id: str,
        listing_id: str,
        quantity: int,
    ) -> XianshiPurchase:
        operation_id = str(operation_id).strip()
        buyer_id = str(buyer_id)
        listing_id = str(listing_id)
        quantity = max(1, int(quantity))
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self.ensure_schema(conn)
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT listing_id, buyer_id, seller_id, goods_id, name,
                           goods_type, quantity, total_cost
                    FROM xianshi_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    conn.rollback()
                    return self._row_purchase("duplicate", existing)

                cur.execute(
                    """
                    SELECT user_id, goods_id, name, type, price, quantity
                    FROM xianshi_item WHERE id=%s
                    """,
                    (listing_id,),
                )
                listing = cur.fetchone()
                if listing is None:
                    conn.rollback()
                    return XianshiPurchase("listing_missing", listing_id, buyer_id)

                seller_id = str(listing[0])
                goods_id = int(listing[1])
                name = str(listing[2])
                goods_type = str(listing[3])
                price = int(listing[4])
                stock = int(listing[5])
                total_cost = price * quantity
                result = XianshiPurchase(
                    "purchased",
                    listing_id,
                    buyer_id,
                    seller_id,
                    goods_id,
                    name,
                    goods_type,
                    quantity,
                    total_cost,
                )

                if seller_id == buyer_id:
                    conn.rollback()
                    return replace(result, status="self_purchase")
                if stock != -1 and stock < quantity:
                    conn.rollback()
                    return replace(result, status="stock_insufficient")

                cur.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (buyer_id,))
                buyer = cur.fetchone()
                if buyer is None:
                    conn.rollback()
                    return replace(result, status="buyer_missing")
                if int(buyer[0] or 0) < total_cost:
                    conn.rollback()
                    return replace(result, status="stone_insufficient")

                if seller_id != "0":
                    cur.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (seller_id,))
                    if cur.fetchone() is None:
                        conn.rollback()
                        return replace(result, status="seller_missing")

                cur.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (buyer_id, goods_id),
                )
                inventory = cur.fetchone()
                current_quantity = int(inventory[0] or 0) if inventory else 0
                if current_quantity + quantity > self._max_goods_num:
                    conn.rollback()
                    return replace(result, status="inventory_full")

                cur.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s",
                    (total_cost, buyer_id),
                )
                if seller_id != "0":
                    cur.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                        (total_cost, seller_id),
                    )

                if stock != -1:
                    if stock == quantity:
                        cur.execute("DELETE FROM xianshi_item WHERE id=%s", (listing_id,))
                    else:
                        cur.execute(
                            "UPDATE xianshi_item SET quantity=quantity-%s WHERE id=%s",
                            (quantity, listing_id),
                        )

                cur.execute(
                    """
                    INSERT INTO back (
                        user_id, goods_id, goods_name, goods_type, goods_num,
                        create_time, update_time, bind_num
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
                    ON CONFLICT (user_id, goods_id) DO UPDATE SET
                        goods_name=EXCLUDED.goods_name,
                        goods_type=EXCLUDED.goods_type,
                        goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num,
                        bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.bind_num,
                        update_time=CURRENT_TIMESTAMP
                    """,
                    (buyer_id, goods_id, name, goods_type, quantity, quantity),
                )
                cur.execute(
                    """
                    INSERT INTO xianshi_operations (
                        operation_id, listing_id, buyer_id, seller_id, goods_id,
                        name, goods_type, quantity, total_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        listing_id,
                        buyer_id,
                        seller_id,
                        goods_id,
                        name,
                        goods_type,
                        quantity,
                        total_cost,
                    ),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


__all__ = ["TradeRepository", "XianshiPurchase"]
