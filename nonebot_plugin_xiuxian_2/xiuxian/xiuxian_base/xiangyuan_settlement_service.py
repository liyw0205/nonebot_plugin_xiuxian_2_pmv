from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class XiangyuanCreateResult:
    status: str
    gift_id: int = 0
    send_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


@dataclass(frozen=True)
class XiangyuanClaimResult:
    status: str
    gift_id: int = 0
    giver_name: str = ""
    stone: int = 0
    items: tuple[tuple[int, str, int], ...] = ()
    received: int = 0
    receiver_count: int = 0
    remaining_stone: int = 0
    receive_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class XiangyuanSettlementService:
    """Store xiangyuan pools in the database and settle both sides atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_groups ("
            "group_id TEXT PRIMARY KEY,next_gift_id INTEGER NOT NULL DEFAULT 1,"
            "legacy_imported INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_gifts ("
            "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,giver_id TEXT NOT NULL,"
            "giver_name TEXT NOT NULL,stone_amount INTEGER NOT NULL,remaining_stone INTEGER NOT NULL,"
            "receiver_count INTEGER NOT NULL,received INTEGER NOT NULL DEFAULT 0,create_time TEXT NOT NULL,"
            "PRIMARY KEY(group_id,gift_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_gift_items ("
            "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,goods_id INTEGER NOT NULL,"
            "goods_name TEXT NOT NULL,goods_type TEXT NOT NULL,quantity INTEGER NOT NULL,"
            "PRIMARY KEY(group_id,gift_id,goods_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_receivers ("
            "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,user_id TEXT NOT NULL,"
            "stone INTEGER NOT NULL,items TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(group_id,gift_id,user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_create_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gift_id INTEGER NOT NULL,"
            "send_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_claim_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.xiangyuan_limit ("
            "user_id TEXT PRIMARY KEY,send_count INTEGER DEFAULT 0,receive_count INTEGER DEFAULT 0,"
            "last_reset_date TEXT DEFAULT '')"
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(xiangyuan_limit)").fetchall()
        }
        for name, sql_type, default in (
            ("send_count", "INTEGER", "0"),
            ("receive_count", "INTEGER", "0"),
            ("last_reset_date", "TEXT", "''"),
        ):
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.xiangyuan_limit ADD COLUMN "
                    f"{db_backend.quote_ident(name)} {sql_type} DEFAULT {default}"
                )

    @staticmethod
    def _normalize_items(items) -> tuple[tuple[int, str, str, int], ...]:
        merged: dict[int, list] = {}
        for item in items or ():
            goods_id = int(item["goods_id"])
            quantity = int(item["quantity"])
            if quantity <= 0:
                continue
            metadata = [str(item["name"]), str(item["type"])]
            if goods_id in merged and merged[goods_id][:2] != metadata:
                raise ValueError("conflicting xiangyuan item metadata")
            merged.setdefault(goods_id, metadata + [0])[2] += quantity
        return tuple((goods_id, *values) for goods_id, values in sorted(merged.items()))

    @classmethod
    def _import_legacy(cls, conn, group_id: str, legacy_data) -> None:
        row = conn.execute(
            "SELECT legacy_imported FROM xiangyuan_groups WHERE group_id=%s", (group_id,)
        ).fetchone()
        if row is not None and int(row[0]) == 1:
            return
        data = legacy_data if isinstance(legacy_data, dict) else {}
        gifts = data.get("gifts", {}) if isinstance(data.get("gifts", {}), dict) else {}
        largest_id = 0
        for raw_id, gift in gifts.items():
            if not isinstance(gift, dict):
                continue
            gift_id = int(gift.get("id", raw_id))
            largest_id = max(largest_id, gift_id)
            conn.execute(
                "INSERT OR IGNORE INTO xiangyuan_gifts (group_id,gift_id,giver_id,giver_name,"
                "stone_amount,remaining_stone,receiver_count,received,create_time) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    group_id, gift_id, str(gift.get("giver_id", "")), str(gift.get("giver_name", "")),
                    int(gift.get("stone_amount", 0)), int(gift.get("remaining_stone", 0)),
                    int(gift.get("receiver_count", 0)), int(gift.get("received", 0)),
                    str(gift.get("create_time", "")),
                ),
            )
            for goods_id, name, item_type, quantity in cls._normalize_items(gift.get("items", ())):
                conn.execute(
                    "INSERT OR IGNORE INTO xiangyuan_gift_items "
                    "(group_id,gift_id,goods_id,goods_name,goods_type,quantity) VALUES (%s,%s,%s,%s,%s,%s)",
                    (group_id, gift_id, goods_id, name, item_type, quantity),
                )
            for receiver in gift.get("receivers", ()):
                conn.execute(
                    "INSERT OR IGNORE INTO xiangyuan_receivers "
                    "(group_id,gift_id,user_id,stone,items) VALUES (%s,%s,%s,%s,%s)",
                    (group_id, gift_id, str(receiver), 0, "[]"),
                )
        next_id = max(int(data.get("last_id", 1) or 1), largest_id + 1)
        conn.execute(
            "INSERT INTO xiangyuan_groups (group_id,next_gift_id,legacy_imported) VALUES (%s,%s,1) "
            "ON CONFLICT(group_id) DO UPDATE SET next_gift_id=MAX(xiangyuan_groups.next_gift_id,EXCLUDED.next_gift_id),legacy_imported=1",
            (group_id, next_id),
        )

    def create(
        self, operation_id, group_id, giver_id, giver_name, stone, items,
        receiver_count, send_limit, *, legacy_data=None,
    ) -> XiangyuanCreateResult:
        operation_id, group_id, giver_id = str(operation_id).strip(), str(group_id), str(giver_id)
        giver_name = str(giver_name)
        stone, receiver_count, send_limit = map(int, (stone, receiver_count, send_limit))
        item_rows = self._normalize_items(items)
        if not operation_id or stone < 0 or receiver_count <= 0 or send_limit <= 0 or (stone == 0 and not item_rows):
            raise ValueError("valid xiangyuan gift is required")
        payload = json.dumps(
            [group_id, giver_id, giver_name, stone, item_rows, receiver_count, send_limit],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy(conn, group_id, legacy_data)
                previous = conn.execute(
                    "SELECT payload,gift_id,send_count FROM xiangyuan_create_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    status = "duplicate" if str(previous[0]) == payload else "operation_conflict"
                    return XiangyuanCreateResult(status, int(previous[1]), int(previous[2]))
                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (giver_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return XiangyuanCreateResult("user_missing")
                conn.execute(
                    "INSERT OR IGNORE INTO player_data.xiangyuan_limit "
                    "(user_id,send_count,receive_count,last_reset_date) VALUES (%s,0,0,'')", (giver_id,)
                )
                count = int(conn.execute(
                    "SELECT COALESCE(send_count,0) FROM player_data.xiangyuan_limit WHERE user_id=%s",
                    (giver_id,),
                ).fetchone()[0])
                if count >= send_limit:
                    conn.rollback()
                    return XiangyuanCreateResult("limit_reached", send_count=count)
                if int(user[0]) < stone:
                    conn.rollback()
                    return XiangyuanCreateResult("stone_insufficient", send_count=count)
                back_columns = {
                    str(row[1]) for row in conn.execute("PRAGMA table_info(back)").fetchall()
                }
                state_sql = "COALESCE(state,0)" if "state" in back_columns else "0"
                bind_sql = "COALESCE(bind_num,0)" if "bind_num" in back_columns else "0"
                for goods_id, _, _, quantity in item_rows:
                    row = conn.execute(
                        f"SELECT COALESCE(goods_num,0)-{bind_sql}-{state_sql} FROM back "
                        "WHERE user_id=%s AND goods_id=%s", (giver_id, goods_id),
                    ).fetchone()
                    if row is None or int(row[0]) < quantity:
                        conn.rollback()
                        return XiangyuanCreateResult("item_insufficient", send_count=count)
                gift_id = int(conn.execute(
                    "SELECT next_gift_id FROM xiangyuan_groups WHERE group_id=%s", (group_id,)
                ).fetchone()[0])
                conn.execute("UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s", (stone, giver_id))
                for goods_id, _, _, quantity in item_rows:
                    conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s,update_time=%s WHERE user_id=%s AND goods_id=%s",
                        (quantity, datetime.now(), giver_id, goods_id),
                    )
                conn.execute(
                    "INSERT INTO xiangyuan_gifts (group_id,gift_id,giver_id,giver_name,stone_amount,"
                    "remaining_stone,receiver_count,received,create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,0,%s)",
                    (group_id, gift_id, giver_id, giver_name, stone, stone, receiver_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
                for goods_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO xiangyuan_gift_items "
                        "(group_id,gift_id,goods_id,goods_name,goods_type,quantity) VALUES (%s,%s,%s,%s,%s,%s)",
                        (group_id, gift_id, goods_id, name, item_type, quantity),
                    )
                conn.execute("UPDATE xiangyuan_groups SET next_gift_id=%s WHERE group_id=%s", (gift_id + 1, group_id))
                conn.execute(
                    "UPDATE player_data.xiangyuan_limit SET send_count=%s WHERE user_id=%s", (count + 1, giver_id)
                )
                conn.execute(
                    "INSERT INTO xiangyuan_create_operations (operation_id,payload,gift_id,send_count) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, gift_id, count + 1),
                )
                conn.commit()
                return XiangyuanCreateResult("applied", gift_id, count + 1)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _claim_result_from_json(status: str, raw: str) -> XiangyuanClaimResult:
        value = json.loads(raw)
        return XiangyuanClaimResult(status, int(value[0]), str(value[1]), int(value[2]), tuple(tuple(item) for item in value[3]), int(value[4]), int(value[5]), int(value[6]), int(value[7]))

    def claim(
        self, operation_id, group_id, gift_id, user_id, stone_reward, item_ids,
        receive_limit, max_goods_num, *, legacy_data=None,
    ) -> XiangyuanClaimResult:
        operation_id, group_id, user_id = str(operation_id).strip(), str(group_id), str(user_id)
        gift_id, stone_reward, receive_limit, max_goods_num = map(int, (gift_id, stone_reward, receive_limit, max_goods_num))
        item_ids = tuple(sorted({int(value) for value in item_ids}))
        if not operation_id or gift_id <= 0 or stone_reward < 0 or receive_limit <= 0 or max_goods_num <= 0:
            raise ValueError("valid xiangyuan claim is required")
        payload = json.dumps([group_id, gift_id, user_id, stone_reward, item_ids, receive_limit, max_goods_num], separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy(conn, group_id, legacy_data)
                previous = conn.execute(
                    "SELECT payload,result FROM xiangyuan_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return XiangyuanClaimResult("operation_conflict")
                    return self._claim_result_from_json("duplicate", str(previous[1]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return XiangyuanClaimResult("user_missing")
                conn.execute(
                    "INSERT OR IGNORE INTO player_data.xiangyuan_limit "
                    "(user_id,send_count,receive_count,last_reset_date) VALUES (%s,0,0,'')", (user_id,)
                )
                receive_count = int(conn.execute(
                    "SELECT COALESCE(receive_count,0) FROM player_data.xiangyuan_limit WHERE user_id=%s", (user_id,)
                ).fetchone()[0])
                if receive_count >= receive_limit:
                    conn.rollback()
                    return XiangyuanClaimResult("limit_reached", receive_count=receive_count)
                gift = conn.execute(
                    "SELECT giver_name,remaining_stone,receiver_count,received FROM xiangyuan_gifts "
                    "WHERE group_id=%s AND gift_id=%s", (group_id, gift_id),
                ).fetchone()
                if gift is None or int(gift[3]) >= int(gift[2]):
                    conn.rollback()
                    return XiangyuanClaimResult("unavailable", gift_id=gift_id)
                if conn.execute(
                    "SELECT 1 FROM xiangyuan_receivers WHERE group_id=%s AND gift_id=%s AND user_id=%s",
                    (group_id, gift_id, user_id),
                ).fetchone():
                    conn.rollback()
                    return XiangyuanClaimResult("already_received", gift_id=gift_id)
                remaining_stone = int(gift[1])
                is_last = int(gift[3]) + 1 >= int(gift[2])
                if stone_reward > remaining_stone or (is_last and stone_reward != remaining_stone):
                    conn.rollback()
                    return XiangyuanClaimResult("state_changed", gift_id=gift_id)
                available_items = {}
                for row in conn.execute(
                    "SELECT goods_id,goods_name,goods_type,quantity FROM xiangyuan_gift_items "
                    "WHERE group_id=%s AND gift_id=%s AND quantity>0", (group_id, gift_id),
                ).fetchall():
                    available_items[int(row[0])] = (str(row[1]), str(row[2]), int(row[3]))
                if any(item_id not in available_items for item_id in item_ids):
                    conn.rollback()
                    return XiangyuanClaimResult("state_changed", gift_id=gift_id)
                awarded = tuple((item_id, available_items[item_id][0], 1) for item_id in item_ids)
                for item_id, _, amount in awarded:
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(current[0]) if current else 0) + amount > max_goods_num:
                        conn.rollback()
                        return XiangyuanClaimResult("inventory_full", gift_id=gift_id)
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s", (stone_reward, user_id))
                back_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(back)").fetchall()}
                now = datetime.now()
                for item_id, name, amount in awarded:
                    item_type = available_items[item_id][1]
                    if "bind_num" in back_columns:
                        conn.execute(
                            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+1,"
                            "bind_num=COALESCE(back.bind_num,0)+1,update_time=EXCLUDED.update_time",
                            (user_id, item_id, name, item_type, amount, now, now, amount),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+1,update_time=EXCLUDED.update_time",
                            (user_id, item_id, name, item_type, amount, now, now),
                        )
                    conn.execute(
                        "UPDATE xiangyuan_gift_items SET quantity=quantity-1 WHERE group_id=%s AND gift_id=%s AND goods_id=%s",
                        (group_id, gift_id, item_id),
                    )
                received = int(gift[3]) + 1
                remaining_stone -= stone_reward
                conn.execute(
                    "UPDATE xiangyuan_gifts SET received=%s,remaining_stone=%s WHERE group_id=%s AND gift_id=%s",
                    (received, remaining_stone, group_id, gift_id),
                )
                conn.execute(
                    "INSERT INTO xiangyuan_receivers (group_id,gift_id,user_id,stone,items) VALUES (%s,%s,%s,%s,%s)",
                    (group_id, gift_id, user_id, stone_reward, json.dumps(awarded, ensure_ascii=True)),
                )
                conn.execute(
                    "UPDATE player_data.xiangyuan_limit SET receive_count=%s WHERE user_id=%s", (receive_count + 1, user_id)
                )
                result_data = [gift_id, str(gift[0]), stone_reward, awarded, received, int(gift[2]), remaining_stone, receive_count + 1]
                conn.execute(
                    "INSERT INTO xiangyuan_claim_operations (operation_id,payload,result) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(result_data, ensure_ascii=True)),
                )
                conn.commit()
                return self._claim_result_from_json("applied", json.dumps(result_data))
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def get_group(self, group_id, *, legacy_data=None) -> dict:
        group_id = str(group_id)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy(conn, group_id, legacy_data)
                group = conn.execute("SELECT next_gift_id FROM xiangyuan_groups WHERE group_id=%s", (group_id,)).fetchone()
                gifts = {}
                for row in conn.execute(
                    "SELECT gift_id,giver_id,giver_name,stone_amount,remaining_stone,receiver_count,received,create_time "
                    "FROM xiangyuan_gifts WHERE group_id=%s ORDER BY gift_id", (group_id,),
                ).fetchall():
                    gift_id = int(row[0])
                    item_rows = conn.execute(
                        "SELECT goods_id,goods_name,goods_type,quantity FROM xiangyuan_gift_items "
                        "WHERE group_id=%s AND gift_id=%s ORDER BY goods_id", (group_id, gift_id),
                    ).fetchall()
                    receivers = conn.execute(
                        "SELECT user_id FROM xiangyuan_receivers WHERE group_id=%s AND gift_id=%s ORDER BY created_at,user_id",
                        (group_id, gift_id),
                    ).fetchall()
                    gifts[str(gift_id)] = {
                        "id": gift_id, "giver_id": str(row[1]), "giver_name": str(row[2]),
                        "stone_amount": int(row[3]), "remaining_stone": int(row[4]),
                        "items": [{"goods_id": int(item[0]), "name": str(item[1]), "type": str(item[2]), "quantity": int(item[3])} for item in item_rows],
                        "receiver_count": int(row[5]), "received": int(row[6]),
                        "receivers": [str(receiver[0]) for receiver in receivers], "create_time": str(row[7]),
                    }
                conn.commit()
                return {"gifts": gifts, "last_id": int(group[0]) if group else 1}
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def clear_all(self, max_goods_num: int) -> tuple[int, int, int, int]:
        """Refund every active database pool and clear its authoritative state."""
        max_goods_num = int(max_goods_num)
        if max_goods_num <= 0:
            raise ValueError("max_goods_num must be positive")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                if not conn.table_exists("xiangyuan_gifts"):
                    conn.rollback()
                    return 0, 0, 0, 0
                rows = conn.execute(
                    "SELECT group_id,gift_id,giver_id,remaining_stone FROM xiangyuan_gifts"
                ).fetchall()
                if not rows:
                    conn.rollback()
                    return 0, 0, 0, 0
                groups = {str(row[0]) for row in rows}
                refund_stone = 0
                refund_items = 0
                back_columns = {
                    str(row[1]) for row in conn.execute("PRAGMA table_info(back)").fetchall()
                }
                now = datetime.now()
                for group_id, gift_id, giver_id, remaining_stone in rows:
                    remaining_stone = int(remaining_stone)
                    if remaining_stone > 0:
                        conn.execute(
                            "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                            (remaining_stone, str(giver_id)),
                        )
                        refund_stone += remaining_stone
                    for item in conn.execute(
                        "SELECT goods_id,goods_name,goods_type,quantity FROM xiangyuan_gift_items "
                        "WHERE group_id=%s AND gift_id=%s AND quantity>0",
                        (str(group_id), int(gift_id)),
                    ).fetchall():
                        goods_id, name, item_type, quantity = int(item[0]), str(item[1]), str(item[2]), int(item[3])
                        current = conn.execute(
                            "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                            (str(giver_id), goods_id),
                        ).fetchone()
                        if (int(current[0]) if current else 0) + quantity > max_goods_num:
                            conn.rollback()
                            raise ValueError("inventory_full")
                        if "bind_num" in back_columns:
                            conn.execute(
                                "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                                "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,"
                                "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                                (str(giver_id), goods_id, name, item_type, quantity, now, now, quantity),
                            )
                        else:
                            conn.execute(
                                "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                                "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,update_time=EXCLUDED.update_time",
                                (str(giver_id), goods_id, name, item_type, quantity, now, now),
                            )
                        refund_items += quantity
                conn.execute("DELETE FROM xiangyuan_receivers")
                conn.execute("DELETE FROM xiangyuan_gift_items")
                conn.execute("DELETE FROM xiangyuan_gifts")
                conn.execute("UPDATE xiangyuan_groups SET next_gift_id=1")
                conn.commit()
                return len(groups), len(rows), refund_stone, refund_items
            except Exception:
                conn.rollback()
                raise


__all__ = ["XiangyuanClaimResult", "XiangyuanCreateResult", "XiangyuanSettlementService"]
