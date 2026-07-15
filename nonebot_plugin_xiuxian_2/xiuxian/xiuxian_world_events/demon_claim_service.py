from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DemonClaimResult:
    status: str
    stone: int = 0
    exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DemonClaimService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def _ensure_ops(self, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS demon_claim_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
            "stone INTEGER NOT NULL DEFAULT 0, exp INTEGER NOT NULL DEFAULT 0, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(demon_claim_operations)").fetchall()}
        if "stone" not in cols:
            try:
                conn.execute("ALTER TABLE demon_claim_operations ADD COLUMN stone INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
        if "exp" not in cols:
            try:
                conn.execute("ALTER TABLE demon_claim_operations ADD COLUMN exp INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass

    def get_result(self, operation_id: str) -> DemonClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_ops(conn)
            old = conn.execute(
                "SELECT COALESCE(stone,0),COALESCE(exp,0) FROM demon_claim_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return DemonClaimResult("duplicate", int(old[0]), int(old[1]))

    def claim(
        self,
        operation_id,
        event_key,
        event_id,
        user_id,
        expected_claimed,
        stone,
        exp,
        items,
        max_goods_num,
    ):
        operation_id, event_key, event_id, user_id = map(str, (operation_id, event_key, event_id, user_id))
        stone, exp, max_goods_num = int(stone), int(exp), int(max_goods_num)
        claimed = dict(expected_claimed)
        rewards = tuple(
            (int(x["id"]), str(x["name"]), str(x["type"]), int(x["amount"]))
            for x in items
            if int(x.get("amount", 0)) > 0
        )
        if not operation_id or min(stone, exp, max_goods_num) < 0:
            raise ValueError("valid claim and rewards are required")
        # Request identity only; claimed map / reward amounts are concurrency/outcome.
        payload = json.dumps(
            [event_key, event_id, user_id],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_ops(conn)
                old = conn.execute(
                    "SELECT payload,COALESCE(stone,0),COALESCE(exp,0) FROM demon_claim_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return DemonClaimResult("state_changed")
                    return DemonClaimResult("duplicate", int(old[1]), int(old[2]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return DemonClaimResult("user_missing")
                row = conn.execute(
                    "SELECT event_id, claimed FROM player_data.world_event_state WHERE user_id=%s",
                    (event_key,),
                ).fetchone()
                if row is None or str(row[0]) != event_id:
                    conn.rollback()
                    return DemonClaimResult("state_changed")
                try:
                    current = json.loads(str(row[1])) if row[1] else {}
                except (TypeError, ValueError):
                    conn.rollback()
                    return DemonClaimResult("state_changed")
                if current != claimed:
                    conn.rollback()
                    return DemonClaimResult("state_changed")
                if current.get(user_id):
                    conn.rollback()
                    return DemonClaimResult("already_claimed")
                for item_id, _, _, amount in rewards:
                    inv = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(inv[0]) if inv else 0) + amount > max_goods_num:
                        conn.rollback()
                        return DemonClaimResult("inventory_full")
                current[user_id] = True
                conn.execute(
                    "UPDATE player_data.world_event_state SET claimed=%s WHERE user_id=%s",
                    (json.dumps(current, ensure_ascii=False), event_key),
                )
                conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS INTEGER)+%s, "
                    "exp=CAST(COALESCE(exp,0) AS INTEGER)+%s WHERE user_id=%s",
                    (stone, exp, user_id),
                )
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, "
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, "
                        "update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute(
                    "INSERT INTO demon_claim_operations(operation_id,payload,stone,exp) "
                    "VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone, exp),
                )
                conn.commit()
                return DemonClaimResult("applied", stone, exp)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DemonClaimResult", "DemonClaimService"]
