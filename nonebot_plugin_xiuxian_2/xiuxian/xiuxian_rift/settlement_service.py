from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class RiftSettlementResult:
    status: str
    explore_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftSettlementService:
    """Close an active rift and apply its already-rolled resource delta atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_rift, expected_user, delta, items=(), max_goods_num=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
        expected = tuple(int(expected_user.get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        changes = tuple(int(delta.get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        rewards = tuple((int(x["id"]), str(x["name"]), str(x["type"]), int(x["amount"])) for x in items)
        payload = json.dumps([user_id, snapshot, expected, changes, rewards, int(max_goods_num)], ensure_ascii=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS rift_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,explore_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                conn.execute("CREATE TABLE IF NOT EXISTS rift_settlement_counts(user_id TEXT PRIMARY KEY,explore_count INTEGER NOT NULL)")
                old = conn.execute("SELECT payload,explore_count FROM rift_settlement_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback(); return RiftSettlementResult("duplicate" if str(old[0]) == payload else "state_changed", int(old[1]) if str(old[0]) == payload else 0)
                entry = conn.execute("SELECT rift_data,status FROM rift_entries WHERE user_id=%s", (user_id,)).fetchone()
                cd = conn.execute("SELECT type FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                user = conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if entry is None or str(entry[1]) != "active": conn.rollback(); return RiftSettlementResult("not_active")
                if json.loads(str(entry[0])) != json.loads(snapshot) or cd is None or int(cd[0]) != 3: conn.rollback(); return RiftSettlementResult("state_changed")
                if user is None: conn.rollback(); return RiftSettlementResult("user_missing")
                if tuple(map(int, user)) != expected: conn.rollback(); return RiftSettlementResult("state_changed")
                if any(expected[i] + changes[i] < 0 for i in range(4)): conn.rollback(); return RiftSettlementResult("resource_missing")
                for item_id, _, _, amount in rewards:
                    row = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > int(max_goods_num): conn.rollback(); return RiftSettlementResult("inventory_full")
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s,exp=exp+%s,hp=hp+%s,mp=mp+%s WHERE user_id=%s", (*changes, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num", (user_id,item_id,name,item_type,amount,now,now,amount))
                conn.execute("UPDATE rift_entries SET status='settled' WHERE user_id=%s", (user_id,))
                conn.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s", (user_id,))
                conn.execute("INSERT INTO rift_settlement_counts VALUES(%s,1) ON CONFLICT(user_id) DO UPDATE SET explore_count=rift_settlement_counts.explore_count+1", (user_id,))
                count = int(conn.execute("SELECT explore_count FROM rift_settlement_counts WHERE user_id=%s", (user_id,)).fetchone()[0])
                conn.execute("INSERT INTO rift_settlement_operations VALUES(%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id,payload,count))
                conn.commit(); return RiftSettlementResult("applied", count)
            except Exception:
                conn.rollback(); raise


__all__ = ["RiftSettlementResult", "RiftSettlementService"]
