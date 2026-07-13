from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class RiftKeySettlementResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftKeySettlementService:
    """Atomically consume a key and claim the current active rift for settlement."""

    def __init__(self, database: str | Path, lock: RLock | None = None, operation_table: str = "rift_key_operations") -> None:
        self._database = Path(database)
        self._lock = lock or RLock()
        self._operation_table = operation_table

    def settle(self, operation_id, user_id, item_id, rift_data) -> RiftKeySettlementResult:
        return self._settle(operation_id, user_id, item_id, rift_data)

    def _settle(self, operation_id, user_id, item_id, rift_data) -> RiftKeySettlementResult:
        operation_id = str(operation_id).strip()
        user_id, item_id = str(user_id), int(item_id)
        snapshot = json.dumps(rift_data, ensure_ascii=False, sort_keys=True)
        if not operation_id or not user_id or item_id <= 0:
            raise ValueError("valid operation, user and item are required")
        payload = json.dumps([user_id, item_id, snapshot], ensure_ascii=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                table = db_backend.quote_ident(self._operation_table)
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute(f"SELECT payload FROM {table} WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return RiftKeySettlementResult("duplicate" if str(old[0]) == payload else "state_changed")
                entry = conn.execute("SELECT rift_data,status FROM rift_entries WHERE user_id=%s", (user_id,)).fetchone()
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if entry is None or str(entry[1]) != "active" or cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftKeySettlementResult("not_active")
                if json.loads(str(entry[0])) != json.loads(snapshot):
                    conn.rollback()
                    return RiftKeySettlementResult("state_changed")
                item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return RiftKeySettlementResult("item_missing")
                conn.execute("UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s", (user_id, item_id))
                conn.execute("UPDATE rift_entries SET status='settled' WHERE user_id=%s", (user_id,))
                conn.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=3", (user_id,))
                conn.execute(f"INSERT INTO {table} VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return RiftKeySettlementResult("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["RiftKeySettlementResult", "RiftKeySettlementService"]
