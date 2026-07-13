from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuArrayUpgradeResult:
    status: str
    level: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"upgraded", "duplicate"}


class DongfuArrayUpgradeService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database, self._lock = Path(game_database), Path(player_database), lock or RLock()

    def upgrade(self, operation_id, user_id, expected_level, next_level, stone_cost, item_id, item_cost):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, next_level, stone_cost, item_id, item_cost = map(int, (expected_level, next_level, stone_cost, item_id, item_cost))
        if not operation_id or expected_level < 0 or next_level != expected_level + 1 or stone_cost < 0 or item_cost < 0:
            raise ValueError("valid array upgrade is required")
        payload = "|".join(map(str, (user_id, expected_level, next_level, stone_cost, item_id, item_cost)))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True; conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_array_upgrade_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,level INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload,level FROM dongfu_array_upgrade_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback(); return DongfuArrayUpgradeResult("duplicate", int(old[1])) if str(old[0]) == payload else DongfuArrayUpgradeResult("state_changed")
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None: conn.rollback(); return DongfuArrayUpgradeResult("user_missing")
                if int(user[0] or 0) < stone_cost: conn.rollback(); return DongfuArrayUpgradeResult("stone_insufficient")
                row = conn.execute('SELECT built,array_level FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)).fetchone()
                if row is None or int(row[0] or 0) != 1: conn.rollback(); return DongfuArrayUpgradeResult("dongfu_missing")
                if int(row[1] or 0) != expected_level: conn.rollback(); return DongfuArrayUpgradeResult("state_changed")
                if item_cost:
                    item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if item is None or int(item[0] or 0) < item_cost: conn.rollback(); return DongfuArrayUpgradeResult("item_insufficient")
                stone = conn.execute("UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s", (stone_cost, user_id, stone_cost))
                item = conn.execute("UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num>=%s", (item_cost, user_id, item_id, item_cost)) if item_cost else None
                dongfu = conn.execute('UPDATE player_data."dongfu_status" SET array_level=%s WHERE user_id=%s AND array_level=%s', (next_level, user_id, expected_level))
                if stone.rowcount != 1 or dongfu.rowcount != 1 or (item is not None and item.rowcount != 1): conn.rollback(); return DongfuArrayUpgradeResult("state_changed")
                conn.execute("INSERT INTO dongfu_array_upgrade_operations (operation_id,payload,level) VALUES (%s,%s,%s)", (operation_id, payload, next_level)); conn.commit(); return DongfuArrayUpgradeResult("upgraded", next_level)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuArrayUpgradeResult", "DongfuArrayUpgradeService"]
