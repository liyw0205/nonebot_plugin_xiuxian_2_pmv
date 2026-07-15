from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class LoveSandUseResult:
    status: str
    gained: int = 0
    stone_num: int = 0
    item_remaining: int = 0

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}


class LoveSandUseService:
    def __init__(self, game_db, impart_db, player_db, lock=None):
        self.game_db, self.impart_db, self.player_db = map(Path, (game_db, impart_db, player_db))
        self.lock = lock or RLock()

    def get_result(self, operation_id: str) -> LoveSandUseResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS love_sand_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,"
                "stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)"
            )
            old = conn.execute(
                "SELECT gained,stone_num,item_remaining FROM love_sand_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return LoveSandUseResult("duplicate", int(old[0]), int(old[1]), int(old[2]))

    def apply(self, operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, gained, expected_item_count, expected_stone_num = map(
            int, (item_id, quantity, gained, expected_item_count, expected_stone_num)
        )
        if not operation_id or quantity <= 0 or gained < 0:
            raise ValueError("invalid love sand request")
        # Request identity only; expected_* and gained outcome not part of key.
        payload = json.dumps(
            [user_id, item_id, quantity],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self.impart_db),))
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS love_sand_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,"
                    "stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)"
                )
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
                for column in ("思恋流沙使用", "思恋结晶获取"):
                    try:
                        conn.execute(
                            f'ALTER TABLE player_data.statistics ADD COLUMN "{column}" INTEGER DEFAULT 0'
                        )
                    except db_backend.Error:
                        pass
                old = conn.execute(
                    "SELECT payload,gained,stone_num,item_remaining FROM love_sand_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return LoveSandUseResult(
                        "duplicate" if str(old[0]) == payload else "operation_conflict",
                        int(old[1]),
                        int(old[2]),
                        int(old[3]),
                    )
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                impart = conn.execute(
                    "SELECT stone_num FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if (
                    not item
                    or not impart
                    or int(item[0]) != expected_item_count
                    or int(impart[0]) != expected_stone_num
                ):
                    conn.rollback()
                    return LoveSandUseResult("state_changed")
                if expected_item_count < quantity:
                    conn.rollback()
                    return LoveSandUseResult("item_missing")
                remaining, stone_num = expected_item_count - quantity, expected_stone_num + gained
                changed = conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)=%s",
                    (
                        remaining,
                        min(max(0, int(item[1]) - quantity), remaining),
                        user_id,
                        item_id,
                        expected_item_count,
                    ),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return LoveSandUseResult("state_changed")
                conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET stone_num=%s WHERE user_id=%s",
                    (stone_num, user_id),
                )
                conn.execute(
                    'INSERT INTO player_data.statistics(user_id,"思恋流沙使用","思恋结晶获取") '
                    "VALUES (%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                    '"思恋流沙使用"=COALESCE(statistics."思恋流沙使用",0)+EXCLUDED."思恋流沙使用",'
                    '"思恋结晶获取"=COALESCE(statistics."思恋结晶获取",0)+EXCLUDED."思恋结晶获取"',
                    (user_id, quantity, gained),
                )
                conn.execute(
                    "INSERT INTO love_sand_operations VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, gained, stone_num, remaining),
                )
                conn.commit()
                return LoveSandUseResult("applied", gained, stone_num, remaining)
            except Exception:
                conn.rollback()
                raise


__all__ = ["LoveSandUseResult", "LoveSandUseService"]
