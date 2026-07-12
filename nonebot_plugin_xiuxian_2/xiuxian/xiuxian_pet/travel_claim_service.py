from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetTravelClaimResult:
    status: str
    stone: int
    exp: int
    items: tuple[tuple[int, int], ...]


class PetTravelClaimService:
    """Consume a completed travel and grant all rewards atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def claim(self, operation_id, user_id, expected_travel, stone, exp, items, max_goods_num) -> PetTravelClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone = int(stone)
        exp = int(exp)
        max_goods_num = int(max_goods_num)
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items
            if int(item.get("id", 0)) > 0 and int(item.get("amount", 0)) > 0
        )
        if not operation_id or not isinstance(expected_travel, dict) or min(stone, exp, max_goods_num) < 0:
            raise ValueError("valid operation, travel and rewards are required")
        travel_json = json.dumps(expected_travel, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        payload = json.dumps([user_id, expected_travel, stone, exp, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        def result(status: str) -> PetTravelClaimResult:
            granted = status in {"applied", "duplicate"}
            return PetTravelClaimResult(status, stone if granted else 0, exp if granted else 0, tuple((row[0], row[3]) for row in rewards) if granted else ())

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_travel_claim_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload FROM pet_travel_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate" if str(previous[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("player_pet",)
                ).fetchone()
                columns = (
                    {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(player_pet)").fetchall()}
                    if table is not None else set()
                )
                if "travel" not in columns:
                    conn.rollback()
                    return result("state_changed")
                row = conn.execute("SELECT travel FROM player_data.player_pet WHERE user_id=%s", (user_id,)).fetchone()
                try:
                    current_travel = json.loads(str(row[0])) if row and row[0] else None
                except (TypeError, ValueError):
                    conn.rollback()
                    return result("state_changed")
                current_json = json.dumps(current_travel, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                if current_json != travel_json:
                    conn.rollback()
                    return result("state_changed")
                for item_id, _, _, amount in rewards:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + amount > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

                if conn.execute(
                    "UPDATE player_data.player_pet SET travel=NULL WHERE user_id=%s AND travel=%s", (user_id, row[0])
                ).rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute("INSERT INTO pet_travel_claim_operations VALUES (%s, %s, CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["PetTravelClaimResult", "PetTravelClaimService"]
