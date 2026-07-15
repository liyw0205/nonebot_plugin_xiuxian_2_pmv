from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuHarvestResult:
    status: str
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"harvested", "duplicate"}


class DongfuHarvestSettlementService:
    """Clear matured plots and add their fixed harvest in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, slot_numbers) -> str:
        return DongfuHarvestSettlementService._canonical([str(user_id), tuple(sorted({int(v) for v in slot_numbers}))])

    def get_result(self, operation_id: str) -> DongfuHarvestResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dongfu_harvest_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,rewards TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,rewards FROM dongfu_harvest_operations WHERE operation_id=%s", (operation_id,)
            ).fetchone()
            if old is None:
                return None
            return DongfuHarvestResult(
                "duplicate", tuple(tuple(map(int, value)) for value in json.loads(str(old[1])))
            )

    @staticmethod
    def _canonical(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def harvest(self, operation_id, user_id, expected_slots, slot_numbers, rewards, max_goods_num, settled_at):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_slots = list(expected_slots)
        slot_numbers = tuple(sorted({int(value) for value in slot_numbers}))
        reward_rows = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in rewards if int(item["amount"]) > 0
        )
        max_goods_num = int(max_goods_num)
        settled_at = str(settled_at)
        if not operation_id or not slot_numbers or max_goods_num < 0:
            raise ValueError("valid operation, plots and capacity are required")
        # Request identity only — rewards stored in rewards column; slots are concurrency checks.
        payload = self._payload(user_id, slot_numbers)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dongfu_harvest_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,rewards TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute("SELECT payload,rewards FROM dongfu_harvest_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return DongfuHarvestResult("state_changed", ())
                    return DongfuHarvestResult("duplicate", tuple(tuple(map(int, value)) for value in json.loads(str(old[1]))))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return DongfuHarvestResult("user_missing", ())

                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(dongfu_status)").fetchall()}
                required = {"built", "plant_slots", "planting", "plant_seed_id", "plant_start", "plant_finish", "harvest_settlement"}
                if not required.issubset(columns):
                    conn.rollback()
                    return DongfuHarvestResult("state_changed", ())
                row = conn.execute(
                    'SELECT built,plant_slots FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)
                ).fetchone()
                if row is None or int(row[0] or 0) != 1:
                    conn.rollback()
                    return DongfuHarvestResult("dongfu_missing", ())
                try:
                    actual_slots = json.loads(str(row[1]))
                except (TypeError, ValueError):
                    conn.rollback()
                    return DongfuHarvestResult("state_changed", ())
                if self._canonical(actual_slots) != self._canonical(expected_slots):
                    conn.rollback()
                    return DongfuHarvestResult("state_changed", ())
                for slot_no in slot_numbers:
                    if slot_no < 1 or slot_no > len(actual_slots):
                        conn.rollback()
                        return DongfuHarvestResult("state_changed", ())
                    finish = str(actual_slots[slot_no - 1].get("plant_finish") or "")
                    if not finish or finish > settled_at:
                        conn.rollback()
                        return DongfuHarvestResult("not_mature", ())

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for item_id, name, item_type, amount in reward_rows:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(item[0]) if item else 0) + amount > max_goods_num:
                        conn.rollback()
                        return DongfuHarvestResult("inventory_full", ())

                for slot_no in slot_numbers:
                    actual_slots[slot_no - 1] = {"slot": slot_no, "seed_id": 0, "seed_name": "", "plant_start": "", "plant_finish": "", "fertilizer": 0}
                active = next((slot for slot in actual_slots if int(slot.get("seed_id") or 0) > 0), None)
                legacy = (1, int(active["seed_id"]), active["plant_start"], active["plant_finish"]) if active else (0, 0, "", "")
                conn.execute(
                    'UPDATE player_data."dongfu_status" SET plant_slots=%s,planting=%s,plant_seed_id=%s,plant_start=%s,plant_finish=%s,harvest_settlement=%s WHERE user_id=%s',
                    (self._canonical(actual_slots), *legacy, "", user_id),
                )
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                compact = tuple(sorted(totals.items()))
                conn.execute("INSERT INTO dongfu_harvest_operations (operation_id,payload,rewards) VALUES (%s,%s,%s)", (operation_id, payload, json.dumps(compact)))
                conn.commit()
                return DongfuHarvestResult("harvested", compact)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuHarvestResult", "DongfuHarvestSettlementService"]
