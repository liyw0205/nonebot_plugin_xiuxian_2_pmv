from __future__ import annotations

from contextlib import closing
import json
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuPlantResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"planted", "duplicate"}


class DongfuPlantService:
    """Consume one seed and occupy an empty dongfu plot atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def plant(self, operation_id, user_id, expected_slots, slot_no, seed_id, seed_name, plant_start, plant_finish):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        try:
            expected_slot_data = json.loads(expected_slots)
        except (TypeError, ValueError):
            raise ValueError("expected slots must be valid JSON")
        expected_slots = json.dumps(expected_slot_data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        expected_slots, slot_no, seed_id = str(expected_slots), int(slot_no), int(seed_id)
        seed_name, plant_start, plant_finish = str(seed_name), str(plant_start), str(plant_finish)
        if not operation_id or slot_no < 1 or seed_id <= 0 or not plant_finish:
            raise ValueError("valid operation, seed and plot are required")
        payload = "|".join((user_id, expected_slots, str(slot_no), str(seed_id), seed_name, plant_start, plant_finish))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dongfu_plant_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute("SELECT payload FROM dongfu_plant_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return DongfuPlantResult("duplicate" if str(old[0]) == payload else "state_changed")
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(dongfu_status)").fetchall()}
                if not {"built", "plant_slots", "planting", "plant_seed_id", "plant_start", "plant_finish"}.issubset(columns):
                    conn.rollback()
                    return DongfuPlantResult("state_changed")
                dongfu = conn.execute(
                    'SELECT built,plant_slots FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)
                ).fetchone()
                if dongfu is None or int(dongfu[0] or 0) != 1:
                    conn.rollback()
                    return DongfuPlantResult("dongfu_missing")
                try:
                    actual_slots = json.loads(str(dongfu[1] or ""))
                except (TypeError, ValueError):
                    conn.rollback()
                    return DongfuPlantResult("state_changed")
                if json.dumps(actual_slots, ensure_ascii=False, sort_keys=True, separators=(",", ":")) != expected_slots:
                    conn.rollback()
                    return DongfuPlantResult("state_changed")
                slots = actual_slots
                if slot_no > len(slots) or int(slots[slot_no - 1].get("seed_id") or 0) != 0:
                    conn.rollback()
                    return DongfuPlantResult("plot_occupied")
                seed = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, seed_id)).fetchone()
                if seed is None or int(seed[0] or 0) < 1:
                    conn.rollback()
                    return DongfuPlantResult("seed_insufficient")
                slots[slot_no - 1] = {"slot": slot_no, "seed_id": seed_id, "seed_name": seed_name, "plant_start": plant_start, "plant_finish": plant_finish, "fertilizer": 0}
                active = next(slot for slot in slots if int(slot.get("seed_id") or 0) > 0)
                deducted = conn.execute("UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s AND goods_num>=1", (user_id, seed_id))
                updated = conn.execute(
                    'UPDATE player_data."dongfu_status" SET plant_slots=%s,planting=%s,plant_seed_id=%s,plant_start=%s,plant_finish=%s WHERE user_id=%s',
                    (json.dumps(slots, ensure_ascii=False), 1, int(active["seed_id"]), active["plant_start"], active["plant_finish"], user_id),
                )
                if deducted.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return DongfuPlantResult("state_changed")
                conn.execute("INSERT INTO dongfu_plant_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload))
                conn.commit()
                return DongfuPlantResult("planted")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuPlantResult", "DongfuPlantService"]
