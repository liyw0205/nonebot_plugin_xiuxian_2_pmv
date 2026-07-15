from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuFertilizeResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"fertilized", "duplicate"}


class DongfuFertilizeService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database, self._lock = Path(game_database), Path(player_database), lock or RLock()

    @staticmethod
    def _payload(user_id, slot_no, item_id) -> str:
        return "|".join(map(str, (user_id, int(slot_no), int(item_id))))

    def get_result(self, operation_id: str) -> DongfuFertilizeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS dongfu_fertilize_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            old = conn.execute("SELECT payload FROM dongfu_fertilize_operations WHERE operation_id=%s", (operation_id,)).fetchone()
            if old is None:
                return None
            return DongfuFertilizeResult("duplicate")

    @staticmethod
    def _canonical(slots) -> str:
        return json.dumps(slots, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def fertilize(self, operation_id, user_id, expected_slots, slot_no, item_id, fertilizer_max):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        try: expected_slots = self._canonical(json.loads(expected_slots))
        except (TypeError, ValueError): raise ValueError("expected slots must be valid JSON")
        slot_no, item_id, fertilizer_max = map(int, (slot_no, item_id, fertilizer_max))
        if not operation_id or slot_no < 1 or item_id <= 0 or fertilizer_max < 1: raise ValueError("valid fertilize operation is required")
        payload = self._payload(user_id, slot_no, item_id)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True; conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_fertilize_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload FROM dongfu_fertilize_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None: conn.rollback(); return DongfuFertilizeResult("duplicate" if str(old[0]) == payload else "state_changed")
                row = conn.execute('SELECT built,plant_slots FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)).fetchone()
                if row is None or int(row[0] or 0) != 1: conn.rollback(); return DongfuFertilizeResult("dongfu_missing")
                try: slots = json.loads(str(row[1] or ""))
                except (TypeError, ValueError): conn.rollback(); return DongfuFertilizeResult("state_changed")
                if self._canonical(slots) != expected_slots: conn.rollback(); return DongfuFertilizeResult("state_changed")
                if slot_no > len(slots) or int(slots[slot_no - 1].get("seed_id") or 0) <= 0: conn.rollback(); return DongfuFertilizeResult("plot_empty")
                if int(slots[slot_no - 1].get("fertilizer") or 0) >= fertilizer_max: conn.rollback(); return DongfuFertilizeResult("fertilizer_full")
                item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                if item is None or int(item[0] or 0) < 1: conn.rollback(); return DongfuFertilizeResult("item_insufficient")
                slots[slot_no - 1]["fertilizer"] = int(slots[slot_no - 1].get("fertilizer") or 0) + 1
                deducted = conn.execute("UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s AND goods_num>=1", (user_id, item_id))
                updated = conn.execute('UPDATE player_data."dongfu_status" SET plant_slots=%s WHERE user_id=%s', (self._canonical(slots), user_id))
                if deducted.rowcount != 1 or updated.rowcount != 1: conn.rollback(); return DongfuFertilizeResult("state_changed")
                conn.execute("INSERT INTO dongfu_fertilize_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload)); conn.commit(); return DongfuFertilizeResult("fertilized")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuFertilizeResult", "DongfuFertilizeService"]
