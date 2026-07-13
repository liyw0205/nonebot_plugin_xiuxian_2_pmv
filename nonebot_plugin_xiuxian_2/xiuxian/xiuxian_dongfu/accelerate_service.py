from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuAccelerateResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"accelerated", "duplicate"}


class DongfuAccelerateService:
    """Consume an accelerate item and shorten one plot's finish time atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _canonical(slots) -> str:
        return json.dumps(slots, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def accelerate(self, operation_id, user_id, expected_slots, slot_no, item_id, now, new_finish):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        try:
            expected_slots = self._canonical(json.loads(expected_slots))
        except (TypeError, ValueError):
            raise ValueError("expected slots must be valid JSON")
        slot_no, item_id = int(slot_no), int(item_id)
        now, new_finish = str(now), str(new_finish)
        if not operation_id or slot_no < 1 or item_id <= 0 or not now or not new_finish:
            raise ValueError("valid operation, item, plot and times are required")
        payload = "|".join((user_id, expected_slots, str(slot_no), str(item_id), now, new_finish))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dongfu_accelerate_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute("SELECT payload FROM dongfu_accelerate_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return DongfuAccelerateResult("duplicate" if str(old[0]) == payload else "state_changed")
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(dongfu_status)").fetchall()}
                required = {"built", "plant_slots", "planting", "plant_seed_id", "plant_start", "plant_finish"}
                if not required.issubset(columns):
                    conn.rollback()
                    return DongfuAccelerateResult("state_changed")
                row = conn.execute('SELECT built,plant_slots FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)).fetchone()
                if row is None or int(row[0] or 0) != 1:
                    conn.rollback()
                    return DongfuAccelerateResult("dongfu_missing")
                try:
                    slots = json.loads(str(row[1] or ""))
                except (TypeError, ValueError):
                    conn.rollback()
                    return DongfuAccelerateResult("state_changed")
                if self._canonical(slots) != expected_slots:
                    conn.rollback()
                    return DongfuAccelerateResult("state_changed")
                if slot_no > len(slots) or int(slots[slot_no - 1].get("seed_id") or 0) <= 0:
                    conn.rollback()
                    return DongfuAccelerateResult("plot_empty")
                old_finish = str(slots[slot_no - 1].get("plant_finish") or "")
                if not old_finish or old_finish <= now:
                    conn.rollback()
                    return DongfuAccelerateResult("already_mature")
                item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                if item is None or int(item[0] or 0) < 1:
                    conn.rollback()
                    return DongfuAccelerateResult("item_insufficient")
                slots[slot_no - 1]["plant_finish"] = new_finish
                active = next((slot for slot in slots if int(slot.get("seed_id") or 0) > 0), None)
                legacy = (1, int(active["seed_id"]), active.get("plant_start", ""), active.get("plant_finish", "")) if active else (0, 0, "", "")
                deducted = conn.execute("UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s AND goods_num>=1", (user_id, item_id))
                updated = conn.execute(
                    'UPDATE player_data."dongfu_status" SET plant_slots=%s,planting=%s,plant_seed_id=%s,plant_start=%s,plant_finish=%s WHERE user_id=%s',
                    (self._canonical(slots), *legacy, user_id),
                )
                if deducted.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return DongfuAccelerateResult("state_changed")
                conn.execute("INSERT INTO dongfu_accelerate_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload))
                conn.commit()
                return DongfuAccelerateResult("accelerated")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuAccelerateResult", "DongfuAccelerateService"]
