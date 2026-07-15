from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from datetime import datetime

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

    @staticmethod
    def _payload(user_id, slot_no, seed_id) -> str:
        return "|".join((str(user_id), str(int(slot_no)), str(int(seed_id))))

    def get_result(self, operation_id: str) -> DongfuPlantResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dongfu_plant_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload FROM dongfu_plant_operations WHERE operation_id=%s", (operation_id,)
            ).fetchone()
            if old is None:
                return None
            return DongfuPlantResult("duplicate")

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
        # Request identity only — slot occupancy/seed stock are concurrency checks; times are outcomes.
        payload = self._payload(user_id, slot_no, seed_id)

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
    def _payload(user_id, slot_no, item_id) -> str:
        return "|".join((str(user_id), str(int(slot_no)), str(int(item_id))))

    def get_result(self, operation_id: str) -> DongfuAccelerateResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dongfu_accelerate_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute("SELECT payload FROM dongfu_accelerate_operations WHERE operation_id=%s", (operation_id,)).fetchone()
            if old is None:
                return None
            return DongfuAccelerateResult("duplicate")

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
        payload = self._payload(user_id, slot_no, item_id)

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

@dataclass(frozen=True)
class DongfuExpansion:
    status: str
    user_id: str
    previous_count: int = 0
    current_count: int = 0
    deed_cost: int = 0
    stone_cost: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"expanded", "duplicate"}

class DongfuExpansionService:
    """Expand dongfu plots while charging game and player assets atomically."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _as_int(value: object) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dongfu_expansion_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                previous_count INTEGER NOT NULL,
                current_count INTEGER NOT NULL,
                deed_cost INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def expand(
        self,
        operation_id,
        user_id,
        *,
        deed_id: int,
        base_plot_count: int,
        max_plot_count: int,
        stone_cost_per_level: int,
    ) -> DongfuExpansion:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    """
                    SELECT previous_count, current_count, deed_cost, stone_cost
                    FROM dongfu_expansion_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return DongfuExpansion(
                        "duplicate",
                        user_id,
                        self._as_int(previous[0]),
                        self._as_int(previous[1]),
                        self._as_int(previous[2]),
                        self._as_int(previous[3]),
                    )

                user = conn.execute(
                    "SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return DongfuExpansion("user_missing", user_id)

                dongfu = conn.execute(
                    'SELECT built, plot_count FROM player_data."dongfu_status" WHERE user_id=%s',
                    (user_id,),
                ).fetchone()
                if dongfu is None or self._as_int(dongfu[0]) != 1:
                    conn.rollback()
                    return DongfuExpansion("dongfu_missing", user_id)

                previous_count = max(base_plot_count, self._as_int(dongfu[1]))
                if previous_count >= max_plot_count:
                    conn.rollback()
                    return DongfuExpansion(
                        "max_plots", user_id, previous_count, previous_count
                    )

                current_count = previous_count + 1
                deed_cost = current_count - base_plot_count
                stone_cost = stone_cost_per_level * deed_cost
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, int(deed_id)),
                ).fetchone()
                if item is None or self._as_int(item[0]) < deed_cost:
                    conn.rollback()
                    return DongfuExpansion(
                        "deed_insufficient",
                        user_id,
                        previous_count,
                        previous_count,
                        deed_cost,
                        stone_cost,
                    )
                if self._as_int(user[0]) < stone_cost:
                    conn.rollback()
                    return DongfuExpansion(
                        "stone_insufficient",
                        user_id,
                        previous_count,
                        previous_count,
                        deed_cost,
                        stone_cost,
                    )

                deducted_item = conn.execute(
                    """
                    UPDATE back SET goods_num=goods_num-%s
                    WHERE user_id=%s AND goods_id=%s AND goods_num >= %s
                    """,
                    (deed_cost, user_id, int(deed_id), deed_cost),
                )
                deducted_stone = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) WHERE user_id=%s AND stone >= %s",
                    (stone_cost, user_id, stone_cost),
                )
                updated_dongfu = conn.execute(
                    """
                    UPDATE player_data."dongfu_status" SET plot_count=%s
                    WHERE user_id=%s AND CAST(plot_count AS INTEGER)=%s
                    """,
                    (str(current_count), user_id, previous_count),
                )
                if deducted_item.rowcount != 1:
                    conn.rollback()
                    return DongfuExpansion("deed_changed", user_id, previous_count, previous_count)
                if deducted_stone.rowcount != 1:
                    conn.rollback()
                    return DongfuExpansion("stone_changed", user_id, previous_count, previous_count)
                if updated_dongfu.rowcount != 1:
                    conn.rollback()
                    return DongfuExpansion("dongfu_changed", user_id, previous_count, previous_count)

                conn.execute(
                    """
                    INSERT INTO dongfu_expansion_operations (
                        operation_id, user_id, previous_count, current_count, deed_cost, stone_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (operation_id, user_id, previous_count, current_count, deed_cost, stone_cost),
                )
                conn.commit()
                return DongfuExpansion(
                    "expanded", user_id, previous_count, current_count, deed_cost, stone_cost
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

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
                stone = conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) WHERE user_id=%s AND stone>=%s", (stone_cost, user_id, stone_cost))
                item = conn.execute("UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num>=%s", (item_cost, user_id, item_id, item_cost)) if item_cost else None
                dongfu = conn.execute('UPDATE player_data."dongfu_status" SET array_level=%s WHERE user_id=%s AND array_level=%s', (next_level, user_id, expected_level))
                if stone.rowcount != 1 or dongfu.rowcount != 1 or (item is not None and item.rowcount != 1): conn.rollback(); return DongfuArrayUpgradeResult("state_changed")
                conn.execute("INSERT INTO dongfu_array_upgrade_operations (operation_id,payload,level) VALUES (%s,%s,%s)", (operation_id, payload, next_level)); conn.commit(); return DongfuArrayUpgradeResult("upgraded", next_level)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class DongfuPatrolResult:
    status: str
    patrol_count: int = 0
    patrol_guard: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"patrolled", "duplicate"}

class DongfuPatrolService:
    """Settle stamina, patrol state and fixed patrol rewards atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, day) -> str:
        return json.dumps((str(user_id), str(day)), ensure_ascii=False, separators=(",", ":"))

    def get_result(self, operation_id: str) -> DongfuPatrolResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS dongfu_patrol_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,patrol_count INTEGER NOT NULL,patrol_guard INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            old = conn.execute("SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=%s", (operation_id,)).fetchone()
            if old is None:
                return None
            return DongfuPatrolResult("duplicate", int(old[1]), int(old[2]))

    def patrol(self, operation_id, user_id, day, stamina_cost, daily_limit, stone_gain, reward=None, max_goods_num=999999999):
        operation_id, user_id, day = str(operation_id).strip(), str(user_id), str(day)
        stamina_cost, daily_limit, stone_gain, max_goods_num = map(int, (stamina_cost, daily_limit, stone_gain, max_goods_num))
        reward = tuple(reward) if reward else None
        if not operation_id or not day or stamina_cost < 0 or daily_limit < 1 or stone_gain < 0:
            raise ValueError("valid patrol operation is required")
        if reward is not None and (len(reward) != 3 or int(reward[0]) <= 0 or int(reward[2]) <= 0):
            raise ValueError("reward must contain item id, name and amount")
        # Request identity only — stone/item gains are outcomes; limits/stamina are concurrency checks.
        payload = self._payload(user_id, day)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_patrol_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,patrol_count INTEGER NOT NULL,patrol_guard INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return DongfuPatrolResult("duplicate", int(old[1]), int(old[2])) if str(old[0]) == payload else DongfuPatrolResult("state_changed")
                user = conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return DongfuPatrolResult("user_missing")
                if int(user[0] or 0) < stamina_cost:
                    conn.rollback()
                    return DongfuPatrolResult("stamina_insufficient")
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(dongfu_status)").fetchall()}
                if not {"built", "patrol_date", "patrol_count", "patrol_guard"}.issubset(columns):
                    conn.rollback()
                    return DongfuPatrolResult("state_changed")
                row = conn.execute('SELECT built,patrol_date,patrol_count,patrol_guard FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)).fetchone()
                if row is None or int(row[0] or 0) != 1:
                    conn.rollback()
                    return DongfuPatrolResult("dongfu_missing")
                count, guard = (int(row[2] or 0), int(row[3] or 0)) if str(row[1] or "") == day else (0, 0)
                if count >= daily_limit:
                    conn.rollback()
                    return DongfuPatrolResult("daily_limit", count, guard)
                if reward:
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, int(reward[0]))).fetchone()
                    if (int(item[0]) if item else 0) + int(reward[2]) > max_goods_num:
                        conn.rollback()
                        return DongfuPatrolResult("inventory_full", count, guard)
                count, guard = count + 1, min(3, guard + 1)
                stamina = conn.execute("UPDATE user_xiuxian SET user_stamina=user_stamina-%s,stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s AND user_stamina>=%s", (stamina_cost, stone_gain, user_id, stamina_cost))
                dongfu = conn.execute('UPDATE player_data."dongfu_status" SET patrol_date=%s,patrol_count=%s,patrol_guard=%s WHERE user_id=%s', (day, count, guard, user_id))
                if stamina.rowcount != 1 or dongfu.rowcount != 1:
                    conn.rollback()
                    return DongfuPatrolResult("state_changed")
                if reward:
                    item_id, item_name, amount = int(reward[0]), str(reward[1]), int(reward[2])
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num", (user_id, item_id, item_name, "特殊物品", amount, amount))
                conn.execute("INSERT INTO dongfu_patrol_operations (operation_id,payload,patrol_count,patrol_guard) VALUES (%s,%s,%s,%s)", (operation_id, payload, count, guard))
                conn.commit()
                return DongfuPatrolResult("patrolled", count, guard)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

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

@dataclass(frozen=True)
class DongfuVisitRewardResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"rewarded", "duplicate"}

class DongfuVisitRewardService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database, self._lock = Path(game_database), Path(player_database), lock or RLock()

    def reward(self, operation_id, visitor_id, target_id, gain):
        operation_id, visitor_id, target_id, gain = str(operation_id).strip(), str(visitor_id), str(target_id), int(gain)
        if not operation_id or not visitor_id or not target_id or visitor_id == target_id or gain < 0:
            raise ValueError("valid visit reward is required")
        payload = "|".join((visitor_id, target_id, str(gain)))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True; conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_visit_reward_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload FROM dongfu_visit_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback(); return DongfuVisitRewardResult("duplicate" if str(old[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (visitor_id,)).fetchone() is None:
                    conn.rollback(); return DongfuVisitRewardResult("user_missing")
                visitor = conn.execute('SELECT built FROM player_data."dongfu_status" WHERE user_id=%s', (visitor_id,)).fetchone()
                target = conn.execute('SELECT built FROM player_data."dongfu_status" WHERE user_id=%s', (target_id,)).fetchone()
                if visitor is None or int(visitor[0] or 0) != 1 or target is None or int(target[0] or 0) != 1:
                    conn.rollback(); return DongfuVisitRewardResult("dongfu_changed")
                updated = conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (gain, visitor_id))
                if updated.rowcount != 1:
                    conn.rollback(); return DongfuVisitRewardResult("state_changed")
                conn.execute("INSERT INTO dongfu_visit_reward_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload)); conn.commit(); return DongfuVisitRewardResult("rewarded")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class InfiltrateSuccessResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}

class InfiltrateSuccessService:
    """Settle every state change produced by a successful infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database = Path(game_database), Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _canonical(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def settle(self, operation_id, visitor_id, target_id, day, mode_field, mode_limit, target_limit,
               expected_slots, slot_no, new_finish, rewards, stone, consume_guard, max_goods_num):
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(str, (visitor_id, target_id, day, mode_field))
        mode_limit, target_limit, slot_no, stone, max_goods_num = map(int, (mode_limit, target_limit, slot_no, stone, max_goods_num))
        consume_guard = int(bool(consume_guard))
        try:
            expected_slots = self._canonical(json.loads(expected_slots))
        except (TypeError, ValueError):
            raise ValueError("expected slots must be valid JSON")
        reward_rows = tuple((int(row[0]), str(row[1]), str(row[2]), int(row[3])) for row in rewards)
        new_finish = str(new_finish or "")
        if not operation_id or visitor_id == target_id or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}:
            raise ValueError("valid infiltration success operation is required")
        payload = self._canonical((visitor_id, target_id, day, mode_field, mode_limit, target_limit, expected_slots, slot_no, new_finish, reward_rows, stone, consume_guard, max_goods_num))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_infiltrate_success_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,infiltrate_left INTEGER NOT NULL,intrude_left INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload,infiltrate_left,intrude_left FROM dongfu_infiltrate_success_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return InfiltrateSuccessResult("duplicate", int(old[1]), int(old[2])) if str(old[0]) == payload else InfiltrateSuccessResult("state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (visitor_id,)).fetchone() is None:
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                visitor = conn.execute(f'SELECT built,infiltrate_date,{mode_field} FROM player_data."dongfu_status" WHERE user_id=%s', (visitor_id,)).fetchone()
                target = conn.execute('SELECT built,intrude_date,intrude_count,patrol_guard,plant_slots FROM player_data."dongfu_status" WHERE user_id=%s', (target_id,)).fetchone()
                if visitor is None or target is None or int(visitor[0] or 0) != 1 or int(target[0] or 0) != 1:
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                try:
                    slots = json.loads(str(target[4] or ""))
                except (TypeError, ValueError):
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                if self._canonical(slots) != expected_slots or slot_no < 1 or slot_no > len(slots):
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                mode_count = int(visitor[2] or 0) if str(visitor[1] or "") == day else 0
                intrude_count = int(target[2] or 0) if str(target[1] or "") == day else 0
                if mode_count >= mode_limit or intrude_count >= target_limit:
                    conn.rollback(); return InfiltrateSuccessResult("daily_limit")
                totals, metadata = {}, {}
                for item_id, name, item_type, amount in reward_rows:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = name, item_type
                for item_id, amount in totals.items():
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (visitor_id, item_id)).fetchone()
                    if (int(item[0]) if item else 0) + amount > max_goods_num:
                        conn.rollback(); return InfiltrateSuccessResult("inventory_full")
                if new_finish:
                    slots[slot_no - 1]["plant_finish"] = new_finish
                mode_count, intrude_count = mode_count + 1, intrude_count + 1
                conn.execute(f'UPDATE player_data."dongfu_status" SET infiltrate_date=%s,{mode_field}=%s WHERE user_id=%s', (day, mode_count, visitor_id))
                conn.execute('UPDATE player_data."dongfu_status" SET intrude_date=%s,intrude_count=%s,patrol_guard=MAX(patrol_guard-%s,0),plant_slots=%s WHERE user_id=%s', (day, intrude_count, consume_guard, self._canonical(slots), target_id))
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, visitor_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time", (visitor_id, item_id, name, item_type, amount, now, now, amount))
                left = max(0, mode_limit - mode_count), max(0, target_limit - intrude_count)
                conn.execute("INSERT INTO dongfu_infiltrate_success_operations (operation_id,payload,infiltrate_left,intrude_left) VALUES (%s,%s,%s,%s)", (operation_id, payload, *left))
                conn.commit()
                return InfiltrateSuccessResult("settled", *left)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class InfiltrateFailureResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}

class InfiltrateFailureService:
    """Atomically settle a detected and failed dongfu infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(
        self, operation_id, visitor_id, target_id, day, mode_field,
        mode_limit, target_limit, loss, consume_guard,
    ) -> InfiltrateFailureResult:
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(str, (visitor_id, target_id, day, mode_field))
        mode_limit, target_limit, loss = map(int, (mode_limit, target_limit, loss))
        consume_guard = int(bool(consume_guard))
        if (
            not operation_id
            or visitor_id == target_id
            or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}
            or mode_limit < 1
            or target_limit < 1
            or loss < 0
        ):
            raise ValueError("valid infiltration failure operation is required")
        payload = "|".join(map(str, (visitor_id, target_id, day, mode_field, mode_limit, target_limit, loss, consume_guard)))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dongfu_infiltrate_failure_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "infiltrate_left INTEGER NOT NULL,intrude_left INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,infiltrate_left,intrude_left "
                    "FROM dongfu_infiltrate_failure_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return InfiltrateFailureResult("state_changed")
                    return InfiltrateFailureResult("duplicate", int(old[1]), int(old[2]))

                user = conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (visitor_id,)).fetchone()
                visitor = conn.execute(
                    f'SELECT built,infiltrate_date,{mode_field} '
                    'FROM player_data."dongfu_status" WHERE user_id=%s',
                    (visitor_id,),
                ).fetchone()
                target = conn.execute(
                    'SELECT built,intrude_date,intrude_count,patrol_guard '
                    'FROM player_data."dongfu_status" WHERE user_id=%s',
                    (target_id,),
                ).fetchone()
                if (
                    user is None or visitor is None or target is None
                    or int(visitor[0] or 0) != 1 or int(target[0] or 0) != 1
                ):
                    conn.rollback()
                    return InfiltrateFailureResult("state_changed")

                mode_count = int(visitor[2] or 0) if str(visitor[1] or "") == day else 0
                intrude_count = int(target[2] or 0) if str(target[1] or "") == day else 0
                if mode_count >= mode_limit or intrude_count >= target_limit:
                    conn.rollback()
                    return InfiltrateFailureResult(
                        "daily_limit", max(0, mode_limit - mode_count), max(0, target_limit - intrude_count)
                    )

                mode_count += 1
                intrude_count += 1
                visitor_update = conn.execute(
                    f'UPDATE player_data."dongfu_status" SET infiltrate_date=%s,{mode_field}=%s WHERE user_id=%s',
                    (day, mode_count, visitor_id),
                )
                target_update = conn.execute(
                    'UPDATE player_data."dongfu_status" '
                    'SET intrude_date=%s,intrude_count=%s,patrol_guard=MAX(patrol_guard-%s,0) '
                    'WHERE user_id=%s',
                    (day, intrude_count, consume_guard, target_id),
                )
                stone_update = conn.execute(
                    "UPDATE user_xiuxian SET stone=MAX(stone-%s,0) WHERE user_id=%s",
                    (loss, visitor_id),
                )
                if visitor_update.rowcount != 1 or target_update.rowcount != 1 or stone_update.rowcount != 1:
                    conn.rollback()
                    return InfiltrateFailureResult("state_changed")

                left = max(0, mode_limit - mode_count), max(0, target_limit - intrude_count)
                conn.execute(
                    "INSERT INTO dongfu_infiltrate_failure_operations "
                    "(operation_id,payload,infiltrate_left,intrude_left) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, *left),
                )
                conn.commit()
                return InfiltrateFailureResult("settled", *left)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "DongfuPlantResult",
    "DongfuPlantService",
    "DongfuAccelerateResult",
    "DongfuAccelerateService",
    "DongfuFertilizeResult",
    "DongfuFertilizeService",
    "DongfuExpansion",
    "DongfuExpansionService",
    "DongfuArrayUpgradeResult",
    "DongfuArrayUpgradeService",
    "DongfuPatrolResult",
    "DongfuPatrolService",
    "DongfuHarvestResult",
    "DongfuHarvestSettlementService",
    "DongfuVisitRewardResult",
    "DongfuVisitRewardService",
    "InfiltrateSuccessResult",
    "InfiltrateSuccessService",
    "InfiltrateFailureResult",
    "InfiltrateFailureService",
]
