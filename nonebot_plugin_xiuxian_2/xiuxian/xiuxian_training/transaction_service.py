from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from datetime import date, datetime
from ..xiuxian_utils import db_backend
from datetime import datetime

TRAINING_FIELDS = (
    "progress",
    "last_time",
    "points",
    "completed",
    "max_progress",
    "last_event",
    "weekly_purchases",
)

def _as_date(value=None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))

def _normalize_weekly_purchases(value, today=None) -> tuple[dict[str, int | str], bool]:
    today = _as_date(today)
    changed = False
    if isinstance(value, str):
        try:
            value = json.loads(value) if value else {}
        except (TypeError, ValueError):
            value = {}
            changed = True
    if not isinstance(value, dict):
        value = {}
        changed = True

    try:
        reset = date.fromisoformat(str(value.get("_last_reset", "")))
    except (TypeError, ValueError):
        reset = None
    if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
        return {"_last_reset": today.isoformat()}, True

    weekly: dict[str, int | str] = {"_last_reset": reset.isoformat()}
    for raw_key, raw_amount in value.items():
        key = str(raw_key)
        if key == "_last_reset":
            continue
        try:
            amount = int(raw_amount)
        except (TypeError, ValueError):
            changed = True
            continue
        if amount < 0:
            changed = True
            continue
        weekly[key] = amount
        if key != raw_key or not isinstance(raw_amount, int) or isinstance(raw_amount, bool):
            changed = True
    return weekly, changed

def normalize_weekly_purchases(value, today=None) -> dict[str, int | str]:
    """Return a canonical purchase snapshot for the ISO week containing today."""
    return _normalize_weekly_purchases(value, today)[0]

class TrainingStateService:
    """Initialize and advance a player's training state in one transaction."""

    _COLUMN_DEFINITIONS = {
        "progress": "TEXT DEFAULT '0'",
        "last_time": "TEXT DEFAULT NULL",
        "points": "TEXT DEFAULT '0'",
        "completed": "TEXT DEFAULT '0'",
        "max_progress": "TEXT DEFAULT '0'",
        "last_event": "TEXT DEFAULT ''",
        "weekly_purchases": "TEXT DEFAULT NULL",
    }

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _period_key(today: date) -> str:
        iso = today.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    @staticmethod
    def _integer(value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _last_time(value) -> datetime | None:
        if value is None or str(value).strip() in {"", "None", "null"}:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    @classmethod
    def _state(cls, row, weekly_purchases) -> dict:
        return {
            "progress": cls._integer(row[0]),
            "last_time": cls._last_time(row[1]),
            "points": cls._integer(row[2]),
            "completed": cls._integer(row[3]),
            "max_progress": cls._integer(row[4]),
            "last_event": str(row[5] or ""),
            "weekly_purchases": weekly_purchases,
        }

    @staticmethod
    def _snapshot(state: dict) -> str:
        snapshot = dict(state)
        if isinstance(snapshot["last_time"], datetime):
            snapshot["last_time"] = snapshot["last_time"].strftime("%Y-%m-%d %H:%M:%S")
        return json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS training ("
            "user_id TEXT PRIMARY KEY,progress TEXT DEFAULT '0',last_time TEXT DEFAULT NULL,"
            "points TEXT DEFAULT '0',completed TEXT DEFAULT '0',max_progress TEXT DEFAULT '0',"
            "last_event TEXT DEFAULT '',weekly_purchases TEXT DEFAULT NULL)"
        )
        columns = {str(column[1]) for column in conn.execute("PRAGMA table_info(training)").fetchall()}
        for field, definition in cls._COLUMN_DEFINITIONS.items():
            if field not in columns:
                conn.execute(f'ALTER TABLE training ADD COLUMN "{field}" {definition}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS training_state_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _insert_operation(conn, operation_id, user_id, kind, period_key, state) -> None:
        conn.execute(
            "INSERT INTO training_state_operations("
            "operation_id,user_id,kind,period_key,snapshot,created_at) "
            "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (operation_id, user_id, kind, period_key, TrainingStateService._snapshot(state)),
        )

    def get(self, user_id, today=None) -> dict:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        today = _as_date(today)
        period_key = self._period_key(today)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT progress,last_time,points,completed,max_progress,last_event,"
                    "weekly_purchases FROM training WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    weekly = {"_last_reset": today.isoformat()}
                    state = self._state((0, None, 0, 0, 0, "", weekly), weekly)
                    conn.execute(
                        "INSERT INTO training("
                        "user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            user_id,
                            state["progress"],
                            None,
                            state["points"],
                            state["completed"],
                            state["max_progress"],
                            state["last_event"],
                            json.dumps(weekly, ensure_ascii=True, sort_keys=True),
                        ),
                    )
                    self._insert_operation(
                        conn,
                        f"training-state-init:{user_id}",
                        user_id,
                        "initialize",
                        period_key,
                        state,
                    )
                    conn.commit()
                    return state

                weekly, weekly_changed = _normalize_weekly_purchases(row[6], today)
                state = self._state(row, weekly)
                if weekly_changed:
                    updated = conn.execute(
                        "UPDATE training SET weekly_purchases=%s WHERE user_id=%s",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError("training state changed")
                    self._insert_operation(
                        conn,
                        f"training-state-week:{user_id}:{period_key}",
                        user_id,
                        "week",
                        period_key,
                        state,
                    )
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class TrainingEventResult:
    status: str
    message: str = ""

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}

class TrainingEventService:
    _FIELDS = ("progress", "last_time", "points", "completed", "max_progress", "last_event", "weekly_purchases")

    def __init__(self, game_db, player_db, lock=None):
        self.game_db, self.player_db = Path(game_db), Path(player_db)
        self.lock = lock or RLock()

    @staticmethod
    def _value(key, value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "weekly_purchases" else str(value)

    @classmethod
    def _state_matches(cls, current, expected_state):
        for key, actual in zip(cls._FIELDS, current):
            expected = expected_state[key]
            if key == "weekly_purchases":
                try:
                    if json.loads(str(actual)) == expected:
                        continue
                except (TypeError, ValueError):
                    pass
            elif str(actual) == str(expected):
                continue
            return False
        return True

    @staticmethod
    def _stored_result(payload, user_id):
        try:
            stored = json.loads(str(payload))
            if str(stored[0]) != user_id or not isinstance(stored[2], dict):
                return TrainingEventResult("operation_conflict")
            return TrainingEventResult("duplicate", str(stored[2].get("last_event", "")))
        except (IndexError, TypeError, ValueError):
            return TrainingEventResult("operation_conflict")

    def apply(self, operation_id, user_id, expected_state, state, expected_user, stone_delta=0,
              exp_delta=0, hp_delta=0, items=(), max_goods_num=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_state, state, expected_user = dict(expected_state), dict(state), dict(expected_user)
        stone_delta, exp_delta, hp_delta, max_goods_num = map(int, (stone_delta, exp_delta, hp_delta, max_goods_num))
        rewards = tuple((int(x["id"]), str(x["name"]), str(x["type"]), int(x["amount"])) for x in items if int(x["amount"]) != 0)
        payload = json.dumps([user_id, expected_state, state, expected_user, stone_delta, exp_delta, hp_delta, rewards, max_goods_num], ensure_ascii=True, sort_keys=True, default=str)
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),)); conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS training_event_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
                old = conn.execute("SELECT payload FROM training_event_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback(); return self._stored_result(old[0], user_id)
                user = conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                wanted = tuple(int(expected_user[k]) for k in ("stone", "exp", "hp", "mp"))
                if user is None or tuple(map(int, user)) != wanted:
                    conn.rollback(); return TrainingEventResult("state_changed")
                current = conn.execute("SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases FROM player_data.training WHERE user_id=%s", (user_id,)).fetchone()
                if current is None or not self._state_matches(current, expected_state):
                    conn.rollback(); return TrainingEventResult("state_changed")
                for item_id, _, _, amount in rewards:
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    count = int(row[0]) if row else 0
                    if count + amount < 0:
                        conn.rollback(); return TrainingEventResult("item_missing")
                    if amount > 0 and count + amount > max_goods_num:
                        conn.rollback(); return TrainingEventResult("inventory_full")
                if wanted[0] + stone_delta < 0 or wanted[1] + exp_delta < 0 or wanted[2] + hp_delta < 0:
                    conn.rollback(); return TrainingEventResult("resource_missing")
                conn.execute("UPDATE player_data.training SET progress=%s,last_time=%s,points=%s,completed=%s,max_progress=%s,last_event=%s,weekly_purchases=%s WHERE user_id=%s", tuple(self._value(k, state[k]) for k in self._FIELDS) + (user_id,))
                conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL),exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL),hp=CAST(COALESCE(hp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone_delta, exp_delta, hp_delta, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    if amount > 0:
                        conn.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num,update_time=EXCLUDED.update_time", (user_id,item_id,name,item_type,amount,now,now,amount))
                    else:
                        conn.execute("UPDATE back SET goods_num=goods_num+%s,bind_num=MIN(COALESCE(bind_num,0),goods_num+%s) WHERE user_id=%s AND goods_id=%s", (amount, amount, user_id, item_id))
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
                try: conn.execute('ALTER TABLE player_data.statistics ADD COLUMN "历练次数" INTEGER DEFAULT 0')
                except db_backend.Error: pass
                conn.execute('INSERT INTO player_data.statistics(user_id,"历练次数") VALUES (%s,1) ON CONFLICT(user_id) DO UPDATE SET "历练次数"=COALESCE(statistics."历练次数",0)+1', (user_id,))
                conn.execute("INSERT INTO training_event_operations VALUES (%s,%s)", (operation_id, payload)); conn.commit()
                return TrainingEventResult("applied", str(state.get("last_event", "")))
            except Exception:
                conn.rollback(); raise

@dataclass(frozen=True)
class TrainingCompletionResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TrainingCompletionService:
    """Atomically save a completed training cycle and its game rewards."""

    _STATE_FIELDS = ("progress", "last_time", "points", "completed", "max_progress", "last_event", "weekly_purchases")

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def complete(self, operation_id, user_id, expected_state, state, stone, exp, items, max_goods_num) -> TrainingCompletionResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected, updated = dict(expected_state), dict(state)
        stone, exp, max_goods_num = map(int, (stone, exp, max_goods_num))
        rewards = tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in items if int(item.get("amount", 0)) > 0)
        if not operation_id or min(stone, exp, max_goods_num) < 0 or not all(key in expected and key in updated for key in self._STATE_FIELDS):
            raise ValueError("valid operation, training state and rewards are required")
        def state_value(key, value):
            return json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "weekly_purchases" else str(value)
        def state_matches(current):
            for key, actual in zip(self._STATE_FIELDS, current):
                if key == "weekly_purchases":
                    try:
                        if json.loads(str(actual)) == expected[key]:
                            continue
                    except (TypeError, ValueError):
                        pass
                elif str(actual) == str(expected[key]):
                    continue
                return False
            return True
        payload = json.dumps([user_id, expected, updated, stone, exp, rewards, max_goods_num], ensure_ascii=True, sort_keys=True, default=str)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS training_completion_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM training_completion_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    return TrainingCompletionResult("duplicate" if str(previous[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return TrainingCompletionResult("user_missing")
                current = conn.execute("SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases FROM player_data.training WHERE user_id=%s", (user_id,)).fetchone()
                if current is None or not state_matches(current):
                    conn.rollback()
                    return TrainingCompletionResult("state_changed")
                for item_id, _, _, amount in rewards:
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return TrainingCompletionResult("inventory_full")
                conn.execute("UPDATE player_data.training SET progress=%s,last_time=%s,points=%s,completed=%s,max_progress=%s,last_event=%s,weekly_purchases=%s WHERE user_id=%s", tuple(state_value(key, updated[key]) for key in self._STATE_FIELDS) + (user_id,))
                conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL), exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, exp, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, update_time=EXCLUDED.update_time", (user_id,item_id,name,item_type,amount,now,now,amount))
                conn.execute("INSERT INTO training_completion_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id,payload))
                conn.commit()
                return TrainingCompletionResult("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class TrainingPurchaseResult:
    status: str
    quantity: int
    cost: int
    points: int
    purchased: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TrainingPurchaseService:
    """Exchange training points for an inventory item in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def purchase(
        self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost,
        weekly_limit, expected_points, expected_weekly_purchases, max_goods_num, bind_flag=1,
        today=None,
    ) -> TrainingPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, unit_cost, weekly_limit = map(int, (item_id, quantity, unit_cost, weekly_limit))
        expected_points, max_goods_num = map(int, (expected_points, max_goods_num))
        item_name, item_type = str(item_name), str(item_type)
        bind_flag = 1 if int(bind_flag) == 1 else 0
        today = today or date.today()
        weekly = normalize_weekly_purchases(expected_weekly_purchases, today)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_points, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        payload = json.dumps(
            [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_points, weekly, max_goods_num, bind_flag],
            ensure_ascii=True, sort_keys=True,
        )

        def result(status: str, points=expected_points, purchased=0, inventory=0) -> TrainingPurchaseResult:
            succeeded = status in {"applied", "duplicate"}
            return TrainingPurchaseResult(status, quantity if succeeded else 0, quantity * unit_cost if succeeded else 0, int(points), int(purchased), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS training_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, points INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, points, purchased, inventory FROM training_purchase_operations "
                    "WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return TrainingPurchaseResult("duplicate", *(int(value) for value in previous[1:]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                columns = {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(training)").fetchall()}
                if not {"points", "weekly_purchases"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                training = conn.execute(
                    "SELECT COALESCE(points, 0), COALESCE(weekly_purchases, '{}') FROM player_data.training WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if training is None:
                    conn.rollback()
                    return result("state_changed")
                current_weekly = normalize_weekly_purchases(training[1], today)
                if int(training[0]) != expected_points or current_weekly != weekly:
                    conn.rollback()
                    return result("state_changed")
                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return result("limit_reached", purchased=purchased)
                cost = quantity * unit_cost
                if expected_points < cost:
                    conn.rollback()
                    return result("points_insufficient", purchased=purchased)
                inventory_row = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                inventory = int(inventory_row[0]) if inventory_row else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", purchased=purchased, inventory=inventory)
                points, purchased, inventory = expected_points - cost, purchased + quantity, inventory + quantity
                weekly[str(item_id)] = purchased
                if conn.execute(
                    "UPDATE player_data.training SET points=%s, weekly_purchases=%s WHERE user_id=%s AND COALESCE(points, 0)=%s",
                    (points, json.dumps(weekly, ensure_ascii=False), user_id, expected_points),
                ).rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                now = datetime.now()
                conn.execute(
                    "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                    "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=CASE WHEN %s=1 THEN "
                    "COALESCE(back.bind_num, 0)+EXCLUDED.goods_num ELSE COALESCE(back.bind_num, 0) END",
                    (user_id, item_id, item_name, item_type, quantity, now, now, quantity if bind_flag else 0, bind_flag),
                )
                conn.execute(
                    "INSERT INTO training_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, points, purchased, inventory),
                )
                conn.commit()
                return TrainingPurchaseResult("applied", quantity, cost, points, purchased, inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class TrainingResetResult:
    status: str
    task_status: str = ""
    reset_date: str = ""
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TrainingResetService:
    """Reset a frozen player set in resumable cross-database chunks."""

    _STATE_FIELDS = (
        "progress",
        "last_time",
        "points",
        "completed",
        "max_progress",
        "last_event",
        "weekly_purchases",
    )

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
    def _normalize_date(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _payload(operator_id: str) -> str:
        return json.dumps(
            {"action": "training_reset", "operator_id": operator_id},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_training_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reset_date TEXT NOT NULL,"
            "total INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0,"
            "changed INTEGER NOT NULL DEFAULT 0,skipped INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_training_reset_targets("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',previous_state TEXT,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _result(conn, operation_id: str, status: str) -> TrainingResetResult:
        row = conn.execute(
            "SELECT status,reset_date,total,completed,changed,skipped "
            "FROM admin_training_reset_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return TrainingResetResult(status)
        return TrainingResetResult(
            status=status,
            task_status=str(row[0]),
            reset_date=str(row[1]),
            total=int(row[2]),
            completed=int(row[3]),
            changed=int(row[4]),
            skipped=int(row[5]),
        )

    @classmethod
    def _snapshot(cls, row) -> str:
        return json.dumps(
            dict(zip(cls._STATE_FIELDS, row)),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def _state_changed(row, weekly_purchases: dict[str, str]) -> bool:
        try:
            current_weekly = json.loads(str(row[6])) if row[6] else {}
        except (TypeError, ValueError):
            current_weekly = row[6]
        return (
            int(row[0] or 0) != 0
            or row[1] is not None
            or int(row[2] or 0) != 0
            or int(row[3] or 0) != 0
            or int(row[4] or 0) != 0
            or str(row[5] or "") != ""
            or current_weekly != weekly_purchases
        )

    def reset(
        self,
        operation_id,
        operator_id,
        *,
        chunk_size=500,
        reset_date=None,
        updated_at=None,
    ) -> TrainingResetResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        chunk_size = max(1, int(chunk_size))
        reset_date = self._normalize_date(reset_date)
        updated_at = str(updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not operation_id or not operator_id:
            raise ValueError("operation and operator are required")
        payload = self._payload(operator_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT payload,reset_date,status FROM admin_training_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO admin_training_reset_operations("
                        "operation_id,payload,reset_date,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                        (
                            operation_id,
                            payload,
                            reset_date,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO admin_training_reset_targets("
                        "operation_id,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (operation_id, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, operation_id, "applied")
                else:
                    if str(operation[0]) != payload:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result
                    reset_date = str(operation[1])
                    if str(operation[2]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM admin_training_reset_targets "
                    "WHERE operation_id=%s AND status='pending' ORDER BY user_id LIMIT %s",
                    (operation_id, chunk_size),
                ).fetchall()
                changed = 0
                skipped = 0
                weekly_purchases = {"_last_reset": reset_date}
                weekly_json = json.dumps(
                    weekly_purchases,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    user_exists = conn.execute(
                        "SELECT 1 FROM user_xiuxian WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    training = None
                    if user_exists is not None:
                        training = conn.execute(
                            "SELECT progress,last_time,points,completed,max_progress,last_event,"
                            "weekly_purchases FROM player_data.training WHERE user_id=%s",
                            (user_id,),
                        ).fetchone()
                    if training is None:
                        skipped += 1
                        conn.execute(
                            "UPDATE admin_training_reset_targets SET status='skipped',updated_at=%s "
                            "WHERE operation_id=%s AND user_id=%s AND status='pending'",
                            (updated_at, operation_id, user_id),
                        )
                        continue

                    previous_state = self._snapshot(training)
                    changed += int(self._state_changed(training, weekly_purchases))
                    updated = conn.execute(
                        "UPDATE player_data.training SET progress=0,last_time=NULL,points=0,"
                        "completed=0,max_progress=0,last_event='',weekly_purchases=%s "
                        "WHERE user_id=%s",
                        (weekly_json, user_id),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError(
                            "training reset target changed"
                        )
                    target_updated = conn.execute(
                        "UPDATE admin_training_reset_targets SET status='applied',"
                        "previous_state=%s,updated_at=%s WHERE operation_id=%s AND user_id=%s "
                        "AND status='pending'",
                        (previous_state, updated_at, operation_id, user_id),
                    )
                    if target_updated.rowcount != 1:
                        raise db_backend.IntegrityError(
                            "training reset progress changed"
                        )

                progress = conn.execute(
                    "SELECT COUNT(*),"
                    "COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM admin_training_reset_targets WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE admin_training_reset_operations SET completed=%s,"
                    "changed=changed+%s,skipped=skipped+%s,status=%s,updated_at=%s "
                    "WHERE operation_id=%s",
                    (
                        completed,
                        changed,
                        skipped,
                        task_status,
                        updated_at,
                        operation_id,
                    ),
                )
                result = self._result(conn, operation_id, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "TrainingStateService",
    "TrainingEventResult",
    "TrainingEventService",
    "TrainingCompletionResult",
    "TrainingCompletionService",
    "TrainingPurchaseResult",
    "TrainingPurchaseService",
    "TrainingResetResult",
    "TrainingResetService",
    "TRAINING_FIELDS",
    "normalize_weekly_purchases",
]
