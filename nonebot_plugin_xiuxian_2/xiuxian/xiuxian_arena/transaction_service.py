from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import RLock
from datetime import date, datetime
from ..xiuxian_utils import db_backend
from datetime import date

ARENA_FIELDS = (
    "score",
    "total_wins",
    "total_losses",
    "daily_challenges_used",
    "daily_extra_challenges",
    "daily_challenge_buys",
    "last_reset_date",
    "last_buy_date",
    "last_challenge_time",
    "win_streak",
    "max_win_streak",
    "rank",
    "honor_points",
    "total_honor_earned",
    "weekly_purchases",
)

_INTEGER_DEFAULTS = {
    "score": 1000,
    "total_wins": 0,
    "total_losses": 0,
    "daily_challenges_used": 0,
    "daily_extra_challenges": 0,
    "daily_challenge_buys": 0,
    "win_streak": 0,
    "max_win_streak": 0,
    "honor_points": 0,
    "total_honor_earned": 0,
}

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
    return _normalize_weekly_purchases(value, today)[0]

def normalize_daily_purchase_state(bought, extra, last_buy_date, today=None) -> tuple[int, int, str]:
    today = _as_date(today)
    try:
        last_buy = date.fromisoformat(str(last_buy_date))
    except (TypeError, ValueError):
        last_buy = None
    if last_buy != today:
        return 0, 0, today.isoformat()
    try:
        bought = max(0, int(bought or 0))
    except (TypeError, ValueError):
        bought = 0
    try:
        extra = max(0, int(extra or 0))
    except (TypeError, ValueError):
        extra = 0
    return bought, extra, last_buy.isoformat()

class ArenaStateService:
    """Initialize and advance arena read state in one player-database transaction."""

    _COLUMN_DEFINITIONS = {
        "score": "TEXT DEFAULT '1000'",
        "total_wins": "TEXT DEFAULT '0'",
        "total_losses": "TEXT DEFAULT '0'",
        "daily_challenges_used": "TEXT DEFAULT '0'",
        "daily_extra_challenges": "TEXT DEFAULT '0'",
        "daily_challenge_buys": "TEXT DEFAULT '0'",
        "last_reset_date": "TEXT DEFAULT ''",
        "last_buy_date": "TEXT DEFAULT ''",
        "last_challenge_time": "TEXT DEFAULT ''",
        "win_streak": "TEXT DEFAULT '0'",
        "max_win_streak": "TEXT DEFAULT '0'",
        "rank": "TEXT DEFAULT '青铜'",
        "honor_points": "TEXT DEFAULT '0'",
        "total_honor_earned": "TEXT DEFAULT '0'",
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
    def _integer(value, default) -> tuple[int, bool]:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return int(default), True
        return normalized, value is None

    @staticmethod
    def _business_date(value, today) -> tuple[str, bool]:
        try:
            normalized = date.fromisoformat(str(value)).isoformat()
        except (TypeError, ValueError):
            return today.isoformat(), True
        return normalized, False

    @classmethod
    def _normalize_state(cls, row, today, weekly) -> tuple[dict, bool]:
        state = {}
        repair = False
        for index, field in enumerate(ARENA_FIELDS[:-1]):
            value = row[index]
            if field in _INTEGER_DEFAULTS:
                state[field], changed = cls._integer(value, _INTEGER_DEFAULTS[field])
            elif field in {"last_reset_date", "last_buy_date"}:
                state[field], changed = cls._business_date(value, today)
            elif field == "rank":
                state[field], changed = (str(value), False) if value else ("青铜", True)
            else:
                state[field], changed = (str(value or ""), value is None)
            repair = repair or changed
        state["weekly_purchases"] = weekly
        return state, repair

    @staticmethod
    def _snapshot(state) -> str:
        return json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena ("
            "user_id TEXT PRIMARY KEY,score TEXT DEFAULT '1000',total_wins TEXT DEFAULT '0',"
            "total_losses TEXT DEFAULT '0',daily_challenges_used TEXT DEFAULT '0',"
            "daily_extra_challenges TEXT DEFAULT '0',daily_challenge_buys TEXT DEFAULT '0',"
            "last_reset_date TEXT DEFAULT '',last_buy_date TEXT DEFAULT '',"
            "last_challenge_time TEXT DEFAULT '',win_streak TEXT DEFAULT '0',"
            "max_win_streak TEXT DEFAULT '0',rank TEXT DEFAULT '青铜',honor_points TEXT DEFAULT '0',"
            "total_honor_earned TEXT DEFAULT '0',weekly_purchases TEXT DEFAULT NULL)"
        )
        columns = {str(column[1]) for column in conn.execute("PRAGMA table_info(arena)").fetchall()}
        for field, definition in cls._COLUMN_DEFINITIONS.items():
            if field not in columns:
                conn.execute(f'ALTER TABLE arena ADD COLUMN "{field}" {definition}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_state_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _insert_operation(conn, operation_id, user_id, kind, period_key, state) -> None:
        conn.execute(
            "INSERT INTO arena_state_operations("
            "operation_id,user_id,kind,period_key,snapshot,created_at) "
            "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (operation_id, user_id, kind, period_key, ArenaStateService._snapshot(state)),
        )

    @staticmethod
    def _default_state(today) -> dict:
        state = dict(_INTEGER_DEFAULTS)
        state.update(
            {
                "last_reset_date": today.isoformat(),
                "last_buy_date": today.isoformat(),
                "last_challenge_time": "",
                "rank": "青铜",
                "weekly_purchases": {"_last_reset": today.isoformat()},
            }
        )
        return {field: state[field] for field in ARENA_FIELDS}

    @staticmethod
    def _state_values(state) -> tuple:
        return tuple(
            json.dumps(state[field], ensure_ascii=True, sort_keys=True)
            if field == "weekly_purchases"
            else state[field]
            for field in ARENA_FIELDS
        )

    def get(self, user_id, today=None) -> dict:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        today = _as_date(today)
        day_key = today.isoformat()
        period_key = self._period_key(today)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    f"SELECT {','.join(ARENA_FIELDS)} FROM arena WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    state = self._default_state(today)
                    conn.execute(
                        f"INSERT INTO arena(user_id,{','.join(ARENA_FIELDS)}) "
                        f"VALUES(%s,{','.join(['%s'] * len(ARENA_FIELDS))})",
                        (user_id, *self._state_values(state)),
                    )
                    self._insert_operation(
                        conn, f"arena-state-init:{user_id}", user_id,
                        "initialize", day_key, state,
                    )
                    conn.commit()
                    return state

                weekly, weekly_changed = _normalize_weekly_purchases(row[-1], today)
                state, repair = self._normalize_state(row, today, weekly)
                daily_changed = state["last_buy_date"] != day_key
                if daily_changed:
                    state["daily_challenge_buys"] = 0
                    state["daily_extra_challenges"] = 0
                    state["last_buy_date"] = day_key

                if repair:
                    assignments = ",".join(f'"{field}"=%s' for field in ARENA_FIELDS[:-1])
                    conn.execute(
                        f"UPDATE arena SET {assignments} WHERE user_id=%s",
                        (*self._state_values(state)[:-1], user_id),
                    )
                if daily_changed:
                    conn.execute(
                        "UPDATE arena SET daily_challenge_buys=0,daily_extra_challenges=0,"
                        "last_buy_date=%s WHERE user_id=%s",
                        (day_key, user_id),
                    )
                if weekly_changed:
                    conn.execute(
                        "UPDATE arena SET weekly_purchases=%s WHERE user_id=%s",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )

                if repair:
                    self._insert_operation(
                        conn, f"arena-state-normalize:{user_id}", user_id,
                        "normalize", day_key, state,
                    )
                if daily_changed:
                    self._insert_operation(
                        conn, f"arena-state-buy-day:{user_id}:{day_key}", user_id,
                        "day", day_key, state,
                    )
                if weekly_changed:
                    self._insert_operation(
                        conn, f"arena-state-week:{user_id}:{period_key}", user_id,
                        "week", period_key, state,
                    )
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class ArenaPurchaseResult:
    status: str
    quantity: int
    cost: int
    honor_points: int
    purchased: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaPurchaseService:
    """Exchange honor points for an inventory item in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def purchase(
        self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost,
        weekly_limit, expected_honor, expected_weekly_purchases, max_goods_num, bind_flag=1,
        today=None,
    ) -> ArenaPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, unit_cost, weekly_limit = map(int, (item_id, quantity, unit_cost, weekly_limit))
        expected_honor, max_goods_num = map(int, (expected_honor, max_goods_num))
        item_name, item_type = str(item_name), str(item_type)
        bind_flag = 1 if int(bind_flag) == 1 else 0
        today = today or date.today()
        weekly = normalize_weekly_purchases(expected_weekly_purchases, today)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_honor, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        # Request identity only — mutable snapshots stay out of payload so replays
        # can reuse the first result after honor/weekly state has already changed.
        payload = json.dumps(
            [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num, bind_flag],
            ensure_ascii=True, sort_keys=True,
        )

        def result(status: str, honor_points=expected_honor, purchased=0, inventory=0) -> ArenaPurchaseResult:
            succeeded = status in {"applied", "duplicate"}
            return ArenaPurchaseResult(status, quantity if succeeded else 0, quantity * unit_cost if succeeded else 0, int(honor_points), int(purchased), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, honor_points INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, honor_points, purchased, inventory FROM arena_purchase_operations "
                    "WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return ArenaPurchaseResult("duplicate", *(int(value) for value in previous[1:]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                columns = {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()}
                if not {"honor_points", "weekly_purchases"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                arena = conn.execute(
                    "SELECT COALESCE(honor_points, 0), COALESCE(weekly_purchases, '{}') FROM player_data.arena WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if arena is None:
                    conn.rollback()
                    return result("state_changed")
                current_weekly = normalize_weekly_purchases(arena[1], today)
                if int(arena[0]) != expected_honor or current_weekly != weekly:
                    conn.rollback()
                    return result("state_changed")
                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return result("limit_reached", purchased=purchased)
                cost = quantity * unit_cost
                if expected_honor < cost:
                    conn.rollback()
                    return result("honor_insufficient", purchased=purchased)
                inventory_row = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                inventory = int(inventory_row[0]) if inventory_row else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", purchased=purchased, inventory=inventory)
                honor_points, purchased, inventory = expected_honor - cost, purchased + quantity, inventory + quantity
                weekly[str(item_id)] = purchased
                if conn.execute(
                    "UPDATE player_data.arena SET honor_points=%s, weekly_purchases=%s WHERE user_id=%s AND COALESCE(honor_points, 0)=%s",
                    (honor_points, json.dumps(weekly, ensure_ascii=False), user_id, expected_honor),
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
                    "INSERT INTO arena_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, honor_points, purchased, inventory),
                )
                conn.commit()
                return ArenaPurchaseResult("applied", quantity, cost, honor_points, purchased, inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ArenaChallengePurchaseResult:
    status: str
    amount: int
    cost: int
    stone: int
    bought: int
    extra: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaChallengePurchaseService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database = Path(game_database), Path(player_database)
        self._lock = lock or RLock()

    def purchase(
        self, operation_id, user_id, amount, unit_cost, daily_limit, expected_stone,
        expected_bought, expected_extra, expected_last_buy_date, today=None,
    ):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra = map(int, (amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra))
        if not operation_id or min(amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra) < 0 or amount == 0:
            raise ValueError("valid operation and purchase state are required")
        today = today or date.today()
        expected_bought, expected_extra, expected_last_buy_date = normalize_daily_purchase_state(
            expected_bought, expected_extra, expected_last_buy_date, today
        )
        # Request identity only — daily counters/stone snapshots are concurrency checks,
        # not part of the idempotent request key.
        payload = json.dumps(
            [user_id, amount, unit_cost, daily_limit],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS arena_challenge_purchase_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, amount INTEGER NOT NULL, cost INTEGER NOT NULL, stone INTEGER NOT NULL, bought INTEGER NOT NULL, extra INTEGER NOT NULL)")
                previous = conn.execute("SELECT payload,amount,cost,stone,bought,extra FROM arena_challenge_purchase_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    return ArenaChallengePurchaseResult("duplicate", *(map(int, previous[1:]))) if str(previous[0]) == payload else ArenaChallengePurchaseResult("state_changed", 0, 0, expected_stone, expected_bought, expected_extra)
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                arena = conn.execute("SELECT daily_challenge_buys,daily_extra_challenges,last_buy_date FROM player_data.arena WHERE user_id=%s", (user_id,)).fetchone()
                current = normalize_daily_purchase_state(*arena, today) if arena else None
                if not user or current != (expected_bought, expected_extra, expected_last_buy_date) or int(user[0]) != expected_stone:
                    conn.rollback(); return ArenaChallengePurchaseResult("state_changed", 0, 0, expected_stone, expected_bought, expected_extra)
                real_amount = min(amount, max(0, daily_limit - expected_bought))
                cost = real_amount * unit_cost
                if real_amount == 0:
                    conn.rollback(); return ArenaChallengePurchaseResult("limit_reached", 0, 0, expected_stone, expected_bought, expected_extra)
                if expected_stone < cost:
                    conn.rollback(); return ArenaChallengePurchaseResult("stone_insufficient", 0, 0, expected_stone, expected_bought, expected_extra)
                stone, bought, extra = expected_stone - cost, expected_bought + real_amount, expected_extra + real_amount
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (stone, user_id))
                conn.execute("UPDATE player_data.arena SET daily_challenge_buys=%s,daily_extra_challenges=%s,last_buy_date=%s WHERE user_id=%s", (bought, extra, expected_last_buy_date, user_id))
                conn.execute("INSERT INTO arena_challenge_purchase_operations VALUES (%s,%s,%s,%s,%s,%s,%s)", (operation_id, payload, real_amount, cost, stone, bought, extra))
                conn.commit(); return ArenaChallengePurchaseResult("applied", real_amount, cost, stone, bought, extra)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ArenaChallengeTicketResult:
    status: str
    used_tickets: int
    item_remaining: int
    challenges_used: int
    challenges_remaining: int
    challenge_cap: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaChallengeTicketService:
    """Consume challenge tickets and restore arena attempts in one transaction."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def use(
        self,
        operation_id,
        user_id,
        item_id,
        requested_count,
        expected_item_count,
        expected_challenges_used,
        expected_extra_challenges,
        challenge_cap,
    ) -> ArenaChallengeTicketResult:
        operation_id = str(operation_id).strip()
        user_id, item_id = str(user_id), int(item_id)
        requested_count, expected_item_count = map(int, (requested_count, expected_item_count))
        expected_challenges_used, expected_extra_challenges, challenge_cap = map(
            int, (expected_challenges_used, expected_extra_challenges, challenge_cap)
        )
        if not operation_id or requested_count <= 0 or min(
            expected_item_count,
            expected_challenges_used,
            expected_extra_challenges,
            challenge_cap,
        ) < 0:
            raise ValueError("valid operation and non-negative arena snapshot are required")
        # Request identity only — inventory/used counters are concurrency checks.
        payload = json.dumps(
            [
                user_id,
                item_id,
                requested_count,
                challenge_cap,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        def result(status: str) -> ArenaChallengeTicketResult:
            return ArenaChallengeTicketResult(
                status,
                0,
                expected_item_count,
                expected_challenges_used,
                max(0, challenge_cap - expected_challenges_used),
                challenge_cap,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_challenge_ticket_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tickets INTEGER NOT NULL,"
                    "item_remaining INTEGER NOT NULL,challenges_used INTEGER NOT NULL,"
                    "challenges_remaining INTEGER NOT NULL,challenge_cap INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap "
                    "FROM arena_challenge_ticket_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("operation_conflict")
                    return ArenaChallengeTicketResult(
                        "duplicate", *(int(value) for value in previous[1:])
                    )

                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()
                }
                if not {"daily_challenges_used", "daily_extra_challenges"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                arena = conn.execute(
                    "SELECT COALESCE(daily_challenges_used,0),COALESCE(daily_extra_challenges,0) "
                    "FROM player_data.arena WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if arena is None or tuple(int(value or 0) for value in arena) != (
                    expected_challenges_used,
                    expected_extra_challenges,
                ):
                    conn.rollback()
                    return result("state_changed")
                if item is None or int(item[0]) <= 0:
                    conn.rollback()
                    return result("item_missing")
                if int(item[0]) != expected_item_count:
                    conn.rollback()
                    return result("state_changed")
                if expected_challenges_used <= 0:
                    conn.rollback()
                    return result("no_challenges_used")

                used_tickets = min(
                    requested_count, expected_item_count, expected_challenges_used
                )
                item_remaining = expected_item_count - used_tickets
                challenges_used = expected_challenges_used - used_tickets
                challenges_remaining = max(0, challenge_cap - challenges_used)
                bound = int(item[1])
                bind_remaining = min(
                    bound - used_tickets if bound >= used_tickets else bound,
                    item_remaining,
                )
                updated = conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)=%s",
                    (item_remaining, bind_remaining, user_id, item_id, expected_item_count),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                # Historical rows may store daily counters as text; cast before compare.
                updated = conn.execute(
                    "UPDATE player_data.arena SET daily_challenges_used=%s WHERE user_id=%s "
                    "AND CAST(COALESCE(daily_challenges_used,0) AS INTEGER)=%s "
                    "AND CAST(COALESCE(daily_extra_challenges,0) AS INTEGER)=%s",
                    (
                        challenges_used,
                        user_id,
                        expected_challenges_used,
                        expected_extra_challenges,
                    ),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO arena_challenge_ticket_operations("
                    "operation_id,payload,used_tickets,item_remaining,challenges_used,"
                    "challenges_remaining,challenge_cap) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        used_tickets,
                        item_remaining,
                        challenges_used,
                        challenges_remaining,
                        challenge_cap,
                    ),
                )
                conn.commit()
                return ArenaChallengeTicketResult(
                    "applied",
                    used_tickets,
                    item_remaining,
                    challenges_used,
                    challenges_remaining,
                    challenge_cap,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ArenaChallengeSettlementResult:
    status: str
    outcome: str = ""
    challenger_score: int = 0
    challenger_rank: str = ""
    opponent_score: int | None = None
    score_delta: int = 0
    used: int = 0
    remaining: int = 0
    stamina: int = 0
    challenged_at: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaChallengeSettlementService:
    """Commit arena challenge cost and battle outcome in one transaction."""

    _BATTLE_FIELDS = (
        "score",
        "total_wins",
        "total_losses",
        "win_streak",
        "max_win_streak",
        "rank",
    )
    _CHALLENGER_FIELDS = _BATTLE_FIELDS + (
        "daily_challenges_used",
        "daily_extra_challenges",
        "last_challenge_time",
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
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_challenge_settlement_operations("
            "operation_id TEXT PRIMARY KEY,challenger_id TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _arena_snapshot(cls, value, challenger: bool) -> dict:
        data = dict(value)
        fields = cls._CHALLENGER_FIELDS if challenger else cls._BATTLE_FIELDS
        result = {}
        for field in fields:
            if field in {"rank", "last_challenge_time"}:
                result[field] = str(data.get(field) or "")
            else:
                result[field] = int(data.get(field, 0) or 0)
        return result

    @staticmethod
    def _player_snapshot(value, challenger: bool) -> dict:
        data = dict(value)
        result = {"hp": int(data["hp"]), "mp": int(data["mp"])}
        if challenger:
            result["user_stamina"] = int(data.get("user_stamina", 0) or 0)
        return result

    @staticmethod
    def _rank(score: int) -> str:
        if score >= 3200:
            return "王者"
        if score >= 2700:
            return "钻石"
        if score >= 2300:
            return "铂金"
        if score >= 1900:
            return "黄金"
        if score >= 1500:
            return "白银"
        return "青铜"

    @staticmethod
    def _encode_result(result: ArenaChallengeSettlementResult) -> str:
        return json.dumps(
            asdict(result), ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _decode_result(status: str, raw: str) -> ArenaChallengeSettlementResult:
        data = json.loads(raw)
        data["status"] = status
        return ArenaChallengeSettlementResult(**data)

    def get_result(
        self, operation_id, challenger_id=None
    ) -> ArenaChallengeSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT challenger_id,result_json FROM arena_challenge_settlement_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            if challenger_id is not None and str(row[0]) != str(challenger_id):
                return ArenaChallengeSettlementResult("operation_conflict")
            return self._decode_result("duplicate", str(row[1]))

    @classmethod
    def _normalize_arena_row(cls, row, challenger: bool) -> tuple:
        fields = cls._CHALLENGER_FIELDS if challenger else cls._BATTLE_FIELDS
        values = []
        for index, field in enumerate(fields):
            value = row[index]
            if field in {"rank", "last_challenge_time"}:
                values.append(str(value or ""))
            else:
                values.append(int(value or 0))
        return tuple(values)

    def settle(
        self,
        operation_id,
        challenger_id,
        opponent_id,
        outcome,
        challenge_cap,
        stamina_cost,
        challenged_at,
        expected_challenger_arena,
        expected_opponent_arena,
        expected_challenger_player,
        expected_opponent_player,
        final_challenger_hp,
        final_challenger_mp,
        final_opponent_hp,
        final_opponent_mp,
        win_points,
        lose_points,
        no_match_points,
    ) -> ArenaChallengeSettlementResult:
        operation_id = str(operation_id).strip()
        challenger_id = str(challenger_id)
        opponent_id = "" if opponent_id is None else str(opponent_id)
        outcome = str(outcome)
        challenge_cap, stamina_cost = int(challenge_cap), int(stamina_cost)
        challenged_at = str(challenged_at)
        challenger = self._arena_snapshot(expected_challenger_arena, True)
        opponent = (
            None
            if expected_opponent_arena is None
            else self._arena_snapshot(expected_opponent_arena, False)
        )
        challenger_player = self._player_snapshot(expected_challenger_player, True)
        opponent_player = (
            None
            if expected_opponent_player is None
            else self._player_snapshot(expected_opponent_player, False)
        )
        final_challenger_hp = max(1, int(final_challenger_hp))
        final_challenger_mp = max(1, int(final_challenger_mp))
        final_opponent_hp = (
            None if final_opponent_hp is None else max(1, int(final_opponent_hp))
        )
        final_opponent_mp = (
            None if final_opponent_mp is None else max(1, int(final_opponent_mp))
        )
        win_points, lose_points, no_match_points = map(
            int, (win_points, lose_points, no_match_points)
        )
        if (
            not operation_id
            or not challenger_id
            or not challenged_at
            or outcome not in {"win", "loss", "draw", "no_match"}
            or min(
                challenge_cap,
                stamina_cost,
                win_points,
                lose_points,
                no_match_points,
            )
            < 0
        ):
            raise ValueError("valid arena challenge settlement is required")
        if outcome != "no_match" and (
            not opponent_id or opponent is None or opponent_player is None
        ):
            raise ValueError("battle opponent snapshots are required")
        if opponent_id and opponent_id == challenger_id:
            raise ValueError("challenger and opponent must differ")

        payload = json.dumps(
            [
                challenger_id,
                opponent_id,
                outcome,
                challenge_cap,
                stamina_cost,
                challenged_at,
                challenger,
                opponent,
                challenger_player,
                opponent_player,
                final_challenger_hp,
                final_challenger_mp,
                final_opponent_hp,
                final_opponent_mp,
                win_points,
                lose_points,
                no_match_points,
            ],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        def current_result(status: str) -> ArenaChallengeSettlementResult:
            used = challenger["daily_challenges_used"]
            return ArenaChallengeSettlementResult(
                status=status,
                outcome=outcome,
                challenger_score=challenger["score"],
                challenger_rank=challenger["rank"],
                opponent_score=None if opponent is None else opponent["score"],
                score_delta=(
                    win_points
                    if outcome == "win"
                    else no_match_points if outcome == "no_match" else 0
                ),
                used=used,
                remaining=max(0, challenge_cap - used),
                stamina=challenger_player["user_stamina"],
                challenged_at=challenger["last_challenge_time"],
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM arena_challenge_settlement_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return current_result("operation_conflict")
                    return self._decode_result("duplicate", str(previous[1]))

                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA player_data.table_info(arena)"
                    ).fetchall()
                }
                if not set(self._CHALLENGER_FIELDS).issubset(columns):
                    conn.rollback()
                    return current_result("state_changed")

                challenger_arena_row = conn.execute(
                    "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank,"
                    "daily_challenges_used,daily_extra_challenges,COALESCE(last_challenge_time,'') "
                    "FROM player_data.arena WHERE user_id=%s",
                    (challenger_id,),
                ).fetchone()
                challenger_player_row = conn.execute(
                    "SELECT COALESCE(hp,0),COALESCE(mp,0),COALESCE(user_stamina,0) "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (challenger_id,),
                ).fetchone()
                if (
                    challenger_arena_row is None
                    or challenger_player_row is None
                    or self._normalize_arena_row(challenger_arena_row, True)
                    != tuple(challenger[field] for field in self._CHALLENGER_FIELDS)
                    or tuple(map(int, challenger_player_row))
                    != (
                        challenger_player["hp"],
                        challenger_player["mp"],
                        challenger_player["user_stamina"],
                    )
                ):
                    conn.rollback()
                    return current_result("state_changed")

                if opponent is not None:
                    opponent_arena_row = conn.execute(
                        "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank "
                        "FROM player_data.arena WHERE user_id=%s",
                        (opponent_id,),
                    ).fetchone()
                    opponent_player_row = conn.execute(
                        "SELECT COALESCE(hp,0),COALESCE(mp,0) FROM user_xiuxian "
                        "WHERE user_id=%s",
                        (opponent_id,),
                    ).fetchone()
                    if (
                        opponent_arena_row is None
                        or opponent_player_row is None
                        or self._normalize_arena_row(opponent_arena_row, False)
                        != tuple(opponent[field] for field in self._BATTLE_FIELDS)
                        or tuple(map(int, opponent_player_row))
                        != (opponent_player["hp"], opponent_player["mp"])
                    ):
                        conn.rollback()
                        return current_result("state_changed")

                used = challenger["daily_challenges_used"]
                if used >= challenge_cap:
                    conn.rollback()
                    return current_result("limit_reached")
                if challenger_player["user_stamina"] < stamina_cost:
                    conn.rollback()
                    return current_result("stamina_insufficient")

                challenger_new = dict(challenger)
                opponent_new = None if opponent is None else dict(opponent)
                if outcome == "win":
                    challenger_new["score"] += win_points
                    challenger_new["total_wins"] += 1
                    challenger_new["win_streak"] += 1
                    challenger_new["max_win_streak"] = max(
                        challenger_new["max_win_streak"], challenger_new["win_streak"]
                    )
                    opponent_new["score"] = max(0, opponent_new["score"] - lose_points)
                    opponent_new["total_losses"] += 1
                    opponent_new["win_streak"] = 0
                elif outcome in {"loss", "draw"}:
                    challenger_new["total_losses"] += 1
                    challenger_new["win_streak"] = 0
                else:
                    challenger_new["score"] += no_match_points
                    challenger_new["total_losses"] += 1
                    challenger_new["win_streak"] = 0
                challenger_new["rank"] = self._rank(challenger_new["score"])
                challenger_new["daily_challenges_used"] = used + 1
                challenger_new["last_challenge_time"] = challenged_at
                if opponent_new is not None:
                    opponent_new["rank"] = self._rank(opponent_new["score"])

                if conn.execute(
                    "UPDATE player_data.arena SET score=%s,total_wins=%s,total_losses=%s,"
                    "win_streak=%s,max_win_streak=%s,rank=%s,daily_challenges_used=%s,"
                    "daily_extra_challenges=%s,last_challenge_time=%s WHERE user_id=%s",
                    tuple(
                        challenger_new[field] for field in self._CHALLENGER_FIELDS
                    )
                    + (challenger_id,),
                ).rowcount != 1:
                    raise RuntimeError("challenger arena state changed")
                if opponent_new is not None and conn.execute(
                    "UPDATE player_data.arena SET score=%s,total_wins=%s,total_losses=%s,"
                    "win_streak=%s,max_win_streak=%s,rank=%s WHERE user_id=%s",
                    tuple(opponent_new[field] for field in self._BATTLE_FIELDS)
                    + (opponent_id,),
                ).rowcount != 1:
                    raise RuntimeError("opponent arena state changed")

                stamina = challenger_player["user_stamina"] - stamina_cost
                if conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,user_stamina=%s WHERE user_id=%s "
                    "AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s "
                    "AND COALESCE(user_stamina,0)=%s",
                    (
                        final_challenger_hp,
                        final_challenger_mp,
                        stamina,
                        challenger_id,
                        challenger_player["hp"],
                        challenger_player["mp"],
                        challenger_player["user_stamina"],
                    ),
                ).rowcount != 1:
                    raise RuntimeError("challenger player state changed")
                if opponent_player is not None and conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s "
                    "AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s",
                    (
                        final_opponent_hp,
                        final_opponent_mp,
                        opponent_id,
                        opponent_player["hp"],
                        opponent_player["mp"],
                    ),
                ).rowcount != 1:
                    raise RuntimeError("opponent player state changed")

                result = ArenaChallengeSettlementResult(
                    status="applied",
                    outcome=outcome,
                    challenger_score=challenger_new["score"],
                    challenger_rank=challenger_new["rank"],
                    opponent_score=(
                        None if opponent_new is None else opponent_new["score"]
                    ),
                    score_delta=(
                        win_points
                        if outcome == "win"
                        else no_match_points if outcome == "no_match" else 0
                    ),
                    used=challenger_new["daily_challenges_used"],
                    remaining=max(
                        0, challenge_cap - challenger_new["daily_challenges_used"]
                    ),
                    stamina=stamina,
                    challenged_at=challenged_at,
                )
                conn.execute(
                    "INSERT INTO arena_challenge_settlement_operations("
                    "operation_id,challenger_id,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (
                        operation_id,
                        challenger_id,
                        payload,
                        self._encode_result(result),
                    ),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ArenaBattleSettlementResult:
    status: str
    challenger_score: int
    challenger_rank: str
    opponent_score: int | None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaBattleSettlementService:
    """Commit arena score, records and battle vitals as one operation."""

    _ARENA_FIELDS = ("score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank")

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _arena_snapshot(value):
        data = dict(value)
        return {
            "score": int(data["score"]), "total_wins": int(data["total_wins"]),
            "total_losses": int(data["total_losses"]), "win_streak": int(data["win_streak"]),
            "max_win_streak": int(data["max_win_streak"]), "rank": str(data["rank"]),
        }

    @staticmethod
    def _player_snapshot(value):
        data = dict(value)
        return {"hp": int(data["hp"]), "mp": int(data["mp"])}

    def settle(
        self, operation_id, challenger_id, opponent_id, outcome,
        expected_challenger_arena, expected_opponent_arena,
        expected_challenger_player, expected_opponent_player,
        final_challenger_hp, final_challenger_mp, final_opponent_hp, final_opponent_mp,
        win_points, lose_points, no_match_points,
    ) -> ArenaBattleSettlementResult:
        operation_id, challenger_id = str(operation_id).strip(), str(challenger_id)
        opponent_id = "" if opponent_id is None else str(opponent_id)
        outcome = str(outcome)
        challenger = self._arena_snapshot(expected_challenger_arena)
        opponent = None if expected_opponent_arena is None else self._arena_snapshot(expected_opponent_arena)
        challenger_player = self._player_snapshot(expected_challenger_player)
        opponent_player = None if expected_opponent_player is None else self._player_snapshot(expected_opponent_player)
        final_challenger_hp, final_challenger_mp = max(1, int(final_challenger_hp)), max(1, int(final_challenger_mp))
        final_opponent_hp = None if final_opponent_hp is None else max(1, int(final_opponent_hp))
        final_opponent_mp = None if final_opponent_mp is None else max(1, int(final_opponent_mp))
        win_points, lose_points, no_match_points = map(int, (win_points, lose_points, no_match_points))
        if not operation_id or outcome not in {"win", "loss", "draw", "no_match"} or min(win_points, lose_points, no_match_points) < 0:
            raise ValueError("valid operation, outcome and score rules are required")
        if outcome != "no_match" and (not opponent_id or opponent is None or opponent_player is None):
            raise ValueError("battle opponent snapshots are required")
        payload = json.dumps([
            challenger_id, opponent_id, outcome, challenger, opponent, challenger_player, opponent_player,
            final_challenger_hp, final_challenger_mp, final_opponent_hp, final_opponent_mp,
            win_points, lose_points, no_match_points,
        ], ensure_ascii=True, sort_keys=True)

        def rank(score):
            return "王者" if score >= 3200 else "钻石" if score >= 2700 else "铂金" if score >= 2300 else "黄金" if score >= 1900 else "白银" if score >= 1500 else "青铜"

        def result(status, challenger_score=challenger["score"], challenger_rank=challenger["rank"], opponent_score=None):
            return ArenaBattleSettlementResult(status, int(challenger_score), str(challenger_rank), None if opponent_score is None else int(opponent_score))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_battle_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,challenger_score INTEGER NOT NULL,"
                    "challenger_rank TEXT NOT NULL,opponent_score INTEGER,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,challenger_score,challenger_rank,opponent_score FROM arena_battle_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("state_changed") if str(previous[0]) != payload else result("duplicate", previous[1], previous[2], previous[3])
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()}
                if not set(self._ARENA_FIELDS).issubset(columns):
                    conn.rollback(); return result("state_changed")

                ids = [challenger_id] + ([opponent_id] if opponent_id else [])
                arena_rows, player_rows = {}, {}
                for user_id in ids:
                    arena_rows[user_id] = conn.execute(
                        "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank FROM player_data.arena WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    player_rows[user_id] = conn.execute(
                        "SELECT COALESCE(hp,0),COALESCE(mp,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                    ).fetchone()
                expected_arena = {challenger_id: challenger, **({opponent_id: opponent} if opponent_id else {})}
                expected_players = {challenger_id: challenger_player, **({opponent_id: opponent_player} if opponent_id else {})}
                for user_id in ids:
                    row, player_row = arena_rows[user_id], player_rows[user_id]
                    expected = expected_arena[user_id]
                    if row is None or player_row is None or (
                        int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]), str(row[5])
                    ) != tuple(expected[field] for field in self._ARENA_FIELDS) or tuple(map(int, player_row)) != (
                        expected_players[user_id]["hp"], expected_players[user_id]["mp"]
                    ):
                        conn.rollback(); return result("state_changed")

                challenger_new = dict(challenger)
                opponent_new = None if opponent is None else dict(opponent)
                if outcome == "win":
                    challenger_new["score"] += win_points
                    challenger_new["total_wins"] += 1
                    challenger_new["win_streak"] += 1
                    challenger_new["max_win_streak"] = max(challenger_new["max_win_streak"], challenger_new["win_streak"])
                    opponent_new["score"] = max(0, opponent_new["score"] - lose_points)
                    opponent_new["total_losses"] += 1
                    opponent_new["win_streak"] = 0
                elif outcome in {"loss", "draw"}:
                    challenger_new["total_losses"] += 1
                    challenger_new["win_streak"] = 0
                else:
                    challenger_new["score"] += no_match_points
                    challenger_new["total_losses"] += 1
                    challenger_new["win_streak"] = 0
                challenger_new["rank"] = rank(challenger_new["score"])
                if opponent_new is not None:
                    opponent_new["rank"] = rank(opponent_new["score"])

                def update_arena(user_id, data):
                    return conn.execute(
                        "UPDATE player_data.arena SET score=%s,total_wins=%s,total_losses=%s,win_streak=%s,max_win_streak=%s,rank=%s WHERE user_id=%s",
                        tuple(data[field] for field in self._ARENA_FIELDS) + (user_id,),
                    ).rowcount

                if update_arena(challenger_id, challenger_new) != 1:
                    conn.rollback(); return result("state_changed")
                if opponent_new is not None and update_arena(opponent_id, opponent_new) != 1:
                    conn.rollback(); return result("state_changed")
                if conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s",
                    (final_challenger_hp, final_challenger_mp, challenger_id, challenger_player["hp"], challenger_player["mp"]),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                if opponent_player is not None and conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s",
                    (final_opponent_hp, final_opponent_mp, opponent_id, opponent_player["hp"], opponent_player["mp"]),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                opponent_score = opponent_new["score"] if opponent_new is not None else None
                conn.execute(
                    "INSERT INTO arena_battle_settlement_operations "
                    "(operation_id,payload,challenger_score,challenger_rank,opponent_score) VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, challenger_new["score"], challenger_new["rank"], opponent_score),
                )
                conn.commit()
                return result("applied", challenger_new["score"], challenger_new["rank"], opponent_score)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class ArenaWeeklyRankReductionResult:
    status: str
    business_week: str
    task_status: str = ""
    reduce_steps: int = 0
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0
    conflicted: int = 0
    last_error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaWeeklyRankReductionService:
    """Reduce a week-frozen arena player set in durable chunks."""

    RANKS = ("青铜", "白银", "黄金", "铂金", "钻石", "王者")
    INITIAL_SCORES = {
        "青铜": 1000,
        "白银": 1500,
        "黄金": 1900,
        "铂金": 2300,
        "钻石": 2700,
        "王者": 3200,
    }

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_week(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            iso = value.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        text = str(value).strip()
        if "-W" in text:
            year_text, week_text = text.split("-W", 1)
            monday = date.fromisocalendar(int(year_text), int(week_text), 1)
            iso = monday.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        return ArenaWeeklyRankReductionService._normalize_week(date.fromisoformat(text))

    @classmethod
    def _rank_for_score(cls, score: int) -> str:
        if score >= 3200:
            return "王者"
        if score >= 2700:
            return "钻石"
        if score >= 2300:
            return "铂金"
        if score >= 1900:
            return "黄金"
        if score >= 1500:
            return "白银"
        return "青铜"

    @classmethod
    def _target(cls, score: int, rank: str, reduce_steps: int) -> tuple[str, int]:
        current_rank = rank if rank in cls.RANKS else cls._rank_for_score(score)
        target_index = max(0, cls.RANKS.index(current_rank) - reduce_steps)
        target_rank = cls.RANKS[target_index]
        return target_rank, cls.INITIAL_SCORES[target_rank]

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena(user_id TEXT PRIMARY KEY,score INTEGER DEFAULT 1000,"
            "rank TEXT DEFAULT '青铜',win_streak INTEGER DEFAULT 0)"
        )
        columns = set(conn.column_names("arena"))
        missing = {
            "score": "INTEGER DEFAULT 1000",
            "rank": "TEXT DEFAULT '青铜'",
            "win_streak": "INTEGER DEFAULT 0",
        }
        for name, definition in missing.items():
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE arena ADD COLUMN {db_backend.quote_ident(name)} {definition}"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_operations("
            "business_week TEXT PRIMARY KEY,reduce_steps INTEGER NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "skipped INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',last_error TEXT NOT NULL DEFAULT '',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_targets("
            "business_week TEXT NOT NULL,user_id TEXT NOT NULL,ordinal INTEGER NOT NULL,"
            "previous_score INTEGER NOT NULL,previous_rank TEXT NOT NULL,"
            "previous_win_streak INTEGER NOT NULL,target_score INTEGER NOT NULL,"
            "target_rank TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "error_text TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_week,user_id))"
        )

    @staticmethod
    def _result(conn, business_week: str, status: str) -> ArenaWeeklyRankReductionResult:
        row = conn.execute(
            "SELECT status,reduce_steps,total,completed,changed,skipped,conflicted,last_error "
            "FROM arena_weekly_rank_reduction_operations WHERE business_week=%s",
            (business_week,),
        ).fetchone()
        if row is None:
            return ArenaWeeklyRankReductionResult(status, business_week)
        return ArenaWeeklyRankReductionResult(
            status=status,
            business_week=business_week,
            task_status=str(row[0]),
            reduce_steps=int(row[1]),
            total=int(row[2]),
            completed=int(row[3]),
            changed=int(row[4]),
            skipped=int(row[5]),
            conflicted=int(row[6]),
            last_error=str(row[7] or ""),
        )

    def reduce(
        self,
        business_week=None,
        reduce_steps=2,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> ArenaWeeklyRankReductionResult:
        business_week = self._normalize_week(business_week)
        reduce_steps = max(0, int(reduce_steps))
        chunk_size = max(1, int(chunk_size))
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            operation_created = False
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT reduce_steps,status FROM arena_weekly_rank_reduction_operations "
                    "WHERE business_week=%s",
                    (business_week,),
                ).fetchone()
                if operation is None:
                    users = tuple(
                        (
                            str(row[0]),
                            int(row[1] or 0),
                            str(row[2] or ""),
                            int(row[3] or 0),
                        )
                        for row in conn.execute(
                            "SELECT user_id,COALESCE(score,1000),COALESCE(rank,''),"
                            "COALESCE(win_streak,0) FROM arena ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not users else "running"
                    conn.execute(
                        "INSERT INTO arena_weekly_rank_reduction_operations("
                        "business_week,reduce_steps,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s)",
                        (
                            business_week,
                            reduce_steps,
                            len(users),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    targets = []
                    for ordinal, (user_id, score, rank, streak) in enumerate(users):
                        target_rank, target_score = self._target(
                            score, rank, reduce_steps
                        )
                        targets.append(
                            (
                                business_week,
                                user_id,
                                ordinal,
                                score,
                                rank,
                                streak,
                                target_score,
                                target_rank,
                                updated_at,
                            )
                        )
                    conn.executemany(
                        "INSERT INTO arena_weekly_rank_reduction_targets("
                        "business_week,user_id,ordinal,previous_score,previous_rank,"
                        "previous_win_streak,target_score,target_rank,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        targets,
                    )
                    conn.commit()
                    operation_created = True
                    if not users:
                        return self._result(conn, business_week, "applied")
                else:
                    if int(operation[0]) != reduce_steps:
                        result = self._result(
                            conn, business_week, "operation_conflict"
                        )
                        conn.rollback()
                        return result
                    if str(operation[1]) == "completed":
                        result = self._result(conn, business_week, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id,previous_score,previous_rank,previous_win_streak,"
                    "target_score,target_rank FROM arena_weekly_rank_reduction_targets "
                    "WHERE business_week=%s AND status='pending' ORDER BY ordinal LIMIT %s",
                    (business_week, chunk_size),
                ).fetchall()
                changed = 0
                skipped = 0
                conflicted = 0
                for row in pending:
                    user_id = str(row[0])
                    previous = (int(row[1]), str(row[2]), int(row[3]))
                    target = (int(row[4]), str(row[5]), 0)
                    current = conn.execute(
                        "SELECT COALESCE(score,1000),COALESCE(rank,''),"
                        "COALESCE(win_streak,0) FROM arena WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    if current is None:
                        skipped += 1
                        conn.execute(
                            "UPDATE arena_weekly_rank_reduction_targets SET status='skipped',"
                            "error_text='user_missing',updated_at=%s WHERE business_week=%s "
                            "AND user_id=%s AND status='pending'",
                            (updated_at, business_week, user_id),
                        )
                        continue
                    actual = (int(current[0]), str(current[1]), int(current[2]))
                    if actual != previous:
                        skipped += 1
                        conflicted += 1
                        conn.execute(
                            "UPDATE arena_weekly_rank_reduction_targets SET status='conflict',"
                            "error_text='state_changed',updated_at=%s WHERE business_week=%s "
                            "AND user_id=%s AND status='pending'",
                            (updated_at, business_week, user_id),
                        )
                        continue
                    if actual != target:
                        updated = conn.execute(
                            "UPDATE arena SET score=%s,rank=%s,win_streak=0 WHERE user_id=%s "
                            "AND COALESCE(score,1000)=%s AND COALESCE(rank,'')=%s "
                            "AND COALESCE(win_streak,0)=%s",
                            (
                                target[0],
                                target[1],
                                user_id,
                                previous[0],
                                previous[1],
                                previous[2],
                            ),
                        )
                        if updated.rowcount != 1:
                            raise db_backend.IntegrityError(
                                "arena weekly rank target changed"
                            )
                        changed += 1
                    conn.execute(
                        "UPDATE arena_weekly_rank_reduction_targets SET status='applied',"
                        "error_text='',updated_at=%s WHERE business_week=%s AND user_id=%s "
                        "AND status='pending'",
                        (updated_at, business_week, user_id),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM arena_weekly_rank_reduction_targets WHERE business_week=%s",
                    (business_week,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE arena_weekly_rank_reduction_operations SET completed=%s,"
                    "changed=changed+%s,skipped=skipped+%s,conflicted=conflicted+%s,"
                    "status=%s,last_error='',updated_at=%s WHERE business_week=%s",
                    (
                        completed,
                        changed,
                        skipped,
                        conflicted,
                        task_status,
                        updated_at,
                        business_week,
                    ),
                )
                result = self._result(conn, business_week, "applied")
                conn.commit()
                return result
            except Exception as exc:
                conn.rollback()
                if operation_created or self._operation_exists(conn, business_week):
                    try:
                        conn.execute("BEGIN IMMEDIATE")
                        conn.execute(
                            "UPDATE arena_weekly_rank_reduction_operations SET "
                            "last_error=%s,updated_at=%s WHERE business_week=%s",
                            (str(exc), updated_at, business_week),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                raise

    @staticmethod
    def _operation_exists(conn, business_week: str) -> bool:
        try:
            return conn.execute(
                "SELECT 1 FROM arena_weekly_rank_reduction_operations "
                "WHERE business_week=%s",
                (business_week,),
            ).fetchone() is not None
        except Exception:
            return False

@dataclass(frozen=True)
class ArenaSeasonRewardResult:
    status: str
    honor: int
    honor_points: int
    total_honor_earned: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ArenaSeasonRewardService:
    """Grant one frozen arena ranking reward once per season."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def claim(
        self, operation_id, user_id, season_key, expected_score, expected_rank,
        expected_position, expected_honor, expected_total_honor, base_honor,
        ranking_bonus, items=(), max_goods_num=999999999, *, expected_reset=None,
    ) -> ArenaSeasonRewardResult:
        operation_id, user_id, season_key = str(operation_id).strip(), str(user_id), str(season_key).strip()
        expected_score, expected_position, expected_honor, expected_total_honor = map(
            int, (expected_score, expected_position, expected_honor, expected_total_honor)
        )
        base_honor, ranking_bonus, max_goods_num = map(int, (base_honor, ranking_bonus, max_goods_num))
        expected_rank = str(expected_rank)
        reset = None if expected_reset is None else {
            "daily_challenges_used": int(expected_reset["daily_challenges_used"]),
            "daily_extra_challenges": int(expected_reset["daily_extra_challenges"]),
            "daily_challenge_buys": int(expected_reset["daily_challenge_buys"]),
            "last_reset_date": str(expected_reset["last_reset_date"] or ""),
            "last_buy_date": str(expected_reset["last_buy_date"] or ""),
        }
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]), 1 if int(item.get("bind", 1)) == 1 else 0)
            for item in items if int(item.get("amount", 0)) > 0
        )
        if not operation_id or not season_key or min(
            expected_score, expected_honor, expected_total_honor,
            base_honor, ranking_bonus, max_goods_num,
        ) < 0:
            raise ValueError("valid operation, season and reward snapshot are required")
        payload = json.dumps([
            user_id, season_key, expected_score, expected_rank, expected_position,
            expected_honor, expected_total_honor, base_honor, ranking_bonus, rewards, max_goods_num, reset,
        ], ensure_ascii=True, sort_keys=True)
        total_reward = base_honor + ranking_bonus

        def result(status, honor=0, balance=expected_honor, earned=expected_total_honor):
            return ArenaSeasonRewardResult(status, int(honor), int(balance), int(earned))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_season_reward_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,season_key TEXT NOT NULL,user_id TEXT NOT NULL,"
                    "honor INTEGER NOT NULL,honor_points INTEGER NOT NULL,total_honor_earned INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,UNIQUE(season_key,user_id))"
                )
                previous = conn.execute(
                    "SELECT payload,honor,honor_points,total_honor_earned FROM arena_season_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("state_changed") if str(previous[0]) != payload else result("duplicate", *previous[1:])
                claimed = conn.execute(
                    "SELECT honor,honor_points,total_honor_earned FROM arena_season_reward_operations WHERE season_key=%s AND user_id=%s",
                    (season_key, user_id),
                ).fetchone()
                if claimed is not None:
                    conn.rollback(); return result("already_claimed", *claimed)
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback(); return result("user_missing")
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()}
                required = {"score", "rank", "honor_points", "total_honor_earned"}
                if reset is not None:
                    required.update(reset)
                if not required.issubset(columns):
                    conn.rollback(); return result("state_changed")
                arena = conn.execute(
                    "SELECT COALESCE(score,0),COALESCE(rank,''),COALESCE(honor_points,0),COALESCE(total_honor_earned,0) "
                    "FROM player_data.arena WHERE user_id=%s", (user_id,),
                ).fetchone()
                if arena is None or (int(arena[0]), str(arena[1]), int(arena[2]), int(arena[3])) != (
                    expected_score, expected_rank, expected_honor, expected_total_honor
                ):
                    conn.rollback(); return result("state_changed")
                ranking = conn.execute(
                    "SELECT COUNT(*)+1 FROM player_data.arena WHERE COALESCE(score,0)>%s", (expected_score,)
                ).fetchone()
                if ranking is None or (expected_position > 0 and int(ranking[0]) != expected_position):
                    conn.rollback(); return result("state_changed")
                if reset is not None:
                    reset_row = conn.execute(
                        "SELECT COALESCE(daily_challenges_used,0),COALESCE(daily_extra_challenges,0),"
                        "COALESCE(daily_challenge_buys,0),COALESCE(last_reset_date,''),COALESCE(last_buy_date,'') "
                        "FROM player_data.arena WHERE user_id=%s", (user_id,),
                    ).fetchone()
                    if reset_row is None or (
                        int(reset_row[0]), int(reset_row[1]), int(reset_row[2]), str(reset_row[3]), str(reset_row[4])
                    ) != tuple(reset[key] for key in (
                        "daily_challenges_used", "daily_extra_challenges", "daily_challenge_buys",
                        "last_reset_date", "last_buy_date",
                    )):
                        conn.rollback(); return result("state_changed")
                for item_id, _, _, amount, _ in rewards:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + amount > max_goods_num:
                        conn.rollback(); return result("inventory_full")
                honor_points, total_honor = expected_honor + total_reward, expected_total_honor + total_reward
                if conn.execute(
                    "UPDATE player_data.arena SET honor_points=%s,total_honor_earned=%s WHERE user_id=%s "
                    "AND COALESCE(score,0)=%s AND COALESCE(rank,'')=%s AND COALESCE(honor_points,0)=%s "
                    "AND COALESCE(total_honor_earned,0)=%s",
                    (honor_points, total_honor, user_id, expected_score, expected_rank, expected_honor, expected_total_honor),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                if reset is not None:
                    conn.execute(
                        "UPDATE player_data.arena SET daily_challenges_used=0,daily_extra_challenges=0,"
                        "daily_challenge_buys=0,last_reset_date=%s,last_buy_date=%s WHERE user_id=%s",
                        (season_key, season_key, user_id),
                    )
                for item_id, name, item_type, amount, bind in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=CURRENT_TIMESTAMP",
                        (user_id, item_id, name, item_type, amount, amount if bind else 0),
                    )
                conn.execute(
                    "INSERT INTO arena_season_reward_operations "
                    "(operation_id,payload,season_key,user_id,honor,honor_points,total_honor_earned) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, season_key, user_id, total_reward, honor_points, total_honor),
                )
                conn.commit()
                return result("applied", total_reward, honor_points, total_honor)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "ArenaStateService",
    "ArenaPurchaseResult",
    "ArenaPurchaseService",
    "ArenaChallengePurchaseResult",
    "ArenaChallengePurchaseService",
    "ArenaChallengeTicketResult",
    "ArenaChallengeTicketService",
    "ArenaChallengeSettlementResult",
    "ArenaChallengeSettlementService",
    "ArenaBattleSettlementResult",
    "ArenaBattleSettlementService",
    "ArenaWeeklyRankReductionResult",
    "ArenaWeeklyRankReductionService",
    "ArenaSeasonRewardResult",
    "ArenaSeasonRewardService",
    "ARENA_FIELDS",
    "normalize_weekly_purchases",
    "normalize_daily_purchase_state",
]
