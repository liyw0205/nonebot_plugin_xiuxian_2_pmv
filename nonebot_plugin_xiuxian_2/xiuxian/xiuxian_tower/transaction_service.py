from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from datetime import date, datetime
from ..xiuxian_utils import db_backend
from datetime import datetime

TOWER_FIELDS = ("current_floor", "max_floor", "score", "weekly_purchases")

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
    """Return a canonical tower-shop snapshot for today's ISO week."""
    return _normalize_weekly_purchases(value, today)[0]

class TowerStateService:
    """Initialize and advance a player's tower state in one transaction."""

    _COLUMN_DEFINITIONS = {
        "current_floor": "TEXT DEFAULT '0'",
        "max_floor": "TEXT DEFAULT '0'",
        "score": "TEXT DEFAULT '0'",
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

    @classmethod
    def _state(cls, row, weekly_purchases) -> dict:
        return {
            "current_floor": cls._integer(row[0]),
            "max_floor": cls._integer(row[1]),
            "score": cls._integer(row[2]),
            "weekly_purchases": weekly_purchases,
        }

    @staticmethod
    def _snapshot(state: dict) -> str:
        return json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tower ("
            "user_id TEXT PRIMARY KEY,current_floor TEXT DEFAULT '0',max_floor TEXT DEFAULT '0',"
            "score TEXT DEFAULT '0',weekly_purchases TEXT DEFAULT NULL)"
        )
        columns = {str(column[1]) for column in conn.execute("PRAGMA table_info(tower)").fetchall()}
        for field, definition in cls._COLUMN_DEFINITIONS.items():
            if field not in columns:
                conn.execute(f'ALTER TABLE tower ADD COLUMN "{field}" {definition}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tower_state_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _insert_operation(conn, operation_id, user_id, kind, period_key, state) -> None:
        conn.execute(
            "INSERT INTO tower_state_operations("
            "operation_id,user_id,kind,period_key,snapshot,created_at) "
            "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (operation_id, user_id, kind, period_key, TowerStateService._snapshot(state)),
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
                    "SELECT current_floor,max_floor,score,weekly_purchases "
                    "FROM tower WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    weekly = {"_last_reset": today.isoformat()}
                    state = self._state((0, 0, 0, weekly), weekly)
                    conn.execute(
                        "INSERT INTO tower("
                        "user_id,current_floor,max_floor,score,weekly_purchases) "
                        "VALUES(%s,%s,%s,%s,%s)",
                        (
                            user_id,
                            state["current_floor"],
                            state["max_floor"],
                            state["score"],
                            json.dumps(weekly, ensure_ascii=True, sort_keys=True),
                        ),
                    )
                    self._insert_operation(
                        conn,
                        f"tower-state-init:{user_id}",
                        user_id,
                        "initialize",
                        period_key,
                        state,
                    )
                    conn.commit()
                    return state

                weekly, weekly_changed = _normalize_weekly_purchases(row[3], today)
                state = self._state(row, weekly)
                if weekly_changed:
                    updated = conn.execute(
                        "UPDATE tower SET weekly_purchases=%s WHERE user_id=%s",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError("tower state changed")
                    self._insert_operation(
                        conn,
                        f"tower-state-week:{user_id}:{period_key}",
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
class TowerPurchaseResult:
    status: str
    quantity: int
    cost: int
    score: int
    purchased: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TowerPurchaseService:
    """Exchange tower score for inventory items in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, item_id, quantity, unit_cost, weekly_limit, max_goods_num, bind_flag) -> str:
        # Request identity only — score/weekly snapshots are concurrency checks.
        return json.dumps(
            [str(user_id), int(item_id), int(quantity), int(unit_cost), int(weekly_limit), int(max_goods_num), int(bind_flag)],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    def purchase(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        unit_cost,
        weekly_limit,
        expected_score,
        expected_weekly_purchases,
        max_goods_num,
        bind_flag=1,
        today=None,
    ) -> TowerPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        unit_cost = int(unit_cost)
        weekly_limit = int(weekly_limit)
        expected_score = int(expected_score)
        max_goods_num = int(max_goods_num)
        bind_flag = 1 if int(bind_flag) == 1 else 0
        today = today or date.today()
        weekly = normalize_weekly_purchases(expected_weekly_purchases, today)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_score, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        payload = self._payload(user_id, item_id, quantity, unit_cost, weekly_limit, max_goods_num, bind_flag)

        def result(status: str, score=expected_score, purchased=0, inventory=0) -> TowerPurchaseResult:
            return TowerPurchaseResult(
                status,
                quantity if status in {"applied", "duplicate"} else 0,
                quantity * unit_cost if status in {"applied", "duplicate"} else 0,
                int(score),
                int(purchased),
                int(inventory),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS tower_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, score INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, score, purchased, inventory FROM tower_purchase_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return TowerPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                user = conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s",
                    ("tower",),
                ).fetchone()
                columns = (
                    {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(tower)").fetchall()}
                    if table is not None
                    else set()
                )
                if not {"score", "weekly_purchases"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                tower = conn.execute(
                    "SELECT COALESCE(score, 0), COALESCE(weekly_purchases, '{}') FROM player_data.tower WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if tower is None:
                    conn.rollback()
                    return result("state_changed")
                current_weekly = normalize_weekly_purchases(tower[1], today)
                if int(tower[0]) != expected_score or current_weekly != weekly:
                    conn.rollback()
                    return result("state_changed")

                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return result("limit_reached", purchased=purchased)
                cost = quantity * unit_cost
                if expected_score < cost:
                    conn.rollback()
                    return result("score_insufficient", purchased=purchased)
                inventory_row = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory = int(inventory_row[0]) if inventory_row else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", purchased=purchased, inventory=inventory)

                new_score = expected_score - cost
                new_purchased = purchased + quantity
                new_inventory = inventory + quantity
                weekly[str(item_id)] = new_purchased
                if (
                    conn.execute(
                        "UPDATE player_data.tower SET score=%s, weekly_purchases=%s WHERE user_id=%s AND COALESCE(score, 0)=%s",
                        (new_score, json.dumps(weekly, ensure_ascii=False), user_id, expected_score),
                    ).rowcount
                    != 1
                ):
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
                    "INSERT INTO tower_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, new_score, new_purchased, new_inventory),
                )
                conn.commit()
                return TowerPurchaseResult("applied", quantity, cost, new_score, new_purchased, new_inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class TowerSettlementResult:
    status: str
    score: int
    stone: int
    exp: int
    floor: int = 0
    challenge_succeeded: bool = True
    rewards: tuple = ()
    stamina_cost: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TowerSettlementService:
    """Commit one tower-floor result and all rewards across both databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, floor, stamina_cost) -> str:
        # Request identity only — tower/player snapshots and rewards are outcomes/concurrency checks.
        return json.dumps([str(user_id), int(floor), int(stamina_cost)], ensure_ascii=True, separators=(",", ":"))

    def _ensure_ops_table(self, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tower_settlement_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
            "result_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(tower_settlement_operations)").fetchall()}
        if "result_json" not in cols:
            try:
                conn.execute("ALTER TABLE tower_settlement_operations ADD COLUMN result_json TEXT")
            except Exception:
                pass

    def _result_from_row(self, status: str, payload: str, result_json: str | None) -> TowerSettlementResult:
        floor = 0
        stamina_cost = 0
        try:
            body = json.loads(payload or "[]")
            if isinstance(body, list) and len(body) >= 3:
                floor = int(body[1] or 0)
                stamina_cost = int(body[2] or 0)
        except Exception:
            pass
        score = stone = exp = 0
        challenge_succeeded = True
        rewards: tuple = ()
        if result_json:
            try:
                data = json.loads(result_json)
                score = int(data.get("score") or 0)
                stone = int(data.get("stone") or 0)
                exp = int(data.get("exp") or 0)
                challenge_succeeded = bool(data.get("challenge_succeeded", True))
                rewards = tuple(tuple(x) for x in (data.get("rewards") or []))
                floor = int(data.get("floor") or floor)
                stamina_cost = int(data.get("stamina_cost") or stamina_cost)
            except Exception:
                pass
        return TowerSettlementResult(
            status,
            score,
            stone,
            exp,
            floor=floor,
            challenge_succeeded=challenge_succeeded,
            rewards=rewards,
            stamina_cost=stamina_cost,
        )

    def get_result(self, operation_id: str) -> TowerSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_ops_table(conn)
            row = conn.execute(
                "SELECT payload, result_json FROM tower_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_row("duplicate", str(row[0] or ""), row[1])

    def settle(
        self,
        operation_id,
        user_id,
        expected_tower,
        floor,
        score,
        stone,
        exp,
        items,
        max_goods_num,
        *,
        expected_player=None,
        final_hp=None,
        final_mp=None,
        stamina_cost=0,
        challenge_succeeded=True,
    ) -> TowerSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = {key: int(dict(expected_tower)[key]) for key in ("current_floor", "max_floor", "score")}
        floor, score, stone, exp, max_goods_num, stamina_cost = map(
            int, (floor, score, stone, exp, max_goods_num, stamina_cost)
        )
        player = None if expected_player is None else {key: int(dict(expected_player)[key]) for key in ("hp", "mp", "user_stamina")}
        final_hp = None if final_hp is None else max(1, int(final_hp))
        final_mp = None if final_mp is None else max(1, int(final_mp))
        challenge_succeeded = bool(challenge_succeeded)
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items
            if int(item.get("amount", 0)) > 0
        )
        if not operation_id or floor <= 0 or min(score, stone, exp, max_goods_num, *expected.values()) < 0:
            raise ValueError("valid operation, tower state and rewards are required")
        payload = self._payload(user_id, floor, stamina_cost)
        result_json = json.dumps(
            {
                "score": score,
                "stone": stone,
                "exp": exp,
                "floor": floor,
                "stamina_cost": stamina_cost,
                "challenge_succeeded": challenge_succeeded,
                "rewards": [list(r) for r in rewards],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )

        def result(status: str) -> TowerSettlementResult:
            ok = status in {"applied", "duplicate"}
            return TowerSettlementResult(
                status,
                score if ok else 0,
                stone if ok else 0,
                exp if ok else 0,
                floor=floor if ok else 0,
                challenge_succeeded=challenge_succeeded if ok else True,
                rewards=rewards if ok else (),
                stamina_cost=stamina_cost if ok else 0,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_ops_table(conn)
                previous = conn.execute(
                    "SELECT payload, result_json FROM tower_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return self._result_from_row("duplicate", str(previous[0] or ""), previous[1])
                user = conn.execute(
                    "SELECT hp, mp, user_stamina FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if player is not None and tuple(map(int, user)) != (player["hp"], player["mp"], player["user_stamina"]):
                    conn.rollback()
                    return result("state_changed")
                if player is not None and player["user_stamina"] < stamina_cost:
                    conn.rollback()
                    return result("stamina_insufficient")
                columns = {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(tower)").fetchall()}
                if not {"current_floor", "max_floor", "score"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                tower = conn.execute(
                    "SELECT current_floor, max_floor, score FROM player_data.tower WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if tower is None or tuple(map(int, tower)) != (
                    expected["current_floor"],
                    expected["max_floor"],
                    expected["score"],
                ):
                    conn.rollback()
                    return result("state_changed")
                for item_id, _, _, amount in rewards:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + amount > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")
                if challenge_succeeded:
                    new_max_floor = max(expected["max_floor"], floor)
                    if (
                        conn.execute(
                            "UPDATE player_data.tower SET current_floor=%s, max_floor=%s, score=%s WHERE user_id=%s",
                            (floor, new_max_floor, expected["score"] + score, user_id),
                        ).rowcount
                        != 1
                    ):
                        conn.rollback()
                        return result("state_changed")
                if player is None:
                    conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL), exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                        (stone, exp, user_id),
                    )
                else:
                    conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL), exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL), hp=%s, mp=%s, user_stamina=user_stamina-%s WHERE user_id=%s",
                        (stone, exp, final_hp, final_mp, stamina_cost, user_id),
                    )
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, "
                        "update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute(
                    "INSERT INTO tower_settlement_operations(operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "TowerStateService",
    "TowerPurchaseResult",
    "TowerPurchaseService",
    "TowerSettlementResult",
    "TowerSettlementService",
    "normalize_weekly_purchases",
]
