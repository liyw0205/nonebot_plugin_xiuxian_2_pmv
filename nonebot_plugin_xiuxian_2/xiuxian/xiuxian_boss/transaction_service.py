from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from datetime import date, datetime
from ..xiuxian_utils import db_backend
from datetime import datetime
from typing import Any
from typing import Any, Callable
from typing import Any, Callable, Iterable

def normalize_weekly_purchases(value, today=None):
    today = today or date.today()
    weekly = {str(key): item for key, item in dict(value or {}).items()}
    try:
        reset = date.fromisoformat(str(weekly.get("_last_reset", "")))
    except ValueError:
        reset = None
    if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
        return {"_last_reset": today.isoformat()}
    return weekly

@dataclass(frozen=True)
class BossPurchaseResult:
    status: str
    quantity: int
    cost: int
    integral: int
    purchased: int
    inventory: int

class BossPurchaseService:
    """Exchange world-boss integral for an item in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

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
        expected_integral,
        expected_weekly_purchases,
        max_goods_num,
        today=None,
    ) -> BossPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        unit_cost = int(unit_cost)
        weekly_limit = int(weekly_limit)
        expected_integral = int(expected_integral)
        max_goods_num = int(max_goods_num)
        today = today or date.today()
        weekly = normalize_weekly_purchases(expected_weekly_purchases, today)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_integral, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        # Request identity only — integral/weekly snapshots are concurrency checks.
        payload = json.dumps(
            [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        def rejected(status: str, purchased=0, inventory=0) -> BossPurchaseResult:
            return BossPurchaseResult(status, 0, 0, expected_integral, int(purchased), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS boss_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, integral INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, integral, purchased, inventory FROM boss_purchase_operations "
                    "WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return BossPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return rejected("user_missing")
                required = (("boss", {"weekly_purchases"}), ("boss_limit", {"integral"}))
                for table, fields in required:
                    exists = conn.execute(
                        "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", (table,)
                    ).fetchone()
                    columns = (
                        {str(column[1]) for column in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}
                        if exists is not None else set()
                    )
                    if not fields.issubset(columns):
                        conn.rollback()
                        return rejected("state_changed")
                integral_row = conn.execute(
                    "SELECT COALESCE(integral, 0) FROM player_data.boss_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                weekly_row = conn.execute(
                    "SELECT COALESCE(weekly_purchases, '{}') FROM player_data.boss WHERE user_id=%s", (user_id,)
                ).fetchone()
                if integral_row is None or weekly_row is None:
                    conn.rollback()
                    return rejected("state_changed")
                try:
                    current_weekly = json.loads(str(weekly_row[0])) if weekly_row[0] else {}
                except (TypeError, ValueError):
                    conn.rollback()
                    return rejected("state_changed")
                current_weekly = normalize_weekly_purchases(current_weekly, today)
                if int(integral_row[0]) != expected_integral or current_weekly != weekly:
                    conn.rollback()
                    return rejected("state_changed")

                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return rejected("limit_reached", purchased)
                cost = quantity * unit_cost
                if expected_integral < cost:
                    conn.rollback()
                    return rejected("integral_insufficient", purchased)
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                inventory = int(item[0]) if item else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return rejected("inventory_full", purchased, inventory)

                new_integral = expected_integral - cost
                new_purchased = purchased + quantity
                new_inventory = inventory + quantity
                weekly[str(item_id)] = new_purchased
                if conn.execute(
                    "UPDATE player_data.boss_limit SET integral=%s WHERE user_id=%s AND COALESCE(integral, 0)=%s",
                    (new_integral, user_id, expected_integral),
                ).rowcount != 1:
                    conn.rollback()
                    return rejected("state_changed")
                conn.execute(
                    "UPDATE player_data.boss SET weekly_purchases=%s WHERE user_id=%s",
                    (json.dumps(weekly, ensure_ascii=False), user_id),
                )
                now = datetime.now()
                conn.execute(
                    "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                    "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.goods_num",
                    (user_id, item_id, item_name, item_type, quantity, now, now, quantity),
                )
                conn.execute(
                    "INSERT INTO boss_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, new_integral, new_purchased, new_inventory),
                )
                conn.commit()
                return BossPurchaseResult("applied", quantity, cost, new_integral, new_purchased, new_inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class WorldBossBattleSettlementResult:
    status: str
    boss_hp: int
    stamina: int
    battle_count: int
    stone: int
    exp: int
    integral: int
    activity_lines: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorldBossBattleSettlementService:
    """Persist one fixed world-boss battle result as a single transaction."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        activity_database: str | Path | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._activity_database = Path(activity_database) if activity_database else None
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _loads(value: Any, default: Any) -> Any:
        if isinstance(value, type(default)):
            return value
        try:
            parsed = json.loads(value or "")
        except (TypeError, ValueError):
            return default
        return parsed if isinstance(parsed, type(default)) else default

    def get_result(self, operation_id: str) -> WorldBossBattleSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS world_boss_battle_operations(operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,boss_hp INTEGER NOT NULL,stamina INTEGER NOT NULL,battle_count INTEGER NOT NULL,"
                "stone INTEGER NOT NULL,exp INTEGER NOT NULL,integral INTEGER NOT NULL,activity_lines TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT boss_hp,stamina,battle_count,stone,exp,integral,activity_lines "
                "FROM world_boss_battle_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return WorldBossBattleSettlementResult(
                "duplicate",
                int(previous[0]),
                int(previous[1]),
                int(previous[2]),
                int(previous[3]),
                int(previous[4]),
                int(previous[5]),
                tuple(self._loads(previous[6], [])),
            )

    @staticmethod
    def _ensure_column(conn, schema: str, table: str, field: str, data_type: str) -> None:
        columns = {str(row[1]) for row in conn.execute(f"PRAGMA {schema}.table_info({table})").fetchall()}
        if field not in columns:
            conn.execute(
                f"ALTER TABLE {schema}.{db_backend.quote_ident(table)} "
                f"ADD COLUMN {db_backend.quote_ident(field)} {data_type} DEFAULT NULL"
            )

    @classmethod
    def _increment_stat(cls, conn, user_id: str, field: str, amount: int = 1) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics (user_id TEXT PRIMARY KEY)")
        cls._ensure_column(conn, "player_data", "statistics", field, "INTEGER")
        field_sql = db_backend.quote_ident(field)
        conn.execute(
            f"INSERT INTO player_data.statistics(user_id,{field_sql}) VALUES (%s,%s) "
            f"ON CONFLICT(user_id) DO UPDATE SET {field_sql}=COALESCE({field_sql},0)+excluded.{field_sql}",
            (user_id, int(amount)),
        )

    @classmethod
    def _record_tasks(cls, conn, user_id: str, daily_period: str, weekly_period: str) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.xiuxian_tasks (user_id TEXT PRIMARY KEY)")
        for field in ("daily_period", "daily_progress", "daily_claimed", "weekly_period", "weekly_progress", "weekly_claimed"):
            cls._ensure_column(conn, "player_data", "xiuxian_tasks", field, "TEXT")
        row = conn.execute(
            "SELECT daily_period,daily_progress,daily_claimed,weekly_period,weekly_progress,weekly_claimed "
            "FROM player_data.xiuxian_tasks WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        values = list(row) if row else ["", "{}", "[]", "", "{}", "[]"]
        for prefix, period, target in (("daily", daily_period, 1), ("weekly", weekly_period, 150)):
            offset = 0 if prefix == "daily" else 3
            progress = cls._loads(values[offset + 1], {}) if str(values[offset] or "") == period else {}
            claimed = cls._loads(values[offset + 2], []) if str(values[offset] or "") == period else []
            progress["daily_boss" if prefix == "daily" else "weekly_boss"] = min(
                target,
                int(progress.get("daily_boss" if prefix == "daily" else "weekly_boss", 0) or 0) + 1,
            )
            values[offset:offset + 3] = [period, cls._json(progress), cls._json(claimed)]
        conn.execute(
            "INSERT INTO player_data.xiuxian_tasks(user_id,daily_period,daily_progress,daily_claimed,"
            "weekly_period,weekly_progress,weekly_claimed) VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(user_id) DO UPDATE SET daily_period=excluded.daily_period,"
            "daily_progress=excluded.daily_progress,daily_claimed=excluded.daily_claimed,"
            "weekly_period=excluded.weekly_period,weekly_progress=excluded.weekly_progress,"
            "weekly_claimed=excluded.weekly_claimed",
            (user_id, *values),
        )

    @classmethod
    def _apply_activity_damage(cls, conn, user_id: str, raw_damage: int, activities: list[dict]) -> list[str]:
        if not activities:
            return []
        # SQLite INTEGER is signed 64-bit; activity max_hp derived from eternal-realm
        # totals can overflow and abort the whole world-boss settlement.
        sqlite_max = 2**63 - 1

        def _safe_int(value, default=0) -> int:
            try:
                number = int(value)
            except (TypeError, ValueError):
                number = int(default)
            if number < 0:
                return 0
            return min(number, sqlite_max)

        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity.activity_boss_state(activity_key TEXT PRIMARY KEY,"
            "hp_left INTEGER NOT NULL,max_hp INTEGER NOT NULL,update_time TEXT DEFAULT '')"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity.activity_boss_damage(activity_key TEXT NOT NULL,user_id TEXT NOT NULL,"
            "total_damage INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity.activity_boss_fight_log(id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "activity_key TEXT NOT NULL,user_id TEXT NOT NULL,damage INTEGER NOT NULL DEFAULT 0,"
            "fight_date TEXT DEFAULT '',source TEXT DEFAULT '',create_time TEXT DEFAULT '')"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity.activity_boss_milestone(activity_key TEXT NOT NULL,"
            "milestone_key TEXT NOT NULL,unlocked_time TEXT DEFAULT '',PRIMARY KEY(activity_key,milestone_key))"
        )
        now = datetime.now()
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")
        today = now.strftime("%Y-%m-%d")
        lines: list[str] = []
        for activity in activities:
            key = str(activity["key"])
            limit = max(1, int(activity.get("daily_fight_limit", 3)))
            used = conn.execute(
                "SELECT COUNT(*) FROM activity.activity_boss_fight_log WHERE activity_key=%s AND user_id=%s "
                "AND fight_date=%s AND source IN ('coop','world_boss')",
                (key, user_id, today),
            ).fetchone()[0]
            if int(used) >= limit:
                continue
            max_hp = max(1, _safe_int(activity.get("max_hp"), 1))
            row = conn.execute(
                "SELECT hp_left,max_hp FROM activity.activity_boss_state WHERE activity_key=%s", (key,)
            ).fetchone()
            hp_left = max_hp if row is None else max(0, _safe_int(row[0], 0))
            if row is None:
                conn.execute(
                    "INSERT INTO activity.activity_boss_state(activity_key,hp_left,max_hp,update_time) VALUES (%s,%s,%s,%s)",
                    (key, max_hp, max_hp, now_text),
                )
            multiplier = max(0.0, float(activity.get("multiplier", 1.0)))
            cap = max(1, _safe_int(max_hp * float(activity.get("hit_hp_cap_ratio", 0.01)), 1))
            scaled = max(1, _safe_int(raw_damage * multiplier, 1))
            damage = min(scaled, cap, hp_left, sqlite_max)
            if damage <= 0:
                continue
            new_hp = max(0, hp_left - damage)
            conn.execute(
                "UPDATE activity.activity_boss_state SET hp_left=%s,max_hp=%s,update_time=%s WHERE activity_key=%s",
                (new_hp, max_hp, now_text, key),
            )
            conn.execute(
                "INSERT INTO activity.activity_boss_damage(activity_key,user_id,total_damage,update_time) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT(activity_key,user_id) DO UPDATE SET "
                "total_damage=MIN(%s, COALESCE(activity_boss_damage.total_damage,0)+excluded.total_damage),"
                "update_time=excluded.update_time",
                (key, user_id, damage, now_text, sqlite_max),
            )
            conn.execute(
                "INSERT INTO activity.activity_boss_fight_log(activity_key,user_id,damage,fight_date,source,create_time) "
                "VALUES (%s,%s,%s,%s,'world_boss',%s)",
                (key, user_id, damage, today, now_text),
            )
            percent_left = 100.0 * new_hp / max_hp
            for milestone in activity.get("server_milestones", []):
                threshold = float(milestone.get("hp_percent", 0))
                if percent_left <= threshold:
                    conn.execute(
                        "INSERT OR IGNORE INTO activity.activity_boss_milestone(activity_key,milestone_key,unlocked_time) "
                        "VALUES (%s,%s,%s)",
                        (key, str(milestone.get("key") or f"p{threshold}"), now_text),
                    )
            lines.append(
                f"活动首领·{activity.get('boss_name', '活动首领')} 计入伤害 {damage}，剩余 {new_hp}/{max_hp}"
            )
        return lines

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_bosses: list[dict],
        settled_bosses: list[dict],
        boss_index: int,
        expected_stamina: int,
        stamina_cost: int,
        expected_hp: int,
        expected_mp: int,
        final_hp: int,
        final_mp: int,
        expected_exp: int,
        exp_reward: int,
        expected_stone: int,
        stone_reward: int,
        expected_daily_stone: int,
        expected_daily_integral: int,
        expected_total_integral: int,
        integral_reward: int,
        expected_battle_count: int,
        battle_limit: int,
        expected_checked_at: str,
        checked_at: str,
        item: dict | None,
        max_goods_num: int,
        actual_damage: int,
        killed: bool,
        daily_period: str,
        weekly_period: str,
        activity_bosses: list[dict] | None = None,
    ) -> WorldBossBattleSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        boss_index = int(boss_index)
        item = dict(item or {})
        activity_bosses = list(activity_bosses or [])
        if not operation_id or not (0 <= boss_index < len(expected_bosses)):
            raise ValueError("valid operation and boss index are required")
        payload_data = {
            "user_id": user_id,
            "boss_index": boss_index,
            "stamina_cost": int(stamina_cost),
            "battle_limit": int(battle_limit),
            "max_goods_num": int(max_goods_num),
            "daily_period": str(daily_period),
            "weekly_period": str(weekly_period),
        }
        payload = self._json(payload_data)

        def result(status: str, boss_hp: int = 0, stamina: int | None = None, count: int | None = None,
                   stone: int | None = None, exp: int | None = None, integral: int | None = None,
                   lines: tuple[str, ...] = ()) -> WorldBossBattleSettlementResult:
            return WorldBossBattleSettlementResult(
                status, int(boss_hp), int(expected_stamina if stamina is None else stamina),
                int(expected_battle_count if count is None else count),
                int(expected_stone if stone is None else stone), int(expected_exp if exp is None else exp),
                int(expected_total_integral if integral is None else integral), lines,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached_player = attached_activity = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached_player = True
                if self._activity_database and activity_bosses:
                    self._activity_database.parent.mkdir(parents=True, exist_ok=True)
                    conn.execute("ATTACH DATABASE %s AS activity", (str(self._activity_database),))
                    attached_activity = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS world_boss_battle_operations(operation_id TEXT PRIMARY KEY,"
                    "payload TEXT NOT NULL,boss_hp INTEGER NOT NULL,stamina INTEGER NOT NULL,battle_count INTEGER NOT NULL,"
                    "stone INTEGER NOT NULL,exp INTEGER NOT NULL,integral INTEGER NOT NULL,activity_lines TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,boss_hp,stamina,battle_count,stone,exp,integral,activity_lines "
                    "FROM world_boss_battle_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return result("duplicate", *map(int, previous[1:7]), tuple(self._loads(previous[7], [])))

                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.world_boss_state(state_key TEXT PRIMARY KEY,"
                    "bosses TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                state = conn.execute(
                    "SELECT bosses FROM player_data.world_boss_state WHERE state_key='global'"
                ).fetchone()
                if state is None:
                    conn.execute(
                        "INSERT INTO player_data.world_boss_state(state_key,bosses,updated_at) VALUES ('global',%s,%s)",
                        (self._json(expected_bosses), str(checked_at)),
                    )
                elif self._loads(state[0], []) != expected_bosses:
                    conn.rollback()
                    return result("boss_changed")

                user = conn.execute(
                    "SELECT COALESCE(user_stamina,0),COALESCE(hp,0),COALESCE(mp,0),COALESCE(exp,0),COALESCE(stone,0) "
                    "FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cooldown = conn.execute(
                    "SELECT COALESCE(last_check_info_time,'') FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None or cooldown is None:
                    conn.rollback()
                    return result("user_missing")
                expected_user = (int(expected_stamina), int(expected_hp), int(expected_mp), int(expected_exp), int(expected_stone))
                if tuple(map(int, user)) != expected_user or str(cooldown[0]) != str(expected_checked_at or ""):
                    conn.rollback()
                    return result("state_changed")
                if expected_stamina < stamina_cost:
                    conn.rollback()
                    return result("stamina_insufficient")
                if expected_hp <= expected_exp / 10:
                    conn.rollback()
                    return result("hp_insufficient")

                conn.execute("CREATE TABLE IF NOT EXISTS player_data.boss(user_id TEXT PRIMARY KEY)")
                for field in ("boss_stone", "boss_integral", "boss_battle_count"):
                    self._ensure_column(conn, "player_data", "boss", field, "INTEGER")
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.boss_limit(user_id TEXT PRIMARY KEY)")
                self._ensure_column(conn, "player_data", "boss_limit", "integral", "INTEGER")
                daily = conn.execute(
                    "SELECT COALESCE(boss_stone,0),COALESCE(boss_integral,0),COALESCE(boss_battle_count,0) "
                    "FROM player_data.boss WHERE user_id=%s", (user_id,)
                ).fetchone()
                total = conn.execute(
                    "SELECT COALESCE(integral,0) FROM player_data.boss_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                actual_daily = tuple(map(int, daily)) if daily else (0, 0, 0)
                if actual_daily != (int(expected_daily_stone), int(expected_daily_integral), int(expected_battle_count)) \
                        or (int(total[0]) if total else 0) != int(expected_total_integral):
                    conn.rollback()
                    return result("state_changed")
                if expected_battle_count >= battle_limit:
                    conn.rollback()
                    return result("limit_reached")

                quantity = max(0, int(item.get("quantity", 0) or 0))
                item_id = int(item.get("id", 0) or 0)
                if quantity:
                    current_item = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(current_item[0]) if current_item else 0) + quantity > int(max_goods_num):
                        conn.rollback()
                        return result("inventory_full")

                stamina = int(expected_stamina) - int(stamina_cost)
                stone = int(expected_stone) + int(stone_reward)
                exp = int(expected_exp) + int(exp_reward)
                count = int(expected_battle_count) + 1
                integral = int(expected_total_integral) + int(integral_reward)
                conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s,hp=%s,mp=%s,exp=%s,stone=%s WHERE user_id=%s",
                    (stamina, max(1, int(final_hp)), max(1, int(final_mp)), exp, stone, user_id),
                )
                conn.execute("UPDATE user_cd SET last_check_info_time=%s WHERE user_id=%s", (str(checked_at), user_id))
                conn.execute(
                    "INSERT INTO player_data.boss(user_id,boss_stone,boss_integral,boss_battle_count) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET boss_stone=excluded.boss_stone,"
                    "boss_integral=excluded.boss_integral,boss_battle_count=excluded.boss_battle_count",
                    (user_id, int(expected_daily_stone) + int(stone_reward),
                     int(expected_daily_integral) + int(integral_reward), count),
                )
                conn.execute(
                    "INSERT INTO player_data.boss_limit(user_id,integral) VALUES (%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET integral=excluded.integral", (user_id, integral),
                )
                if quantity:
                    now = datetime.now()
                    bound = quantity if bool(item.get("bind")) else 0
                    conn.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                        "update_time=excluded.update_time",
                        (user_id, item_id, str(item.get("name", "")), str(item.get("type", "")), quantity, now, now, bound),
                    )
                conn.execute(
                    "UPDATE player_data.world_boss_state SET bosses=%s,updated_at=%s WHERE state_key='global'",
                    (self._json(settled_bosses), str(checked_at)),
                )
                self._increment_stat(conn, user_id, "讨伐世界BOSS")
                if killed:
                    self._increment_stat(conn, user_id, "击败世界BOSS")
                self._record_tasks(conn, user_id, str(daily_period), str(weekly_period))
                lines = self._apply_activity_damage(conn, user_id, int(actual_damage), activity_bosses) if attached_activity else []
                boss_hp = (
                    int(settled_bosses[boss_index].get("气血", 0))
                    if 0 <= boss_index < len(settled_bosses) else 0
                )
                conn.execute(
                    "INSERT INTO world_boss_battle_operations(operation_id,payload,boss_hp,stamina,battle_count,stone,exp,"
                    "integral,activity_lines) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, boss_hp, stamina, count, stone, exp, integral, self._json(lines)),
                )
                conn.commit()
                return result("applied", boss_hp, stamina, count, stone, exp, integral, tuple(lines))
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached_activity:
                    conn.execute("DETACH DATABASE activity")
                if attached_player:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class WorldBossManualSpawnResult:
    status: str
    bosses: tuple[dict[str, Any], ...] = ()
    boss: dict[str, Any] | None = None
    revision: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"spawned", "duplicate"}

class WorldBossManualSpawnService:
    """Replace one realm's world-boss session and record the operation atomically."""

    def __init__(
        self,
        player_database: str | Path,
        config_loader: Callable[[], dict[str, Any]],
        lock: RLock | None = None,
    ) -> None:
        self._player_database = Path(player_database)
        self._config_loader = config_loader
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def config_snapshot(cls, config: dict[str, Any], realm: str) -> dict[str, Any]:
        return {
            "realm": str(realm),
            "names": list(config.get("Boss名字", [])),
            "stones": list(config.get("Boss灵石", {}).get(realm, [])),
            "multipliers": dict(config.get("Boss倍率", {})),
        }

    @staticmethod
    def _valid_boss(boss: dict[str, Any], config: dict[str, Any]) -> bool:
        required = {"name", "jj", "气血", "总血量", "真元", "攻击", "max_stone", "stone"}
        if not required.issubset(boss) or boss["jj"] != config["realm"]:
            return False
        if boss["name"] not in config["names"]:
            return False
        if boss["max_stone"] not in config["stones"] or boss["stone"] != boss["max_stone"]:
            return False
        try:
            return all(int(boss[field]) >= 0 for field in ("气血", "总血量", "真元", "攻击", "stone"))
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_state ("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "revision INTEGER NOT NULL DEFAULT 0)"
        )
        columns = set(conn.column_names("world_boss_state"))
        if "revision" not in columns:
            conn.execute(
                "ALTER TABLE world_boss_state ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_manual_spawn_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _result_from_json(
        cls,
        value: Any,
        status: str,
    ) -> WorldBossManualSpawnResult:
        stored = json.loads(str(value))
        return WorldBossManualSpawnResult(
            status,
            tuple(dict(boss) for boss in stored["bosses"]),
            dict(stored["boss"]),
            int(stored.get("revision", 0)),
        )

    def snapshot(self) -> tuple[list[dict[str, Any]], int]:
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
            ).fetchone()
            if row is None:
                return [], 0
            try:
                bosses = json.loads(row[0])
            except (TypeError, ValueError):
                bosses = []
            if not isinstance(bosses, list):
                bosses = []
            return [dict(boss) for boss in bosses if isinstance(boss, dict)], int(
                row[1] or 0
            )

    def get_result(self, operation_id: str) -> WorldBossManualSpawnResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT result_json FROM world_boss_manual_spawn_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_json(row[0], "duplicate")

    def spawn(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        expected_bosses: list[dict[str, Any]],
        expected_config: dict[str, Any],
        boss: dict[str, Any],
    ) -> WorldBossManualSpawnResult:
        operation_id = str(operation_id).strip()
        expected_revision = int(expected_revision)
        expected_bosses = list(expected_bosses)
        expected_config = dict(expected_config)
        boss = dict(boss)
        if not operation_id:
            raise ValueError("operation_id is required")

        payload = self._json({
            "expected_revision": expected_revision,
            "expected_bosses": expected_bosses,
            "expected_config": expected_config,
            "boss": boss,
        })

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                self._ensure_schema(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT payload,result_json FROM world_boss_manual_spawn_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if previous[0] != payload:
                        return WorldBossManualSpawnResult("operation_conflict")
                    return self._result_from_json(previous[1], "duplicate")

                realm = str(boss.get("jj", ""))
                current_config = self.config_snapshot(self._config_loader(), realm)
                if current_config != expected_config or not self._valid_boss(boss, current_config):
                    conn.rollback()
                    return WorldBossManualSpawnResult("config_changed")

                row = conn.execute(
                    "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
                ).fetchone()
                if row is None:
                    current_bosses = []
                    current_revision = 0
                else:
                    try:
                        current_bosses = json.loads(row[0])
                    except (TypeError, ValueError):
                        current_bosses = []
                    current_revision = int(row[1] or 0)
                if (
                    not isinstance(current_bosses, list)
                    or current_revision != expected_revision
                    or self._json(current_bosses) != self._json(expected_bosses)
                ):
                    conn.rollback()
                    return WorldBossManualSpawnResult("session_changed")

                bosses = [item for item in current_bosses if item.get("jj") != realm]
                bosses.append(boss)
                bosses_json = self._json(bosses)
                revision = current_revision + 1
                conn.execute(
                    "INSERT INTO world_boss_state(state_key,bosses,updated_at,revision) "
                    "VALUES ('global',%s,CURRENT_TIMESTAMP,%s) "
                    "ON CONFLICT(state_key) DO UPDATE SET "
                    "bosses=excluded.bosses,updated_at=excluded.updated_at,"
                    "revision=excluded.revision",
                    (bosses_json, revision),
                )
                result_json = self._json(
                    {"bosses": bosses, "boss": boss, "revision": revision}
                )
                conn.execute(
                    "INSERT INTO world_boss_manual_spawn_operations(operation_id,payload,result_json) "
                    "VALUES (%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return WorldBossManualSpawnResult(
                    "spawned", tuple(bosses), boss, revision
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class WorldBossFullRefreshResult:
    status: str
    revision: int = 0
    bosses: tuple[dict[str, Any], ...] = ()
    trigger: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"refreshed", "duplicate"}

class WorldBossFullRefreshService:
    """Atomically replace the complete world-boss session with a fixed plan."""

    def __init__(
        self,
        player_database: str | Path,
        config_loader: Callable[[], dict[str, Any]],
        lock: RLock | None = None,
    ) -> None:
        self._player_database = Path(player_database)
        self._config_loader = config_loader
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def config_snapshot(
        cls,
        config: dict[str, Any],
        realms: Iterable[str],
    ) -> dict[str, Any]:
        normalized_realms = [str(realm) for realm in realms]
        stones = config.get("Boss灵石", {})
        return {
            "realms": normalized_realms,
            "names": list(config.get("Boss名字", [])),
            "stones": {
                realm: list(stones.get(realm, []))
                for realm in normalized_realms
            },
            "multipliers": dict(config.get("Boss倍率", {})),
        }

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_state("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "revision INTEGER NOT NULL DEFAULT 0)"
        )
        columns = set(conn.column_names("world_boss_state"))
        if "revision" not in columns:
            conn.execute(
                "ALTER TABLE world_boss_state ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_full_refresh_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @classmethod
    def _decode_bosses(cls, value) -> list[dict[str, Any]]:
        try:
            bosses = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        if not isinstance(bosses, list):
            return []
        return [dict(boss) for boss in bosses if isinstance(boss, dict)]

    @staticmethod
    def _valid_bosses(
        bosses: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> bool:
        realms = list(config.get("realms", []))
        if [str(boss.get("jj", "")) for boss in bosses] != realms:
            return False
        if len(set(realms)) != len(realms):
            return False
        names = set(config.get("names", []))
        stones = config.get("stones", {})
        required = {
            "name",
            "jj",
            "气血",
            "总血量",
            "真元",
            "攻击",
            "max_stone",
            "stone",
        }
        for boss in bosses:
            realm = str(boss.get("jj", ""))
            if not required.issubset(boss) or boss.get("name") not in names:
                return False
            if boss.get("max_stone") not in stones.get(realm, []):
                return False
            if boss.get("stone") != boss.get("max_stone"):
                return False
            try:
                if any(
                    int(boss[field]) < 0
                    for field in ("气血", "总血量", "真元", "攻击", "stone")
                ):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    @classmethod
    def _result_from_json(
        cls,
        value,
        status: str,
    ) -> WorldBossFullRefreshResult:
        stored = json.loads(str(value))
        return WorldBossFullRefreshResult(
            status,
            int(stored["revision"]),
            tuple(dict(boss) for boss in stored["bosses"]),
            str(stored["trigger"]),
        )

    def snapshot(self) -> tuple[list[dict[str, Any]], int]:
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
            ).fetchone()
            if row is None:
                return [], 0
            return self._decode_bosses(row[0]), int(row[1] or 0)

    def get_result(self, operation_id: str) -> WorldBossFullRefreshResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT result_json FROM world_boss_full_refresh_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_json(row[0], "duplicate")

    def refresh(
        self,
        *,
        operation_id: str,
        trigger: str,
        expected_revision: int,
        expected_bosses: list[dict[str, Any]],
        expected_config: dict[str, Any],
        bosses: list[dict[str, Any]],
    ) -> WorldBossFullRefreshResult:
        operation_id = str(operation_id).strip()
        trigger = str(trigger).strip()
        expected_revision = int(expected_revision)
        expected_bosses = [dict(boss) for boss in expected_bosses]
        expected_config = dict(expected_config)
        bosses = [dict(boss) for boss in bosses]
        if not operation_id or trigger not in {"manual", "scheduled"}:
            raise ValueError("valid operation and trigger are required")
        payload = self._json(
            {
                "trigger": trigger,
                "expected_revision": expected_revision,
                "expected_bosses": expected_bosses,
                "expected_config": expected_config,
                "bosses": bosses,
            }
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                self._ensure_schema(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT payload,result_json FROM world_boss_full_refresh_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorldBossFullRefreshResult("operation_conflict")
                    return self._result_from_json(previous[1], "duplicate")

                realms = list(expected_config.get("realms", []))
                current_config = self.config_snapshot(self._config_loader(), realms)
                if current_config != expected_config or not self._valid_bosses(
                    bosses,
                    current_config,
                ):
                    conn.rollback()
                    return WorldBossFullRefreshResult("config_changed")

                row = conn.execute(
                    "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
                ).fetchone()
                current_bosses = self._decode_bosses(row[0]) if row else []
                current_revision = int(row[1] or 0) if row else 0
                if (
                    current_revision != expected_revision
                    or self._json(current_bosses) != self._json(expected_bosses)
                ):
                    conn.rollback()
                    return WorldBossFullRefreshResult("session_changed")

                revision = expected_revision + 1
                conn.execute(
                    "INSERT INTO world_boss_state(state_key,bosses,updated_at,revision) "
                    "VALUES('global',%s,CURRENT_TIMESTAMP,%s) "
                    "ON CONFLICT(state_key) DO UPDATE SET bosses=excluded.bosses,"
                    "updated_at=excluded.updated_at,revision=excluded.revision",
                    (self._json(bosses), revision),
                )
                result_json = self._json(
                    {
                        "revision": revision,
                        "bosses": bosses,
                        "trigger": trigger,
                    }
                )
                conn.execute(
                    "INSERT INTO world_boss_full_refresh_operations("
                    "operation_id,payload,result_json,created_at) "
                    "VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return WorldBossFullRefreshResult(
                    "refreshed",
                    revision,
                    tuple(bosses),
                    trigger,
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class WorldBossPunishmentResult:
    status: str
    action: str = ""
    revision: int = 0
    bosses: tuple[dict[str, Any], ...] = ()
    deleted_bosses: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"punished", "duplicate"}

class WorldBossPunishmentService:
    """Atomically remove one or all bosses from the current world-boss session."""

    def __init__(
        self,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode_bosses(value: Any) -> list[dict[str, Any]]:
        try:
            bosses = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        if not isinstance(bosses, list):
            return []
        return [dict(boss) for boss in bosses if isinstance(boss, dict)]

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_state("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "revision INTEGER NOT NULL DEFAULT 0)"
        )
        columns = set(conn.column_names("world_boss_state"))
        if "revision" not in columns:
            conn.execute(
                "ALTER TABLE world_boss_state ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_punishment_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @classmethod
    def _result_from_json(
        cls,
        value: Any,
        status: str,
    ) -> WorldBossPunishmentResult:
        stored = json.loads(str(value))
        return WorldBossPunishmentResult(
            status=status,
            action=str(stored["action"]),
            revision=int(stored["revision"]),
            bosses=tuple(dict(boss) for boss in stored["bosses"]),
            deleted_bosses=tuple(
                dict(boss) for boss in stored["deleted_bosses"]
            ),
        )

    def snapshot(self) -> tuple[list[dict[str, Any]], int]:
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
            ).fetchone()
            if row is None:
                return [], 0
            return self._decode_bosses(row[0]), int(row[1] or 0)

    def get_result(self, operation_id: str) -> WorldBossPunishmentResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT result_json FROM world_boss_punishment_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_json(row[0], "duplicate")

    def punish(
        self,
        *,
        operation_id: str,
        action: str,
        expected_revision: int,
        expected_bosses: list[dict[str, Any]],
        boss_number: int | None = None,
    ) -> WorldBossPunishmentResult:
        operation_id = str(operation_id).strip()
        action = str(action).strip()
        expected_revision = int(expected_revision)
        expected_bosses = [dict(boss) for boss in expected_bosses]
        boss_number = int(boss_number) if boss_number is not None else None
        if not operation_id or action not in {"single", "all"}:
            raise ValueError("valid operation and punishment action are required")
        if action == "single" and (boss_number is None or boss_number <= 0):
            raise ValueError("single punishment requires a positive boss number")

        payload = self._json(
            {
                "action": action,
                "expected_revision": expected_revision,
                "expected_bosses": expected_bosses,
                "boss_number": boss_number,
            }
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                self._ensure_schema(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT payload,result_json FROM world_boss_punishment_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorldBossPunishmentResult("operation_conflict")
                    return self._result_from_json(previous[1], "duplicate")

                row = conn.execute(
                    "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
                ).fetchone()
                current_bosses = self._decode_bosses(row[0]) if row else []
                current_revision = int(row[1] or 0) if row else 0
                if (
                    current_revision != expected_revision
                    or self._json(current_bosses) != self._json(expected_bosses)
                ):
                    conn.rollback()
                    return WorldBossPunishmentResult("session_changed")
                if not current_bosses:
                    conn.rollback()
                    return WorldBossPunishmentResult("empty")

                if action == "single":
                    index = int(boss_number) - 1
                    if index < 0 or index >= len(current_bosses):
                        conn.rollback()
                        return WorldBossPunishmentResult("invalid_target")
                    deleted_bosses = [current_bosses[index]]
                    bosses = current_bosses[:index] + current_bosses[index + 1 :]
                else:
                    deleted_bosses = current_bosses
                    bosses = []

                revision = current_revision + 1
                conn.execute(
                    "UPDATE world_boss_state SET bosses=%s,updated_at=CURRENT_TIMESTAMP,"
                    "revision=%s WHERE state_key='global'",
                    (self._json(bosses), revision),
                )
                result_json = self._json(
                    {
                        "action": action,
                        "revision": revision,
                        "bosses": bosses,
                        "deleted_bosses": deleted_bosses,
                    }
                )
                conn.execute(
                    "INSERT INTO world_boss_punishment_operations("
                    "operation_id,payload,result_json,created_at) "
                    "VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return WorldBossPunishmentResult(
                    status="punished",
                    action=action,
                    revision=revision,
                    bosses=tuple(bosses),
                    deleted_bosses=tuple(deleted_bosses),
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class WorldBossDailyLimitResetResult:
    status: str
    business_date: str
    task_status: str = ""
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorldBossDailyLimitResetService:
    """Reset a date-frozen set of world-boss limit rows in durable chunks."""

    _FIELDS = ("boss_integral", "boss_stone", "boss_battle_count")

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
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

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS boss(user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("boss"))
        for field in cls._FIELDS:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE boss ADD COLUMN {db_backend.quote_ident(field)} "
                    "INTEGER DEFAULT 0"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_daily_limit_reset_operations("
            "business_date TEXT PRIMARY KEY,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "skipped INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'running',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_daily_limit_reset_targets("
            "business_date TEXT NOT NULL,user_id TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',previous_integral INTEGER,"
            "previous_stone INTEGER,previous_battle_count INTEGER,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_date,user_id))"
        )

    @staticmethod
    def _result(conn, business_date: str, status: str):
        row = conn.execute(
            "SELECT status,total,completed,changed,skipped "
            "FROM world_boss_daily_limit_reset_operations WHERE business_date=%s",
            (business_date,),
        ).fetchone()
        if row is None:
            return WorldBossDailyLimitResetResult(status, business_date)
        return WorldBossDailyLimitResetResult(
            status=status,
            business_date=business_date,
            task_status=str(row[0]),
            total=int(row[1]),
            completed=int(row[2]),
            changed=int(row[3]),
            skipped=int(row[4]),
        )

    def reset(
        self,
        business_date=None,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> WorldBossDailyLimitResetResult:
        business_date = self._normalize_date(business_date)
        chunk_size = max(1, int(chunk_size))
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT status FROM world_boss_daily_limit_reset_operations "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT user_id FROM boss ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO world_boss_daily_limit_reset_operations("
                        "business_date,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s)",
                        (
                            business_date,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO world_boss_daily_limit_reset_targets("
                        "business_date,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (business_date, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, business_date, "applied")
                elif str(operation[0]) == "completed":
                    result = self._result(conn, business_date, "duplicate")
                    conn.rollback()
                    return result
                else:
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM world_boss_daily_limit_reset_targets "
                    "WHERE business_date=%s AND status='pending' "
                    "ORDER BY user_id LIMIT %s",
                    (business_date, chunk_size),
                ).fetchall()
                changed = 0
                skipped = 0
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    row = conn.execute(
                        "SELECT COALESCE(boss_integral,0),COALESCE(boss_stone,0),"
                        "COALESCE(boss_battle_count,0) FROM boss WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    if row is None:
                        skipped += 1
                        conn.execute(
                            "UPDATE world_boss_daily_limit_reset_targets SET "
                            "status='skipped',updated_at=%s WHERE business_date=%s "
                            "AND user_id=%s AND status='pending'",
                            (updated_at, business_date, user_id),
                        )
                        continue

                    previous = tuple(int(value or 0) for value in row)
                    updated = conn.execute(
                        "UPDATE boss SET boss_integral=0,boss_stone=0,"
                        "boss_battle_count=0 WHERE user_id=%s",
                        (user_id,),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError(
                            "world boss daily reset target changed"
                        )
                    changed += int(any(previous))
                    conn.execute(
                        "UPDATE world_boss_daily_limit_reset_targets SET "
                        "status='applied',previous_integral=%s,previous_stone=%s,"
                        "previous_battle_count=%s,updated_at=%s WHERE business_date=%s "
                        "AND user_id=%s AND status='pending'",
                        (*previous, updated_at, business_date, user_id),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' "
                    "THEN 1 ELSE 0 END),0) FROM world_boss_daily_limit_reset_targets "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE world_boss_daily_limit_reset_operations SET completed=%s,"
                    "changed=changed+%s,skipped=skipped+%s,status=%s,updated_at=%s "
                    "WHERE business_date=%s",
                    (
                        completed,
                        changed,
                        skipped,
                        task_status,
                        updated_at,
                        business_date,
                    ),
                )
                result = self._result(conn, business_date, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class BossRewardResult:
    status: str
    exp: int
    stone: int
    integral: int
    item_quantity: int
    total_exp: int
    wallet_stone: int
    daily_stone: int
    daily_integral: int
    total_integral: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class BossRewardService:
    """Apply all personal world-boss rewards in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def grant(self, operation_id, user_id, expected_daily_stone, expected_daily_integral,
              expected_total_integral, expected_exp, stone, integral, exp=0, item_id=0,
              item_name="", item_type="", item_quantity=0, item_bind=0, max_goods_num=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        values = [expected_daily_stone, expected_daily_integral, expected_total_integral, expected_exp,
                  stone, integral, exp, item_id, item_quantity, max_goods_num]
        (expected_daily_stone, expected_daily_integral, expected_total_integral, expected_exp,
         stone, integral, exp, item_id, item_quantity, max_goods_num) = map(int, values)
        item_bind = 1 if int(item_bind) == 1 else 0
        if not operation_id or min(values) < 0:
            raise ValueError("valid operation, snapshots and rewards are required")
        payload = json.dumps([user_id, *values, str(item_name), str(item_type), item_bind], ensure_ascii=True)

        def rejected(status, wallet=0):
            return BossRewardResult(status, 0, 0, 0, 0, expected_exp, int(wallet),
                                    expected_daily_stone, expected_daily_integral, expected_total_integral)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS boss_reward_operations (operation_id TEXT PRIMARY KEY, "
                    "payload TEXT NOT NULL, exp INTEGER NOT NULL, stone INTEGER NOT NULL, integral INTEGER NOT NULL, "
                    "item_quantity INTEGER NOT NULL, total_exp INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, "
                    "daily_stone INTEGER NOT NULL, daily_integral INTEGER NOT NULL, total_integral INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,exp,stone,integral,item_quantity,total_exp,wallet_stone,daily_stone,daily_integral,total_integral "
                    "FROM boss_reward_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    return rejected("state_changed") if str(previous[0]) != payload else BossRewardResult(
                        "duplicate", *(int(value) for value in previous[1:]))
                user = conn.execute("SELECT COALESCE(stone,0),COALESCE(exp,0) FROM user_xiuxian WHERE user_id=%s",
                                    (user_id,)).fetchone()
                if user is None:
                    conn.rollback(); return rejected("user_missing")
                for table, fields in (("boss", {"boss_stone", "boss_integral"}), ("boss_limit", {"integral"})):
                    exists = conn.execute("SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", (table,)).fetchone()
                    columns = {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()} if exists else set()
                    if not fields.issubset(columns):
                        conn.rollback(); return rejected("state_changed", user[0])
                daily = conn.execute("SELECT COALESCE(boss_stone,0),COALESCE(boss_integral,0) FROM player_data.boss WHERE user_id=%s", (user_id,)).fetchone()
                total = conn.execute("SELECT COALESCE(integral,0) FROM player_data.boss_limit WHERE user_id=%s", (user_id,)).fetchone()
                current = (int(daily[0]) if daily else 0, int(daily[1]) if daily else 0,
                           int(total[0]) if total else 0, int(user[1]))
                if current != (expected_daily_stone, expected_daily_integral, expected_total_integral, expected_exp):
                    conn.rollback(); return rejected("state_changed", user[0])
                if item_quantity:
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(item[0]) if item else 0) + item_quantity > max_goods_num:
                        conn.rollback(); return rejected("inventory_full", user[0])
                total_exp, wallet = expected_exp + exp, int(user[0]) + stone
                if conn.execute("UPDATE user_xiuxian SET stone=%s,exp=%s WHERE user_id=%s AND COALESCE(stone,0)=%s AND COALESCE(exp,0)=%s",
                                (wallet, total_exp, user_id, int(user[0]), expected_exp)).rowcount != 1:
                    conn.rollback(); return rejected("state_changed", user[0])
                daily_stone, daily_integral, total_integral = (expected_daily_stone + stone,
                                                               expected_daily_integral + integral,
                                                               expected_total_integral + integral)
                if daily is None:
                    conn.execute("INSERT INTO player_data.boss (user_id,boss_stone,boss_integral) VALUES (%s,%s,%s)", (user_id, daily_stone, daily_integral))
                else:
                    conn.execute("UPDATE player_data.boss SET boss_stone=%s,boss_integral=%s WHERE user_id=%s", (daily_stone, daily_integral, user_id))
                if total is None:
                    conn.execute("INSERT INTO player_data.boss_limit (user_id,integral) VALUES (%s,%s)", (user_id, total_integral))
                else:
                    conn.execute("UPDATE player_data.boss_limit SET integral=%s WHERE user_id=%s", (total_integral, user_id))
                if item_quantity:
                    now, bound = datetime.now(), item_quantity if item_bind else 0
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                                 (user_id, item_id, str(item_name), str(item_type), item_quantity, now, now, bound))
                conn.execute("INSERT INTO boss_reward_operations VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                             (operation_id, payload, exp, stone, integral, item_quantity, total_exp, wallet,
                              daily_stone, daily_integral, total_integral))
                conn.commit()
                return BossRewardResult("applied", exp, stone, integral, item_quantity, total_exp, wallet,
                                        daily_stone, daily_integral, total_integral)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "BossPurchaseResult",
    "BossPurchaseService",
    "WorldBossBattleSettlementResult",
    "WorldBossBattleSettlementService",
    "WorldBossManualSpawnResult",
    "WorldBossManualSpawnService",
    "WorldBossFullRefreshResult",
    "WorldBossFullRefreshService",
    "WorldBossPunishmentResult",
    "WorldBossPunishmentService",
    "WorldBossDailyLimitResetResult",
    "WorldBossDailyLimitResetService",
    "BossRewardResult",
    "BossRewardService",
    "normalize_weekly_purchases",
]
