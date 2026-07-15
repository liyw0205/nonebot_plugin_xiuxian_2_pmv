from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ..xiuxian_utils import db_backend


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


__all__ = ["WorldBossBattleSettlementResult", "WorldBossBattleSettlementService"]
