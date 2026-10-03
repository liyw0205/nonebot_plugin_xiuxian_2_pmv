from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class WorldBossBattleSettlementResult:
    status: str
    boss_hp: int = 0
    stamina: int = 0
    battle_count: int = 0
    stone: int = 0
    exp: int = 0
    integral: int = 0
    activity_lines: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def __getitem__(self, key: str) -> Any:
        """Keep the historical mapping-style replay API readable during cutover."""
        return getattr(self, key)


@dataclass(frozen=True)
class WorldBossDailyLimitSnapshot:
    battle_count: int = 0
    integral: int = 0
    stone: int = 0


class WorldBossBattleSettlementSqlRepository:
    """Feature-owned world-boss settlement across game/player/activity DBs."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        activity_database: str | Path | None = None,
        *,
        clock=None,
        lock: RLock | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.activity_database = str(activity_database) if activity_database else None
        self.clock = clock or SystemClock()
        self.lock = lock or RLock()

    @staticmethod
    def _integer(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError, OverflowError):
            return 0

    def daily_limit_snapshot(self, user_id: str) -> WorldBossDailyLimitSnapshot:
        user_id = str(user_id).strip()
        if not user_id or not Path(self.player_database).is_file():
            return WorldBossDailyLimitSnapshot()

        fields = {
            "battle_count": "boss_battle_count",
            "integral": "boss_integral",
            "stone": "boss_stone",
        }
        with self.lock, DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            columns = {
                str(row["name"]).casefold()
                for row in uow.query_all('PRAGMA table_info("boss")')
            }
            if "user_id" not in columns:
                return WorldBossDailyLimitSnapshot()
            selected = [
                f'"{column}" AS "{name}"'
                for name, column in fields.items()
                if column in columns
            ]
            if not selected:
                return WorldBossDailyLimitSnapshot()
            row = uow.query_one(
                f'SELECT {", ".join(selected)} FROM "boss" WHERE user_id=? LIMIT 1',
                (user_id,),
            )
        if row is None:
            return WorldBossDailyLimitSnapshot()
        return WorldBossDailyLimitSnapshot(
            self._integer(row.get("battle_count")),
            self._integer(row.get("integral")),
            self._integer(row.get("stone")),
        )

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

    @staticmethod
    def _result(
        status: str,
        *,
        expected_stamina: int,
        expected_battle_count: int,
        expected_stone: int,
        expected_exp: int,
        expected_integral: int,
        boss_hp: int = 0,
        stamina: int | None = None,
        battle_count: int | None = None,
        stone: int | None = None,
        exp: int | None = None,
        integral: int | None = None,
        activity_lines: tuple[str, ...] = (),
    ) -> WorldBossBattleSettlementResult:
        return WorldBossBattleSettlementResult(
            status,
            int(boss_hp),
            int(expected_stamina if stamina is None else stamina),
            int(expected_battle_count if battle_count is None else battle_count),
            int(expected_stone if stone is None else stone),
            int(expected_exp if exp is None else exp),
            int(expected_integral if integral is None else integral),
            tuple(activity_lines),
        )

    def settlement_result(self, operation_id: str) -> WorldBossBattleSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        try:
            with self.lock, DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT boss_hp,stamina,battle_count,stone,exp,integral,activity_lines "
                    "FROM world_boss_battle_operations WHERE operation_id=?",
                    (operation_id,),
                )
        except Exception as exc:
            if "no such table" in str(exc).lower():
                return None
            raise
        if row is None:
            return None
        lines = self._loads(row.get("activity_lines"), [])
        return WorldBossBattleSettlementResult(
            "duplicate",
            int(row["boss_hp"] or 0),
            int(row["stamina"] or 0),
            int(row["battle_count"] or 0),
            int(row["stone"] or 0),
            int(row["exp"] or 0),
            int(row["integral"] or 0),
            tuple(str(line) for line in lines),
        )

    def _increment_stat(self, uow: DatabaseUnitOfWork, user_id: str, field: str) -> None:
        quoted = '"' + field.replace('"', '""') + '"'
        uow.execute(
            f"INSERT INTO player_data.statistics(user_id,{quoted}) VALUES (?,1) "
            f"ON CONFLICT(user_id) DO UPDATE SET {quoted}=COALESCE({quoted},0)+1",
            (user_id,),
        )

    def _record_tasks(self, uow: DatabaseUnitOfWork, user_id: str, daily_period: str, weekly_period: str) -> None:
        row = uow.query_one(
            "SELECT daily_period,daily_progress,daily_claimed,weekly_period,weekly_progress,weekly_claimed "
            "FROM player_data.xiuxian_tasks WHERE user_id=?",
            (user_id,),
        )
        values = list(row.values()) if row else ["", "{}", "[]", "", "{}", "[]"]
        for prefix, period, target in (("daily", daily_period, 1), ("weekly", weekly_period, 150)):
            offset = 0 if prefix == "daily" else 3
            progress = self._loads(values[offset + 1], {}) if str(values[offset] or "") == period else {}
            claimed = self._loads(values[offset + 2], []) if str(values[offset] or "") == period else []
            key = "daily_boss" if prefix == "daily" else "weekly_boss"
            progress[key] = min(target, int(progress.get(key, 0) or 0) + 1)
            values[offset:offset + 3] = [period, self._json(progress), self._json(claimed)]
        uow.execute(
            "INSERT INTO player_data.xiuxian_tasks(user_id,daily_period,daily_progress,daily_claimed,"
            "weekly_period,weekly_progress,weekly_claimed) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET daily_period=excluded.daily_period,"
            "daily_progress=excluded.daily_progress,daily_claimed=excluded.daily_claimed,"
            "weekly_period=excluded.weekly_period,weekly_progress=excluded.weekly_progress,"
            "weekly_claimed=excluded.weekly_claimed",
            (user_id, *values),
        )

    def _apply_activity_damage(
        self,
        uow: DatabaseUnitOfWork,
        user_id: str,
        raw_damage: int,
        activities: list[Mapping[str, Any]],
        prefix: str,
    ) -> list[str]:
        if not activities:
            return []
        sqlite_max = 2**63 - 1
        stamp = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
        today = self.clock.now().strftime("%Y-%m-%d")
        state_table = f"{prefix}activity_boss_state"
        damage_table = f"{prefix}activity_boss_damage"
        fight_table = f"{prefix}activity_boss_fight_log"
        milestone_table = f"{prefix}activity_boss_milestone"
        lines: list[str] = []

        def safe_int(value: Any, default: int = 0) -> int:
            try:
                number = int(value)
            except (TypeError, ValueError):
                number = int(default)
            return min(sqlite_max, max(0, number))

        for activity in activities:
            key = str(activity["key"])
            limit = max(1, int(activity.get("daily_fight_limit", 3)))
            used = uow.query_one(
                f"SELECT COUNT(*) AS count FROM {fight_table} WHERE activity_key=? AND user_id=? "
                "AND fight_date=? AND source IN ('coop','world_boss')",
                (key, user_id, today),
            )["count"]
            if int(used) >= limit:
                continue
            max_hp = max(1, safe_int(activity.get("max_hp"), 1))
            row = uow.query_one(f"SELECT hp_left,max_hp FROM {state_table} WHERE activity_key=?", (key,))
            hp_left = max_hp if row is None else max(0, safe_int(row["hp_left"]))
            if row is None:
                uow.execute(
                    f"INSERT INTO {state_table}(activity_key,hp_left,max_hp,update_time) VALUES (?,?,?,?)",
                    (key, max_hp, max_hp, stamp),
                )
            multiplier = max(0.0, float(activity.get("multiplier", 1.0)))
            cap = max(1, safe_int(max_hp * float(activity.get("hit_hp_cap_ratio", 0.01)), 1))
            scaled = max(1, safe_int(raw_damage * multiplier, 1))
            damage = min(scaled, cap, hp_left, sqlite_max)
            if damage <= 0:
                continue
            new_hp = max(0, hp_left - damage)
            changed = uow.execute(
                f"UPDATE {state_table} SET hp_left=?,max_hp=?,update_time=? WHERE activity_key=? AND hp_left=?",
                (new_hp, max_hp, stamp, key, hp_left),
            )
            if changed.rowcount != 1:
                raise RuntimeError("activity boss state changed")
            uow.execute(
                f"INSERT INTO {damage_table}(activity_key,user_id,total_damage,update_time) VALUES (?,?,?,?) "
                "ON CONFLICT(activity_key,user_id) DO UPDATE SET "
                "total_damage=MIN(?,COALESCE(activity_boss_damage.total_damage,0)+excluded.total_damage),"
                "update_time=excluded.update_time",
                (key, user_id, damage, stamp, sqlite_max),
            )
            uow.execute(
                f"INSERT INTO {fight_table}(activity_key,user_id,damage,fight_date,source,create_time) "
                "VALUES (?,?,?,?,'world_boss',?)",
                (key, user_id, damage, today, stamp),
            )
            percent_left = 100.0 * new_hp / max_hp
            for milestone in activity.get("server_milestones", []):
                threshold = float(milestone.get("hp_percent", 0))
                if percent_left <= threshold:
                    uow.execute(
                        f"INSERT OR IGNORE INTO {milestone_table}(activity_key,milestone_key,unlocked_time) "
                        "VALUES (?,?,?)",
                        (key, str(milestone.get("key") or f"p{threshold}"), stamp),
                    )
            lines.append(f"活动首领·{activity.get('boss_name', '活动首领')} 计入伤害 {damage}，剩余 {new_hp}/{max_hp}")
        return lines

    def settle(self, *, operation_id: str, user_id: str, expected_bosses: list[dict], settled_bosses: list[dict], boss_index: int, expected_stamina: int, stamina_cost: int, expected_hp: int, expected_mp: int, final_hp: int, final_mp: int, expected_exp: int, exp_reward: int, expected_stone: int, stone_reward: int, expected_daily_stone: int, expected_daily_integral: int, expected_total_integral: int, integral_reward: int, expected_battle_count: int, battle_limit: int, expected_checked_at: str, checked_at: str, item: dict | None, max_goods_num: int, actual_damage: int, killed: bool, daily_period: str, weekly_period: str, activity_bosses: list[dict] | None = None) -> WorldBossBattleSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        boss_index = int(boss_index)
        item = dict(item or {})
        activity_bosses = list(activity_bosses or [])
        if not operation_id or not user_id or not (0 <= boss_index < len(expected_bosses)):
            raise ValueError("valid operation and boss index are required")
        payload = self._json({
            "user_id": user_id,
            "boss_index": boss_index,
            "stamina_cost": int(stamina_cost),
            "battle_limit": int(battle_limit),
            "max_goods_num": int(max_goods_num),
            "daily_period": str(daily_period),
            "weekly_period": str(weekly_period),
        })
        with self.lock, DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            activity_prefix = ""
            if activity_bosses and self.activity_database and Path(self.activity_database).resolve() != Path(self.game_database).resolve():
                uow.attach_database(self.activity_database, "activity")
                activity_prefix = "activity."
            previous = uow.query_one(
                    "SELECT payload,boss_hp,stamina,battle_count,stone,exp,integral,activity_lines "
                    "FROM world_boss_battle_operations WHERE operation_id=?",
                    (operation_id,),
                )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return self._result("state_changed", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)
                lines = self._loads(previous["activity_lines"], [])
                return self._result("duplicate", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral, boss_hp=previous["boss_hp"], stamina=previous["stamina"], battle_count=previous["battle_count"], stone=previous["stone"], exp=previous["exp"], integral=previous["integral"], activity_lines=tuple(lines))

            state = uow.query_one("SELECT bosses FROM player_data.world_boss_state WHERE state_key='global'")
            expected_state_payload = None if state is None else str(state["bosses"])
            if state is not None and self._loads(state["bosses"], []) != expected_bosses:
                return self._result("boss_changed", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)

            user = uow.query_one("SELECT COALESCE(user_stamina,0) AS stamina,COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp,COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            cooldown = uow.query_one("SELECT COALESCE(last_check_info_time,'') AS checked_at FROM user_cd WHERE user_id=?", (user_id,))
            if user is None or cooldown is None:
                return self._result("user_missing", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)
            if (int(user["stamina"]), int(user["hp"]), int(user["mp"]), int(user["exp"]), int(user["stone"])) != (int(expected_stamina), int(expected_hp), int(expected_mp), int(expected_exp), int(expected_stone)) or str(cooldown["checked_at"]) != str(expected_checked_at or ""):
                return self._result("state_changed", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)
            if expected_stamina < stamina_cost:
                return self._result("stamina_insufficient", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)
            if expected_hp <= expected_exp / 10:
                return self._result("hp_insufficient", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)

            daily = uow.query_one("SELECT COALESCE(boss_stone,0) AS stone,COALESCE(boss_integral,0) AS integral,COALESCE(boss_battle_count,0) AS count FROM player_data.boss WHERE user_id=?", (user_id,))
            total = uow.query_one("SELECT COALESCE(integral,0) AS integral FROM player_data.boss_limit WHERE user_id=?", (user_id,))
            actual_daily = (0, 0, 0) if daily is None else (int(daily["stone"]), int(daily["integral"]), int(daily["count"]))
            actual_total = 0 if total is None else int(total["integral"])
            if actual_daily != (int(expected_daily_stone), int(expected_daily_integral), int(expected_battle_count)) or actual_total != int(expected_total_integral):
                return self._result("state_changed", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)
            if expected_battle_count >= battle_limit:
                return self._result("limit_reached", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)

            quantity = max(0, int(item.get("quantity", 0) or 0))
            item_id = int(item.get("id", 0) or 0)
            if quantity:
                current = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if (int(current["goods_num"]) if current else 0) + quantity > int(max_goods_num):
                    return self._result("inventory_full", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral)

            stamina = int(expected_stamina) - int(stamina_cost)
            stone = int(expected_stone) + int(stone_reward)
            exp = int(expected_exp) + int(exp_reward)
            count = int(expected_battle_count) + 1
            integral = int(expected_total_integral) + int(integral_reward)
            if uow.execute("UPDATE user_xiuxian SET user_stamina=?,hp=?,mp=?,exp=?,stone=? WHERE user_id=? AND user_stamina=? AND hp=? AND mp=? AND exp=? AND stone=?", (stamina, max(1, int(final_hp)), max(1, int(final_mp)), exp, stone, user_id, expected_stamina, expected_hp, expected_mp, expected_exp, expected_stone)).rowcount != 1:
                raise RuntimeError("player state changed")
            if uow.execute("UPDATE user_cd SET last_check_info_time=? WHERE user_id=? AND last_check_info_time=?", (str(checked_at), user_id, str(expected_checked_at))).rowcount != 1:
                raise RuntimeError("cooldown changed")
            if daily is None:
                uow.execute("INSERT INTO player_data.boss(user_id,boss_stone,boss_integral,boss_battle_count) VALUES (?,?,?,?)", (user_id, int(expected_daily_stone) + int(stone_reward), int(expected_daily_integral) + int(integral_reward), count))
            elif uow.execute("UPDATE player_data.boss SET boss_stone=?,boss_integral=?,boss_battle_count=? WHERE user_id=? AND boss_stone=? AND boss_integral=? AND boss_battle_count=?", (int(expected_daily_stone) + int(stone_reward), int(expected_daily_integral) + int(integral_reward), count, user_id, expected_daily_stone, expected_daily_integral, expected_battle_count)).rowcount != 1:
                raise RuntimeError("boss state changed")
            if total is None:
                uow.execute("INSERT INTO player_data.boss_limit(user_id,integral) VALUES (?,?)", (user_id, integral))
            elif uow.execute("UPDATE player_data.boss_limit SET integral=? WHERE user_id=? AND integral=?", (integral, user_id, expected_total_integral)).rowcount != 1:
                raise RuntimeError("boss integral changed")
            if quantity:
                stamp = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
                bind = quantity if bool(item.get("bind")) else 0
                columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
                if "bind_num" in columns:
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time", (user_id, item_id, str(item.get("name", "")), str(item.get("type", "")), quantity, stamp, stamp, bind))
                else:
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) VALUES(?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,update_time=excluded.update_time", (user_id, item_id, str(item.get("name", "")), str(item.get("type", "")), quantity, stamp, stamp))
            if expected_state_payload is None:
                uow.execute("INSERT INTO player_data.world_boss_state(state_key,bosses,updated_at) VALUES ('global',?,?)", (self._json(settled_bosses), str(checked_at)))
            elif uow.execute("UPDATE player_data.world_boss_state SET bosses=?,updated_at=? WHERE state_key='global' AND bosses=?", (self._json(settled_bosses), str(checked_at), expected_state_payload)).rowcount != 1:
                raise RuntimeError("world boss state changed")
            self._increment_stat(uow, user_id, "讨伐世界BOSS")
            if killed:
                self._increment_stat(uow, user_id, "击败世界BOSS")
            self._record_tasks(uow, user_id, str(daily_period), str(weekly_period))
            lines = self._apply_activity_damage(uow, user_id, int(actual_damage), activity_bosses, activity_prefix) if activity_bosses and self.activity_database else []
            boss_hp = int(settled_bosses[boss_index].get("气血", 0)) if 0 <= boss_index < len(settled_bosses) else 0
            uow.execute("INSERT INTO world_boss_battle_operations(operation_id,payload,boss_hp,stamina,battle_count,stone,exp,integral,activity_lines) VALUES(?,?,?,?,?,?,?,?,?)", (operation_id, payload, boss_hp, stamina, count, stone, exp, integral, self._json(lines)))
            return self._result("applied", expected_stamina=expected_stamina, expected_battle_count=expected_battle_count, expected_stone=expected_stone, expected_exp=expected_exp, expected_integral=expected_total_integral, boss_hp=boss_hp, stamina=stamina, battle_count=count, stone=stone, exp=exp, integral=integral, activity_lines=tuple(lines))


__all__ = ["WorldBossBattleSettlementResult", "WorldBossBattleSettlementSqlRepository"]
