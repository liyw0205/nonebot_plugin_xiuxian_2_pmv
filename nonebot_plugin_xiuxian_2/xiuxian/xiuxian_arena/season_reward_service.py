from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


__all__ = ["ArenaSeasonRewardResult", "ArenaSeasonRewardService"]
