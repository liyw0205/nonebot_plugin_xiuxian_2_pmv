from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ArenaSeasonRewardResult:
    status: str
    honor: int
    honor_points: int
    total_honor_earned: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaSeasonRewardRepository:
    """Apply one daily arena reward and counter reset across game/player DBs."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _result(status: str, honor: int, honor_points: int, earned: int) -> ArenaSeasonRewardResult:
        return ArenaSeasonRewardResult(status, int(honor), int(honor_points), int(earned))

    def daily_candidates(self) -> tuple[dict[str, Any], ...]:
        with DatabaseUnitOfWork(self.player_database) as uow:
            rows = uow.query_all(
                "SELECT a.user_id,COALESCE(a.score,0) AS score,COALESCE(a.rank,'') AS rank,"
                "COALESCE(a.honor_points,0) AS honor_points,"
                "COALESCE(a.total_honor_earned,0) AS total_honor_earned,"
                "COALESCE(a.daily_challenges_used,0) AS daily_challenges_used,"
                "COALESCE(a.daily_extra_challenges,0) AS daily_extra_challenges,"
                "COALESCE(a.daily_challenge_buys,0) AS daily_challenge_buys,"
                "COALESCE(a.last_reset_date,'') AS last_reset_date,"
                "COALESCE(a.last_buy_date,'') AS last_buy_date,"
                "1+(SELECT COUNT(*) FROM arena AS higher WHERE COALESCE(higher.score,0)>COALESCE(a.score,0)) AS position "
                "FROM arena AS a ORDER BY COALESCE(a.score,0) DESC,a.user_id"
            )
        return tuple(dict(row) for row in rows)

    def claim(
        self,
        operation_id: str,
        user_id: str,
        season_key: str,
        expected_score: int,
        expected_rank: str,
        expected_position: int,
        expected_honor: int,
        expected_total_honor: int,
        base_honor: int,
        ranking_bonus: int,
        items: Sequence[Mapping[str, Any]] = (),
        max_goods_num: int = 999999999,
        *,
        expected_reset: Mapping[str, Any] | None = None,
    ) -> ArenaSeasonRewardResult:
        operation_id, user_id, season_key = str(operation_id).strip(), str(user_id), str(season_key).strip()
        expected_score, expected_position, expected_honor, expected_total_honor = map(
            int, (expected_score, expected_position, expected_honor, expected_total_honor)
        )
        base_honor, ranking_bonus, max_goods_num = map(int, (base_honor, ranking_bonus, max_goods_num))
        reset = None if expected_reset is None else {
            "daily_challenges_used": int(expected_reset["daily_challenges_used"]),
            "daily_extra_challenges": int(expected_reset["daily_extra_challenges"]),
            "daily_challenge_buys": int(expected_reset["daily_challenge_buys"]),
            "last_reset_date": str(expected_reset["last_reset_date"] or ""),
            "last_buy_date": str(expected_reset["last_buy_date"] or ""),
        }
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]), 1 if int(item.get("bind", 1)) == 1 else 0)
            for item in items
            if int(item.get("amount", 0)) > 0
        )
        if not operation_id or not season_key or min(
            expected_score, expected_honor, expected_total_honor, base_honor, ranking_bonus, max_goods_num
        ) < 0:
            raise ValueError("valid operation, season and reward snapshot are required")
        payload = json.dumps(
            [user_id, season_key, expected_score, str(expected_rank), expected_position, expected_honor,
             expected_total_honor, base_honor, ranking_bonus, rewards, max_goods_num, reset],
            ensure_ascii=True,
            sort_keys=True,
        )
        total_reward = base_honor + ranking_bonus

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            previous = uow.query_one(
                "SELECT payload,honor,honor_points,total_honor_earned "
                "FROM arena_season_reward_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return self._result("state_changed", 0, expected_honor, expected_total_honor)
                return self._result(
                    "duplicate", previous["honor"], previous["honor_points"], previous["total_honor_earned"]
                )
            claimed = uow.query_one(
                "SELECT honor,honor_points,total_honor_earned FROM arena_season_reward_operations "
                "WHERE season_key=? AND user_id=?",
                (season_key, user_id),
            )
            if claimed is not None:
                return self._result("already_claimed", claimed["honor"], claimed["honor_points"], claimed["total_honor_earned"])
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return self._result("user_missing", 0, expected_honor, expected_total_honor)
            columns = {str(row[1]) for row in uow.execute("PRAGMA player_data.table_info(arena)").fetchall()}
            required = {"score", "rank", "honor_points", "total_honor_earned"}
            if reset is not None:
                required.update(reset)
            if not required.issubset(columns):
                return self._result("state_changed", 0, expected_honor, expected_total_honor)
            arena = uow.query_one(
                "SELECT COALESCE(score,0) AS score,COALESCE(rank,'') AS rank,"
                "COALESCE(honor_points,0) AS honor_points,"
                "COALESCE(total_honor_earned,0) AS total_honor_earned "
                "FROM player_data.arena WHERE user_id=?",
                (user_id,),
            )
            if arena is None or (
                int(arena["score"]), str(arena["rank"]), int(arena["honor_points"]), int(arena["total_honor_earned"])
            ) != (expected_score, str(expected_rank), expected_honor, expected_total_honor):
                return self._result("state_changed", 0, expected_honor, expected_total_honor)
            ranking = uow.query_one(
                "SELECT COUNT(*)+1 AS position FROM player_data.arena WHERE COALESCE(score,0)>?",
                (expected_score,),
            )
            if ranking is None or (expected_position > 0 and int(ranking["position"]) != expected_position):
                return self._result("state_changed", 0, expected_honor, expected_total_honor)
            if reset is not None:
                reset_row = uow.query_one(
                    "SELECT COALESCE(daily_challenges_used,0) AS daily_challenges_used,"
                    "COALESCE(daily_extra_challenges,0) AS daily_extra_challenges,"
                    "COALESCE(daily_challenge_buys,0) AS daily_challenge_buys,"
                    "COALESCE(last_reset_date,'') AS last_reset_date,"
                    "COALESCE(last_buy_date,'') AS last_buy_date "
                    "FROM player_data.arena WHERE user_id=?",
                    (user_id,),
                )
                if reset_row is None or any(str(reset_row[key]) != str(value) for key, value in reset.items()):
                    return self._result("state_changed", 0, expected_honor, expected_total_honor)
            for item_id, _name, _type, amount, _bound in rewards:
                inventory = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                inventory_total = int(inventory["goods_num"]) + amount if inventory else amount
                if inventory_total > max_goods_num:
                    return self._result("inventory_full", 0, expected_honor, expected_total_honor)
            honor_points, total_honor = expected_honor + total_reward, expected_total_honor + total_reward
            changed = uow.execute(
                "UPDATE player_data.arena SET honor_points=?,total_honor_earned=? WHERE user_id=? "
                "AND COALESCE(score,0)=? AND COALESCE(rank,'')=? AND COALESCE(honor_points,0)=? "
                "AND COALESCE(total_honor_earned,0)=?",
                (honor_points, total_honor, user_id, expected_score, str(expected_rank), expected_honor, expected_total_honor),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", 0, expected_honor, expected_total_honor)
            if reset is not None:
                uow.execute(
                    "UPDATE player_data.arena SET daily_challenges_used=0,daily_extra_challenges=0,"
                    "daily_challenge_buys=0,last_reset_date=?,last_buy_date=? WHERE user_id=?",
                    (season_key, season_key, user_id),
                )
            for item_id, name, item_type, amount, bound in rewards:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=CURRENT_TIMESTAMP",
                    (user_id, item_id, name, item_type, amount, amount if bound else 0),
                )
            uow.execute(
                "INSERT INTO arena_season_reward_operations "
                "(operation_id,payload,season_key,user_id,honor,honor_points,total_honor_earned) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, payload, season_key, user_id, total_reward, honor_points, total_honor),
            )
            return self._result("applied", total_reward, honor_points, total_honor)


__all__ = ["ArenaSeasonRewardRepository", "ArenaSeasonRewardResult"]
