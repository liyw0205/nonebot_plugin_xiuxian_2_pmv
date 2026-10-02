from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class NormalTrainingCompleteResult:
    def __init__(self, status: str, kind: str = "", create_time: str = "", exp_gain: int = 0, stone_gain: int = 0, hp_gain: int = 0, mp_gain: int = 0) -> None:
        self.status, self.kind, self.create_time = status, kind, create_time
        self.exp_gain, self.stone_gain, self.hp_gain, self.mp_gain = exp_gain, stone_gain, hp_gain, mp_gain


class NormalTrainingCompleteSqlRepository:
    _GAME_COLUMNS = {
        "normal_training_operations": {
            "operation_id", "user_id", "payload", "kind", "create_time",
            "scheduled_time", "status", "result_json",
        },
        "user_xiuxian": {"user_id", "exp", "stone", "hp", "mp", "atk", "power"},
        "user_cd": {"user_id", "type", "create_time", "scheduled_time"},
    }
    _STAT_COLUMNS = {
        "cultivation": ("修炼次数", "修炼修为"),
        "mining": ("凡人挖矿次数", "灵石获取"),
    }

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f"PRAGMA {schema}.table_info({table})")
        }

    def complete(self, operation_id: str, task_period: str, user_id: str | None = None) -> NormalTrainingCompleteResult:
        if not operation_id or not task_period:
            raise ValueError("operation and task period are required")
        if not Path(self.game_database).is_file():
            return NormalTrainingCompleteResult("schema_missing")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            return self.complete_in_uow(uow, operation_id, task_period, user_id)

    def complete_in_uow(
        self,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        task_period: str,
        user_id: str | None,
    ) -> NormalTrainingCompleteResult:
        if not operation_id or not task_period:
            raise ValueError("operation and task period are required")
        for table, required in self._GAME_COLUMNS.items():
            if not required <= self._columns(uow, table):
                return NormalTrainingCompleteResult("schema_missing")
        row = uow.query_one(
            "SELECT user_id,payload,kind,create_time,status,result_json "
            "FROM normal_training_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return NormalTrainingCompleteResult("operation_missing")
        saved_user_id = str(row["user_id"])
        kind = str(row["kind"])
        create_time = str(row["create_time"])
        if (user_id is not None and str(user_id) != saved_user_id) or kind not in self._STAT_COLUMNS:
            return NormalTrainingCompleteResult("operation_conflict", kind, create_time)
        if str(row["status"]) == "applied":
            saved = json.loads(str(row["result_json"] or "{}"))
            return NormalTrainingCompleteResult("duplicate", kind, create_time, **saved)
        if str(row["status"]) != "pending":
            return NormalTrainingCompleteResult("state_changed", kind, create_time)
        values = json.loads(str(row["payload"]))
        expected_exp, expected_stone, reward, exp_cap = map(int, values[2:6])
        multiplier = float(values[6])
        user = uow.query_one(
            "SELECT COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone,"
            "COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp "
            "FROM user_xiuxian WHERE user_id=?",
            (saved_user_id,),
        )
        cd = uow.query_one(
            "SELECT type,create_time FROM user_cd WHERE user_id=?", (saved_user_id,)
        )
        if user is None or cd is None:
            return NormalTrainingCompleteResult("user_missing", kind, create_time)
        if (
            (int(user["exp"]), int(user["stone"])) != (expected_exp, expected_stone)
            or int(cd["type"] or 0) != 5
            or str(cd["create_time"]) != create_time
        ):
            return NormalTrainingCompleteResult("state_changed", kind, create_time)
        if not Path(self.player_database).is_file():
            return NormalTrainingCompleteResult("schema_missing", kind, create_time)
        uow.attach_database(self.player_database, "player_data")
        stat_columns = self._columns(uow, "statistics", "player_data")
        if not {"user_id", *self._STAT_COLUMNS[kind]} <= stat_columns:
            return NormalTrainingCompleteResult("schema_missing", kind, create_time)
        if kind == "cultivation":
            task_columns = self._columns(uow, "xiuxian_tasks", "player_data")
            if not {"user_id", "weekly_period", "weekly_progress"} <= task_columns:
                return NormalTrainingCompleteResult("schema_missing", kind, create_time)

        exp_gain = min(reward, max(0, exp_cap - expected_exp)) if kind == "cultivation" else 0
        stone_gain = reward if kind == "mining" else 0
        hp_gain = mp_gain = 0
        if kind == "cultivation":
            new_exp = expected_exp + exp_gain
            new_hp = min(new_exp // 2, int(user["hp"]) + expected_exp // 10)
            new_mp = min(new_exp, int(user["mp"]) + expected_exp // 20)
            hp_gain, mp_gain = max(0, new_hp - int(user["hp"])), max(0, new_mp - int(user["mp"]))
            changed = uow.execute(
                "UPDATE user_xiuxian SET exp=?,hp=?,mp=?,atk=?,power=ROUND(?*?,0) "
                "WHERE user_id=? AND COALESCE(exp,0)=? AND COALESCE(stone,0)=?",
                (new_exp, new_hp, new_mp, expected_exp // 10, new_exp, multiplier,
                 saved_user_id, expected_exp, expected_stone),
            )
        else:
            changed = uow.execute(
                "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? "
                "WHERE user_id=? AND COALESCE(exp,0)=? AND COALESCE(stone,0)=?",
                (stone_gain, saved_user_id, expected_exp, expected_stone),
            )
        if changed.rowcount != 1:
            raise RuntimeError("normal training player snapshot changed during settlement")

        stat_amounts = (
            ("修炼次数", 1), ("修炼修为", exp_gain)
        ) if kind == "cultivation" else (("凡人挖矿次数", 1), ("灵石获取", stone_gain))
        self._increment_statistics(uow, saved_user_id, stat_amounts)
        if kind == "cultivation":
            self._increment_weekly_task(uow, saved_user_id, task_period)

        cleared = uow.execute(
            "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
            "WHERE user_id=? AND type=5 AND CAST(create_time AS TEXT)=?",
            (saved_user_id, create_time),
        )
        if cleared.rowcount != 1:
            raise RuntimeError("normal training cooldown changed during settlement")
        saved = {
            "exp_gain": exp_gain, "stone_gain": stone_gain,
            "hp_gain": hp_gain, "mp_gain": mp_gain,
        }
        changed = uow.execute(
            "UPDATE normal_training_operations SET status='applied',result_json=? "
            "WHERE operation_id=? AND status='pending'",
            (json.dumps(saved, separators=(",", ":")), operation_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("normal training receipt changed during settlement")
        return NormalTrainingCompleteResult("applied", kind, create_time, **saved)

    @staticmethod
    def _increment_statistics(
        uow: DatabaseUnitOfWork, user_id: str, amounts: tuple[tuple[str, int], ...]
    ) -> None:
        fields = tuple(field for field, _ in amounts)
        assignments = ",".join(f'"{field}"=COALESCE("{field}",0)+?' for field in fields)
        changed = uow.execute(
            f"UPDATE player_data.statistics SET {assignments} WHERE user_id=?",
            tuple(amount for _, amount in amounts) + (user_id,),
        )
        if changed.rowcount == 0:
            names = ",".join(f'"{field}"' for field in fields)
            placeholders = ",".join("?" for _ in range(len(fields) + 1))
            uow.execute(
                f"INSERT INTO player_data.statistics(user_id,{names}) VALUES({placeholders})",
                (user_id,) + tuple(amount for _, amount in amounts),
            )

    @staticmethod
    def _increment_weekly_task(uow: DatabaseUnitOfWork, user_id: str, task_period: str) -> None:
        row = uow.query_one(
            "SELECT weekly_period,weekly_progress FROM player_data.xiuxian_tasks WHERE user_id=?",
            (user_id,),
        )
        progress = {}
        if row is not None and str(row["weekly_period"] or "") == task_period:
            try:
                progress = json.loads(str(row["weekly_progress"] or "{}"))
            except (TypeError, ValueError):
                progress = {}
        progress["weekly_out_closing"] = min(
            7200, int(progress.get("weekly_out_closing", 0) or 0) + 1
        )
        encoded = json.dumps(progress, ensure_ascii=False, separators=(",", ":"))
        changed = uow.execute(
            "UPDATE player_data.xiuxian_tasks SET weekly_period=?,weekly_progress=? WHERE user_id=?",
            (task_period, encoded, user_id),
        )
        if changed.rowcount == 0:
            uow.execute(
                "INSERT INTO player_data.xiuxian_tasks(user_id,weekly_period,weekly_progress) VALUES(?,?,?)",
                (user_id, task_period, encoded),
            )


__all__ = ["NormalTrainingCompleteSqlRepository", "NormalTrainingCompleteResult"]
