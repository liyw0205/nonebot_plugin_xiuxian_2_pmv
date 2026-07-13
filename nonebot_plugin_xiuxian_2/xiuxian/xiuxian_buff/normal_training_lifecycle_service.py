from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import increment_stat


@dataclass(frozen=True)
class NormalTrainingResult:
    status: str
    kind: str = ""
    create_time: str = ""
    exp_gain: int = 0
    stone_gain: int = 0
    hp_gain: int = 0
    mp_gain: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"started", "applied", "duplicate"}


class NormalTrainingLifecycleService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(values) -> str:
        return json.dumps(values, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _ensure_tables(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS normal_training_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
            "kind TEXT NOT NULL,create_time TEXT NOT NULL,scheduled_time TEXT NOT NULL,"
            "status TEXT NOT NULL,result_json TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def start(
        self,
        operation_id,
        user_id,
        *,
        kind,
        expected_exp,
        expected_stone,
        reward,
        exp_cap,
        power_multiplier,
        duration_seconds=60,
        now: datetime | None = None,
    ) -> NormalTrainingResult:
        operation_id, user_id, kind = str(operation_id).strip(), str(user_id), str(kind).strip()
        expected_exp, expected_stone, reward, exp_cap = (
            int(expected_exp), int(expected_stone), int(reward), int(exp_cap)
        )
        duration_seconds = int(duration_seconds)
        power_multiplier = float(power_multiplier)
        if not operation_id or kind not in {"cultivation", "mining"} or min(
            expected_exp, expected_stone, reward, exp_cap, duration_seconds
        ) < 0 or power_multiplier < 0:
            raise ValueError("invalid normal training lifecycle arguments")
        payload = self._payload([
            user_id, kind, expected_exp, expected_stone, reward, exp_cap,
            power_multiplier, duration_seconds,
        ])
        current = now or datetime.now()
        create_time = current.strftime("%Y-%m-%d %H:%M:%S.%f")
        scheduled_time = (current + timedelta(seconds=duration_seconds)).strftime("%Y-%m-%d %H:%M:%S.%f")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                previous = conn.execute(
                    "SELECT payload,kind,create_time,status,result_json FROM normal_training_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return NormalTrainingResult("operation_conflict")
                    saved = json.loads(str(previous[4])) if previous[4] else {}
                    return NormalTrainingResult(
                        "duplicate" if str(previous[3]) == "applied" else "started",
                        str(previous[1]), str(previous[2]), int(saved.get("exp_gain", 0)),
                        int(saved.get("stone_gain", 0)), int(saved.get("hp_gain", 0)),
                        int(saved.get("mp_gain", 0)),
                    )
                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return NormalTrainingResult("user_missing")
                if int(cd[0]) != 0:
                    conn.rollback()
                    return NormalTrainingResult("state_changed")
                if (int(user[0]), int(user[1])) != (expected_exp, expected_stone):
                    conn.rollback()
                    return NormalTrainingResult("state_changed")
                changed = conn.execute(
                    "UPDATE user_cd SET type=5,create_time=%s,scheduled_time=%s WHERE user_id=%s AND COALESCE(type,0)=0",
                    (create_time, scheduled_time, user_id),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NormalTrainingResult("state_changed")
                conn.execute(
                    "INSERT INTO normal_training_operations "
                    "(operation_id,user_id,payload,kind,create_time,scheduled_time,status) VALUES (%s,%s,%s,%s,%s,%s,'pending')",
                    (operation_id, user_id, payload, kind, create_time, scheduled_time),
                )
                conn.commit()
                return NormalTrainingResult("started", kind, create_time)
            except Exception:
                conn.rollback()
                raise

    def complete(self, operation_id, *, task_period: str) -> NormalTrainingResult:
        operation_id = str(operation_id).strip()
        task_period = str(task_period).strip()
        if not operation_id or not task_period:
            raise ValueError("operation and task period are required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                row = conn.execute(
                    "SELECT user_id,payload,kind,create_time,status,result_json FROM normal_training_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return NormalTrainingResult("operation_missing")
                user_id, payload, kind, create_time, status = map(str, row[:5])
                if status == "applied":
                    conn.rollback()
                    saved = json.loads(str(row[5]))
                    return NormalTrainingResult("duplicate", kind, create_time, **saved)
                values = json.loads(payload)
                expected_exp, expected_stone, reward, exp_cap = map(int, values[2:6])
                power_multiplier = float(values[6])
                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0),COALESCE(hp,0),COALESCE(mp,0) "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                cd = conn.execute("SELECT type,create_time FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return NormalTrainingResult("user_missing")
                if (int(user[0]), int(user[1])) != (expected_exp, expected_stone) or int(cd[0] or 0) != 5 or str(cd[1]) != create_time:
                    conn.rollback()
                    return NormalTrainingResult("state_changed", kind, create_time)

                exp_gain = min(reward, max(0, exp_cap - expected_exp)) if kind == "cultivation" else 0
                stone_gain = reward if kind == "mining" else 0
                hp_gain = mp_gain = 0
                if kind == "cultivation":
                    new_exp = expected_exp + exp_gain
                    old_hp, old_mp = int(user[2]), int(user[3])
                    new_hp = min(new_exp // 2, old_hp + expected_exp // 10)
                    new_mp = min(new_exp, old_mp + expected_exp // 20)
                    hp_gain, mp_gain = max(0, new_hp - old_hp), max(0, new_mp - old_mp)
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET exp=%s,hp=%s,mp=%s,atk=%s,power=ROUND(%s*%s,0) "
                        "WHERE user_id=%s AND COALESCE(exp,0)=%s AND COALESCE(stone,0)=%s",
                        (new_exp, new_hp, new_mp, expected_exp // 10, new_exp, power_multiplier,
                         user_id, expected_exp, expected_stone),
                    )
                    increment_stat(conn, user_id, "修炼次数", 1)
                    increment_stat(conn, user_id, "修炼修为", exp_gain)
                    self._increment_training_task(conn, user_id, task_period)
                else:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s AND COALESCE(exp,0)=%s AND COALESCE(stone,0)=%s",
                        (stone_gain, user_id, expected_exp, expected_stone),
                    )
                    increment_stat(conn, user_id, "凡人挖矿次数", 1)
                    increment_stat(conn, user_id, "灵石获取", stone_gain)
                cleared = conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=5 AND CAST(create_time AS TEXT)=%s",
                    (user_id, create_time),
                )
                if changed.rowcount != 1 or cleared.rowcount != 1:
                    conn.rollback()
                    return NormalTrainingResult("state_changed", kind, create_time)
                saved = {"exp_gain": exp_gain, "stone_gain": stone_gain, "hp_gain": hp_gain, "mp_gain": mp_gain}
                conn.execute(
                    "UPDATE normal_training_operations SET status='applied',result_json=%s WHERE operation_id=%s AND status='pending'",
                    (json.dumps(saved, separators=(",", ":")), operation_id),
                )
                conn.commit()
                return NormalTrainingResult("applied", kind, create_time, **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    @staticmethod
    def _increment_training_task(conn, user_id: str, task_period: str) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.xiuxian_tasks (user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(xiuxian_tasks)").fetchall()}
        for field in ("weekly_period", "weekly_progress"):
            if field not in columns:
                conn.execute(f"ALTER TABLE player_data.xiuxian_tasks ADD COLUMN {field} TEXT")
        row = conn.execute(
            "SELECT weekly_period,weekly_progress FROM player_data.xiuxian_tasks WHERE user_id=%s", (user_id,)
        ).fetchone()
        progress = {}
        if row is not None and str(row[0] or "") == task_period:
            try:
                progress = json.loads(str(row[1] or "{}"))
            except (TypeError, ValueError):
                progress = {}
        progress["weekly_out_closing"] = min(7200, int(progress.get("weekly_out_closing", 0) or 0) + 1)
        encoded = json.dumps(progress, ensure_ascii=False, separators=(",", ":"))
        changed = conn.execute(
            "UPDATE player_data.xiuxian_tasks SET weekly_period=%s,weekly_progress=%s WHERE user_id=%s",
            (task_period, encoded, user_id),
        )
        if changed.rowcount == 0:
            conn.execute(
                "INSERT INTO player_data.xiuxian_tasks (user_id,weekly_period,weekly_progress) VALUES (%s,%s,%s)",
                (user_id, task_period, encoded),
            )


__all__ = ["NormalTrainingLifecycleService", "NormalTrainingResult"]
