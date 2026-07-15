from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from .past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    canonical,
    encode_field,
    normalize_state,
)
from datetime import datetime
from .past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    canonical,
    encode_field,
    new_default_state,
    normalize_state,
)
from .past_life_state import (
    INTEGER_FIELDS,
    JSON_FIELDS,
    PAST_LIFE_FIELDS,
    canonical as _canonical,
    normalize_state,
)

IMMUTABLE_RUN_FIELDS = (
    "alloc",
    "talent",
    "birth_scenario",
    "event_indices",
    "event_snapshots",
    "last_run_time",
    "total_runs",
    "best_ending",
    "best_score",
    "endings_log",
    "achievement_points",
)

@dataclass(frozen=True)
class PastLifeChoiceResult:
    status: str
    response: dict = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PastLifeChoiceService:
    """Atomically advance a non-terminal past-life stage and journal its reply."""

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
    def ensure_operation_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_choice_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "payload TEXT NOT NULL,response_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def record_result(
        cls,
        conn,
        operation_id: str,
        user_id: str,
        kind: str,
        payload: str,
        response: dict,
    ) -> str:
        cls.ensure_operation_schema(conn)
        previous = conn.execute(
            "SELECT user_id,kind,payload,response_json "
            "FROM past_life_choice_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        response_json = canonical(response)
        if previous is not None:
            if (
                str(previous[0]) == str(user_id)
                and str(previous[1]) == str(kind)
                and str(previous[2]) == str(payload)
                and str(previous[3]) == response_json
            ):
                return "duplicate"
            return "operation_conflict"
        conn.execute(
            "INSERT INTO past_life_choice_operations("
            "operation_id,user_id,kind,payload,response_json) VALUES(%s,%s,%s,%s,%s)",
            (operation_id, str(user_id), str(kind), str(payload), response_json),
        )
        return "applied"

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(past_life)").fetchall()
        }
        for field_name in PAST_LIFE_FIELDS:
            if field_name not in columns:
                data_type = "INTEGER" if field_name in INTEGER_FIELDS else "TEXT"
                conn.execute(
                    "ALTER TABLE player_data.past_life ADD COLUMN "
                    f"{db_backend.quote_ident(field_name)} {data_type} DEFAULT NULL"
                )
        cls.ensure_operation_schema(conn)

    @staticmethod
    def _read_state(conn, user_id: str) -> dict | None:
        cursor = conn.execute(
            "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [str(column[0]) for column in cursor.description]
        return normalize_state(dict(zip(columns, row)))

    @staticmethod
    def _write_state(conn, user_id: str, state: dict) -> None:
        assignments = ",".join(
            f"{db_backend.quote_ident(name)}=%s" for name in PAST_LIFE_FIELDS
        )
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        changed = conn.execute(
            f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
            (*values, user_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("past life state disappeared during choice settlement")

    def get_result(
        self, operation_id, user_id=None
    ) -> PastLifeChoiceResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self.ensure_operation_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT user_id,response_json FROM past_life_choice_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            if user_id is not None and str(row[0]) != str(user_id):
                return PastLifeChoiceResult("operation_conflict")
            return PastLifeChoiceResult("duplicate", json.loads(str(row[1])))

    def advance(
        self,
        operation_id,
        user_id,
        choice_idx,
        expected_state,
        final_state,
        response,
    ) -> PastLifeChoiceResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        choice_idx = int(choice_idx)
        expected = normalize_state(expected_state)
        final = normalize_state(final_state)
        response = dict(response)
        if (
            not operation_id
            or not user_id
            or choice_idx <= 0
            or not response.get("message")
            or response.get("is_end") is not False
            or int(expected["state"]) != 2
            or int(final["state"]) != 2
            or int(final["stage"]) != int(expected["stage"]) + 1
            or int(final["revision"]) != int(expected["revision"]) + 1
        ):
            raise ValueError("valid non-terminal past life choice is required")
        for field_name in IMMUTABLE_RUN_FIELDS:
            if canonical(final[field_name]) != canonical(expected[field_name]):
                raise ValueError(f"past life choice changed immutable field: {field_name}")

        payload = canonical(
            {
                "user_id": user_id,
                "choice_idx": choice_idx,
                "expected": expected,
                "final": final,
            }
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
                    "SELECT user_id,response_json FROM past_life_choice_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id:
                        return PastLifeChoiceResult("operation_conflict")
                    return PastLifeChoiceResult(
                        "duplicate", json.loads(str(previous[1]))
                    )

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                current = self._read_state(conn, user_id)
                if user is None or current is None:
                    conn.rollback()
                    return PastLifeChoiceResult("user_missing")
                if canonical(current) != canonical(expected):
                    conn.rollback()
                    return PastLifeChoiceResult("state_changed")

                self._write_state(conn, user_id, final)
                recorded = self.record_result(
                    conn,
                    operation_id,
                    user_id,
                    "advance",
                    payload,
                    response,
                )
                if recorded != "applied":
                    conn.rollback()
                    return PastLifeChoiceResult(recorded)
                conn.commit()
                return PastLifeChoiceResult("applied", response)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

STATISTICS_FIELD = "前尘往事次数"
@dataclass(frozen=True)
class PastLifeStartResult:
    status: str
    message: str = ""
    choices_count: int = 0
    alloc: dict = field(default_factory=dict)
    talent: str = ""
    birth_scenario: str = ""
    revision: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PastLifeStartService:
    """Atomically create one frozen past-life run and its derived statistic."""

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
            "CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(past_life)").fetchall()
        }
        for field_name in PAST_LIFE_FIELDS:
            if field_name not in columns:
                data_type = "INTEGER" if field_name in INTEGER_FIELDS else "TEXT"
                conn.execute(
                    "ALTER TABLE player_data.past_life ADD COLUMN "
                    f"{db_backend.quote_ident(field_name)} {data_type} DEFAULT NULL"
                )

        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)"
        )
        statistics_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()
        }
        if STATISTICS_FIELD not in statistics_columns:
            conn.execute(
                "ALTER TABLE player_data.statistics ADD COLUMN "
                f"{db_backend.quote_ident(STATISTICS_FIELD)} INTEGER DEFAULT 0"
            )

        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_start_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _encode_result(result: PastLifeStartResult) -> str:
        return json.dumps(
            asdict(result), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _decode_result(status: str, raw: str) -> PastLifeStartResult:
        value = json.loads(raw)
        value["status"] = status
        return PastLifeStartResult(**value)

    @staticmethod
    def _parse_time(value) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _read_state(conn, user_id: str) -> tuple[dict, bool]:
        cursor = conn.execute(
            "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return new_default_state(), False
        columns = [str(column[0]) for column in cursor.description]
        return normalize_state(dict(zip(columns, row))), True

    @staticmethod
    def _write_state(conn, user_id: str, state: dict, exists: bool) -> None:
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        if exists:
            assignments = ",".join(
                f"{db_backend.quote_ident(name)}=%s" for name in PAST_LIFE_FIELDS
            )
            conn.execute(
                f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
                (*values, user_id),
            )
            return

        fields = ("user_id", *PAST_LIFE_FIELDS)
        conn.execute(
            "INSERT INTO player_data.past_life("
            + ",".join(db_backend.quote_ident(name) for name in fields)
            + ") VALUES("
            + ",".join("%s" for _ in fields)
            + ")",
            (user_id, *values),
        )

    @staticmethod
    def _increment_statistic(conn, user_id: str) -> None:
        field_sql = db_backend.quote_ident(STATISTICS_FIELD)
        changed = conn.execute(
            f"UPDATE player_data.statistics SET {field_sql}=COALESCE({field_sql},0)+1 "
            "WHERE user_id=%s",
            (user_id,),
        )
        if changed.rowcount == 0:
            conn.execute(
                f"INSERT INTO player_data.statistics(user_id,{field_sql}) VALUES(%s,1)",
                (user_id,),
            )

    def get_result(
        self, operation_id, user_id=None
    ) -> PastLifeStartResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS past_life_start_operations("
                "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
                "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.commit()
            row = conn.execute(
                "SELECT user_id,result_json FROM past_life_start_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            if user_id is not None and str(row[0]) != str(user_id):
                return PastLifeStartResult("operation_conflict")
            return self._decode_result("duplicate", str(row[1]))

    def start(
        self,
        operation_id,
        user_id,
        expected_state,
        *,
        alloc,
        accumulated,
        talent,
        birth_scenario,
        event_indices,
        event_snapshots,
        first_stage_message,
        choices_count,
        refresh_slot_start,
    ) -> PastLifeStartResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        alloc = dict(alloc)
        accumulated = dict(accumulated)
        talent = str(talent)
        birth_scenario = str(birth_scenario)
        event_indices = [int(value) for value in event_indices]
        event_snapshots = list(event_snapshots)
        first_stage_message = str(first_stage_message)
        choices_count = int(choices_count)
        slot_start = self._parse_time(refresh_slot_start)
        if (
            not operation_id
            or not user_id
            or not talent
            or not birth_scenario
            or not first_stage_message
            or choices_count <= 0
            or slot_start is None
            or not event_indices
            or len(event_indices) != len(event_snapshots)
        ):
            raise ValueError("complete past life start plan is required")

        expected = normalize_state(expected_state)
        plan = {
            "alloc": alloc,
            "accumulated": accumulated,
            "talent": talent,
            "birth_scenario": birth_scenario,
            "event_indices": event_indices,
            "event_snapshots": event_snapshots,
        }
        payload = canonical(
            {
                "user_id": user_id,
                "plan": plan,
                "message": first_stage_message,
                "choices_count": choices_count,
                "refresh_slot_start": slot_start.strftime("%Y-%m-%d %H:%M:%S"),
            }
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
                    "SELECT user_id,result_json FROM past_life_start_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id:
                        return PastLifeStartResult("operation_conflict")
                    return self._decode_result("duplicate", str(previous[1]))

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return PastLifeStartResult("user_missing")

                current, exists = self._read_state(conn, user_id)
                if int(current["state"]) not in {0, 1}:
                    conn.rollback()
                    return PastLifeStartResult("already_started")

                last_run = self._parse_time(current.get("last_run_time"))
                if last_run is not None and last_run >= slot_start:
                    conn.rollback()
                    return PastLifeStartResult("cooldown")

                if canonical(current) != canonical(expected):
                    conn.rollback()
                    return PastLifeStartResult("state_changed")

                persisted = dict(current)
                persisted.update(plan)
                persisted.update(
                    {
                        "state": 2,
                        "stage": 0,
                        "revision": int(current.get("revision", 0) or 0) + 1,
                        "total_score": 0,
                        "score_breakdown": {},
                        "early_death_rolls": {},
                        "history": [],
                    }
                )
                self._write_state(conn, user_id, persisted, exists)
                self._increment_statistic(conn, user_id)

                result = PastLifeStartResult(
                    "applied",
                    first_stage_message,
                    choices_count,
                    alloc,
                    talent,
                    birth_scenario,
                    int(persisted["revision"]),
                )
                conn.execute(
                    "INSERT INTO past_life_start_operations("
                    "operation_id,user_id,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (operation_id, user_id, payload, self._encode_result(result)),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class PastLifeFinalSettlementResult:
    status: str
    rewards: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PastLifeFinalSettlementService:
    def __init__(self, game_db, player_db, lock=None, max_goods_num=1000):
        self.game_db = Path(game_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()
        self.max_goods_num = max(1, int(max_goods_num))

    def _ensure_schema(self, conn):
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(past_life)").fetchall()
        }
        for field in PAST_LIFE_FIELDS:
            if field not in columns:
                data_type = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                conn.execute(
                    f"ALTER TABLE player_data.past_life ADD COLUMN "
                    f"{db_backend.quote_ident(field)} {data_type} DEFAULT NULL"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_final_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        operation_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(past_life_final_operations)").fetchall()
        }
        if "user_id" not in operation_columns:
            conn.execute(
                "ALTER TABLE past_life_final_operations ADD COLUMN user_id TEXT"
            )

    def settle(
        self,
        operation_id,
        user_id,
        expected_state,
        final_state,
        ending_name,
        score,
        exp_reward,
        stone_reward,
        achievement_points,
        item_reward=None,
        completed_at=None,
        choice_response=None,
    ) -> PastLifeFinalSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        score = int(score)
        exp_reward = int(exp_reward)
        stone_reward = int(stone_reward)
        achievement_points = int(achievement_points)
        if not operation_id or min(score, exp_reward, stone_reward, achievement_points) < 0:
            raise ValueError("invalid past life final settlement")

        item = None
        if item_reward:
            item = {
                "id": int(item_reward["id"]),
                "name": str(item_reward["name"]),
                "type": str(item_reward["type"]),
                "num": max(0, int(item_reward.get("num", 1))),
            }
        completed_at = str(completed_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        choice_response = None if choice_response is None else dict(choice_response)
        expected_snapshot = normalize_state(expected_state)
        payload = _canonical({
            "user_id": user_id,
            "expected": expected_snapshot,
            "ending": str(ending_name),
            "score": score,
            "exp": exp_reward,
            "stone": stone_reward,
            "points": achievement_points,
            "item": item,
            "completed_at": completed_at,
            "choice_response": choice_response,
        })

        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT user_id,payload,result_json FROM past_life_final_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    old_user_id = old[0]
                    if old_user_id is None:
                        try:
                            old_user_id = json.loads(str(old[1])).get("user_id")
                        except (TypeError, ValueError):
                            old_user_id = None
                    if str(old_user_id) != user_id:
                        return PastLifeFinalSettlementResult("operation_conflict", {})
                    return PastLifeFinalSettlementResult("duplicate", json.loads(str(old[2])))

                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                row = conn.execute(
                    "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None or row is None:
                    conn.rollback()
                    return PastLifeFinalSettlementResult("user_missing", {})
                columns = [str(col[0]) for col in conn.execute(
                    "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
                ).description]
                current = normalize_state(
                    {columns[index]: value for index, value in enumerate(row)}
                )
                for field in PAST_LIFE_FIELDS:
                    if _canonical(current[field]) != _canonical(expected_snapshot[field]):
                        conn.rollback()
                        return PastLifeFinalSettlementResult("state_changed", {})

                persisted = dict(final_state)
                previous_runs = int(expected_state.get("total_runs", 0) or 0)
                previous_best = int(expected_state.get("best_score", 0) or 0)
                previous_points = int(expected_state.get("achievement_points", 0) or 0)
                endings_log = list(expected_state.get("endings_log", []) or [])
                endings_log.append({
                    "run_number": previous_runs + 1,
                    "name": str(ending_name),
                    "score": score,
                    "time": completed_at,
                })
                persisted.update({
                    "state": 0,
                    "last_run_time": completed_at,
                    "total_runs": previous_runs + 1,
                    "best_score": max(previous_best, score),
                    "best_ending": str(ending_name) if score > previous_best else expected_state.get("best_ending", ""),
                    "endings_log": endings_log[-10:],
                    "achievement_points": previous_points + achievement_points,
                })

                conn.execute(
                    "UPDATE user_xiuxian SET exp=COALESCE(exp,0)+%s,stone=COALESCE(stone,0)+%s "
                    "WHERE user_id=%s",
                    (exp_reward, stone_reward, user_id),
                )
                if item and item["num"]:
                    bag = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item["id"]),
                    ).fetchone()
                    if bag:
                        conn.execute(
                            "UPDATE back SET goods_name=%s,goods_type=%s,goods_num=MIN(COALESCE(goods_num,0)+%s,%s),"
                            "update_time=%s WHERE user_id=%s AND goods_id=%s",
                            (item["name"], item["type"], item["num"], self.max_goods_num,
                             completed_at, user_id, item["id"]),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) "
                            "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (user_id, item["id"], item["name"], item["type"],
                             min(item["num"], self.max_goods_num), completed_at, completed_at),
                        )

                assignments = ",".join(
                    f"{db_backend.quote_ident(field)}=%s" for field in PAST_LIFE_FIELDS
                )
                values = []
                for field in PAST_LIFE_FIELDS:
                    value = persisted.get(field)
                    values.append(_canonical(value) if field in JSON_FIELDS else value)
                conn.execute(
                    f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
                    (*values, user_id),
                )
                rewards = {
                    "exp": exp_reward, "stone": stone_reward, "points": achievement_points,
                    "item": item,
                }
                result_json = _canonical(rewards)
                conn.execute(
                    "INSERT INTO past_life_final_operations("
                    "operation_id,user_id,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (operation_id, user_id, payload, result_json),
                )
                if choice_response is not None:
                    choice_status = PastLifeChoiceService.record_result(
                        conn,
                        operation_id,
                        user_id,
                        "final",
                        payload,
                        choice_response,
                    )
                    if choice_status != "applied":
                        conn.rollback()
                        return PastLifeFinalSettlementResult(choice_status, {})
                conn.commit()
                return PastLifeFinalSettlementResult("applied", rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                try:
                    conn.execute("DETACH DATABASE player_data")
                except Exception:
                    pass

@dataclass(frozen=True)
class PastLifeResetResult:
    status: str
    operation_id: str
    mode: str = ""
    clear_history: bool = False
    task_status: str = ""
    user_id: str = ""
    total: int = 0
    processed: int = 0
    applied: int = 0
    conflicted: int = 0
    missing: int = 0
    last_error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "created", "resumed", "duplicate"}

    @property
    def complete(self) -> bool:
        return self.task_status == "completed"

class PastLifeResetService:
    """Reset one player atomically or a frozen all-player set in resumable chunks."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(past_life)").fetchall()
        }
        for field_name in PAST_LIFE_FIELDS:
            if field_name not in columns:
                data_type = "INTEGER" if field_name in INTEGER_FIELDS else "TEXT"
                conn.execute(
                    "ALTER TABLE player_data.past_life ADD COLUMN "
                    f"{db_backend.quote_ident(field_name)} {data_type} DEFAULT NULL"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_reset_operations("
            "operation_id TEXT PRIMARY KEY,mode TEXT NOT NULL,user_id TEXT NOT NULL DEFAULT '',"
            "clear_history INTEGER NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,"
            "total INTEGER NOT NULL DEFAULT 0,processed INTEGER NOT NULL DEFAULT 0,"
            "applied INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
            "missing INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_reset_targets("
            "operation_id TEXT NOT NULL,ordinal INTEGER NOT NULL,user_id TEXT NOT NULL,"
            "expected_json TEXT NOT NULL,final_json TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',error_text TEXT NOT NULL DEFAULT '',"
            "updated_at TEXT NOT NULL,PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _read_state(conn, user_id: str) -> tuple[dict, bool]:
        cursor = conn.execute(
            "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return new_default_state(), False
        columns = [str(column[0]) for column in cursor.description]
        return normalize_state(dict(zip(columns, row))), True

    @staticmethod
    def _write_state(conn, user_id: str, state: dict, exists: bool) -> None:
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        if exists:
            assignments = ",".join(
                f"{db_backend.quote_ident(name)}=%s" for name in PAST_LIFE_FIELDS
            )
            changed = conn.execute(
                f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
                (*values, user_id),
            )
            if changed.rowcount != 1:
                raise RuntimeError("past life reset target disappeared")
            return
        fields = ("user_id", *PAST_LIFE_FIELDS)
        conn.execute(
            "INSERT INTO player_data.past_life("
            + ",".join(db_backend.quote_ident(name) for name in fields)
            + ") VALUES("
            + ",".join("%s" for _ in fields)
            + ")",
            (user_id, *values),
        )

    @staticmethod
    def _reset_state(current: dict, clear_history: bool) -> dict:
        default = new_default_state()
        final = dict(current)
        for field_name in (
            "state",
            "stage",
            "alloc",
            "accumulated",
            "talent",
            "birth_scenario",
            "total_score",
            "score_breakdown",
            "event_indices",
            "event_snapshots",
            "early_death_rolls",
            "history",
            "last_run_time",
        ):
            final[field_name] = default[field_name]
        final["revision"] = int(current.get("revision", 0) or 0) + 1
        if clear_history:
            for field_name in (
                "total_runs",
                "best_ending",
                "best_score",
                "endings_log",
                "achievement_points",
            ):
                final[field_name] = default[field_name]
        return normalize_state(final)

    @staticmethod
    def _payload(mode: str, clear_history: bool, user_id: str = "") -> str:
        return canonical(
            {
                "mode": str(mode),
                "clear_history": bool(clear_history),
                "user_id": str(user_id),
            }
        )

    @staticmethod
    def _result(conn, operation_id: str, status: str) -> PastLifeResetResult:
        row = conn.execute(
            "SELECT mode,clear_history,status,user_id,total,processed,applied,"
            "conflicted,missing,last_error FROM past_life_reset_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return PastLifeResetResult(status, operation_id)
        return PastLifeResetResult(
            status=status,
            operation_id=operation_id,
            mode=str(row[0]),
            clear_history=bool(int(row[1])),
            task_status=str(row[2]),
            user_id=str(row[3] or ""),
            total=int(row[4]),
            processed=int(row[5]),
            applied=int(row[6]),
            conflicted=int(row[7]),
            missing=int(row[8]),
            last_error=str(row[9] or ""),
        )

    def reset_one(
        self,
        operation_id,
        user_id,
        clear_history=False,
        *,
        updated_at=None,
    ) -> PastLifeResetResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        clear_history = bool(clear_history)
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = self._payload("single", clear_history, user_id)

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
                    "SELECT payload FROM past_life_reset_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    result = self._result(
                        conn,
                        operation_id,
                        "duplicate" if str(previous[0]) == payload else "operation_conflict",
                    )
                    conn.rollback()
                    return result

                current, exists = self._read_state(conn, user_id)
                final = self._reset_state(current, clear_history)
                self._write_state(conn, user_id, final, exists)
                conn.execute(
                    "INSERT INTO past_life_reset_operations("
                    "operation_id,mode,user_id,clear_history,payload,status,total,processed,"
                    "applied,created_at,updated_at) VALUES(%s,'single',%s,%s,%s,'completed',1,1,1,%s,%s)",
                    (
                        operation_id,
                        user_id,
                        int(clear_history),
                        payload,
                        updated_at,
                        updated_at,
                    ),
                )
                conn.commit()
                return self._result(conn, operation_id, "applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    def create_all(
        self,
        operation_id,
        clear_history=False,
        *,
        updated_at=None,
    ) -> PastLifeResetResult:
        operation_id = str(operation_id).strip()
        clear_history = bool(clear_history)
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id:
            raise ValueError("operation_id is required")
        payload = self._payload("all", clear_history)

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
                    "SELECT payload,status FROM past_life_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    if str(previous[0]) != payload:
                        status = "operation_conflict"
                    else:
                        status = "duplicate" if str(previous[1]) == "completed" else "resumed"
                    result = self._result(conn, operation_id, status)
                    conn.rollback()
                    return result

                running = conn.execute(
                    "SELECT operation_id,clear_history FROM past_life_reset_operations "
                    "WHERE mode='all' AND status='running' ORDER BY created_at LIMIT 1"
                ).fetchone()
                if running is not None:
                    running_id = str(running[0])
                    status = (
                        "resumed"
                        if bool(int(running[1])) == clear_history
                        else "operation_conflict"
                    )
                    result = self._result(conn, running_id, status)
                    conn.rollback()
                    return result

                cursor = conn.execute(
                    "SELECT * FROM player_data.past_life ORDER BY user_id"
                )
                columns = [str(column[0]) for column in cursor.description]
                users = []
                for row in cursor.fetchall():
                    value = dict(zip(columns, row))
                    user_id = str(value.pop("user_id"))
                    current = normalize_state(value)
                    users.append(
                        (
                            user_id,
                            canonical(current),
                            canonical(self._reset_state(current, clear_history)),
                        )
                    )
                task_status = "completed" if not users else "running"
                conn.execute(
                    "INSERT INTO past_life_reset_operations("
                    "operation_id,mode,clear_history,payload,status,total,created_at,updated_at) "
                    "VALUES(%s,'all',%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        int(clear_history),
                        payload,
                        task_status,
                        len(users),
                        updated_at,
                        updated_at,
                    ),
                )
                conn.executemany(
                    "INSERT INTO past_life_reset_targets("
                    "operation_id,ordinal,user_id,expected_json,final_json,updated_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    [
                        (operation_id, ordinal, *user, updated_at)
                        for ordinal, user in enumerate(users)
                    ],
                )
                conn.commit()
                return self._result(conn, operation_id, "created")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    def run_batch(
        self,
        operation_id,
        *,
        batch_size=500,
        updated_at=None,
    ) -> PastLifeResetResult:
        operation_id = str(operation_id).strip()
        batch_size = max(1, int(batch_size))
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id:
            raise ValueError("operation_id is required")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT mode,status FROM past_life_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if operation is None:
                    conn.rollback()
                    return PastLifeResetResult("not_found", operation_id)
                if str(operation[0]) != "all":
                    result = self._result(conn, operation_id, "operation_conflict")
                    conn.rollback()
                    return result
                if str(operation[1]) == "completed":
                    result = self._result(conn, operation_id, "duplicate")
                    conn.rollback()
                    return result

                targets = conn.execute(
                    "SELECT user_id,expected_json,final_json FROM past_life_reset_targets "
                    "WHERE operation_id=%s AND status='pending' ORDER BY ordinal LIMIT %s",
                    (operation_id, batch_size),
                ).fetchall()
                for target in targets:
                    user_id = str(target[0])
                    expected = normalize_state(json.loads(str(target[1])))
                    final = normalize_state(json.loads(str(target[2])))
                    current, exists = self._read_state(conn, user_id)
                    if not exists:
                        target_status, error_text = "missing", "state_missing"
                    elif canonical(current) != canonical(expected):
                        target_status, error_text = "conflict", "state_changed"
                    else:
                        self._write_state(conn, user_id, final, True)
                        target_status, error_text = "applied", ""
                    conn.execute(
                        "UPDATE past_life_reset_targets SET status=%s,error_text=%s,"
                        "updated_at=%s WHERE operation_id=%s AND user_id=%s "
                        "AND status='pending'",
                        (
                            target_status,
                            error_text,
                            updated_at,
                            operation_id,
                            user_id,
                        ),
                    )

                counts = conn.execute(
                    "SELECT COUNT(*),"
                    "COALESCE(SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='applied' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='conflict' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM past_life_reset_targets WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                task_status = "completed" if int(counts[5]) == 0 else "running"
                conn.execute(
                    "UPDATE past_life_reset_operations SET status=%s,total=%s,processed=%s,"
                    "applied=%s,conflicted=%s,missing=%s,last_error='',updated_at=%s "
                    "WHERE operation_id=%s",
                    (
                        task_status,
                        int(counts[0]),
                        int(counts[1]),
                        int(counts[2]),
                        int(counts[3]),
                        int(counts[4]),
                        updated_at,
                        operation_id,
                    ),
                )
                conn.commit()
                return self._result(conn, operation_id, "applied")
            except Exception as exc:
                conn.rollback()
                self._record_error(operation_id, str(exc), updated_at)
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    def _record_error(self, operation_id: str, error: str, updated_at: str) -> None:
        try:
            with closing(db_backend.connect(self._game_database)) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE past_life_reset_operations SET last_error=%s,updated_at=%s "
                    "WHERE operation_id=%s",
                    (error, updated_at, operation_id),
                )
                conn.commit()
        except Exception:
            pass

    def find_pending_all(self) -> PastLifeResetResult | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                row = conn.execute(
                    "SELECT operation_id FROM past_life_reset_operations "
                    "WHERE mode='all' AND status='running' ORDER BY created_at LIMIT 1"
                ).fetchone()
            except Exception:
                return None
            if row is None:
                return None
            return self._result(conn, str(row[0]), "resumed")

__all__ = [
    "PastLifeChoiceResult",
    "PastLifeChoiceService",
    "PastLifeStartResult",
    "PastLifeStartService",
    "PastLifeFinalSettlementResult",
    "PastLifeFinalSettlementService",
    "PastLifeResetResult",
    "PastLifeResetService",
]
