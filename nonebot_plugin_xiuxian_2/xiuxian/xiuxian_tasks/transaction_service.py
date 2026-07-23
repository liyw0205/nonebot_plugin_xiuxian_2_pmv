from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from datetime import date
from typing import Iterable, Mapping

from datetime import datetime
from typing import Iterable, Mapping
import json

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.numeric_bind import operation_payload_matches

@dataclass(frozen=True)
class TaskRewardClaimResult:
    status: str
    tasks: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TaskRewardClaimService:
    """Atomically claim every eligible task reward in selected cycles."""

    _cycles = ("daily", "weekly")

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
    def _ensure_game_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS task_reward_claim_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,"
            "stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )

    @classmethod
    def _ensure_player_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.xiuxian_tasks("
            "user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(xiuxian_tasks)"
            ).fetchall()
        }
        for cycle in cls._cycles:
            for suffix in ("period", "progress", "claimed"):
                field = f"{cycle}_{suffix}"
                if field not in columns:
                    conn.execute(
                        f"ALTER TABLE player_data.xiuxian_tasks "
                        f"ADD COLUMN {field} TEXT"
                    )

    @staticmethod
    def _decode(value, default):
        if value in (None, ""):
            return default
        try:
            decoded = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return default
        return decoded if isinstance(decoded, type(default)) else default

    @classmethod
    def _normalize_tasks(cls, tasks, cycles: tuple[str, ...]) -> tuple[dict, ...]:
        normalized = []
        seen = set()
        for raw in tasks:
            task = dict(raw)
            key = str(task.get("key", "")).strip()
            cycle = str(task.get("cycle", "")).strip()
            name = str(task.get("name", "")).strip()
            target = int(task.get("target", 0))
            rewards = dict(task.get("rewards") or {})
            unsupported = {
                reward_key
                for reward_key, value in rewards.items()
                if reward_key != "items" and int(value or 0) != 0
            }
            if unsupported:
                raise ValueError(
                    "unsupported task reward types: " + ",".join(sorted(unsupported))
                )
            items = []
            for raw_item in rewards.get("items", []) or []:
                item = dict(raw_item)
                normalized_item = {
                    "id": int(item.get("id", 0)),
                    "name": str(item.get("name", "")).strip(),
                    "type": str(item.get("type", "")).strip(),
                    "amount": int(item.get("amount", 0)),
                    "bind_flag": int(item.get("bind_flag", 1)),
                }
                if (
                    normalized_item["id"] <= 0
                    or not normalized_item["name"]
                    or not normalized_item["type"]
                    or normalized_item["amount"] <= 0
                    or normalized_item["bind_flag"] not in {0, 1}
                ):
                    raise ValueError("complete positive task item rewards are required")
                items.append(normalized_item)
            if (
                not key
                or not name
                or cycle not in cycles
                or target <= 0
                or (cycle, key) in seen
            ):
                raise ValueError("valid unique task definitions are required")
            seen.add((cycle, key))
            normalized.append(
                {
                    "key": key,
                    "cycle": cycle,
                    "name": name,
                    "target": target,
                    "items": items,
                }
            )
        return tuple(normalized)

    @staticmethod
    def _store_item(conn, user_id: str, item: dict, max_goods_num: int) -> None:
        row = conn.execute(
            "SELECT COALESCE(goods_num,0) FROM back "
            "WHERE user_id=%s AND goods_id=%s",
            (user_id, item["id"]),
        ).fetchone()
        previous = int(row[0]) if row else 0
        final = previous + int(item["amount"])
        if final > max_goods_num:
            raise OverflowError("task reward inventory is full")

        now = datetime.now()
        columns = set(conn.column_names("back"))
        if row is None:
            names = [
                "user_id",
                "goods_id",
                "goods_name",
                "goods_type",
                "goods_num",
                "create_time",
                "update_time",
            ]
            values = [
                user_id,
                item["id"],
                item["name"],
                item["type"],
                item["amount"],
                now,
                now,
            ]
            if "bind_num" in columns:
                names.append("bind_num")
                values.append(item["bound_amount"])
            conn.execute(
                f"INSERT INTO back({','.join(names)}) "
                f"VALUES({','.join(['%s'] * len(values))})",
                tuple(values),
            )
            return

        assignments = (
            "goods_name=%s,goods_type=%s,goods_num=%s,update_time=%s"
        )
        values = [item["name"], item["type"], final, now]
        if "bind_num" in columns and item["bound_amount"]:
            assignments += ",bind_num=COALESCE(bind_num,0)+%s"
            values.append(item["bound_amount"])
        values.extend((user_id, item["id"], previous))
        updated = conn.execute(
            f"UPDATE back SET {assignments} WHERE user_id=%s AND goods_id=%s "
            "AND COALESCE(goods_num,0)=%s",
            tuple(values),
        )
        if updated.rowcount != 1:
            raise RuntimeError("task reward inventory changed")

    def get_result(self, operation_id: str) -> TaskRewardClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_game_schema(conn)
            previous = conn.execute(
                "SELECT result_json FROM task_reward_claim_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return TaskRewardClaimResult(
                "duplicate", tuple(json.loads(str(previous[0])))
            )

    def claim(
        self,
        operation_id,
        user_id,
        cycles,
        periods,
        tasks,
        max_goods_num,
    ) -> TaskRewardClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        requested_cycles = set(cycles)
        cycles = tuple(cycle for cycle in self._cycles if cycle in requested_cycles)
        periods = {cycle: str(periods.get(cycle, "")).strip() for cycle in cycles}
        max_goods_num = int(max_goods_num)
        normalized_tasks = self._normalize_tasks(tasks, cycles)
        if (
            not operation_id
            or not user_id
            or not cycles
            or any(not periods[cycle] for cycle in cycles)
            or max_goods_num <= 0
        ):
            raise ValueError("valid task reward claim arguments are required")
        # Request identity only; periods/task defs/max bag are concurrency/outcome context.
        payload = json.dumps(
            [user_id, list(cycles)],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_game_schema(conn)
                self._ensure_player_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM task_reward_claim_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if not operation_payload_matches(previous[0], payload):
                        return TaskRewardClaimResult("operation_conflict")
                    return TaskRewardClaimResult(
                        "duplicate", tuple(json.loads(str(previous[1])))
                    )
                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return TaskRewardClaimResult("user_missing")

                fields = [
                    f"{cycle}_{suffix}"
                    for cycle in self._cycles
                    for suffix in ("period", "progress", "claimed")
                ]
                row = conn.execute(
                    "SELECT " + ",".join(fields) + " "
                    "FROM player_data.xiuxian_tasks WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO player_data.xiuxian_tasks(user_id) VALUES(%s)",
                        (user_id,),
                    )
                    values = {field: None for field in fields}
                else:
                    values = dict(zip(fields, row))

                states = {}
                eligible = []
                for cycle in cycles:
                    if str(values.get(f"{cycle}_period") or "") != periods[cycle]:
                        progress, claimed = {}, []
                    else:
                        progress = self._decode(values.get(f"{cycle}_progress"), {})
                        claimed = self._decode(values.get(f"{cycle}_claimed"), [])
                    claimed = [str(key) for key in claimed]
                    claimed_set = set(claimed)
                    for task in normalized_tasks:
                        if (
                            task["cycle"] == cycle
                            and int(progress.get(task["key"], 0) or 0) >= task["target"]
                            and task["key"] not in claimed_set
                        ):
                            eligible.append(task)
                            claimed.append(task["key"])
                            claimed_set.add(task["key"])
                    states[cycle] = (progress, claimed)

                totals = {}
                for task in eligible:
                    for item in task["items"]:
                        total = totals.get(item["id"])
                        if total is None:
                            total = dict(item)
                            total["bound_amount"] = (
                                item["amount"] if item["bind_flag"] else 0
                            )
                            totals[item["id"]] = total
                        else:
                            if (total["name"], total["type"]) != (
                                item["name"],
                                item["type"],
                            ):
                                raise ValueError("conflicting task item metadata")
                            total["amount"] += item["amount"]
                            if item["bind_flag"]:
                                total["bound_amount"] += item["amount"]
                for item in totals.values():
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item["id"]),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + item["amount"] > max_goods_num:
                        conn.rollback()
                        return TaskRewardClaimResult("inventory_full")

                for item in totals.values():
                    self._store_item(conn, user_id, item, max_goods_num)
                for cycle, (progress, claimed) in states.items():
                    conn.execute(
                        f"UPDATE player_data.xiuxian_tasks SET "
                        f"{cycle}_period=%s,{cycle}_progress=%s,{cycle}_claimed=%s "
                        "WHERE user_id=%s",
                        (
                            periods[cycle],
                            json.dumps(progress, ensure_ascii=False, separators=(",", ":")),
                            json.dumps(claimed, ensure_ascii=False, separators=(",", ":")),
                            user_id,
                        ),
                    )

                result_tasks = tuple(
                    {
                        "key": task["key"],
                        "cycle": task["cycle"],
                        "name": task["name"],
                        "items": task["items"],
                    }
                    for task in eligible
                )
                item_delta = [
                    {key: value for key, value in item.items() if key != "bound_amount"}
                    for item in totals.values()
                ]
                if result_tasks:
                    conn.execute(
                        "INSERT INTO economy_log("
                        "user_id,source,action,item_delta,detail,trace_id,created_at) "
                        "VALUES(%s,'xiuxian_task','claim_task_reward',%s,%s,%s,"
                        "CURRENT_TIMESTAMP)",
                        (
                            user_id,
                            json.dumps(item_delta, ensure_ascii=False, separators=(",", ":")),
                            json.dumps(
                                {"tasks": [task["key"] for task in result_tasks]},
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            operation_id,
                        ),
                    )
                result_json = json.dumps(
                    result_tasks,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO task_reward_claim_operations("
                    "operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return TaskRewardClaimResult("applied", result_tasks)
            except OverflowError:
                conn.rollback()
                return TaskRewardClaimResult("inventory_full")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class TaskProgressEventResult:
    status: str
    completed: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TaskProgressEventService:
    """Apply one gameplay event to every matching daily/weekly task atomically."""

    _cycles = ("daily", "weekly")

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiuxian_tasks(user_id TEXT PRIMARY KEY)"
        )
        columns = set(conn.column_names("xiuxian_tasks"))
        for cycle in cls._cycles:
            for suffix in ("period", "progress", "claimed"):
                field = f"{cycle}_{suffix}"
                if field not in columns:
                    conn.execute(
                        f"ALTER TABLE xiuxian_tasks ADD COLUMN "
                        f"{db_backend.quote_ident(field)} TEXT"
                    )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS task_progress_event_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _decode(value, default):
        if value in (None, ""):
            return default
        try:
            decoded = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return default
        return decoded if isinstance(decoded, type(default)) else default

    @staticmethod
    def _json(value) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _normalize_periods(
        cls, periods: Mapping[str, str], required_cycles: Iterable[str]
    ) -> dict[str, str]:
        required = set(required_cycles)
        normalized = {
            cycle: str(periods.get(cycle, "")).strip()
            for cycle in cls._cycles
            if cycle in required
        }
        if set(normalized) != required or any(not value for value in normalized.values()):
            raise ValueError("current task periods are required")
        return normalized

    @staticmethod
    def _normalize_events(events: Iterable[tuple[str, int]]) -> tuple[dict, ...]:
        normalized = []
        seen = set()
        for raw_key, raw_amount in events:
            key = str(raw_key).strip()
            amount = int(raw_amount)
            if not key or amount <= 0:
                raise ValueError("positive task event updates are required")
            if key in seen:
                raise ValueError("task event keys must be unique")
            seen.add(key)
            normalized.append({"key": key, "amount": amount})
        if not normalized:
            raise ValueError("at least one task event update is required")
        return tuple(normalized)

    @classmethod
    def _normalize_tasks(cls, tasks: Iterable[Mapping]) -> tuple[dict, ...]:
        normalized = []
        seen = set()
        for raw in tasks:
            task = dict(raw)
            key = str(task.get("key", "")).strip()
            cycle = str(task.get("cycle", "")).strip()
            name = str(task.get("name", "")).strip()
            target = int(task.get("target", 0))
            amount = int(task.get("amount", 0))
            identity = (cycle, key)
            if (
                not key
                or cycle not in cls._cycles
                or not name
                or target <= 0
                or amount <= 0
                or identity in seen
            ):
                raise ValueError("valid unique task updates are required")
            seen.add(identity)
            normalized.append(
                {
                    "key": key,
                    "cycle": cycle,
                    "name": name,
                    "target": target,
                    "amount": amount,
                }
            )
        return tuple(normalized)

    @classmethod
    def _load_row(cls, conn, user_id: str) -> dict[str, object]:
        fields = [
            f"{cycle}_{suffix}"
            for cycle in cls._cycles
            for suffix in ("period", "progress", "claimed")
        ]
        row = conn.execute(
            "SELECT " + ",".join(fields) + " FROM xiuxian_tasks WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO xiuxian_tasks(user_id) VALUES(%s)", (user_id,)
            )
            return {field: None for field in fields}
        return dict(zip(fields, row))

    @classmethod
    def _cycle_state(cls, values: Mapping[str, object], cycle: str, period: str):
        if str(values.get(f"{cycle}_period") or "") != period:
            return {}, []
        progress = cls._decode(values.get(f"{cycle}_progress"), {})
        claimed = cls._decode(values.get(f"{cycle}_claimed"), [])
        return progress, [str(key) for key in claimed]

    @classmethod
    def _write_states(cls, conn, user_id: str, states: Mapping[str, tuple[str, dict, list]]) -> None:
        if not states:
            return
        assignments = []
        parameters = []
        for cycle in cls._cycles:
            state = states.get(cycle)
            if state is None:
                continue
            period, progress, claimed = state
            assignments.extend(
                (
                    f"{cycle}_period=%s",
                    f"{cycle}_progress=%s",
                    f"{cycle}_claimed=%s",
                )
            )
            parameters.extend((period, cls._json(progress), cls._json(claimed)))
        parameters.append(user_id)
        conn.execute(
            "UPDATE xiuxian_tasks SET " + ",".join(assignments) + " WHERE user_id=%s",
            tuple(parameters),
        )

    def get_states(
        self, user_id: str, periods: Mapping[str, str]
    ) -> dict[str, tuple[dict[str, int], list[str], str]]:
        """Load selected cycles and reset every stale cycle in one transaction."""
        user_id = str(user_id).strip()
        selected = tuple(cycle for cycle in self._cycles if cycle in periods)
        normalized_periods = self._normalize_periods(periods, selected)
        if not user_id or not selected:
            raise ValueError("user and selected task periods are required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                values = self._load_row(conn, user_id)
                states = {}
                result = {}
                for cycle in selected:
                    period = normalized_periods[cycle]
                    progress, claimed = self._cycle_state(values, cycle, period)
                    states[cycle] = (period, progress, claimed)
                    result[cycle] = (progress, claimed, period)
                self._write_states(conn, user_id, states)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def record(
        self,
        operation_id: str,
        user_id: str,
        events: Iterable[tuple[str, int]],
        periods: Mapping[str, str],
        tasks: Iterable[Mapping],
    ) -> TaskProgressEventResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        normalized_events = self._normalize_events(events)
        normalized_tasks = self._normalize_tasks(tasks)
        cycles = tuple(
            cycle
            for cycle in self._cycles
            if any(task["cycle"] == cycle for task in normalized_tasks)
        )
        normalized_periods = self._normalize_periods(periods, cycles)
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")

        request = {"user_id": user_id, "events": list(normalized_events)}
        payload_data = {
            "request": request,
            "periods": normalized_periods,
            "tasks": normalized_tasks,
        }
        payload = self._json(payload_data)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM task_progress_event_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    try:
                        previous_payload = json.loads(str(previous[0]))
                        previous_result = json.loads(str(previous[1]))
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        raise RuntimeError("invalid stored task progress operation") from exc
                    if previous_payload.get("request") != request:
                        return TaskProgressEventResult("operation_conflict")
                    return TaskProgressEventResult(
                        "duplicate",
                        tuple(str(name) for name in previous_result.get("completed", [])),
                    )

                completed = []
                if normalized_tasks:
                    values = self._load_row(conn, user_id)
                    states = {}
                    for cycle in cycles:
                        period = normalized_periods[cycle]
                        progress, claimed = self._cycle_state(values, cycle, period)
                        claimed_set = set(claimed)
                        for task in normalized_tasks:
                            if task["cycle"] != cycle:
                                continue
                            old_value = max(0, int(progress.get(task["key"], 0) or 0))
                            new_value = min(old_value + task["amount"], task["target"])
                            if new_value != old_value:
                                progress[task["key"]] = new_value
                            if (
                                old_value < task["target"] <= new_value
                                and task["key"] not in claimed_set
                            ):
                                completed.append(task["name"])
                        states[cycle] = (period, progress, claimed)
                    self._write_states(conn, user_id, states)

                result_json = self._json({"completed": completed})
                conn.execute(
                    "INSERT INTO task_progress_event_operations("
                    "operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return TaskProgressEventResult("applied", tuple(completed))
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "TaskRewardClaimResult",
    "TaskRewardClaimService",
    "TaskProgressEventResult",
    "TaskProgressEventService",
]
