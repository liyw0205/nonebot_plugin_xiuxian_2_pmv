"""Bounded mixed-user real-matcher load on the existing synthetic fixture.

The producer has its own clock/thread. One event loop, runtime and database set
serve a complete phase; snapshots are restored only after every task drains.
Run this entrypoint in a fresh process, never alongside another validation.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from collections.abc import Mapping
from contextlib import AsyncExitStack, contextmanager
from contextvars import ContextVar
import fcntl
import gc
import hashlib
import importlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import command_matrix_probe as matrix
from scripts.mixed_rank_bulk_probe import measure_rank_generation_cost

MODES = ("pure", "readmostly", "mixed", "contention")
COUNTERS = ("hits", "misses", "waits", "expirations", "evictions",
            "invalidations", "oversized", "bypasses", "partial_hits")
RANK_OPERATIONS = ("count", "seek_batch", "plan")
RANK_OPERATION_FIELDS = ("statements", "execute_ms", "fetch_ms", "total_ms", "cpu_ms")
RANK_STATE_PREFIX = (
    "SELECT rank_state.exp_generation, rank_state.stone_generation, "
    "target.user_id, xiuxian_int_sort_key_compact(target.exp), "
    "xiuxian_int_sort_key_compact(target.stone) "
    "FROM xiuxian_numeric_rank_generation AS rank_state "
).upper()
PROFILE_DISPLAY_SQL = "SELECT * FROM USER_XIUXIAN WHERE USER_ID=? ORDER BY ROWID ASC LIMIT 1"
ACTIVE = ContextVar("mixed_command_request", default=None)
BACKGROUND_SQL = ContextVar("mixed_command_background_sql", default=None)
CHECKED_ATTACH = ContextVar("mixed_command_checked_attach", default=None)
SQL_CPU_START = ContextVar("mixed_command_sql_cpu_start", default=None)
SQL_DIAGNOSTIC = ContextVar("mixed_command_sql_diagnostic", default=None)
BOUND_ATTACH = re.compile(r'^\s*ATTACH\s+(?:DATABASE\s+)?\?\s+AS\s+(?:[a-zA-Z_]\w*|"(?:[^"]|"")+")\s*;?\s*$', re.IGNORECASE)
def bulk_probe(context, managers, fixture_identity):
    return measure_rank_generation_cost(context.paths.game_db)


BULK_PROBE = bulk_probe


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 3)


def distribution(values):
    return {name: percentile(values, fraction) for name, fraction in
            (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}


def rank_operation(sql):
    upper = matrix._normalized_sql(sql).upper()
    if upper.startswith("EXPLAIN QUERY PLAN"):
        return "plan" if "XIUXIAN_INT_SORT_KEY_COMPACT" in upper else None
    if "NUMERIC_RANK_KEYS" in upper:
        return "seek_batch"
    if any(f'COUNT(DISTINCT XIUXIAN_INT_SORT_KEY_COMPACT("{column}"))' in upper
           for column in ("EXP", "STONE")):
        return "count"
    return None


def database_path(value):
    value = os.fsdecode(value)
    if value == ":memory:" or value.startswith("file::memory:"):
        return None
    if value.startswith("file:"):
        from urllib.parse import unquote, urlsplit
        parsed = urlsplit(value)
        if parsed.netloc not in ("", "localhost"):
            raise ValueError("SQLite URI has a remote authority")
        value = unquote(parsed.path)
    if not value:
        raise ValueError("unnamed temporary database is outside the restore contract")
    return Path(value).resolve()


class ObserverCPU:
    """Bounded, exclusive thread-CPU counters for synchronous observer hooks."""

    BUCKETS = ("record", "begin_statement", "authorizer", "watch")
    MAX_THREADS = 16

    def __init__(self, clock=None):
        self.clock = clock if clock is not None else time.thread_time_ns
        self.local = threading.local()
        self.lock = threading.Lock()
        self.epoch = None
        self.bins = []
        self.overflow = None

    def start_phase(self):
        self.bins = []
        self.overflow = None
        self.epoch = object()

    def end_phase(self):
        self.epoch = None

    def _thread_state(self, epoch):
        state = getattr(self.local, "state", None)
        if state is not None and state[0] is epoch:
            return state
        with self.lock:
            if len(self.bins) < self.MAX_THREADS:
                bucket = {"thread": threading.current_thread().name,
                          "thread_ident": threading.get_ident(), "overflow": False,
                          "buckets": {name: {"calls": 0, "cpu_ns": 0} for name in self.BUCKETS}}
                self.bins.append(bucket)
            else:
                if self.overflow is None:
                    self.overflow = {"thread": "overflow", "thread_ident": None, "overflow": True,
                                     "buckets": {name: {"calls": 0, "cpu_ns": 0} for name in self.BUCKETS}}
                bucket = self.overflow
        state = (epoch, bucket, [])
        self.local.state = state
        return state

    @contextmanager
    def measure(self, bucket):
        if bucket not in self.BUCKETS:
            raise ValueError(f"unknown observer CPU bucket: {bucket}")
        epoch = self.epoch
        if epoch is None:
            yield
            return
        _, thread_bin, stack = self._thread_state(epoch)
        frame = [self.clock(), 0]
        stack.append(frame)
        try:
            yield
        finally:
            elapsed = self.clock() - frame[0]
            stack.pop()
            if stack:
                stack[-1][1] += elapsed
            exclusive = max(0, elapsed - frame[1])
            counter = thread_bin["buckets"][bucket]
            if thread_bin["overflow"]:
                with self.lock:
                    counter["calls"] += 1
                    counter["cpu_ns"] += exclusive
            else:
                counter["calls"] += 1
                counter["cpu_ns"] += exclusive

    def snapshot(self):
        # Call only after all measured threads drain and end_phase disables hooks.
        with self.lock:
            bins = self.bins + ([self.overflow] if self.overflow is not None else [])
            threads = [{**entry, "buckets": {key: dict(value) for key, value in entry["buckets"].items()}}
                       for entry in bins]
        totals = {name: {"calls": sum(entry["buckets"][name]["calls"] for entry in threads),
                         "cpu_ns": sum(entry["buckets"][name]["cpu_ns"] for entry in threads)}
                  for name in self.BUCKETS}
        return {"clock": "time.thread_time_ns", "exclusive": True,
                "max_thread_bins": self.MAX_THREADS + 1, "buckets": totals, "threads": threads}


class QueryRouter:
    """Per-request routing, including work dispatched to context-aware threads."""

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.connections = {}
        self.observed = {}
        self.allowed_databases = None
        self.unattributed = 0
        self.unattributed_details = {}
        self.message_database = None
        self.background = matrix.QueryRecorder()
        self.background.active = True
        self.background_errors = []
        self.phase_active = False
        self.lock = threading.Lock()
        self.connection_serial = 0
        self.observer_cpu = ObserverCPU()

    def connection_entry(self, connection):
        row = ACTIVE.get()
        diagnostic = getattr(connection, "_mixed_diagnostic", None)
        if row is None or diagnostic is None:
            return None
        serial = diagnostic["serial"]
        entries = row["_connection_entries"]
        if serial not in entries:
            if len(entries) >= 32:
                row["connection_diagnostic_omitted_touches"] += 1
                return None
            entry = {"connection": serial, "database": diagnostic["database"],
                     "opened_in_request": diagnostic["opened_by"] is row["_connection_owner"],
                     "open_ms": diagnostic["open_ms"] if diagnostic["opened_by"] is row["_connection_owner"] else 0.0,
                     "udf_calls": 0, "udf_ms": 0.0, "statements": [],
                     "statements_omitted": 0}
            entries[serial] = entry
            row["connection_diagnostics"].append(entry)
        return entries[serial]

    def begin_statement(self, connection, sql):
        with self.observer_cpu.measure("begin_statement"):
            return self._begin_statement(connection, sql)

    def _begin_statement(self, connection, sql):
        diagnostic = getattr(connection, "_mixed_diagnostic", None)
        if diagnostic is None:
            return
        diagnostic["ordinal"] += 1
        diagnostic["statement"] = None
        entry = self.connection_entry(connection)
        if entry is None:
            return
        if len(entry["statements"]) >= 12:
            entry["statements_omitted"] += 1
            return
        statement = {"ordinal": diagnostic["ordinal"],
                     "sql": matrix._normalized_sql(str(sql)), "authorizer_calls": 0,
                     "execute_ms": 0.0, "fetch_ms": 0.0, "cpu_ms": 0.0}
        entry["statements"].append(statement)
        diagnostic["statement"] = statement
        return statement

    def check_path(self, value, *, operation):
        path = database_path(value)
        if path is None:
            return None
        row = self.observed.setdefault(str(path), {"connects": 0, "attaches": 0})
        row[operation] += 1
        if not path.is_relative_to(self.root):
            self.deny(f"database outside isolated root: {path}")
        if self.allowed_databases is not None and path not in self.allowed_databases:
            self.deny(f"database is not covered by the sole restore snapshot: {path}")
        return path

    @staticmethod
    def deny(message):
        row = ACTIVE.get()
        if row is not None:
            row["blocked"].append(message)
        raise RuntimeError(message)

    def authorizer(self, action, first, second, database, trigger):
        if action == sqlite3.SQLITE_ATTACH:
            try:
                # SQLite authorizes a bound ATTACH before resolving its '?'.
                # Only our exact, prevalidated cursor statement may supply it.
                if first is None:
                    if CHECKED_ATTACH.get() is None:
                        self.deny("unvalidated bound ATTACH statement")
                else:
                    self.check_path(first, operation="attaches")
            except (OSError, ValueError, RuntimeError) as exc:
                row = ACTIVE.get()
                if row is not None and str(exc) not in row["blocked"]:
                    row["blocked"].append(str(exc))
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def record(self, connection, sql, phase, elapsed_ms):
        row = ACTIVE.get()
        ended = time.perf_counter()
        cpu_start = SQL_CPU_START.get()
        cpu_ms = (time.thread_time() - cpu_start) * 1000 if cpu_start is not None else None
        with self.observer_cpu.measure("record"):
            return self._record(connection, sql, phase, elapsed_ms, row, ended, cpu_ms)

    def _record(self, connection, sql, phase, elapsed_ms, row, ended, cpu_ms):
        with self.lock:
            if row is None:
                if not self.phase_active:
                    return
                database = self.connections.get(id(connection))
                if BACKGROUND_SQL.get() == "message_writer" and database == self.message_database:
                    self.background.connections = self.connections
                    self.background.record(connection, sql, phase, elapsed_ms)
                    return
                self.unattributed += 1
                key = (database, threading.current_thread().name, threading.get_ident(),
                       matrix._normalized_sql(str(sql)), phase)
                if key in self.unattributed_details or len(self.unattributed_details) < 50:
                    self.unattributed_details[key] = self.unattributed_details.get(key, 0) + 1
                return
            recorder = row["_recorder"]
            recorder.connections = self.connections
            recorder.record(connection, sql, phase, elapsed_ms)
            operation = rank_operation(sql)
            if operation is not None:
                row["_rank_cpu_ms"][operation] += cpu_ms or 0.0
            diagnostic = getattr(connection, "_mixed_diagnostic", None)
            statement = SQL_DIAGNOSTIC.get()
            if statement is not None:
                statement["execute_ms" if phase == "execute" else "fetch_ms"] += elapsed_ms
                statement["cpu_ms"] += cpu_ms or 0.0
            name = threading.current_thread().name
            totals = row["sql_threads"].setdefault(name, {"calls": 0, "wall_ms": 0.0, "cpu_ms": 0.0})
            totals["calls"] += phase == "execute"
            totals["wall_ms"] += elapsed_ms
            totals["cpu_ms"] += cpu_ms or 0.0
            if elapsed_ms >= 5 or "COUNT(DISTINCT" in str(sql).upper():
                if len(row["sql_timeline"]) < 64:
                    row["sql_timeline"].append({"start": ended - elapsed_ms / 1000, "end": ended,
                        "thread": name, "operation": phase, "cpu_ms": cpu_ms,
                        "wall_ms": elapsed_ms, "sql": matrix._normalized_sql(str(sql))})
                else:
                    row["sql_timeline_omitted"] += 1
            if phase == "execute" and str(sql).strip().upper().startswith("BEGIN"):
                row["transaction_begin_ms"].append(elapsed_ms)

    def failure(self, exc, sql):
        row = ACTIVE.get()
        code = getattr(exc, "sqlite_errorcode", None)
        error = {
            "code": code, "name": getattr(exc, "sqlite_errorname", None),
            "sql": matrix._normalized_sql(sql), "error": str(exc),
        }
        if row is None:
            if self.phase_active:
                with self.lock:
                    self.background_errors.append(error)
            return
        row["sql_errors"].append(error)
        if code is not None and code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            row["sqlite_busy_locked"] += 1


ROUTER = None


def prepare_message_writer(message_db, database):
    """Label the actual queue consumer without changing its jobs or execution."""
    if message_db._message_db_writer_started:
        raise RuntimeError("message writer started before SQL ownership instrumentation")
    original_loop = message_db._message_db_writer_loop
    ROUTER.message_database = str(Path(database).resolve())

    def writer_loop():
        token = BACKGROUND_SQL.set("message_writer")
        try:
            original_loop()
        finally:
            BACKGROUND_SQL.reset(token)

    message_db._message_db_writer_loop = writer_loop
    message_db.init_message_db()


def message_queue_state(message_db):
    queue = message_db._message_db_jobs
    with queue.mutex:
        return {"unfinished_jobs": queue.unfinished_tasks,
                "reserved_bytes": queue.reserved_bytes,
                "dropped_jobs": message_db._message_db_dropped_jobs}


async def drain_message_writer(message_db, *, timeout=5):
    started = time.perf_counter()
    while True:
        state = message_queue_state(message_db)
        if state["unfinished_jobs"] == 0 and state["reserved_bytes"] == 0:
            return {**state, "drain_ms": round((time.perf_counter() - started) * 1000, 3)}
        if time.perf_counter() - started >= timeout:
            raise RuntimeError(f"message writer did not drain before snapshot boundary: {state}")
        await asyncio.sleep(.005)


async def drain_rank_workers():
    module = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.exact_rank_reader")
    if module is not None:
        await module.drain_exact_rank_reads()


class MixedCursor(matrix.TimedCursor):
    @contextmanager
    def cpu_clock(self):
        token = SQL_CPU_START.set(time.thread_time())
        diagnostic_token = SQL_DIAGNOSTIC.set(getattr(self, "_mixed_statement", None))
        try:
            yield
        finally:
            SQL_DIAGNOSTIC.reset(diagnostic_token)
            SQL_CPU_START.reset(token)

    def execute(self, sql, parameters=()):
        token = None
        try:
            self._mixed_statement = ROUTER.begin_statement(self.connection, sql)
            if BOUND_ATTACH.fullmatch(str(sql)):
                if not isinstance(parameters, (tuple, list)) or len(parameters) != 1:
                    ROUTER.deny("bound ATTACH must supply exactly one database path")
                checked = ROUTER.check_path(parameters[0], operation="attaches")
                token = CHECKED_ATTACH.set(checked if checked is not None else ":memory:")
            with self.cpu_clock():
                return super().execute(sql, parameters)
        except sqlite3.Error as exc:
            ROUTER.failure(exc, sql)
            raise
        finally:
            if token is not None:
                CHECKED_ATTACH.reset(token)

    def executemany(self, sql, parameters):
        try:
            self._mixed_statement = ROUTER.begin_statement(self.connection, sql)
            with self.cpu_clock():
                return super().executemany(sql, parameters)
        except sqlite3.Error as exc:
            ROUTER.failure(exc, sql)
            raise

    def executescript(self, sql):
        try:
            self._mixed_statement = ROUTER.begin_statement(self.connection, sql)
            with self.cpu_clock():
                return super().executescript(sql)
        except sqlite3.Error as exc:
            ROUTER.failure(exc, sql)
            raise

    def fetchone(self):
        with self.cpu_clock():
            result = super().fetchone()
        row = ACTIVE.get()
        if (row is not None and "dynamic_rank_oracle" in row["request"] and
            result is not None):
            sql = self._matrix_sql.upper()
            if len(result) == 5 and sql.startswith(RANK_STATE_PREFIX):
                row["rank_state_observations"].append({
                    "exp_generation": int(result[0]), "stone_generation": int(result[1]),
                    "actor": str(result[2]) if result[2] is not None else None,
                    "exp_key": bytes(result[3]).hex() if result[3] is not None else None,
                    "stone_key": bytes(result[4]).hex() if result[4] is not None else None,
                })
            elif sql == PROFILE_DISPLAY_SQL:
                from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key

                columns = {column[0]: index for index, column in enumerate(self.description)}
                raw = {name: result[name] if isinstance(result, Mapping) else result[columns[name]]
                       for name in ("user_id", "exp", "stone")}
                keys = {name: sqlite_int_sort_key(raw[name]) for name in ("exp", "stone")}
                row["profile_display_observations"].append({
                    "actor": str(raw["user_id"]) if raw["user_id"] is not None else None,
                    **{f"{name}_key": key.hex() if key is not None else None for name, key in keys.items()},
                })
        return result

    def fetchmany(self, size=None):
        with self.cpu_clock():
            return super().fetchmany(size)

    def fetchall(self):
        with self.cpu_clock():
            return super().fetchall()

    def __next__(self):
        with self.cpu_clock():
            return super().__next__()


class MixedConnection(matrix.TimedConnection):
    def cursor(self, factory=None):
        return sqlite3.Connection.cursor(self, factory or MixedCursor)

    def create_function(self, *args, **kwargs):
        started = time.perf_counter()
        try:
            return super().create_function(*args, **kwargs)
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            entry = ROUTER.connection_entry(self)
            if entry is not None:
                entry["udf_calls"] += 1
                entry["udf_ms"] += elapsed_ms

    @contextmanager
    def control_statement(self, sql):
        started = time.perf_counter()
        cpu_token = SQL_CPU_START.set(time.thread_time())
        diagnostic_token = SQL_DIAGNOSTIC.set(ROUTER.begin_statement(self, sql))
        try:
            yield
        except sqlite3.Error as exc:
            ROUTER.failure(exc, sql)
            raise
        finally:
            try:
                ROUTER.record(self, sql, "execute", (time.perf_counter() - started) * 1000)
            finally:
                SQL_DIAGNOSTIC.reset(diagnostic_token)
                SQL_CPU_START.reset(cpu_token)

    def commit(self):
        with self.control_statement("COMMIT"):
            return super().commit()

    def rollback(self):
        with self.control_statement("ROLLBACK"):
            return super().rollback()


def install_sql_timing(root):
    global ROUTER
    ROUTER = QueryRouter(root)
    matrix.RECORDER = ROUTER

    def connect(database, *args, **kwargs):
        path = ROUTER.check_path(database, operation="connects")
        if "factory" in kwargs and kwargs["factory"] is not MixedConnection:
            raise RuntimeError("custom SQLite factory would bypass mixed-probe instrumentation")
        kwargs["factory"] = MixedConnection
        started = time.perf_counter()
        connection = matrix.ORIGINAL_CONNECT(database, *args, **kwargs)
        opened_ms = (time.perf_counter() - started) * 1000
        row = ACTIVE.get()
        with ROUTER.lock:
            ROUTER.connection_serial += 1
            serial = ROUTER.connection_serial
        diagnostic = {"serial": serial, "database": str(path) if path else ":memory:",
                      "opened_by": row["_connection_owner"] if row is not None else None,
                      "open_ms": opened_ms, "ordinal": 0, "statement": None}
        connection._mixed_diagnostic = diagnostic

        def authorize(action, first, second, database, trigger):
            with ROUTER.observer_cpu.measure("authorizer"):
                statement = SQL_DIAGNOSTIC.get()
                if statement is not None:
                    statement["authorizer_calls"] += 1
                return ROUTER.authorizer(action, first, second, database, trigger)

        connection.set_authorizer(authorize)
        ROUTER.connections[id(connection)] = diagnostic["database"]
        if row is not None:
            row["connect_ms"] += opened_ms
            ROUTER.connection_entry(connection)
        return connection

    sqlite3.connect = connect
    return ROUTER


def wal_sizes(data_dir):
    """Enumerate every directory once; never recurse through directory links."""
    pending = [os.fspath(data_dir)]
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
                elif entry.name.endswith(".db-wal") and entry.is_file():
                    yield entry.stat().st_size


def resource_state(data_dir):
    memory = {}
    for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
        name, _, value = line.partition(":")
        if name in ("VmRSS", "VmSize", "Threads"):
            memory[name] = int(value.split()[0])
    wal = list(wal_sizes(data_dir))
    io = {}
    for line in Path("/proc/self/io").read_text(encoding="ascii").splitlines():
        name, value = line.split(":", 1)
        if name in ("read_bytes", "write_bytes", "syscr", "syscw"):
            io[name] = int(value)
    return {"rss_mb": round(memory.get("VmRSS", 0) / 1024, 3),
            "virtual_mb": round(memory.get("VmSize", 0) / 1024, 3),
            "threads": memory.get("Threads", 0),
            "process_cpu_s": round(time.process_time(), 6), "process_io": io,
            "available_mb": matrix._host_memory().get("available_mb"),
            "disk_free_bytes": shutil.disk_usage(data_dir).free,
            "max_wal_bytes": max(wal, default=0), "total_wal_bytes": sum(wal)}


def resource_violation(state):
    if state["available_mb"] is None or state["available_mb"] < 768:
        return "available RAM below 768 MiB"
    if state["disk_free_bytes"] < 2 * 1024 ** 3:
        return "free disk below 2 GiB"
    if max(state["max_wal_bytes"], state.get("total_wal_bytes", 0)) > 256 * 1024 ** 2:
        return "WAL above 256 MiB"
    if state["rss_mb"] > 900:
        return "RSS above 900 MiB soft stop"
    return None


def write_json_stream(path, value, *, mode="x"):
    # Avoid a second full report allocation under the address-space guard.
    with Path(path).open(mode, encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False)
        stream.write("\n")


class ResourceWatch:
    def __init__(self, data_dir, stop, observer_cpu=None):
        self.data_dir, self.stop = data_dir, stop
        self.observer_cpu = observer_cpu if observer_cpu is not None else ObserverCPU()
        self.done = threading.Event()
        self.reason = None
        self.samples = []
        self.thread = threading.Thread(target=self.run, name="mixed-resource-watch", daemon=True)

    def run(self):
        with self.observer_cpu.measure("watch"):
            self._run()

    def _run(self):
        while not self.done.is_set():
            try:
                state = resource_state(self.data_dir)
                state["at"] = time.perf_counter()
                self.samples.append(state)
                self.reason = resource_violation(state)
                if self.reason:
                    self.stop.set()
                    break
            except OSError as exc:
                self.reason = f"resource watchdog unavailable: {exc}"
                self.stop.set()
                break
            self.done.wait(.25)

    def close(self):
        self.done.set()
        self.thread.join(timeout=2)


def request_row(request, number):
    recorder = matrix.QueryRecorder()
    recorder.active = True
    return {"request": request, "key": request["key"], "command": request["command"],
            "actor": str(request["actor"]), "input_text": request["input_text"],
            "number": request.get("number", number), "replies": [], "errors": [],
            "blocked": [], "logs": [], "sql_errors": [], "sqlite_busy_locked": 0,
            "transaction_begin_ms": [], "connect_ms": 0.0,
            "connection_diagnostics": [], "_connection_entries": {}, "_connection_owner": object(),
            "connection_diagnostic_omitted_touches": 0,
            "sql_threads": {}, "sql_timeline": [], "sql_timeline_omitted": 0,
            "rank_state_observations": [],
            "profile_display_observations": [],
            "_rank_cpu_ms": {operation: 0.0 for operation in RANK_OPERATIONS},
            "_recorder": recorder, "_tasks": []}


def validate_plan(plan, entries):
    if not plan:
        raise ValueError("mixed request plan is empty")
    resolved = {}
    numbers = set()
    for request in plan:
        for name in ("key", "command", "input_text", "actor"):
            if not request.get(name):
                raise ValueError(f"mixed request is missing {name}")
        try:
            if int(request["actor"]) <= 0:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("mixed actor must be a positive numeric user ID") from None
        entry = matrix._resolve_entry(entries, request["command"])
        if request["key"] in resolved and resolved[request["key"]] is not entry:
            raise ValueError("one mixed command key resolves to different matchers")
        resolved[request["key"]] = entry
        if "number" in request:
            if request["number"] in numbers:
                raise ValueError("message IDs in a phase must be unique")
            numbers.add(request["number"])
    return resolved


def plan_summary(plan, resolved):
    return {"requests": len(plan), "users": len({str(row["actor"]) for row in plan}),
            "commands": [{"key": key, "command": entry["command"],
                          "module": entry["module"], "handlers": entry["handlers"],
                          "requests": sum(row["key"] == key for row in plan)}
                         for key, entry in sorted(resolved.items())]}


async def drain_owned(row, *, timeout=1):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        tasks = [task for task in row["_tasks"]
                 if task is not asyncio.current_task() and not task.done()]
        if not tasks:
            break
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            row["blocked"].append("owned background work did not drain")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            break
        await asyncio.wait(tasks, timeout=remaining)
    for task in row["_tasks"]:
        if task.done() and not task.cancelled() and getattr(task, "_log_traceback", False):
            exc = task.exception()
            if exc is not None:
                detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                if detail not in row["errors"]:
                    row["errors"].append(detail)


def recorder_metrics(recorder, *, rank_cpu_ms=None):
    metrics = recorder.snapshot()
    # Count from the complete aggregation, never the display-only top 20 SQLs.
    ranks = {"statements": 0, "exp_recomputations": 0, "stone_recomputations": 0,
             "total_ms": 0.0}
    operations = {operation: {"statements": 0, "execute_ms": 0.0, "fetch_ms": 0.0,
                             "total_ms": 0.0,
                             "cpu_ms": rank_cpu_ms[operation] if rank_cpu_ms is not None else None}
                  for operation in RANK_OPERATIONS}
    for (_, sql), values in recorder.by_sql.items():
        operation = rank_operation(sql)
        if operation is not None:
            totals = operations[operation]
            totals["statements"] += values["count"]
            for phase in ("execute_ms", "fetch_ms"):
                totals[phase] += values[phase]
            totals["total_ms"] += values["execute_ms"] + values["fetch_ms"]
        upper = sql.upper()
        columns = [column for column in ("exp", "stone")
                   if f'COUNT(DISTINCT XIUXIAN_INT_SORT_KEY_COMPACT("{column.upper()}"))' in upper]
        if columns and operation == "count":
            ranks["statements"] += values["count"]
            ranks["total_ms"] += values["execute_ms"] + values["fetch_ms"]
            for column in columns:
                ranks[f"{column}_recomputations"] += values["count"]
    ranks["total_ms"] = round(ranks["total_ms"], 3)
    metrics["exact_rank_scans"] = ranks
    metrics["exact_rank_operations"] = {
        operation: {field: round(value, 3) if value is not None else None
                    for field, value in totals.items()}
        for operation, totals in operations.items()}
    return metrics


async def dispatch_matcher_once(row, matcher, bot, event, state, stack):
    from nonebot.exception import StopPropagation
    from nonebot.message import _check_matcher, _run_matcher

    dependency_cache = {}
    row["matcher_check_started_at"] = time.perf_counter()
    try:
        matched = await _check_matcher(matcher, bot, event, state, stack, dependency_cache)
    finally:
        row["matcher_check_completed_at"] = time.perf_counter()
    if not matched:
        row["blocked"].append("permission/rule rejected before handler")
        return
    row["handler_started_at"] = time.perf_counter()
    try:
        # Mirror check_and_run_matcher with one shared dependency cache.
        await _run_matcher(matcher, bot, event, state, stack, dependency_cache)
    except StopPropagation:
        pass
    finally:
        row["handler_completed_at"] = time.perf_counter()


async def run_phase(plan, entries, *, concurrency, arrival_rate, data_dir, classify):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("mixed phase must run on MainThread for its CPU measurement")
    import nonebot
    from nonebot.adapters.onebot.v11 import Adapter, Bot
    from nonebot.log import logger
    from nonebot.matcher import matchers
    from nonebot.rule import TrieRule
    from nonebot_plugin_xiuxian_2.infrastructure.messaging import MessageGatewayAdapter
    from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _CURRENT_EVENT
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import message_db
    from tests.command_probe import _boundary_denials, make_event, ProbeBlocked

    resolved = validate_plan(plan, entries)
    rows = [request_row(request, 890000000 + index) for index, request in enumerate(plan)]
    bot = Bot(Adapter(nonebot.get_driver()), "900000")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(concurrency)
    producer_done = asyncio.Event()
    stop = threading.Event()
    watch = ResourceWatch(data_dir, stop, ROUTER.observer_cpu)
    tasks, loop_lag, phase_errors = [], [], []
    inflight, peak_inflight = 0, 0
    queue_counts = {"posted": 0, "received": 0, "admitted": 0}
    queue_peaks = {"posted_not_received": 0, "received_not_admitted": 0}
    gc_events, gc_started = [], {}
    previous_factory = loop.get_task_factory()
    queue_before = await drain_message_writer(message_db)
    ROUTER.background.reset()
    ROUTER.background_errors.clear()
    ROUTER.unattributed_details.clear()
    started = time.perf_counter()
    cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    main_cpu_before = time.thread_time_ns()
    phase_thread = threading.current_thread()
    ROUTER.observer_cpu.start_phase()
    unattributed_before = ROUTER.unattributed

    def gc_callback(phase, info):
        key = (threading.get_ident(), info["generation"])
        if phase == "start":
            gc_started[key] = time.perf_counter()
        elif key in gc_started:
            begin = gc_started.pop(key)
            gc_events.append({"start": begin, "end": time.perf_counter(),
                              "generation": info["generation"], "thread": threading.current_thread().name})

    def task_factory(loop, coroutine, **kwargs):
        task = (previous_factory(loop, coroutine, **kwargs) if previous_factory else
                asyncio.Task(coroutine, loop=loop, **kwargs))
        row = ACTIVE.get()
        if row is not None:
            row["_tasks"].append(task)
        return task

    async def transport(self, api, **data):
        row = ACTIVE.get()
        if row is None:
            raise ProbeBlocked("transport has no owning request")
        if api.startswith("send_"):
            row["replies"].append(str(data.get("message", data)))
            return {"message_id": row["number"]}
        if api in {"get_group_info", "get_group_member_info", "get_stranger_info"}:
            return {"group_id": 900100, "user_id": int(row["actor"]),
                    "nickname": "诊断道友", "card": "", "role": "owner", "group_name": "诊断群"}
        raise ProbeBlocked(f"Bot API fixture missing: {api}")

    original_gateway = MessageGatewayAdapter.send

    async def gateway(self, context, reply):
        result = await original_gateway(self, context, reply)
        if not result.ok:
            row = ACTIVE.get()
            (row["errors"] if row is not None else phase_errors).append(
                f"message gateway rejected delivery: {result.error_code}")
        return result

    def capture(message):
        record, row = message.record, ACTIVE.get()
        if row is None:
            if (record["exception"] or record["level"].no >= 40 or
                    str(record["message"]).startswith("[message.db] 后台写入失败")):
                phase_errors.append(str(record["message"]) + str(record["exception"] or ""))
            return
        if record["level"].no >= 30:
            row["logs"].append(str(record["message"]))
        if record["exception"]:
            exc = record["exception"].value
            target = "blocked" if isinstance(exc, ProbeBlocked) else "errors"
            row[target].append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        elif record["level"].no >= 40:
            row["errors"].append(str(record["message"]))

    async def dispatch(row):
        nonlocal inflight, peak_inflight
        row["loop_received_at"] = time.perf_counter()
        queue_counts["received"] += 1
        queue_peaks["received_not_admitted"] = max(queue_peaks["received_not_admitted"],
                                                  queue_counts["received"] - queue_counts["admitted"])
        async with semaphore:
            row["admitted_at"] = time.perf_counter()
            queue_counts["admitted"] += 1
            if stop.is_set():
                row["status"] = "resource_aborted"
                row["outcome"] = "resource_aborted"
                row["completed_at"] = time.perf_counter()
                return
            inflight += 1
            peak_inflight = max(peak_inflight, inflight)
            active_token = ACTIVE.set(row)
            event = make_event(row["input_text"], row["actor"], row["number"],
                               event_type=row["request"].get("event_type", "group"))
            event_token = _CURRENT_EVENT.set(event)
            boundary_token = _boundary_denials.set(row["blocked"])
            try:
                state = {}
                entry = resolved[row["key"]]
                TrieRule.get_value(bot, event, state)
                if entry["matcher"] not in matchers[entry["matcher"].priority]:
                    raise ProbeBlocked("route provider did not select the expected real matcher")
                async with AsyncExitStack() as stack:
                    await asyncio.wait_for(dispatch_matcher_once(
                        row, entry["matcher"], bot, event, state, stack), timeout=15)
                row["status"] = "pass"
            except asyncio.TimeoutError:
                row["status"] = "timeout"
            except asyncio.CancelledError:
                row["status"] = "timeout"
                row["errors"].append("phase deadline cancelled the active request")
            except ProbeBlocked as exc:
                row["blocked"].append(str(exc))
                row["status"] = "blocked"
            except Exception as exc:
                row["errors"].append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
                row["status"] = "exception"
            finally:
                row["owned_drain_started_at"] = time.perf_counter()
                await drain_owned(row)
                row["owned_drain_completed_at"] = time.perf_counter()
                _boundary_denials.reset(boundary_token)
                _CURRENT_EVENT.reset(event_token)
                ACTIVE.reset(active_token)
                inflight -= 1
                row["completed_at"] = time.perf_counter()
                outcome = classify(row["request"], row)
                if isinstance(outcome, str):
                    outcome = {"outcome": outcome}
                row.update(outcome)
                if row["status"] == "timeout":
                    row["outcome"] = "timeout"
                elif row["blocked"]:
                    row["outcome"] = "boundary"
                elif row["errors"]:
                    row["outcome"] = "exception"
                elif not row["replies"]:
                    row["outcome"] = "unclassified"

    def enqueue(row, scheduled_at, producer_posted_at):
        row["scheduled_at"] = scheduled_at
        row["producer_posted_at"] = producer_posted_at
        queue_peaks["posted_not_received"] = max(queue_peaks["posted_not_received"],
                                                 queue_counts["posted"] - queue_counts["received"])
        tasks.append(asyncio.create_task(dispatch(row)))

    def producer():
        anchor = time.perf_counter() + .05
        try:
            for index, row in enumerate(rows):
                scheduled = anchor + index / arrival_rate
                if stop.wait(max(0, scheduled - time.perf_counter())):
                    break
                queue_counts["posted"] += 1
                loop.call_soon_threadsafe(enqueue, row, scheduled, time.perf_counter())
        finally:
            loop.call_soon_threadsafe(producer_done.set)

    async def heartbeat():
        deadline = loop.time() + .025
        while not producer_done.is_set() or any(not task.done() for task in tasks):
            await asyncio.sleep(max(0, deadline - loop.time()))
            now = loop.time()
            loop_lag.append(max(0, now - deadline) * 1000)
            deadline = now + .025

    thread = threading.Thread(target=producer, name="mixed-arrival-producer", daemon=True)
    sink = logger.add(capture, level="WARNING")
    loop.set_task_factory(task_factory)
    ROUTER.phase_active = True
    gc.callbacks.append(gc_callback)
    try:
        with patch.object(Bot, "call_api", transport), patch.object(MessageGatewayAdapter, "send", gateway):
            beat = asyncio.create_task(heartbeat())
            watch.thread.start()
            thread.start()
            try:
                await asyncio.wait_for(producer_done.wait(), timeout=65)
                if tasks:
                    await asyncio.wait_for(asyncio.gather(*tasks), timeout=90)
                await beat
            except Exception as exc:
                phase_errors.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
            finally:
                stop.set()
                thread.join(timeout=2)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                if not beat.done():
                    beat.cancel()
                    await asyncio.gather(beat, return_exceptions=True)
                try:
                    await drain_rank_workers()
                    await drain_message_writer(message_db)
                except RuntimeError as exc:
                    phase_errors.append(str(exc))
                watch.close()
                if watch.thread.is_alive():
                    phase_errors.append("resource watcher did not drain before observer CPU snapshot")
    finally:
        gc.callbacks.remove(gc_callback)
        ROUTER.phase_active = False
        loop.set_task_factory(previous_factory)
        logger.remove(sink)
        ROUTER.observer_cpu.end_phase()
    finished = time.perf_counter()
    cpu_after = resource.getrusage(resource.RUSAGE_SELF)
    main_cpu_after = time.thread_time_ns()
    observer_cpu = ROUTER.observer_cpu.snapshot()
    for row in rows:
        recorder = row.pop("_recorder")
        row.pop("_tasks")
        row.pop("_connection_entries")
        row.pop("_connection_owner")
        metrics = recorder_metrics(recorder, rank_cpu_ms=row.pop("_rank_cpu_ms"))
        row["sql"] = metrics
        row["db_query_count"] = sum(db["query_count"] for db in metrics["by_database"].values())
        row["db_total_ms"] = round(sum(db["total_ms"] for db in metrics["by_database"].values()), 3)
        if "completed_at" not in row:
            row["outcome"] = "timeout" if "loop_received_at" in row else "not_sent"
            continue
        for name, begin, end in (
            ("producer_delay_ms", "scheduled_at", "producer_posted_at"),
            ("loop_dispatch_delay_ms", "producer_posted_at", "loop_received_at"),
            ("queue_ms", "loop_received_at", "admitted_at"),
            ("service_ms", "admitted_at", "completed_at"),
            ("e2e_ms", "scheduled_at", "completed_at"),
            ("matcher_check_ms", "matcher_check_started_at", "matcher_check_completed_at"),
            ("owned_drain_ms", "owned_drain_started_at", "owned_drain_completed_at"),
        ):
            if begin in row and end in row:
                row[name] = round((row[end] - row[begin]) * 1000, 3)
    queue_after = message_queue_state(message_db)
    if queue_after["dropped_jobs"] != queue_before["dropped_jobs"]:
        phase_errors.append("message writer dropped jobs during measured phase")
    background = recorder_metrics(ROUTER.background)
    foreground_queries = sum(row["db_query_count"] for row in rows)
    background_queries = sum(value["query_count"] for value in background["by_database"].values())
    foreground_ms = sum(row["db_total_ms"] for row in rows)
    background_ms = sum(value["total_ms"] for value in background["by_database"].values())
    return {"samples": rows, "planned_requests": len(rows), "arrival_rate_per_second": arrival_rate,
            "concurrency_limit": concurrency, "peak_inflight": peak_inflight,
            "handler_overlap": interval_overlap(rows, "handler_started_at", "handler_completed_at"),
            "load_queue_peaks": queue_peaks, "gc_events": gc_events,
            "loop_lag_ms": distribution(loop_lag), "loop_lag_samples": len(loop_lag),
            "wall_s": round(finished - started, 3),
            "cpu_user_s": round(cpu_after.ru_utime - cpu_before.ru_utime, 3),
            "cpu_system_s": round(cpu_after.ru_stime - cpu_before.ru_stime, 3),
            "main_thread_cpu_ms": round((main_cpu_after - main_cpu_before) / 1000000, 3),
            "phase_thread": {"name": phase_thread.name, "ident": phase_thread.ident,
                             "is_main_thread": phase_thread is threading.main_thread()},
            "observer_cpu": observer_cpu,
            "resource_samples": watch.samples, "resource_stop_reason": watch.reason,
            "resources": {"process_peak_rss_mb": round(cpu_after.ru_maxrss / 1024, 3),
                "minimum_available_mb": min((sample["available_mb"] for sample in watch.samples
                                             if sample["available_mb"] is not None), default=None),
                "minimum_disk_free_bytes": min((sample["disk_free_bytes"] for sample in watch.samples), default=None),
                "maximum_wal_bytes": max((sample["max_wal_bytes"] for sample in watch.samples), default=0)},
            "phase_errors": phase_errors,
            "unattributed_sql_operations": ROUTER.unattributed - unattributed_before,
            "unattributed_sql_details": [
                {"database": database, "thread": thread, "thread_ident": ident,
                 "sql": sql, "operation": operation, "count": count}
                for (database, thread, ident, sql, operation), count in ROUTER.unattributed_details.items()],
            "background_sql": {"message_writer": background, "errors": list(ROUTER.background_errors)},
            "message_queue": {"before": queue_before, "after": queue_after,
                "post_response_drain_ms": round(max(0, finished - max(
                    (row["completed_at"] for row in rows if "completed_at" in row), default=finished)) * 1000, 3),
                "completion_semantics": "response e2e excludes asynchronous message persistence; phase wall includes queue drain"},
            "phase_sql": {"queries": foreground_queries + background_queries,
                          "foreground_queries": foreground_queries,
                          "background_queries": background_queries,
                          "total_ms": round(foreground_ms + background_ms, 3)},
            **summarize(rows)}


def interval_overlap(rows, begin, end):
    points = [(row[begin], 1) for row in rows if begin in row and end in row]
    points += [(row[end], -1) for row in rows if begin in row and end in row]
    current, peak, overlapping_ms, previous = 0, 0, 0.0, None
    for at, delta in sorted(points):
        if previous is not None and current > 1:
            overlapping_ms += (at - previous) * 1000
        current += delta
        peak = max(peak, current)
        previous = at
    return {"peak_active_intervals": peak, "time_with_multiple_active_ms": round(overlapping_ms, 3),
            "interpretation": "wall-interval overlap, not parallel CPU/SQLite execution"}


def summarize_selection(rows):
    outcomes = Counter(row.get("outcome", "unclassified") for row in rows)
    operations = {}
    for operation in RANK_OPERATIONS:
        operations[operation] = {}
        for field in RANK_OPERATION_FIELDS:
            values = [row["sql"]["exact_rank_operations"][operation][field] for row in rows]
            measured = [value for value in values if value is not None]
            operations[operation][field] = round(sum(measured), 3) if measured else None
    workers = [totals for row in rows for name, totals in row["sql_threads"].items()
               if name.startswith("xiuxian-exact-rank")]
    return {"n": len(rows), "outcomes": dict(outcomes),
            "error_rate": sum(outcomes[name] for name in
                              ("exception", "timeout", "boundary", "unclassified", "not_sent", "resource_aborted")) / len(rows) if rows else 0,
            "e2e_ms": distribution([row["e2e_ms"] for row in rows if "e2e_ms" in row]),
            "service_ms": distribution([row["service_ms"] for row in rows if "service_ms" in row]),
            "queue_ms": distribution([row["queue_ms"] for row in rows if "queue_ms" in row]),
            "loop_dispatch_delay_ms": distribution([row["loop_dispatch_delay_ms"] for row in rows if "loop_dispatch_delay_ms" in row]),
            "db_queries": sum(row["db_query_count"] for row in rows),
            "db_total_ms": round(sum(row["db_total_ms"] for row in rows), 3),
            "exact_rank_scans": {field: round(sum(row["sql"]["exact_rank_scans"][field]
                                                  for row in rows), 3)
                                 for field in ("statements", "exp_recomputations", "stone_recomputations", "total_ms")},
            "exact_rank_operations": operations,
            "exact_rank_worker_sql": {field: round(sum(worker[field] for worker in workers), 3)
                                      for field in ("calls", "wall_ms", "cpu_ms")},
            "sqlite_busy_locked": sum(row["sqlite_busy_locked"] for row in rows)}


def summarize(rows):
    by_command = {}
    for key in sorted({row["key"] for row in rows}):
        selected = [row for row in rows if row["key"] == key]
        by_command[key] = {"command": selected[0]["command"], **summarize_selection(selected)}
    profiles = [row for row in rows if row["key"] == "profile_info" and row["request"].get("rank_cohort")]
    by_cohort = {cohort: summarize_selection([
        row for row in profiles if row["request"]["rank_cohort"] == cohort])
        for cohort in sorted({row["request"]["rank_cohort"] for row in profiles})}
    totals = summarize_selection(rows)
    completed = [row for row in rows if "completed_at" in row]
    duration = (max(row["completed_at"] for row in completed) - min(row["scheduled_at"] for row in completed)) if completed else 0
    success = sum(row.get("outcome") == "success" for row in rows)
    return {"commands": by_command, "rank_cohorts": by_cohort, "outcomes": totals["outcomes"],
            "exact_rank_operations": totals["exact_rank_operations"],
            "exact_rank_worker_sql": totals["exact_rank_worker_sql"],
            "load_window_s": round(duration, 6),
            "completed_throughput_per_second": round(len(completed) / duration, 3) if duration else 0,
            "successful_throughput_per_second": round(success / duration, 3) if duration else 0,
            "e2e_ms": distribution([row["e2e_ms"] for row in completed]),
            "queue_ms": distribution([row["queue_ms"] for row in completed]),
            "loop_dispatch_delay_ms": distribution([row["loop_dispatch_delay_ms"] for row in completed]),
            "producer_delay_ms": distribution([row["producer_delay_ms"] for row in completed])}


def source_identity(source_repo):
    package = Path(source_repo) / "nonebot_plugin_xiuxian_2"
    digest = hashlib.sha256()
    files = {}
    for path in sorted(package.rglob("*.py")):
        relative = path.relative_to(package).as_posix()
        value = matrix._sha256(path)
        digest.update((relative + "\0" + value + "\n").encode("utf-8"))
        files[relative] = value
    if not files:
        raise ValueError(f"source package is missing: {package}")
    head = subprocess.run(["git", "-C", str(source_repo), "rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    return {"repo": str(source_repo), "head": head, "runtime_tree_sha256": digest.hexdigest(),
            "python_files": len(files), "file_hashes": files}


def rank_generation_state(game):
    cursor = game.conn.execute("SELECT * FROM xiuxian_numeric_rank_generation WHERE singleton=1")
    row = cursor.fetchone()
    return {column[0]: int(row[index]) for index, column in enumerate(cursor.description)}


@contextmanager
def validation_lock():
    with open("/tmp/nonebot-project-validation.lock", "a", encoding="ascii") as lock:
        deadline = time.monotonic() + 600
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("shared validation lock unavailable after 600 seconds")
                time.sleep(1)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


async def run(args, identity):
    run_started = time.perf_counter()
    run_cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    fixture = args.fixture.resolve()
    data_dir = matrix._fixture_data(fixture)
    root = data_dir / "xiuxian"
    report = args.report.resolve()
    matrix._read_report_path(fixture, report)
    if report.exists() or report.with_suffix(".phases.jsonl").exists():
        raise RuntimeError(f"refusing to overwrite an existing report: {report}")
    fixture_identity = json.loads(matrix._manifest_path(fixture).read_text(encoding="utf-8"))
    if (fixture_identity.get("users") != matrix.TARGET_USERS or
        fixture_identity.get("fixture_id") != "isolated-synthetic-command-matrix-1m-v1"):
        raise RuntimeError("mixed load requires the retained, validated 1M fixture")
    violation = resource_violation(resource_state(data_dir))
    if violation:
        raise RuntimeError(violation)
    database_bytes = sum(path.stat().st_size for path in root.glob("*.db"))
    if shutil.disk_usage(data_dir).free < max(2 * 1024 ** 3, 2 * database_bytes + 1024 ** 3):
        raise RuntimeError("insufficient disk for the sole temporary restore snapshot")
    temp_dir = root / f".mixed-probe-tmp-{os.getpid()}"
    temp_dir.mkdir()
    os.environ["TMPDIR"] = str(temp_dir)
    tempfile.tempdir = str(temp_dir)
    os.environ["XIUXIAN_TEST_DATA_DIR"] = str(root)
    from tests import command_probe as probe
    if Path(probe.tests._data_dir).resolve() != root.resolve():
        raise RuntimeError("mixed load data root does not match tests isolation root")
    probe.isolate_source(args.source_repo)
    probe.install_boundary(root)
    router = install_sql_timing(root)
    context, managers = await probe.prepare(seed=False, skip_training_wait=False)
    for module in (*matrix.MATRIX_MATCHER_MODULES, "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade"):
        importlib.import_module(module)
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import message_db
    prepare_message_writer(message_db, context.paths.message_db)
    fixture_support = importlib.import_module({
        "legacy-v1": "tests.mixed_command_fixture",
        "rank-gameplay-v2": "tests.mixed_rank_gameplay_fixture",
        "rank-range-v3": "tests.mixed_rank_range_fixture",
        "read-strata-v4": "tests.mixed_read_strata_fixture",
        "relation-v5": "tests.mixed_relation_fixture",
        "rank-seek-v6": "tests.mixed_rank_seek_fixture",
        "dynamic-relation-v7": "tests.mixed_dynamic_relation_fixture",
        "multiwriter-v9": "tests.mixed_multiwriter_fixture",
        "player-projection-v10": "tests.mixed_player_projection_fixture",
    }[args.scenario])
    driver_files = [Path(__file__), ROOT / "scripts/command_matrix_probe.py",
                    ROOT / "tests/command_probe.py"]
    driver_files.extend(Path(module.__file__) for name, module in list(sys.modules.items())
                        if name.startswith("tests.mixed_") and getattr(module, "__file__", None))
    driver_identity = {path.relative_to(ROOT).as_posix(): matrix._sha256(path)
                       for path in sorted(set(driver_files))}
    specs = (fixture_support.phase_specs() if args.scenario != "legacy-v1" else [
        {"mode": mode, "concurrency": concurrency, "requests": args.requests_per_phase,
         "arrival_rate": args.arrival_rate}
        for mode in MODES for concurrency in args.concurrency])
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
    entries = probe.inventory()
    if int(managers[0].conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]) != matrix.TARGET_USERS:
        raise RuntimeError("retained synthetic fixture user count is not 1M")
    digest_before = matrix._table_digest(managers[0].conn)
    if fixture_identity.get("user_rank_data_sha256") != digest_before:
        raise RuntimeError("retained fixture rank digest differs from its validated manifest")
    snapshot_dir = root / f".mixed-probe-snapshot-{os.getpid()}"
    snapshot = matrix.DiskFixtureSnapshot(root, managers, snapshot_dir)
    router.allowed_databases = {path.resolve() for path in snapshot.databases}
    result = {"status": "running", "fixture": str(fixture), "source": identity,
              "fixture_identity": fixture_identity, "limits": matrix.MATRIX_LIMITS,
              "entry": "real matcher/provider/permission/rule/DI/handler; mock OneBot transport only",
              "dispatch_contract": "one _check_matcher then _run_matcher; shared per-request dependency cache and exit stack; 15s covers check and run",
              "driver_identity": driver_identity,
              "coverage": "bounded stratified mixed sample; not exhaustive of 674 matchers or background jobs",
              "phases": [], "bulk_probe": None, "preflight_only": args.preflight_only,
              "scenario": args.scenario, "frozen_phase_specs": specs,
              "bulk_only": args.bulk_only,
            "cache_attribution": "exact phase counter deltas; per-command cache counters not measured",
              "background_sql_attribution": "actual message-db queue consumer context and exact DB path; measured per phase including drain, never assigned to a command",
              "sql_count_definition": "execute/executemany/executescript calls plus commit/rollback; fetch time counted separately",
              "exact_rank_operation_definition": "complete per-request SQL aggregation: count=compact COUNT(DISTINCT), seek_batch=non-EXPLAIN numeric_rank_keys CTE, plan=EXPLAIN QUERY PLAN with compact expression; statements count API execute calls, never internal seeks; total wall includes execute+fetch; CPU is null when not routed; worker SQL includes all xiuxian-exact-rank threads, independent of bounded timeline/connection diagnostics",
              "connection_diagnostic_definition": "at most 32 connections/request and first 12 statements/connection/request; stable connection ID and lifetime ordinal; raw open, UDF registration, authorizer callbacks, execute/fetch CPU and wall time; omitted touches/statements counted",
              "observer_cpu_definition": "phase-only synchronous record/begin_statement/authorizer/watch sections use exclusive thread CPU; at most 16 distinct thread bins plus one locked overflow bin; watcher span includes its whole run but thread CPU excludes sleeping; MainThread CPU window matches phase process CPU, ends before summaries, and includes setup/drain",
              "observer_cpu_limits": "observed sections are not total profiler overhead: collector registration/accounting, clock calls and unwrapped hooks are not fully measured; timing/bookkeeping perturbation is uncalibrated; these counters are not strict bounds on uninstrumented cost and cannot be subtracted from latency; authorizer CPU overlaps existing SQL CPU, so never add SQL CPU and observer CPU; residual process/MainThread CPU includes application, background and unmeasured harness work; diagnostic sample throughput is not a production capacity limit",
              "transaction_wait_definition": "BEGIN execution time is an upper bound including SQL execution; not a pure lock-wait timer",
              "resource_before": resource_state(data_dir)}
    try:
        setup_started = time.perf_counter()
        manifest = fixture_support.seed(context, managers, participants=args.participants)
        preflight_phases = []
        for index, spec in enumerate(specs):
            mode, concurrency = spec["mode"], spec["concurrency"]
            plan = fixture_support.request_plan(manifest, mode, spec["requests"], phase_number=index)
            resolved = validate_plan(plan, entries)
            preflight_phases.append({"mode": mode, "concurrency_limit": concurrency,
                                     "arrival_rate": spec["arrival_rate"],
                                     "plan_sha256": hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
                                     **plan_summary(plan, resolved)})
        initial_check = fixture_support.check_invariants(context, managers, manifest, [])
        result["preflight"] = {"phases": preflight_phases, "fixture_manifest": manifest,
            "initial_invariants": initial_check, "setup_s": round(time.perf_counter() - setup_started, 3),
            "total_planned_requests": sum(spec["requests"] for spec in specs),
            "planned_arrival_window_s": round(sum((spec["requests"] - 1) / spec["arrival_rate"] for spec in specs), 3),
            "dispatch_executed": False,
            "scope": "all command routes/registered handlers, seed invariants, observed startup/seed DB boundaries; actual handler SQL remains guarded at runtime"}
        if not initial_check.get("ok"):
            raise RuntimeError("seed invariants failed during preflight")
        phases = [] if args.preflight_only or args.bulk_only else specs
        for index, spec in enumerate(phases):
            mode, concurrency = spec["mode"], spec["concurrency"]
            setup_started = time.perf_counter()
            snapshot.restore()
            manifest = fixture_support.seed(context, managers, participants=args.participants)
            plan = fixture_support.request_plan(manifest, mode, spec["requests"], phase_number=index)
            validate_plan(plan, entries)
            process_read_cache.clear()
            cache_before = process_read_cache.stats()
            generation_before = rank_generation_state(managers[0])
            setup_s = time.perf_counter() - setup_started
            print(f"mixed phase {index + 1}: {mode} concurrency={concurrency} arrivals={spec['arrival_rate']}/s n={len(plan)}", flush=True)
            phase = await run_phase(plan, entries, concurrency=concurrency,
                                    arrival_rate=spec["arrival_rate"], data_dir=data_dir,
                                    classify=fixture_support.classify_result)
            cache_after = process_read_cache.stats()
            generation_after = rank_generation_state(managers[0])
            phase["mode"] = mode
            phase["label"] = spec.get("label", mode)
            phase["request_order"] = spec.get("order", "original")
            phase["setup_s"] = round(setup_s, 3)
            phase["fixture_manifest"] = manifest
            phase["cache"] = {"delta": {key: cache_after.get(key, 0) - cache_before.get(key, 0) for key in COUNTERS},
                              "end_state": cache_after}
            phase["rank_generation"] = {
                "before": generation_before, "after": generation_after,
                "delta": {key: value - generation_before[key] for key, value in generation_after.items()},
            }
            check_started = time.perf_counter()
            phase["invariants"] = fixture_support.check_invariants(context, managers, manifest, phase["samples"])
            phase["invariant_check_s"] = round(time.perf_counter() - check_started, 3)
            result["phases"].append(phase)
            write_json_stream(report.with_suffix(".phases.jsonl"), phase, mode="a")
            print(json.dumps({"phase_complete": index + 1, "outcomes": phase["outcomes"],
                              "invariants_ok": phase["invariants"].get("ok")}), flush=True)
            bad = {"exception", "timeout", "boundary", "unclassified", "not_sent", "resource_aborted"}
            if (phase["resource_stop_reason"] or phase["phase_errors"] or
                phase["unattributed_sql_operations"] or phase["background_sql"]["errors"] or
                not phase["invariants"].get("ok") or
                any(phase["outcomes"].get(key, 0) for key in bad)):
                result["failure_reason"] = f"phase {index + 1} failed safety, classification or consistency checks"
                break
        if BULK_PROBE is not None and args.scenario == "legacy-v1" and not args.preflight_only and "failure_reason" not in result:
            snapshot.restore()
            bulk = BULK_PROBE(context, managers, fixture_identity)
            result["bulk_probe"] = await bulk if inspect.isawaitable(bulk) else bulk
            if result["bulk_probe"].get("status") != "pass":
                raise RuntimeError("bounded generation bulk probe did not pass")
    except Exception as exc:
        result["failure_reason"] = str(exc)
        result["traceback"] = traceback.format_exc()
    finally:
        restore_started = time.perf_counter()
        try:
            await drain_rank_workers()
            await drain_message_writer(message_db, timeout=10)
            snapshot.restore()
            digest_after = matrix._table_digest(managers[0].conn)
        except Exception as exc:
            digest_after = None
            result["failure_reason"] = f"final restore failed; retain snapshot {snapshot_dir}: {exc}"
            result["restore_traceback"] = traceback.format_exc()
        result["restore_s"] = round(time.perf_counter() - restore_started, 3)
        result["original_rank_digest_before"] = digest_before
        result["original_rank_digest_after"] = digest_after
        result["original_rank_digest_restored"] = digest_before == digest_after
        result["snapshot_db_manifest"] = [{"path": path, **observed,
            "snapshot_covered": Path(path) in router.allowed_databases}
            for path, observed in sorted(router.observed.items())]
        if digest_after == digest_before:
            snapshot.close()
        result["resource_after"] = resource_state(data_dir)
        source_copy = data_dir / "command-source"
        if (source_copy / ".rank-hotpath-source").is_file():
            shutil.rmtree(source_copy)
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
    result["status"] = "pass" if ("failure_reason" not in result and result["original_rank_digest_restored"] and
        (args.preflight_only or args.bulk_only or
         len(result["phases"]) == len(specs))) else "failed"
    run_cpu_after = resource.getrusage(resource.RUSAGE_SELF)
    result["process"] = {"wall_s": round(time.perf_counter() - run_started, 3),
        "cpu_user_s": round(run_cpu_after.ru_utime - run_cpu_before.ru_utime, 3),
        "cpu_system_s": round(run_cpu_after.ru_stime - run_cpu_before.ru_stime, 3),
        "max_rss_mb": round(run_cpu_after.ru_maxrss / 1024, 3)}
    write_json_stream(report, result)
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--source-repo", type=Path, default=ROOT)
    parser.add_argument("--scenario", choices=("legacy-v1", "rank-gameplay-v2", "rank-range-v3", "read-strata-v4", "relation-v5", "rank-seek-v6", "dynamic-relation-v7", "multiwriter-v9", "player-projection-v10"), default="legacy-v1")
    parser.add_argument("--concurrency", default="1,5,10,20")
    parser.add_argument("--requests-per-phase", type=int, default=48)
    parser.add_argument("--arrival-rate", type=float, default=10)
    parser.add_argument("--participants", type=int)
    parser.add_argument("--preflight-only", action="store_true",
                        help="resolve every route and validate seed/restore without dispatching load")
    parser.add_argument("--bulk-only", action="store_true",
                        help="supplement drained request evidence with rollback-only bulk cost; no traffic rerun")
    args = parser.parse_args(argv)
    if args.preflight_only and args.bulk_only:
        parser.error("--preflight-only and --bulk-only are mutually exclusive")
    try:
        args.concurrency = [int(value) for value in args.concurrency.split(",")]
    except ValueError:
        parser.error("--concurrency must be comma-separated integers")
    if not args.concurrency or len(set(args.concurrency)) != len(args.concurrency) or any(value < 1 or value > 20 for value in args.concurrency):
        parser.error("concurrency values must be unique, between 1 and 20")
    if not 8 <= args.requests_per_phase <= 160:
        parser.error("--requests-per-phase must be between 8 and 160")
    if not math.isfinite(args.arrival_rate) or not .5 <= args.arrival_rate <= 50:
        parser.error("--arrival-rate must be between 0.5 and 50")
    if args.requests_per_phase / args.arrival_rate > 60:
        parser.error("planned phase duration must not exceed 60 seconds")
    required_participants = 32 if args.scenario == "legacy-v1" else 128
    if args.participants is None:
        args.participants = required_participants
    if args.participants != required_participants:
        parser.error(f"--participants must be {required_participants} for the frozen scenario")
    if args.scenario != "legacy-v1" and args.bulk_only:
        parser.error("--bulk-only applies only to legacy-v1")
    return args


def main(argv=None):
    args = parse_args(argv)
    if args.scenario != "legacy-v1" and os.environ.get("MALLOC_ARENA_MAX") != "2":
        raise RuntimeError("start rank scenarios with MALLOC_ARENA_MAX=2 before Python startup")
    identity = source_identity(args.source_repo.resolve())
    with validation_lock():
        matrix._enforce_matrix_limits()
        affinity = sorted(os.sched_getaffinity(0))[:2]
        os.sched_setaffinity(0, set(affinity))
        matrix.MATRIX_LIMITS["affinity_cpus"] = len(affinity)
        matrix.MATRIX_LIMITS["malloc_arena_max"] = os.environ.get("MALLOC_ARENA_MAX")
        signal.signal(signal.SIGALRM, matrix._raise_wall_timeout)
        signal.alarm(1200)
        try:
            result = asyncio.run(run(args, identity))
        finally:
            signal.alarm(0)
    print(json.dumps({"status": result["status"], "phases": len(result["phases"]),
                      "restored": result["original_rank_digest_restored"],
                      "failure_reason": result.get("failure_reason"), "report": str(args.report)}, ensure_ascii=False))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
