"""Bounded real-matcher command matrix for isolated synthetic fixtures."""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import resource
import shutil
import signal
import sqlite3
import sys
import tempfile
import time
import traceback


REQUESTS = 20
ACTOR = "900001"
TARGET_USERS = 1_000_000
FIXTURE_MARKER = ".codex-runtime-profile-fixture"
SPECS = (
    ("profile_info", "profile/info", "我的修仙信息", "我的修仙信息", False),
    ("asset_overview", "profile/info", "我的资产总览", "我的资产总览", False),
    ("mind_state", "cultivation/status", "我的状态", "我的状态", False),
    ("cultivation_value", "cultivation/status", "我的修为", "我的修为", False),
    ("backpack", "inventory/equipment", "我的背包", "我的背包", False),
    ("pill_backpack", "inventory/equipment", "丹药背包", "丹药背包", False),
    ("herb_backpack", "inventory/equipment", "药材背包", "药材背包", False),
    ("equipment", "inventory/equipment", "我的装备", "我的装备", False),
    ("realm_rank", "ranking", "排行榜", "排行榜", False),
    ("stone_rank", "ranking", "灵石排行榜", "灵石排行榜", False),
    ("power_rank", "global aggregate", "战力排行榜", "战力排行榜", False),
    ("sect_rank", "global aggregate", "宗门排行榜", "宗门排行榜", False),
    ("root_rank", "global aggregate", "轮回排行榜", "轮回排行榜", False),
    ("activity_rank", "global aggregate", "活动排行", "活动排行", False),
    ("season_rank", "global aggregate", "赛季榜", "赛季榜", False),
    ("arena_rank", "global aggregate", "竞技场排行榜", "竞技场排行榜", False),
    ("boss_integral_rank", "global aggregate", "世界BOSS积分排行榜", "世界BOSS积分排行榜", False),
    ("impart_rank", "global aggregate", "虚神界排行榜", "虚神界排行榜", False),
    ("sect_weekly_rank", "global aggregate", "宗门周常排行", "宗门周常排行", False),
    ("sect_power_rank", "global aggregate", "宗门战力排行榜", "宗门战力排行榜", False),
    ("partner_rank", "global aggregate", "道侣排行榜", "道侣排行榜", False),
    ("mentor_rank", "global aggregate", "师徒排行榜", "师徒排行榜", False),
    ("training_rank", "global aggregate", "历练排行榜", "历练排行榜", False),
    ("tower_rank", "global aggregate", "通天塔排行榜", "通天塔排行榜", False),
    ("activity_boss_rank", "global aggregate", "活动首领排行", "活动首领排行", False),
    ("past_life_rank", "global aggregate", "前尘排行", "前尘排行", False),
    ("cultivation_action", "read/action", "修炼", "修炼", True),
    ("closing_action", "read/action", "闭关", "闭关", True),
)
MATRIX_LIMITS: dict[str, int | str] | None = None
MATRIX_MATCHER_MODULES = (
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info.user_info",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly_commands",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tower",
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life",
)

if (
    len(SPECS) != 28
    or len({spec[0] for spec in SPECS}) != len(SPECS)
    or len({spec[2] for spec in SPECS}) != len(SPECS)
):
    raise RuntimeError("matrix must contain 28 unique keys and command names")


def _host_memory() -> dict[str, int | None]:
    values: dict[str, int | None] = {"total_mb": None, "available_mb": None}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            key, raw = line.split(":", 1)
            if key in {"MemTotal", "MemAvailable"}:
                values["total_mb" if key == "MemTotal" else "available_mb"] = int(
                    raw.strip().split()[0]
                ) // 1024
    except (OSError, ValueError, IndexError):
        pass
    return values


def _enforce_matrix_limits() -> None:
    global MATRIX_LIMITS
    if os.environ.get("XIUXIAN_MATRIX_LIMITS") != "1":
        raise SystemExit("refusing an uncapped command matrix; set XIUXIAN_MATRIX_LIMITS=1")

    memory_bytes = 1024 * 1024 * 1024
    old_soft, old_hard = resource.getrlimit(resource.RLIMIT_AS)
    hard = memory_bytes if old_hard == resource.RLIM_INFINITY else min(memory_bytes, old_hard)
    resource.setrlimit(resource.RLIMIT_AS, (min(memory_bytes, hard), hard))

    cpu_seconds = 600
    old_soft, old_hard = resource.getrlimit(resource.RLIMIT_CPU)
    cpu_hard = cpu_seconds + 5 if old_hard == resource.RLIM_INFINITY else min(cpu_seconds + 5, old_hard)
    resource.setrlimit(resource.RLIMIT_CPU, (min(cpu_seconds, cpu_hard), cpu_hard))

    affinity = sorted(os.sched_getaffinity(0))
    if len(affinity) < 2:
        raise SystemExit("refusing command matrix without at least two CPUs for half-CPU affinity")
    keep = max(1, len(affinity) // 2)
    os.sched_setaffinity(0, set(affinity[:keep]))
    MATRIX_LIMITS = {
        "address_space_mb": hard // (1024 * 1024),
        "cpu_time_seconds": min(cpu_seconds, cpu_hard),
        "affinity_cpus": keep,
        "host_cpus": len(affinity),
        "wall_seconds": 1200,
    }


def _raise_wall_timeout(_signum, _frame):
    raise TimeoutError("command matrix exceeded 1200-second wall limit")


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * percentile) - 1)], 3)


def _normalized_sql(sql: str) -> str:
    return " ".join(str(sql).split())


def _db_path(database) -> str:
    if isinstance(database, bytes):
        database = database.decode("utf-8", "replace")
    value = str(database)
    if value.startswith("file:"):
        value = value[5:].split("?", 1)[0]
    return value


class QueryRecorder:
    def __init__(self) -> None:
        self.active = False
        self.by_sql: dict[tuple[str, str], dict[str, float | int]] = {}
        self.by_database: dict[str, dict[str, float | int]] = {}
        self.connections: dict[int, str] = {}

    def reset(self) -> None:
        self.by_sql.clear()
        self.by_database.clear()

    def record(self, connection, sql: str, phase: str, elapsed_ms: float) -> None:
        if not self.active:
            return
        database = self.connections.get(id(connection), "unknown")
        normalized = _normalized_sql(sql)
        query = self.by_sql.setdefault(
            (Path(database).name, normalized),
            {"count": 0, "execute_ms": 0.0, "fetch_ms": 0.0},
        )
        db = self.by_database.setdefault(
            Path(database).name,
            {"query_count": 0, "execute_ms": 0.0, "fetch_ms": 0.0},
        )
        if phase == "execute":
            query["count"] += 1
            db["query_count"] += 1
        query[f"{phase}_ms"] += elapsed_ms
        db[f"{phase}_ms"] += elapsed_ms

    def snapshot(self) -> dict:
        by_database = {
            name: {
                **values,
                "total_ms": round(float(values["execute_ms"]) + float(values["fetch_ms"]), 3),
            }
            for name, values in sorted(self.by_database.items())
        }
        top_queries = []
        for (database, sql), values in sorted(
            self.by_sql.items(),
            key=lambda item: float(item[1]["execute_ms"]) + float(item[1]["fetch_ms"]),
            reverse=True,
        )[:20]:
            top_queries.append(
                {
                    "database": database,
                    "sql": sql,
                    **values,
                    "total_ms": round(float(values["execute_ms"]) + float(values["fetch_ms"]), 3),
                }
            )
        return {"by_database": by_database, "top_queries": top_queries}


RECORDER = QueryRecorder()
ORIGINAL_CONNECT = sqlite3.connect


class TimedCursor(sqlite3.Cursor):
    def __init__(self, connection):
        super().__init__(connection)
        self._matrix_sql = ""

    def execute(self, sql, parameters=()):
        self._matrix_sql = _normalized_sql(sql)
        started = time.perf_counter()
        try:
            return super().execute(sql, parameters)
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "execute", (time.perf_counter() - started) * 1000)

    def executemany(self, sql, seq_of_parameters):
        self._matrix_sql = _normalized_sql(sql)
        started = time.perf_counter()
        try:
            return super().executemany(sql, seq_of_parameters)
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "execute", (time.perf_counter() - started) * 1000)

    def executescript(self, sql_script):
        self._matrix_sql = _normalized_sql(sql_script)
        started = time.perf_counter()
        try:
            return super().executescript(sql_script)
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "execute", (time.perf_counter() - started) * 1000)

    def fetchone(self):
        started = time.perf_counter()
        try:
            return super().fetchone()
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "fetch", (time.perf_counter() - started) * 1000)

    def fetchmany(self, size=None):
        started = time.perf_counter()
        try:
            return super().fetchmany() if size is None else super().fetchmany(size)
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "fetch", (time.perf_counter() - started) * 1000)

    def fetchall(self):
        started = time.perf_counter()
        try:
            return super().fetchall()
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "fetch", (time.perf_counter() - started) * 1000)

    def __next__(self):
        started = time.perf_counter()
        try:
            return super().__next__()
        finally:
            RECORDER.record(self.connection, self._matrix_sql, "fetch", (time.perf_counter() - started) * 1000)


class TimedConnection(sqlite3.Connection):
    def cursor(self, factory=None):
        return super().cursor(factory or TimedCursor)

    def execute(self, sql, parameters=()):
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, seq_of_parameters):
        return self.cursor().executemany(sql, seq_of_parameters)

    def executescript(self, sql_script):
        return self.cursor().executescript(sql_script)

    def close(self):
        try:
            return super().close()
        finally:
            RECORDER.connections.pop(id(self), None)


def _install_sql_timing() -> None:
    def traced_connect(database, *args, **kwargs):
        kwargs.setdefault("factory", TimedConnection)
        connection = ORIGINAL_CONNECT(database, *args, **kwargs)
        RECORDER.connections[id(connection)] = _db_path(database)
        return connection

    sqlite3.connect = traced_connect


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fixture_data(fixture: Path) -> Path:
    if not (fixture / FIXTURE_MARKER).is_file():
        raise SystemExit(f"refusing an unowned fixture without {FIXTURE_MARKER}: {fixture}")
    data = fixture / "data"
    if not (data / "xiuxian" / "xiuxian.db").is_file():
        raise SystemExit(f"fixture database missing under {data}")
    return data


def _manifest_path(fixture: Path) -> Path:
    return fixture / "data" / "command-matrix-fixture.json"


def _table_digest(connection) -> str:
    digest = hashlib.sha256()
    cursor = connection.execute(
        "SELECT user_id, exp, stone FROM user_xiuxian ORDER BY id"
    )
    for row in cursor:
        digest.update("\x1f".join("" if value is None else str(value) for value in row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def expand_fixture(fixture: Path, target_users: int) -> dict:
    if target_users < 1 or target_users > TARGET_USERS:
        raise RuntimeError(f"fixture expansion is limited to 1..{TARGET_USERS} users")
    data = _fixture_data(fixture)
    database = data / "xiuxian" / "xiuxian.db"
    connection = ORIGINAL_CONNECT(database, timeout=30)
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from nonebot_plugin_xiuxian_2.core.numeric import register_numeric_functions

        register_numeric_functions(connection)
        connection.execute("PRAGMA synchronous=NORMAL")
        columns = [row[1] for row in connection.execute("PRAGMA table_info(user_xiuxian)")]
        if not {"id", "user_id", "user_name", "exp", "stone"}.issubset(columns):
            raise RuntimeError(f"unexpected user_xiuxian columns: {columns}")
        count = int(connection.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0])
        if count > target_users:
            raise RuntimeError(f"fixture already has {count} users, above target {target_users}")
        template = connection.execute(
            f"SELECT {', '.join(chr(34) + name + chr(34) for name in columns if name != 'id')} "
            "FROM user_xiuxian WHERE user_id=?",
            (ACTOR,),
        ).fetchone()
        if template is None:
            raise RuntimeError(f"fixture is missing actor {ACTOR}")
        insert_columns = [name for name in columns if name != "id"]
        quoted_columns = ", ".join('"' + name.replace('"', '""') + '"' for name in insert_columns)
        placeholders = ", ".join("?" for _ in insert_columns)
        insert_sql = f"INSERT INTO user_xiuxian ({quoted_columns}) VALUES ({placeholders})"
        indexes = {name for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='user_xiuxian'"
        )}
        removed_probe_indexes = sorted(name for name in indexes if name.startswith("idx_probe_legacy_"))
        connection.execute("BEGIN IMMEDIATE")
        for name in removed_probe_indexes:
            connection.execute('DROP INDEX "' + name.replace('"', '""') + '"')
            indexes.remove(name)
        batch = []
        for sequence in range(count, target_users):
            row = list(template)
            values = dict(zip(insert_columns, row))
            values["user_id"] = str(2_000_000_000_000 + sequence)
            values["user_name"] = f"性能道友{sequence:07d}"
            values["exp"] = str(10**20 + sequence)
            values["stone"] = str(10**21 + sequence * 3)
            batch.append(tuple(values[name] for name in insert_columns))
            if len(batch) == 2000:
                connection.executemany(insert_sql, batch)
                batch.clear()
        if batch:
            connection.executemany(insert_sql, batch)
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        result = {
            "users": int(connection.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]),
            "database_bytes": database.stat().st_size,
            "index_names": sorted(indexes),
            "removed_probe_indexes": removed_probe_indexes,
        }
        result["user_rank_data_sha256"] = _table_digest(connection)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    manifest = {
        "fixture_id": "isolated-synthetic-command-matrix-1m-v1",
        "users": result["users"],
        "database_bytes": result["database_bytes"],
        "user_rank_data_sha256": result["user_rank_data_sha256"],
        "index_names": result["index_names"],
        "removed_probe_indexes": result["removed_probe_indexes"],
    }
    _manifest_path(fixture).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


class DiskFixtureSnapshot:
    def __init__(self, root: Path, managers: tuple, snapshot_dir: Path) -> None:
        self.root = root
        self.managers = managers
        self.snapshot_dir = snapshot_dir
        self.snapshot_dir.mkdir()
        self.json_files = {path: path.read_bytes() for path in root.parent.rglob("*.json")}
        self.databases: dict[Path, Path] = {}
        self.snapshot_connections = []
        for number, path in enumerate(sorted(root.glob("*.db"))):
            snapshot_path = snapshot_dir / f"database-{number}.db"
            with ORIGINAL_CONNECT(path) as source, ORIGINAL_CONNECT(snapshot_path) as target:
                source.backup(target, pages=256)
            self.databases[path] = snapshot_path
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_exp_cd import mentor_exp_cd
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.impart_pk import impart_pk

        self.json_stores = [(store, deepcopy(store.data)) for store in (mentor_exp_cd, impart_pk)]

    def restore(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.lay_out import _shutdown_cooldown_runtimes, limit_all_data
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

        _shutdown_cooldown_runtimes()
        limit_all_data.clear()
        for path in self.root.parent.rglob("*.json"):
            if path not in self.json_files:
                path.unlink()
        for path, content in self.json_files.items():
            if not path.exists() or path.read_bytes() != content:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        for store, data in self.json_stores:
            store.data = deepcopy(data)
        for manager in self.managers:
            connection = getattr(manager, "conn", None)
            if connection is not None:
                connection.rollback()
            for method in ("_clear_read_cache", "_clear_field_list_cache"):
                if callable(getattr(manager, method, None)):
                    getattr(manager, method)()
            for attribute in ("_ensured_tables", "_ensured_fields"):
                if hasattr(manager, attribute):
                    getattr(manager, attribute).clear()
        for path, snapshot_path in self.databases.items():
            with ORIGINAL_CONNECT(snapshot_path) as source, ORIGINAL_CONNECT(path) as target:
                source.backup(target, pages=256)
                target.execute("PRAGMA journal_size_limit=67108864")
                target.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        for database_path in self.databases:
            process_read_cache.invalidate_database(database_path)
            process_read_cache.invalidate_snapshot_database(database_path)
        _assert_wal_limit(self.root.parent, 256 * 1024 * 1024)

    def close(self) -> None:
        shutil.rmtree(self.snapshot_dir)


def _database_files(data_dir: Path) -> dict[str, int]:
    result = {}
    for path in sorted((data_dir / "xiuxian").glob("*.db*")):
        result[path.name] = path.stat().st_size
    return result


def _fixture_table_rows(database_root: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    # Only databases in the isolated runtime data tree are copied/restored by
    # DiskFixtureSnapshot; nearby experiment artifacts are not fixture inputs.
    database_root = database_root.resolve()
    for database in sorted(database_root.glob("*.db")):
        if not database.resolve().is_relative_to(database_root):
            raise RuntimeError(f"fixture database escapes runtime data root: {database}")
        with ORIGINAL_CONNECT(database, timeout=30) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            for (table_name,) in tables:
                quoted = '"' + str(table_name).replace('"', '""') + '"'
                count = connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
                database_name = database.relative_to(database_root.parent).as_posix()
                result[f"{database_name}::{table_name}"] = int(count)
    return result


def _tree_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _assert_wal_limit(data_dir: Path, limit_bytes: int = 256 * 1024 * 1024) -> int:
    largest = 0
    for path in (data_dir / "xiuxian").glob("*.db-wal"):
        largest = max(largest, path.stat().st_size)
    if largest > limit_bytes:
        raise RuntimeError(f"matrix WAL exceeded {limit_bytes} bytes: {largest}")
    return largest


def _resolve_entry(inventory: list[dict], command: str) -> dict:
    matches = [
        item for item in inventory
        if item["command"] == command or command in item["aliases"]
    ]
    if len(matches) != 1:
        raise RuntimeError(f"command {command!r} resolved to {len(matches)} matchers")
    return matches[0]


def _read_report_path(fixture: Path, report: Path) -> None:
    if not report.resolve().is_relative_to((fixture / "data").resolve()):
        raise SystemExit(f"report must remain inside fixture data: {report}")
    report.parent.mkdir(parents=True, exist_ok=True)


async def run_matrix(fixture: Path, report: Path, request_count: int) -> dict:
    fixture = fixture.resolve()
    data_dir = _fixture_data(fixture)
    _read_report_path(fixture, report)
    host_memory_before = _host_memory()
    host_disk_before = shutil.disk_usage(data_dir).free
    fixture_bytes_before = _tree_size(data_dir)
    fixture_manifest = _manifest_path(fixture)
    if not fixture_manifest.is_file():
        raise SystemExit(f"refusing a matrix fixture without {fixture_manifest.name}")
    fixture_identity = json.loads(fixture_manifest.read_text(encoding="utf-8"))
    if (
        fixture_identity.get("fixture_id") != "isolated-synthetic-command-matrix-1m-v1"
        or fixture_identity.get("users") != TARGET_USERS
    ):
        raise SystemExit("refusing a matrix fixture that is not the validated 1M-user dataset")
    available_mb = host_memory_before.get("available_mb")
    if available_mb is None or available_mb < 768:
        raise SystemExit(
            f"insufficient available RAM for bounded matrix: {available_mb} MiB; require 768 MiB"
        )
    xiuxian_data_dir = data_dir / "xiuxian"
    snapshot_database_bytes = sum(
        path.stat().st_size for path in xiuxian_data_dir.glob("*.db") if path.is_file()
    )
    required_free_bytes = max(
        2 * snapshot_database_bytes + 1024 * 1024 * 1024,
        2 * 1024 * 1024 * 1024,
    )
    if host_disk_before < required_free_bytes:
        raise SystemExit(
            f"insufficient free disk for bounded fixture restore: "
            f"available={host_disk_before}, required={required_free_bytes}"
        )
    os.environ["XIUXIAN_TEST_DATA_DIR"] = str(xiuxian_data_dir)
    temp_dir = xiuxian_data_dir / f".command-matrix-tmp-{os.getpid()}"
    temp_dir.mkdir()
    os.environ["TMPDIR"] = str(temp_dir)
    tempfile.tempdir = str(temp_dir)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import tests.command_probe as probe
    boundary_root = Path(probe.tests._data_dir).resolve()
    if boundary_root != xiuxian_data_dir.resolve():
        raise RuntimeError(
            f"test data root mismatch: expected {xiuxian_data_dir.resolve()}, got {boundary_root}"
        )
    for database in boundary_root.glob("*.db"):
        if not database.resolve().is_relative_to(boundary_root):
            raise RuntimeError(f"fixture database escapes runtime data root: {database}")
    probe.isolate_source()
    probe.install_boundary(boundary_root)
    _install_sql_timing()
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    context, managers = await probe.prepare(seed=False)
    # Ensure each statically inventoried handler module has registered its real
    # matcher even when offline plugin discovery omits a legacy package.
    for module_name in MATRIX_MATCHER_MODULES:
        importlib.import_module(module_name)
    entries = probe.inventory()
    resolved = {
        key: _resolve_entry(entries, command)
        for key, _, command, _, _ in SPECS
    }
    game = managers[0]
    fixture_users = int(
        game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]
    )
    if fixture_users != TARGET_USERS:
        raise RuntimeError(
            f"fixture row count changed before matrix: expected {TARGET_USERS}, got {fixture_users}"
        )
    actor_row = game.conn.execute(
        "SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", (ACTOR,)
    ).fetchone()
    if actor_row is None:
        count_row = game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()
        raise RuntimeError(
            f"fixture is missing actor {ACTOR}: data_dir={data_dir}, "
            f"database_path={getattr(game, 'database_path', None)}, "
            f"context_game_db={context.paths.game_db}, users={count_row[0] if count_row else None}"
        )
    fixture_table_rows = _fixture_table_rows(boundary_root)
    if fixture_table_rows.get("xiuxian/xiuxian.db::user_xiuxian") != fixture_users:
        raise RuntimeError("all-table manifest disagrees with the validated user row count")

    # Keep the inventory/equipment path representative while the 1M-row user
    # table stresses profile and ranking reads.
    for goods_id, name, goods_type, quantity in (
        (7001, "精铁符剑", "装备", 1),
        (6001, "修士道袍", "装备", 1),
        (1101, "生骨丹", "丹药", 8),
        (3001, "恒心草", "药材", 30),
    ):
        existing = game.get_item_by_good_id_and_user_id(ACTOR, goods_id)
        if not existing:
            game.send_back(ACTOR, goods_id, name, goods_type, quantity)
    game.conn.execute(
        "UPDATE back SET create_time=%s, update_time=%s WHERE user_id=%s",
        ("2026-01-01 00:00:00", "2026-01-01 00:00:00", ACTOR),
    )
    game._commit_write()
    game._clear_read_cache()
    snapshot_dir = xiuxian_data_dir / f".command-matrix-snapshot-{os.getpid()}"
    if snapshot_dir.exists():
        raise RuntimeError(f"refusing to reuse existing snapshot directory: {snapshot_dir}")
    snapshot = DiskFixtureSnapshot(xiuxian_data_dir, managers, snapshot_dir)
    process_read_cache.clear()
    sample_results = {}
    resource_before = resource.getrusage(resource.RUSAGE_SELF)
    wall_before = time.perf_counter()
    try:
        for spec_index, (key, category, command, input_text, restore_each) in enumerate(SPECS):
            entry = resolved[key]
            if restore_each:
                snapshot.restore()
            cache_before = process_read_cache.stats()
            print(f"matrix {key} ({category})", flush=True)
            samples = []
            aggregate: dict[tuple[str, str], dict[str, float | int]] = {}
            aggregate_db: dict[str, dict[str, float | int]] = {}
            for number in range(request_count):
                if restore_each:
                    snapshot.restore()
                request_cache_before = process_read_cache.stats()
                RECORDER.reset()
                RECORDER.active = True
                cpu_before = resource.getrusage(resource.RUSAGE_SELF)
                started = time.perf_counter()
                try:
                    result = await probe.run_case(
                        entry,
                        "missing",
                        810000 + spec_index * request_count + number,
                        actor=ACTOR,
                        input_text=input_text,
                        timeout=15,
                    )
                finally:
                    RECORDER.active = False
                elapsed_ms = (time.perf_counter() - started) * 1000
                cpu_after = resource.getrusage(resource.RUSAGE_SELF)
                reply = "\n".join(result.get("replies") or [])
                reply_bytes = reply.encode("utf-8")
                query_metrics = RECORDER.snapshot()
                request_cache_after = process_read_cache.stats()
                request_cache_delta = {
                    name: request_cache_after[name] - request_cache_before[name]
                    for name in ("hits", "misses", "waits", "expirations", "evictions", "invalidations", "oversized", "bypasses")
                }
                db_total_ms = sum(item["total_ms"] for item in query_metrics["by_database"].values())
                for query in RECORDER.by_sql.items():
                    aggregate.setdefault(query[0], {"count": 0, "execute_ms": 0.0, "fetch_ms": 0.0})
                    for field, value in query[1].items():
                        aggregate[query[0]][field] += value
                for database, values in query_metrics["by_database"].items():
                    aggregate_db.setdefault(database, {"query_count": 0, "execute_ms": 0.0, "fetch_ms": 0.0})
                    for field in ("query_count", "execute_ms", "fetch_ms"):
                        value = values[field]
                        aggregate_db[database][field] += value
                samples.append(
                    {
                        "latency_ms": round(elapsed_ms, 3),
                        "status": result.get("status"),
                        "reply_bytes": len(reply_bytes),
                        "reply_sha256": hashlib.sha256(reply_bytes).hexdigest(),
                        "errors": result.get("errors", []),
                        "blocked": result.get("blocked", []),
                        "db_query_count": sum(item["query_count"] for item in query_metrics["by_database"].values()),
                        "db_total_ms": round(db_total_ms, 3),
                        "cpu_user_ms": round((cpu_after.ru_utime - cpu_before.ru_utime) * 1000, 3),
                        "cpu_system_ms": round((cpu_after.ru_stime - cpu_before.ru_stime) * 1000, 3),
                        "process_max_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3),
                        "shared_cache_delta": request_cache_delta,
                    }
                )
                _assert_wal_limit(data_dir)
            digests = sorted({sample["reply_sha256"] for sample in samples})
            top_queries = [
                {
                    "database": database,
                    "sql": sql,
                    **values,
                    "total_ms": round(float(values["execute_ms"]) + float(values["fetch_ms"]), 3),
                }
                for (database, sql), values in sorted(
                    aggregate.items(),
                    key=lambda item: float(item[1]["execute_ms"]) + float(item[1]["fetch_ms"]),
                    reverse=True,
                )[:20]
            ]
            cache_after = process_read_cache.stats()
            sample_results[key] = {
                "category": category,
                "command": command,
                "input_text": input_text,
                "entry": {
                    "id": entry["id"],
                    "module": entry["module"],
                    "handlers": entry["handlers"],
                },
                "requests": len(samples),
                "p50_ms": _percentile([sample["latency_ms"] for sample in samples], 0.50),
                "p95_ms": _percentile([sample["latency_ms"] for sample in samples], 0.95),
                "p99_ms": _percentile([sample["latency_ms"] for sample in samples], 0.99),
                "error_rate": sum(
                    bool(sample["status"] != "pass" or sample["errors"] or sample["blocked"])
                    for sample in samples
                ) / len(samples),
                "reply_bytes": sorted({sample["reply_bytes"] for sample in samples}),
                "reply_sha256": digests,
                "reply_digest_stable": len(digests) == 1,
                "cold_miss_latency_ms": [
                    sample["latency_ms"]
                    for sample in samples
                    if sample["shared_cache_delta"]["misses"] > 0
                ],
                "cache_hit_requests": sum(
                    sample["shared_cache_delta"]["hits"] > 0
                    and sample["shared_cache_delta"]["misses"] == 0
                    for sample in samples
                ),
                "cache_miss_requests": sum(
                    sample["shared_cache_delta"]["misses"] > 0 for sample in samples
                ),
                "db_query_count_total": sum(sample["db_query_count"] for sample in samples),
                "db_query_count_per_request": round(
                    sum(sample["db_query_count"] for sample in samples) / len(samples), 3
                ),
                "db_total_ms_total": round(sum(sample["db_total_ms"] for sample in samples), 3),
                "db_total_ms_per_request": round(
                    sum(sample["db_total_ms"] for sample in samples) / len(samples), 3
                ),
                "database_breakdown": {
                    name: {
                        **values,
                        "total_ms": round(float(values["execute_ms"]) + float(values["fetch_ms"]), 3),
                    }
                    for name, values in sorted(aggregate_db.items())
                },
                "top_queries": top_queries,
                "shared_cache": {
                    "counter_delta": {
                        name: cache_after[name] - cache_before[name]
                        for name in (
                            "hits",
                            "misses",
                            "waits",
                            "expirations",
                            "evictions",
                            "invalidations",
                            "oversized",
                            "bypasses",
                        )
                    },
                    "end_state": {
                        name: cache_after[name]
                        for name in (
                            "entries",
                            "inflight",
                            "estimated_bytes",
                            "generation_paths",
                            "max_entries",
                            "max_bytes",
                            "max_inflight",
                        )
                    },
                },
                "samples": samples,
            }
            if restore_each:
                snapshot.restore()
    finally:
        RECORDER.active = False
        snapshot.restore()
        snapshot.close()
        source_copy = data_dir / "command-source"
        if (source_copy / ".rank-hotpath-source").is_file():
            shutil.rmtree(source_copy)
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

    resource_after = resource.getrusage(resource.RUSAGE_SELF)
    manifest_path = _manifest_path(fixture)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
    database = data_dir / "xiuxian" / "xiuxian.db"
    with ORIGINAL_CONNECT(database) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        dataset_users = int(connection.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    database_files_after_cleanup = _database_files(data_dir)
    result = {
        "status": "pass",
        "entry": "real registered OneBot matcher -> handler -> mocked transport via tests.command_probe.run_case",
        "shared_cache_scope": (
            "process-wide and shared across users within this Python process; "
            "not shared across worker processes"
        ),
        "fixture": str(fixture),
        "fixture_manifest": manifest,
        "users": dataset_users,
        "fixture_table_rows": fixture_table_rows,
        "request_count_per_command": request_count,
        "commands": sample_results,
        "database_files_after_cleanup": database_files_after_cleanup,
        "database": {
            "xiuxian_db_bytes": database.stat().st_size,
            "page_count": page_count,
            "page_size": page_size,
            "file_sha256_after_restore": _sha256(database),
        },
        "process": {
            "wall_s": round(time.perf_counter() - wall_before, 3),
            "cpu_user_s": round(resource_after.ru_utime - resource_before.ru_utime, 3),
            "cpu_system_s": round(resource_after.ru_stime - resource_before.ru_stime, 3),
            "max_rss_mb": round(resource_after.ru_maxrss / 1024, 3),
        },
        "host_resources": {
            "memory_before": host_memory_before,
            "memory_after": _host_memory(),
            "disk_available_before_bytes": host_disk_before,
            "disk_available_after_bytes": shutil.disk_usage(data_dir).free,
            "minimum_available_memory_mb": 768,
            "snapshot_database_bytes": snapshot_database_bytes,
            "minimum_free_disk_bytes": required_free_bytes,
            "fixture_bytes_before": fixture_bytes_before,
            "fixture_bytes_after": _tree_size(data_dir),
            "max_wal_bytes": _assert_wal_limit(data_dir),
            "limits_enforced_by": "process address-space/CPU-time limits and CPU affinity"
            if MATRIX_LIMITS
            else "not enforced",
            "limits": MATRIX_LIMITS,
    },
    }
    report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


async def _run(args: argparse.Namespace) -> int:
    fixture = args.fixture.resolve()
    if args.expand_to:
        result = expand_fixture(fixture, args.expand_to)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if not args.report:
        raise SystemExit("--report is required unless --expand-to is used")
    report = args.report.resolve()
    result = await run_matrix(fixture, report, args.requests)
    print(json.dumps({
        "status": result["status"],
        "users": result["users"],
        "commands": {
            key: {
                "p50_ms": value["p50_ms"],
                "p95_ms": value["p95_ms"],
                "error_rate": value["error_rate"],
                "reply_sha256": value["reply_sha256"],
                "db_query_count_per_request": value["db_query_count_per_request"],
                "db_total_ms_per_request": value["db_total_ms_per_request"],
                "shared_cache": value["shared_cache"],
            }
            for key, value in result["commands"].items()
        },
        "report": str(report),
    }, ensure_ascii=False, indent=2))
    errors = [
        (key, sample)
        for key, command in result["commands"].items()
        for sample in command["samples"]
        if sample["status"] != "pass" or sample["errors"] or sample["blocked"]
    ]
    return 1 if errors else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--requests", type=int, default=REQUESTS)
    parser.add_argument("--expand-to", type=int)
    args = parser.parse_args()
    if args.requests < 2 or args.requests > 50:
        parser.error("--requests must be between 2 and 50")
    try:
        import asyncio

        _enforce_matrix_limits()
        if MATRIX_LIMITS:
            signal.signal(signal.SIGALRM, _raise_wall_timeout)
            signal.alarm(int(MATRIX_LIMITS["wall_seconds"]))
        try:
            return asyncio.run(_run(args))
        finally:
            signal.alarm(0)
    except Exception as exc:
        if args.report:
            try:
                report = args.report.resolve()
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(json.dumps({
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            except Exception:
                pass
        raise


if __name__ == "__main__":
    raise SystemExit(main())
