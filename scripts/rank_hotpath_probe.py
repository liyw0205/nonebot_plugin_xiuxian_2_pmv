"""Bounded same-fixture before/after probe for the personal rank hot path.

The fixture must already exist.  This probe never generates users and keeps the
legacy two-query variant as a runtime-only comparison against the checked-in
range-count query.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import resource
import shutil
import sys
import tempfile
import time
import traceback
from contextlib import contextmanager


REQUESTS = 20
COMMAND = "我的修仙信息"
STATE_COMMAND = "我的状态"


def _legacy_rank_sql(column: str) -> str:
    return (
        "SELECT rank_value FROM (SELECT user_id, "
        f'{column}, dense_rank() OVER (ORDER BY xiuxian_int_text("{column}") '
        "COLLATE xiuxian_integer DESC) AS rank_value FROM user_xiuxian) "
        "AS ranked_users WHERE user_id=%s"
    )


def _range_rank_sql(column: str) -> str:
    value = f'xiuxian_int_text("{column}") COLLATE xiuxian_integer'
    return f"""
        WITH target AS (
            SELECT {value} AS value
            FROM user_xiuxian
            WHERE user_id=%s
        )
        SELECT CASE
            WHEN target.value IS NULL THEN 1 + (
                SELECT COUNT(DISTINCT {value})
                FROM user_xiuxian
                WHERE {value} IS NOT NULL
            )
            ELSE 1 + (
                SELECT COUNT(DISTINCT {value})
                FROM user_xiuxian
                WHERE {value} > target.value
            )
        END AS rank_value
        FROM target
    """


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def _rank_explain(connection, sql: str) -> list[str]:
    return [str(row[3]) for row in connection.execute("EXPLAIN QUERY PLAN " + sql, ("900001",)).fetchall()]


def _validate_report(fixture: Path, report: Path) -> None:
    if not report.is_relative_to(fixture / "data"):
        raise SystemExit(f"report must remain inside fixture data: {report}")
    report.parent.mkdir(parents=True, exist_ok=True)


def _write_failure_report(report: Path, fixture: Path, exc: BaseException) -> None:
    payload = {
        "fixture": str(fixture),
        "status": "failed",
        "error_type": type(exc).__name__,
        "error": str(exc),
        "traceback": traceback.format_exc(),
    }
    report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@contextmanager
def _fixture_context(fixture: Path, report: Path):
    """Prepare an isolated import boundary and always remove probe-owned files."""
    fixture = fixture.resolve()
    data_dir = fixture / "data" / "xiuxian"
    if not data_dir.is_dir():
        raise SystemExit(f"fixture data directory is missing: {data_dir}")
    _validate_report(fixture, report)
    source_copy = fixture / "data" / "command-source"
    if source_copy.exists():
        raise SystemExit(f"refusing to remove existing command-source: {source_copy}")
    temp_dir = data_dir / ".rank-hotpath-tmp"
    temp_dir.mkdir(parents=True)

    # tempfile caches the system directory, so set both values before importing
    # tests.command_probe or any plugin module that can allocate a temp file.
    os.environ["TMPDIR"] = str(temp_dir)
    tempfile.tempdir = str(temp_dir)
    os.environ["XIUXIAN_TEST_DATA_DIR"] = str(data_dir)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

    try:
        import tests.command_probe as probe

        probe.isolate_source()
        probe.install_boundary(probe.tests._data_dir)
        yield probe
    except (Exception, SystemExit) as exc:
        try:
            _write_failure_report(report, fixture, exc)
        except Exception:
            # Preserve the original traceback when the fixture itself is broken.
            pass
        raise
    finally:
        if source_copy.exists():
            shutil.rmtree(source_copy)
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


async def _run_variant(probe, entry, game, snapshot, *, legacy: bool) -> dict:
    snapshot.restore()
    manager_type = type(game)
    original_exp = manager_type.get_exp_rank
    original_stone = manager_type.get_stone_rank
    original_read = manager_type._read_query
    rank_sql = []
    rank_total_ms = 0.0

    if legacy:
        def legacy_exp(self, user_id):
            return self._read_query(_legacy_rank_sql("exp"), (user_id,), one=True)

        def legacy_stone(self, user_id):
            return self._read_query(_legacy_rank_sql("stone"), (user_id,), one=True)

        manager_type.get_exp_rank = legacy_exp
        manager_type.get_stone_rank = legacy_stone

    def traced_read(self, sql, params=None, **kwargs):
        nonlocal rank_total_ms
        started = time.perf_counter()
        result = original_read(self, sql, params, **kwargs)
        elapsed_ms = (time.perf_counter() - started) * 1000
        sql_text = str(sql)
        if "xiuxian_int_text" in sql_text and (
            "dense_rank() OVER" in sql_text or "COUNT(DISTINCT" in sql_text
        ):
            rank_total_ms += elapsed_ms
            rank_sql.append({"sql": " ".join(str(sql).split()), "elapsed_ms": round(elapsed_ms, 3)})
        return result

    manager_type._read_query = traced_read
    samples = []
    try:
        for number in range(1, REQUESTS + 1):
            started = time.perf_counter()
            result = await probe.run_case(
                entry,
                "missing",
                number,
                actor="900001",
                input_text=COMMAND,
                timeout=8,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            reply = "".join(result.get("replies") or [])
            samples.append(
                {
                    "latency_ms": round(elapsed_ms, 3),
                    "status": result.get("status"),
                    "reply_bytes": len(reply.encode("utf-8")),
                    "reply_sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest(),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                }
            )
    finally:
        manager_type._read_query = original_read
        manager_type.get_exp_rank = original_exp
        manager_type.get_stone_rank = original_stone

    values = [item["latency_ms"] for item in samples]
    return {
        "variant": "before_legacy_two_rank_reads" if legacy else "after_range_rank_read",
        "requests": len(samples),
        "samples": samples,
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "error_rate": sum(
            bool(item["status"] != "pass" or item["errors"] or item["blocked"])
            for item in samples
        ) / len(samples),
        "reply_bytes": sorted({item["reply_bytes"] for item in samples}),
        "reply_sha256": sorted({item["reply_sha256"] for item in samples}),
        "rank_sql_count": len(rank_sql),
        "rank_sql_total_ms": round(rank_total_ms, 3),
        "rank_sql": rank_sql,
    }


async def _run_profile_variant(
    probe,
    entry,
    game,
    snapshot,
    info_module,
    profile_repository,
    uow_module,
    *,
    legacy: bool,
) -> dict:
    """Compare the live command with and without reusing check_user's profile."""
    snapshot.restore()
    manager_type = type(game)
    original_info = info_module.get_user_xiuxian_info
    original_profile = profile_repository.PlayerProfileSqlRepository.get_user_profile
    original_uow_execute = uow_module.DatabaseUnitOfWork.execute
    original_manager_read = manager_type._read_query
    counters = {
        "profile_reads": 0,
        "profile_sql": [],
        "uow_execute": 0,
        "manager_read": 0,
    }

    def traced_profile(repository, user_id):
        counters["profile_reads"] += 1
        return original_profile(repository, user_id)

    def traced_uow_execute(unit, sql, params=()):
        counters["uow_execute"] += 1
        normalized = " ".join(str(sql).split())
        if "FROM user_xiuxian" in normalized:
            counters["profile_sql"].append(normalized)
        return original_uow_execute(unit, sql, params)

    def traced_manager_read(self, sql, params=None, **kwargs):
        counters["manager_read"] += 1
        return original_manager_read(self, sql, params, **kwargs)

    if legacy:
        async def legacy_info(user_id, *, user_info=None):
            return await original_info(user_id)

        info_module.get_user_xiuxian_info = legacy_info
    profile_repository.PlayerProfileSqlRepository.get_user_profile = traced_profile
    uow_module.DatabaseUnitOfWork.execute = traced_uow_execute
    manager_type._read_query = traced_manager_read
    samples = []
    try:
        for number in range(1, REQUESTS + 1):
            before = {
                "profile_reads": counters["profile_reads"],
                "uow_execute": counters["uow_execute"],
                "manager_read": counters["manager_read"],
            }
            started = time.perf_counter()
            result = await probe.run_case(
                entry,
                "missing",
                number,
                actor="900001",
                input_text=COMMAND,
                timeout=8,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            reply = "".join(result.get("replies") or [])
            samples.append(
                {
                    "latency_ms": round(elapsed_ms, 3),
                    "status": result.get("status"),
                    "reply_bytes": len(reply.encode("utf-8")),
                    "reply_sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest(),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                    "profile_reads": counters["profile_reads"] - before["profile_reads"],
                    "uow_execute": counters["uow_execute"] - before["uow_execute"],
                    "manager_read": counters["manager_read"] - before["manager_read"],
                }
            )
    finally:
        info_module.get_user_xiuxian_info = original_info
        profile_repository.PlayerProfileSqlRepository.get_user_profile = original_profile
        uow_module.DatabaseUnitOfWork.execute = original_uow_execute
        manager_type._read_query = original_manager_read

    values = [item["latency_ms"] for item in samples]
    return {
        "variant": "before_forced_duplicate_profile_read" if legacy else "after_reuse_checked_profile",
        "requests": len(samples),
        "samples": samples,
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "error_rate": sum(
            bool(item["status"] != "pass" or item["errors"] or item["blocked"])
            for item in samples
        ) / len(samples),
        "reply_bytes": sorted({item["reply_bytes"] for item in samples}),
        "reply_sha256": sorted({item["reply_sha256"] for item in samples}),
        "profile_reads": sum(item["profile_reads"] for item in samples),
        "uow_execute": sum(item["uow_execute"] for item in samples),
        "manager_read": sum(item["manager_read"] for item in samples),
        "profile_sql": counters["profile_sql"],
    }


async def _profile_probe(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == COMMAND)
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        info_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info.user_info"
        )
        profile_repository = importlib.import_module(
            "nonebot_plugin_xiuxian_2.features.info.profile_repository"
        )
        uow_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.infrastructure.database.uow"
        )
        data_dir = fixture / "data" / "xiuxian"
        result = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "users": game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0],
            "pragma": {
                name: game.conn.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous", "cache_size")
            },
            "database_files_before": {
                name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
                for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
            },
        }
        result["before"] = await _run_profile_variant(
            probe, entry, game, snapshot, info_module, profile_repository, uow_module, legacy=True
        )
        result["after"] = await _run_profile_variant(
            probe, entry, game, snapshot, info_module, profile_repository, uow_module, legacy=False
        )
        snapshot.restore()
        result["database_files_after"] = {
            name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
            for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
        }
        result["rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


async def _run_base_variant(probe, entry, game, snapshot, info_module, *, legacy: bool) -> dict:
    """Compare the legacy base-attribute profile read with the checked snapshot."""
    snapshot.restore()
    manager_type = type(game)
    original_attributes = info_module.get_player_attributes
    original_user_read = manager_type.get_user_info_with_id
    original_manager_read = manager_type._read_query
    counters = {"base_reads": 0, "base_sql": [], "manager_read": 0}

    def traced_user_read(manager, user_id):
        counters["base_reads"] += 1
        return original_user_read(manager, user_id)

    def traced_manager_read(manager, sql, params=None, **kwargs):
        counters["manager_read"] += 1
        normalized = " ".join(str(sql).split())
        if "SELECT * FROM user_xiuxian" in normalized:
            counters["base_sql"].append(normalized)
        return original_manager_read(manager, sql, params, **kwargs)

    if legacy:
        def legacy_attributes(user_id, **providers):
            providers.pop("base_provider", None)
            return original_attributes(user_id, **providers)

        info_module.get_player_attributes = legacy_attributes
    manager_type.get_user_info_with_id = traced_user_read
    manager_type._read_query = traced_manager_read
    samples = []
    try:
        for number in range(1, REQUESTS + 1):
            before = {
                "base_reads": counters["base_reads"],
                "base_sql": len(counters["base_sql"]),
                "manager_read": counters["manager_read"],
            }
            started = time.perf_counter()
            result = await probe.run_case(
                entry,
                "missing",
                number,
                actor="900001",
                input_text=COMMAND,
                timeout=8,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            reply = "".join(result.get("replies") or [])
            samples.append(
                {
                    "latency_ms": round(elapsed_ms, 3),
                    "status": result.get("status"),
                    "reply_bytes": len(reply.encode("utf-8")),
                    "reply_sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest(),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                    "base_reads": counters["base_reads"] - before["base_reads"],
                    "base_sql": len(counters["base_sql"]) - before["base_sql"],
                    "manager_read": counters["manager_read"] - before["manager_read"],
                }
            )
    finally:
        info_module.get_player_attributes = original_attributes
        manager_type.get_user_info_with_id = original_user_read
        manager_type._read_query = original_manager_read

    values = [item["latency_ms"] for item in samples]
    return {
        "variant": "before_legacy_base_profile_read" if legacy else "after_checked_base_snapshot",
        "requests": len(samples),
        "samples": samples,
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "error_rate": sum(
            bool(item["status"] != "pass" or item["errors"] or item["blocked"])
            for item in samples
        ) / len(samples),
        "reply_bytes": sorted({item["reply_bytes"] for item in samples}),
        "reply_sha256": sorted({item["reply_sha256"] for item in samples}),
        "base_reads": sum(item["base_reads"] for item in samples),
        "base_sql_count": sum(item["base_sql"] for item in samples),
        "manager_read": sum(item["manager_read"] for item in samples),
        "base_sql": counters["base_sql"],
    }


async def _base_probe(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == COMMAND)
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        info_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info.user_info"
        )
        data_dir = fixture / "data" / "xiuxian"
        result = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "users": game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0],
            "pragma": {
                name: game.conn.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous", "cache_size")
            },
            "database_files_before": {
                name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
                for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
            },
        }
        result["before"] = await _run_base_variant(
            probe, entry, game, snapshot, info_module, legacy=True
        )
        result["after"] = await _run_base_variant(
            probe, entry, game, snapshot, info_module, legacy=False
        )
        snapshot.restore()
        result["database_files_after"] = {
            name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
            for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
        }
        result["rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


async def _run_state_variant(
    probe,
    entry,
    game,
    snapshot,
    state_module,
    uow_module,
    *,
    legacy: bool,
) -> dict:
    """Compare the live 我的状态 path with its former duplicate reads."""
    snapshot.restore()
    manager_type = type(game)
    original_check_user = state_module.check_user
    original_base = state_module.get_base_attributes
    original_attributes = state_module.get_player_attributes
    original_user_read = manager_type.get_user_info_with_id
    original_player_data = manager_type.get_player_data
    original_buff_read = manager_type.get_user_buff_info
    original_manager_read = manager_type._read_query
    original_uow_execute = uow_module.DatabaseUnitOfWork.execute
    counters = {
        "user_reads": 0,
        "player_data_calls": 0,
        "player_data_ms": 0.0,
        "buff_reads": 0,
        "manager_read": 0,
        "uow_execute": 0,
        "sql_categories": {"user": 0, "buff": 0, "other": 0},
    }

    def traced_user_read(manager, user_id):
        counters["user_reads"] += 1
        return original_user_read(manager, user_id)

    def traced_buff_read(manager, user_id):
        counters["buff_reads"] += 1
        return original_buff_read(manager, user_id)

    def traced_player_data(manager, user_id, boss=False):
        started = time.perf_counter()
        try:
            return original_player_data(manager, user_id, boss=boss)
        finally:
            counters["player_data_calls"] += 1
            counters["player_data_ms"] += (time.perf_counter() - started) * 1000

    def traced_manager_read(manager, sql, params=None, **kwargs):
        counters["manager_read"] += 1
        normalized = " ".join(str(sql).split()).lower()
        if "user_xiuxian" in normalized:
            counters["sql_categories"]["user"] += 1
        elif "buffinfo" in normalized:
            counters["sql_categories"]["buff"] += 1
        else:
            counters["sql_categories"]["other"] += 1
        return original_manager_read(manager, sql, params, **kwargs)

    def traced_uow_execute(unit, sql, params=()):
        counters["uow_execute"] += 1
        return original_uow_execute(unit, sql, params)

    if legacy:
        def legacy_check_user(event):
            result = original_check_user(event)
            if result[0] and result[1]:
                state_module._sql_message().get_user_info_with_id(result[1]["user_id"])
                state_module._sql_message().get_player_data(result[1]["user_id"])
            return result

        def legacy_base(user_id, **kwargs):
            return original_base(user_id)

        def legacy_attributes(user_id, **providers):
            providers.pop("base_provider", None)
            providers.pop("buff_data_provider", None)
            return original_attributes(user_id, **providers)

        state_module.check_user = legacy_check_user
        state_module.get_base_attributes = legacy_base
        state_module.get_player_attributes = legacy_attributes

    manager_type.get_user_info_with_id = traced_user_read
    manager_type.get_player_data = traced_player_data
    manager_type.get_user_buff_info = traced_buff_read
    manager_type._read_query = traced_manager_read
    uow_module.DatabaseUnitOfWork.execute = traced_uow_execute
    samples = []
    try:
        for number in range(1, REQUESTS + 1):
            before = {
                "user_reads": counters["user_reads"],
                "player_data_calls": counters["player_data_calls"],
                "player_data_ms": counters["player_data_ms"],
                "buff_reads": counters["buff_reads"],
                "manager_read": counters["manager_read"],
                "uow_execute": counters["uow_execute"],
                "sql_categories": dict(counters["sql_categories"]),
            }
            started = time.perf_counter()
            result = await probe.run_case(
                entry,
                "missing",
                number,
                actor="900001",
                input_text=STATE_COMMAND,
                timeout=8,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            reply = "".join(result.get("replies") or [])
            samples.append(
                {
                    "latency_ms": round(elapsed_ms, 3),
                    "status": result.get("status"),
                    "reply_bytes": len(reply.encode("utf-8")),
                    "reply_sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest(),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                    "user_reads": counters["user_reads"] - before["user_reads"],
                    "player_data_calls": counters["player_data_calls"] - before["player_data_calls"],
                    "player_data_ms": round(
                        counters["player_data_ms"] - before["player_data_ms"], 3
                    ),
                    "buff_reads": counters["buff_reads"] - before["buff_reads"],
                    "manager_read": counters["manager_read"] - before["manager_read"],
                    "uow_execute": counters["uow_execute"] - before["uow_execute"],
                    "sql_categories": {
                        key: counters["sql_categories"][key] - before["sql_categories"][key]
                        for key in counters["sql_categories"]
                    },
                }
            )
    finally:
        state_module.check_user = original_check_user
        state_module.get_base_attributes = original_base
        state_module.get_player_attributes = original_attributes
        manager_type.get_user_info_with_id = original_user_read
        manager_type.get_player_data = original_player_data
        manager_type.get_user_buff_info = original_buff_read
        manager_type._read_query = original_manager_read
        uow_module.DatabaseUnitOfWork.execute = original_uow_execute

    values = [item["latency_ms"] for item in samples]
    return {
        "variant": "before_duplicate_state_reads" if legacy else "after_reused_state_snapshots",
        "requests": len(samples),
        "samples": samples,
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "error_rate": sum(
            bool(item["status"] != "pass" or item["errors"] or item["blocked"])
            for item in samples
        ) / len(samples),
        "reply_bytes": sorted({item["reply_bytes"] for item in samples}),
        "reply_sha256": sorted({item["reply_sha256"] for item in samples}),
        "user_reads": sum(item["user_reads"] for item in samples),
        "player_data_calls": sum(item["player_data_calls"] for item in samples),
        "player_data_ms": round(sum(item["player_data_ms"] for item in samples), 3),
        "buff_reads": sum(item["buff_reads"] for item in samples),
        "manager_read": sum(item["manager_read"] for item in samples),
        "uow_execute": sum(item["uow_execute"] for item in samples),
        "sql_categories": counters["sql_categories"],
    }


async def _state_probe(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute(
            "SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)
        ).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == STATE_COMMAND)
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        state_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff"
        )
        uow_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.infrastructure.database.uow"
        )
        data_dir = fixture / "data" / "xiuxian"
        result = {
            "command": STATE_COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "users": game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0],
            "pragma": {
                name: game.conn.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous", "cache_size")
            },
            "database_files_before": {
                name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
                for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
            },
        }
        result["before"] = await _run_state_variant(
            probe, entry, game, snapshot, state_module, uow_module, legacy=True
        )
        result["after"] = await _run_state_variant(
            probe, entry, game, snapshot, state_module, uow_module, legacy=False
        )
        snapshot.restore()
        result["database_files_after"] = {
            name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
            for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
        }
        result["rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


async def _run_buff_variant(
    probe,
    entry,
    game,
    snapshot,
    info_module,
    buff_data_module,
    uow_module,
    *,
    legacy: bool,
) -> dict:
    """Compare uncached BuffInfo reads with the per-request snapshot."""
    snapshot.restore()
    manager_type = type(game)
    original_property = buff_data_module.UserBuffDate.BuffInfo
    original_buff_read = manager_type.get_user_buff_info
    original_manager_read = manager_type._read_query
    original_uow_execute = uow_module.DatabaseUnitOfWork.execute
    counters = {"buff_reads": 0, "manager_read": 0, "uow_execute": 0, "buff_sql": []}

    def traced_buff_read(self, user_id):
        counters["buff_reads"] += 1
        return original_buff_read(self, user_id)

    def traced_manager_read(self, sql, params=None, **kwargs):
        counters["manager_read"] += 1
        normalized = " ".join(str(sql).split())
        if "BuffInfo" in normalized:
            counters["buff_sql"].append(normalized)
        return original_manager_read(self, sql, params, **kwargs)

    def traced_uow_execute(unit, sql, params=()):
        counters["uow_execute"] += 1
        return original_uow_execute(unit, sql, params)

    if legacy:
        def uncached_buff_info(instance):
            return buff_data_module._handle_module().get_user_buff(instance.user_id)

        buff_data_module.UserBuffDate.BuffInfo = property(uncached_buff_info)
    manager_type.get_user_buff_info = traced_buff_read
    manager_type._read_query = traced_manager_read
    uow_module.DatabaseUnitOfWork.execute = traced_uow_execute
    samples = []
    try:
        for number in range(1, REQUESTS + 1):
            before = {key: counters[key] for key in ("buff_reads", "manager_read", "uow_execute")}
            started = time.perf_counter()
            result = await probe.run_case(
                entry,
                "missing",
                number,
                actor="900001",
                input_text=COMMAND,
                timeout=8,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            reply = "".join(result.get("replies") or [])
            samples.append(
                {
                    "latency_ms": round(elapsed_ms, 3),
                    "status": result.get("status"),
                    "reply_bytes": len(reply.encode("utf-8")),
                    "reply_sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest(),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                    **{
                        key: counters[key] - before[key]
                        for key in ("buff_reads", "manager_read", "uow_execute")
                    },
                }
            )
    finally:
        buff_data_module.UserBuffDate.BuffInfo = original_property
        manager_type.get_user_buff_info = original_buff_read
        manager_type._read_query = original_manager_read
        uow_module.DatabaseUnitOfWork.execute = original_uow_execute

    values = [item["latency_ms"] for item in samples]
    return {
        "variant": "before_uncached_buff_reads" if legacy else "after_request_buff_snapshot",
        "requests": len(samples),
        "samples": samples,
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "error_rate": sum(
            bool(item["status"] != "pass" or item["errors"] or item["blocked"])
            for item in samples
        ) / len(samples),
        "reply_bytes": sorted({item["reply_bytes"] for item in samples}),
        "reply_sha256": sorted({item["reply_sha256"] for item in samples}),
        "buff_reads": sum(item["buff_reads"] for item in samples),
        "manager_read": sum(item["manager_read"] for item in samples),
        "uow_execute": sum(item["uow_execute"] for item in samples),
        "buff_sql": counters["buff_sql"],
    }


async def _buff_probe(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == COMMAND)
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        info_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info.user_info"
        )
        buff_data_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.buff_data"
        )
        uow_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.infrastructure.database.uow"
        )
        data_dir = fixture / "data" / "xiuxian"
        result = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "users": game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0],
            "pragma": {
                name: game.conn.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous", "cache_size")
            },
            "database_files_before": {
                name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
                for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
            },
        }
        result["before"] = await _run_buff_variant(
            probe, entry, game, snapshot, info_module, buff_data_module, uow_module, legacy=True
        )
        result["after"] = await _run_buff_variant(
            probe, entry, game, snapshot, info_module, buff_data_module, uow_module, legacy=False
        )
        snapshot.restore()
        result["database_files_after"] = {
            name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
            for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
        }
        result["rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


async def _run_title_variant(
    probe,
    entry,
    game,
    snapshot,
    info_module,
    title_package,
    uow_module,
    *,
    legacy: bool,
) -> dict:
    """Compare the live title snapshot with the former duplicate state read."""
    snapshot.restore()
    manager_type = type(game)
    original_info = info_module.get_user_xiuxian_info
    original_get_state = title_package.title_application.get_state
    original_uow_execute = uow_module.DatabaseUnitOfWork.execute
    counters = {"title_state_reads": 0, "title_sql": [], "uow_execute": 0}

    def traced_get_state(user_id):
        counters["title_state_reads"] += 1
        return original_get_state(user_id)

    def traced_uow_execute(unit, sql, params=()):
        counters["uow_execute"] += 1
        normalized = " ".join(str(sql).split())
        if "FROM title" in normalized:
            counters["title_sql"].append(normalized)
        return original_uow_execute(unit, sql, params)

    if legacy:
        async def legacy_info(user_id, *, user_info=None, equipped_title_id=None):
            # Keep the checked profile, but discard the handler's title snapshot
            # to reproduce the pre-optimization second title read.
            return await original_info(user_id, user_info=user_info)

        info_module.get_user_xiuxian_info = legacy_info
    title_package.title_application.get_state = traced_get_state
    uow_module.DatabaseUnitOfWork.execute = traced_uow_execute
    samples = []
    try:
        for number in range(1, REQUESTS + 1):
            before = {
                "title_state_reads": counters["title_state_reads"],
                "uow_execute": counters["uow_execute"],
                "title_sql": len(counters["title_sql"]),
            }
            started = time.perf_counter()
            result = await probe.run_case(
                entry,
                "missing",
                number,
                actor="900001",
                input_text=COMMAND,
                timeout=8,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            reply = "".join(result.get("replies") or [])
            samples.append(
                {
                    "latency_ms": round(elapsed_ms, 3),
                    "status": result.get("status"),
                    "reply_bytes": len(reply.encode("utf-8")),
                    "reply_sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest(),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                    "title_state_reads": counters["title_state_reads"] - before["title_state_reads"],
                    "title_sql": len(counters["title_sql"]) - before["title_sql"],
                    "uow_execute": counters["uow_execute"] - before["uow_execute"],
                }
            )
    finally:
        info_module.get_user_xiuxian_info = original_info
        title_package.title_application.get_state = original_get_state
        uow_module.DatabaseUnitOfWork.execute = original_uow_execute

    values = [item["latency_ms"] for item in samples]
    return {
        "variant": "before_duplicate_title_state_read" if legacy else "after_reused_title_snapshot",
        "requests": len(samples),
        "samples": samples,
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "error_rate": sum(
            bool(item["status"] != "pass" or item["errors"] or item["blocked"])
            for item in samples
        ) / len(samples),
        "reply_bytes": sorted({item["reply_bytes"] for item in samples}),
        "reply_sha256": sorted({item["reply_sha256"] for item in samples}),
        "title_state_reads": sum(item["title_state_reads"] for item in samples),
        "title_sql_count": sum(item["title_sql"] for item in samples),
        "uow_execute": sum(item["uow_execute"] for item in samples),
        "title_sql": counters["title_sql"],
    }


async def _title_probe(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == COMMAND)
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        info_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info.user_info"
        )
        title_package = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title"
        )
        uow_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.infrastructure.database.uow"
        )
        data_dir = fixture / "data" / "xiuxian"
        result = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "users": game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0],
            "pragma": {
                name: game.conn.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous", "cache_size")
            },
            "database_files_before": {
                name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
                for name in ("player.db", "player.db-wal", "player.db-shm")
            },
        }
        result["before"] = await _run_title_variant(
            probe, entry, game, snapshot, info_module, title_package, uow_module, legacy=True
        )
        result["after"] = await _run_title_variant(
            probe, entry, game, snapshot, info_module, title_package, uow_module, legacy=False
        )
        snapshot.restore()
        result["database_files_after"] = {
            name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
            for name in ("player.db", "player.db-wal", "player.db-shm")
        }
        result["rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


async def _probe(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == COMMAND)
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)

        data_dir = fixture / "data" / "xiuxian"
        exp_sql = (
            "SELECT rank_value FROM (SELECT user_id, exp, dense_rank() OVER "
            "(ORDER BY xiuxian_int_text(exp) COLLATE xiuxian_integer DESC) AS rank_value "
            "FROM user_xiuxian) AS ranked_users WHERE user_id=%s"
        )
        stone_sql = exp_sql.replace("exp", "stone")
        result = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "users": game.conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0],
            "database_files_bytes": {
                name: (data_dir / name).stat().st_size if (data_dir / name).exists() else 0
                for name in ("xiuxian.db", "xiuxian.db-wal", "xiuxian.db-shm")
            },
            "explain": {
                "exp_rank": _rank_explain(game.conn, exp_sql),
                "stone_rank": _rank_explain(game.conn, stone_sql),
                "range_exp_rank": _rank_explain(game.conn, _range_rank_sql("exp")),
                "range_stone_rank": _rank_explain(game.conn, _range_rank_sql("stone")),
            },
        }
        result["before"] = await _run_variant(probe, entry, game, snapshot, legacy=True)
        result["after"] = await _run_variant(probe, entry, game, snapshot, legacy=False)
        snapshot.restore()
        result["rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


async def _smoke(args: argparse.Namespace) -> dict:
    fixture = args.fixture.resolve()
    report = args.report.resolve()
    with _fixture_context(fixture, report) as probe:
        _, managers = await probe.prepare(seed=False)
        game = managers[0]
        if game.conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", ("900001",)).fetchone() is None:
            raise SystemExit("fixture is missing required actor user_id=900001")
        entry = next(item for item in probe.inventory() if item["command"] == COMMAND)
        image_entry = next(
            item for item in probe.inventory() if item["command"] == "我的修仙信息图片版"
        )
        state_entry = next(item for item in probe.inventory() if item["command"] == STATE_COMMAND)
        state_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff"
        )
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        cases = []
        for number, label, case_entry, input_text in (
            (1, "text", entry, COMMAND),
            (2, "alias", entry, "我的存档"),
            (3, "image", image_entry, "我的修仙信息图片版"),
            (4, "state", state_entry, STATE_COMMAND),
        ):
            snapshot.restore()
            result = await probe.run_case(
                case_entry,
                "missing",
                number,
                actor="900001",
                input_text=input_text,
                timeout=8,
            )
            cases.append(
                {
                    "label": label,
                    "command": input_text,
                    "status": result.get("status"),
                    "reply_bytes": len("".join(result.get("replies") or []).encode("utf-8")),
                    "errors": result.get("errors", []),
                    "blocked": result.get("blocked", []),
                }
            )
        snapshot.restore()
        unknown = await probe.run_case(
            state_entry,
            "unregistered_player",
            5,
            input_text=STATE_COMMAND,
            timeout=8,
        )
        unknown_reply = "".join(unknown.get("replies") or [])
        cases.append(
            {
                "label": "state-unregistered",
                "command": STATE_COMMAND,
                "status": unknown.get("status"),
                "reply_bytes": len(unknown_reply.encode("utf-8")),
                "reply_contains": "修仙界没有道友的信息" in unknown_reply,
                "errors": unknown.get("errors", []),
                "blocked": unknown.get("blocked", []),
            }
        )
        original_state_attributes = state_module.get_player_attributes
        state_module.get_player_attributes = lambda *_args, **_kwargs: None
        try:
            snapshot.restore()
            attribute_failure = await probe.run_case(
                state_entry,
                "missing",
                6,
                actor="900001",
                input_text=STATE_COMMAND,
                timeout=8,
            )
        finally:
            state_module.get_player_attributes = original_state_attributes
        failure_reply = "".join(attribute_failure.get("replies") or [])
        cases.append(
            {
                "label": "state-attribute-failure",
                "command": STATE_COMMAND,
                "status": attribute_failure.get("status"),
                "reply_bytes": len(failure_reply.encode("utf-8")),
                "reply_contains": "属性读取失败，请稍后再试。" in failure_reply,
                "errors": attribute_failure.get("errors", []),
                "blocked": attribute_failure.get("blocked", []),
            }
        )
        snapshot.restore()
        payload = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "status": "pass" if all(
                case["status"] == "pass" and not case["errors"] and not case["blocked"]
                and case.get("reply_contains", True)
                for case in cases
            ) else "exception",
            "cases": cases,
        }
        report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if payload["status"] != "pass":
            raise RuntimeError(f"rank hotpath smoke failed: {json.dumps(payload, ensure_ascii=False)}")
        return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true", help="run one live command without performance sampling")
    parser.add_argument("--profile-reuse", action="store_true", help="compare duplicate versus reused profile reads")
    parser.add_argument("--base-reuse", action="store_true", help="compare duplicate versus reused base attribute reads")
    parser.add_argument("--buff-reuse", action="store_true", help="compare duplicate versus per-request BuffInfo reads")
    parser.add_argument("--title-reuse", action="store_true", help="compare duplicate versus reused title state reads")
    parser.add_argument("--state-reuse", action="store_true", help="compare duplicate versus reused 我的状态 reads")
    args = parser.parse_args()
    if sum(bool(value) for value in (args.smoke, args.profile_reuse, args.base_reuse, args.buff_reuse, args.title_reuse, args.state_reuse)) > 1:
        parser.error("probe modes cannot be combined")
    result = asyncio.run(
        _smoke(args)
        if args.smoke
        else _profile_probe(args)
        if args.profile_reuse
        else _base_probe(args)
        if args.base_reuse
        else _buff_probe(args)
        if args.buff_reuse
        else _title_probe(args)
        if args.title_reuse
        else _state_probe(args)
        if args.state_reuse
        else _probe(args)
    )
    if args.smoke:
        print(json.dumps(result, ensure_ascii=False))
    elif args.profile_reuse:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    elif args.base_reuse:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    elif args.buff_reuse:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    elif args.title_reuse:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    elif args.state_reuse:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    else:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
