"""Bounded same-fixture before/after probe for the personal rank hot path.

The fixture must already exist.  This probe never generates users and keeps the
legacy two-query variant as a runtime-only comparison against the checked-in
range-count query.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
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
        snapshot = probe.FixtureSnapshot(probe.tests._data_dir, managers)
        try:
            result = await probe.run_case(
                entry,
                "missing",
                1,
                actor="900001",
                input_text=COMMAND,
                timeout=8,
            )
        finally:
            snapshot.restore()
        payload = {
            "command": COMMAND,
            "entry": "live matcher/handler via tests.command_probe.run_case",
            "fixture": str(fixture),
            "status": result.get("status"),
            "reply_bytes": len("".join(result.get("replies") or []).encode("utf-8")),
            "errors": result.get("errors", []),
            "blocked": result.get("blocked", []),
        }
        report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if payload["status"] != "pass" or payload["errors"] or payload["blocked"]:
            raise RuntimeError(f"rank hotpath smoke failed: {json.dumps(payload, ensure_ascii=False)}")
        return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true", help="run one live command without performance sampling")
    args = parser.parse_args()
    result = asyncio.run(_smoke(args) if args.smoke else _probe(args))
    if args.smoke:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(json.dumps({key: result[key] for key in ("fixture", "users", "before", "after", "rss_mb")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
