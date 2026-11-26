"""Small deterministic contracts for the mixed-load harness, not a load run."""
import asyncio
from contextvars import copy_context
from contextlib import AsyncExitStack
import io
import json
from pathlib import Path
import queue
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import mixed_command_probe as probe


def request(key="profile", actor="900001"):
    return {"key": key, "command": "profile", "input_text": "profile", "actor": actor}


class MixedProbeHelperTests(unittest.TestCase):
    def test_connection_diagnostics_bound_setup_and_keep_cursor_fetch_attribution(self):
        with tempfile.TemporaryDirectory() as directory:
            old_connect, old_router, old_recorder = sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER
            connections = []
            first, second = probe.request_row(request(), 1), probe.request_row(request(), 2)
            token = probe.ACTIVE.set(first)
            try:
                probe.install_sql_timing(directory)
                connection = sqlite3.connect(":memory:")
                connections.append(connection)
                connection.create_function("identity", 1, lambda value: value)
                cursor = connection.execute("SELECT identity(7)")
                connection.execute("SELECT 8").fetchone()
                self.assertEqual(cursor.fetchone()[0], 7)
                entry = first["connection_diagnostics"][0]
                self.assertTrue(entry["opened_in_request"])
                self.assertGreaterEqual(entry["open_ms"], 0)
                self.assertEqual(entry["udf_calls"], 1)
                self.assertGreater(entry["statements"][0]["authorizer_calls"], 0)
                self.assertGreater(entry["statements"][0]["fetch_ms"], 0)
                self.assertEqual([item["ordinal"] for item in entry["statements"]], [1, 2])
                probe.ACTIVE.set(second)
                connection.execute("SELECT 9").fetchone()
                reused = second["connection_diagnostics"][0]
                self.assertEqual(reused["connection"], entry["connection"])
                self.assertFalse(reused["opened_in_request"])
                self.assertEqual(reused["open_ms"], 0)
                self.assertEqual(reused["statements"][0]["ordinal"], 3)
                for _ in range(20):
                    connection.execute("SELECT 10").fetchone()
                self.assertEqual(len(reused["statements"]), 12)
                self.assertEqual(reused["statements_omitted"], 9)
                for _ in range(35):
                    connections.append(sqlite3.connect(":memory:"))
                self.assertEqual(len(second["connection_diagnostics"]), 32)
                self.assertGreater(second["connection_diagnostic_omitted_touches"], 0)
            finally:
                for connection in connections:
                    connection.close()
                probe.ACTIVE.reset(token)
                sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER = old_connect, old_router, old_recorder

    def test_report_streaming_does_not_build_a_second_full_json_string(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "report.json"
            value = {"samples": [{"user": "synthetic", "sql_ms": 1.25}] * 100}
            with patch.object(probe.json, "dumps", side_effect=AssertionError("full allocation")):
                probe.write_json_stream(target, value)
            self.assertEqual(json.loads(target.read_text()), value)
            with self.assertRaises(FileExistsError):
                probe.write_json_stream(target, value)

    def test_percentiles_expose_empty_and_small_sample_limits(self):
        self.assertIsNone(probe.percentile([], .99))
        self.assertEqual(probe.distribution([1, 2, 3, 4]),
                         {"p50": 2, "p95": 4, "p99": 4, "max": 4})

    def test_overlap_distinguishes_interval_overlap_from_a_task_count(self):
        self.assertEqual(probe.interval_overlap([
            {"start": 0, "end": 1}, {"start": 1, "end": 2}], "start", "end")["peak_active_intervals"], 1)
        overlapping = probe.interval_overlap([
            {"start": 0, "end": 2}, {"start": 1, "end": 3}], "start", "end")
        self.assertEqual(overlapping["peak_active_intervals"], 2)
        self.assertEqual(overlapping["time_with_multiple_active_ms"], 1000)

    def test_database_uri_decoding_and_snapshot_membership(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "space name.db"
            self.assertEqual(probe.database_path(path.as_uri() + "?mode=ro"), path)
            self.assertIsNone(probe.database_path(":memory:"))
            router = probe.QueryRouter(root)
            router.allowed_databases = {path}
            self.assertEqual(router.check_path(path, operation="connects"), path)
            with self.assertRaisesRegex(RuntimeError, "not covered"):
                router.check_path(root / "new.db", operation="connects")
            with self.assertRaisesRegex(RuntimeError, "outside isolated root"):
                router.check_path(root.parent / "outside.db", operation="connects")

    def test_attach_authorizer_rejects_outside_without_creating_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            router = probe.QueryRouter(root)
            allowed = root / "allowed.db"
            denied = root.parent / (root.name + "-denied.db")
            router.allowed_databases = {allowed}
            connection = probe.matrix.ORIGINAL_CONNECT(":memory:")
            try:
                connection.set_authorizer(router.authorizer)
                connection.execute("ATTACH DATABASE '" + str(allowed).replace("'", "''") + "' AS allowed")
                connection.execute("DETACH DATABASE allowed")
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute("ATTACH DATABASE ? AS denied", (str(denied),))
                self.assertFalse(denied.exists())
            finally:
                connection.close()

    def test_bound_attach_checks_actual_parameters_even_for_cached_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_connect, old_router, old_recorder = sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER
            connection = None
            try:
                router = probe.install_sql_timing(root)
                allowed, denied = root / "allowed.db", root.parent / (root.name + "-denied.db")
                router.allowed_databases = {allowed}
                connection = sqlite3.connect(":memory:")
                for _ in range(2):
                    connection.execute("ATTACH DATABASE ? AS attached", (str(allowed),))
                    connection.execute("DETACH DATABASE attached")
                with self.assertRaisesRegex(RuntimeError, "outside isolated root"):
                    connection.execute("ATTACH DATABASE ? AS attached", (str(denied),))
                self.assertFalse(denied.exists())
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute("ATTACH DATABASE upper(?) AS attached", (str(allowed),))
            finally:
                if connection is not None:
                    connection.close()
                sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER = old_connect, old_router, old_recorder

    def test_router_does_not_mix_context_owned_queries(self):
        router = probe.QueryRouter(Path(tempfile.gettempdir()))
        connection = object()
        router.connections[id(connection)] = "fixture.db"
        first, second = probe.request_row(request(), 1), probe.request_row(request(actor="900002"), 2)
        context_a, context_b = copy_context(), copy_context()
        context_a.run(probe.ACTIVE.set, first)
        context_b.run(probe.ACTIVE.set, second)
        context_a.run(router.record, connection, "SELECT 1", "execute", 3)
        context_b.run(router.record, connection, "SELECT 2", "execute", 7)
        context_a.run(router.record, connection, "SELECT 1", "fetch", 2)
        self.assertEqual(first["_recorder"].snapshot()["by_database"]["fixture.db"]["total_ms"], 5)
        self.assertEqual(second["_recorder"].snapshot()["by_database"]["fixture.db"]["total_ms"], 7)
        self.assertEqual(len(first["_recorder"].by_sql), 1)
        router.phase_active = True
        router.record(connection, "SELECT 3", "execute", 1)
        self.assertEqual(router.unattributed, 1)

    def test_only_explicit_message_worker_on_its_database_is_background(self):
        router = probe.QueryRouter(Path(tempfile.gettempdir()))
        message, game = object(), object()
        router.message_database = "/tmp/message.db"
        router.connections.update({id(message): "/tmp/message.db", id(game): "/tmp/game.db"})
        router.phase_active = True
        token = probe.BACKGROUND_SQL.set("message_writer")
        try:
            router.record(message, "INSERT INTO messages VALUES (?)", "execute", 2)
            router.record(game, "SELECT 1", "execute", 3)
        finally:
            probe.BACKGROUND_SQL.reset(token)
        router.record(message, "SELECT 2", "execute", 4)
        self.assertEqual(router.unattributed, 2)
        self.assertEqual(len(router.unattributed_details), 2)
        self.assertEqual(router.background.snapshot()["by_database"]["message.db"]["query_count"], 1)
        self.assertEqual(router.background.snapshot()["by_database"]["message.db"]["total_ms"], 2)

    def test_message_worker_wrapper_preserves_real_body_and_rejects_late_install(self):
        observed = []
        module = SimpleNamespace(_message_db_writer_started=False,
                                 _message_db_writer_loop=lambda: observed.append(probe.BACKGROUND_SQL.get()))
        module.init_message_db = lambda: module._message_db_writer_loop()
        with patch.object(probe, "ROUTER", probe.QueryRouter("/tmp")):
            probe.prepare_message_writer(module, "/tmp/message.db")
            self.assertEqual(observed, ["message_writer"])
            self.assertIsNone(probe.BACKGROUND_SQL.get())
            module._message_db_writer_started = True
            with self.assertRaisesRegex(RuntimeError, "started before"):
                probe.prepare_message_writer(module, "/tmp/message.db")

    def test_exact_rank_counters_use_complete_sql_beyond_the_top_twenty(self):
        recorder = probe.matrix.QueryRecorder()
        recorder.active = True
        connection = object()
        recorder.connections[id(connection)] = "game.db"
        for index in range(21):
            recorder.record(connection, f"SELECT {index}", "execute", 10)
        recorder.record(connection, 'SELECT 1 + COUNT(DISTINCT xiuxian_int_sort_key_compact("exp")) '
                        'FROM user_xiuxian WHERE xiuxian_int_sort_key_compact("exp") > ?', "execute", 1)
        metrics = probe.recorder_metrics(recorder)
        self.assertEqual(len(metrics["top_queries"]), 20)
        self.assertEqual(metrics["exact_rank_scans"],
                         {"statements": 1, "exp_recomputations": 1, "stone_recomputations": 0, "total_ms": 1})

    def test_rank_operations_keep_fast_seek_fetch_and_exclude_plans_from_counts(self):
        router = probe.QueryRouter(Path(tempfile.gettempdir()))
        row = probe.request_row(request(), 1)
        connection = SimpleNamespace(_mixed_diagnostic={
            "serial": 1, "database": "game.db", "opened_by": row["_connection_owner"],
            "open_ms": 0.0, "ordinal": 0, "statement": None})
        router.connections[id(connection)] = "game.db"
        count = ('SELECT 1 + COUNT(DISTINCT xiuxian_int_sort_key_compact("exp")) '
                 'FROM user_xiuxian WHERE xiuxian_int_sort_key_compact("exp") > ?')
        seek = ('WITH RECURSIVE numeric_rank_keys(value) AS '
                '(SELECT xiuxian_int_sort_key_compact("stone") FROM user_xiuxian) '
                'SELECT * FROM numeric_rank_keys LIMIT 128')
        token, cpu_token = probe.ACTIVE.set(row), probe.SQL_CPU_START.set(1.0)
        try:
            with patch.object(probe.time, "thread_time", return_value=1.002):
                for index in range(21):
                    sql = f"SELECT {index}"
                    router.begin_statement(connection, sql)
                    router.record(connection, sql, "execute", 10)
                with patch.object(probe.threading, "current_thread",
                                  return_value=SimpleNamespace(name="xiuxian-exact-rank_0")):
                    for sql, execute_ms, fetch_ms in (
                        (count, .5, 1.5), (seek, 1, 2),
                        ("EXPLAIN QUERY PLAN " + count, .25, .5),
                        ("EXPLAIN QUERY PLAN " + seek, .25, .5),
                    ):
                        router.begin_statement(connection, sql)
                        router.record(connection, sql, "execute", execute_ms)
                        router.record(connection, sql, "fetch", fetch_ms)
        finally:
            probe.SQL_CPU_START.reset(cpu_token)
            probe.ACTIVE.reset(token)
        metrics = probe.recorder_metrics(row["_recorder"], rank_cpu_ms=row["_rank_cpu_ms"])
        self.assertEqual(len(metrics["top_queries"]), 20)
        self.assertFalse(any(item["sql"] == seek for item in row["sql_timeline"]))
        self.assertEqual(len(row["connection_diagnostics"][0]["statements"]), 12)
        self.assertEqual(row["connection_diagnostics"][0]["statements_omitted"], 13)
        self.assertEqual(metrics["exact_rank_operations"], {
            "count": {"statements": 1, "execute_ms": .5, "fetch_ms": 1.5, "total_ms": 2, "cpu_ms": 4},
            "seek_batch": {"statements": 1, "execute_ms": 1, "fetch_ms": 2, "total_ms": 3, "cpu_ms": 4},
            "plan": {"statements": 2, "execute_ms": .5, "fetch_ms": 1, "total_ms": 1.5, "cpu_ms": 8},
        })
        self.assertEqual(metrics["exact_rank_scans"],
                         {"statements": 1, "exp_recomputations": 1, "stone_recomputations": 0, "total_ms": 2})
        self.assertEqual(row["sql_threads"]["xiuxian-exact-rank_0"]["calls"], 4)
        self.assertEqual(row["sql_threads"]["xiuxian-exact-rank_0"]["wall_ms"], 6.5)

    def test_cohorts_only_group_profiles_and_keep_all_rank_worker_totals(self):
        rows = []
        connection = object()
        seek = ('WITH RECURSIVE numeric_rank_keys(value) AS '
                '(SELECT xiuxian_int_sort_key_compact("exp") FROM user_xiuxian) '
                'SELECT * FROM numeric_rank_keys')
        for index, (key, cohort, latency) in enumerate((
            ("profile_info", "near", 10), ("profile_info", "near", 30),
            ("profile_info", "deep", 100), ("training", "near", 1000),
        )):
            row = probe.request_row({**request(key=key), "rank_cohort": cohort}, index)
            row["_recorder"].record(connection, seek, "execute", 2)
            row["_recorder"].record(connection, seek, "fetch", 1)
            row["sql"] = probe.recorder_metrics(row["_recorder"], rank_cpu_ms=row["_rank_cpu_ms"])
            row.update(outcome="success", scheduled_at=0, completed_at=latency / 1000,
                       e2e_ms=latency, service_ms=latency, queue_ms=0,
                       loop_dispatch_delay_ms=0, producer_delay_ms=0,
                       db_query_count=1, db_total_ms=3)
            rows.append(row)
        rows[0]["sql_threads"] = {f"xiuxian-exact-rank_{index}":
                                  {"calls": 1, "wall_ms": 2, "cpu_ms": 1} for index in range(25)}
        rows[0]["sql_threads"]["asyncio_0"] = {"calls": 100, "wall_ms": 100, "cpu_ms": 100}
        for row, calls, wall, cpu in ((rows[1], 2, 3, 2), (rows[2], 1, 4, 3), (rows[3], 9, 9, 9)):
            row["sql_threads"] = {"xiuxian-exact-rank_0": {"calls": calls, "wall_ms": wall, "cpu_ms": cpu}}
        summary = probe.summarize(rows)
        self.assertEqual(summary["commands"]["profile_info"]["n"], 3)
        self.assertEqual(summary["commands"]["profile_info"]["exact_rank_worker_sql"],
                         {"calls": 28, "wall_ms": 57, "cpu_ms": 30})
        self.assertEqual(summary["commands"]["profile_info"]["exact_rank_operations"]["seek_batch"]["statements"], 3)
        self.assertEqual(summary["exact_rank_worker_sql"], {"calls": 37, "wall_ms": 66, "cpu_ms": 39})
        near = summary["rank_cohorts"]["near"]
        self.assertEqual(near["n"], 2)
        self.assertEqual(near["e2e_ms"], {"p50": 10, "p95": 30, "p99": 30, "max": 30})
        self.assertEqual(near["db_queries"], 2)
        self.assertEqual(near["db_total_ms"], 6)
        self.assertEqual(near["exact_rank_worker_sql"], {"calls": 27, "wall_ms": 53, "cpu_ms": 27})
        self.assertEqual(near["exact_rank_operations"]["seek_batch"]["total_ms"], 6)
        self.assertEqual(summary["rank_cohorts"]["deep"]["n"], 1)
        self.assertNotIn("cache", near)
        self.assertNotIn("cache", summary["commands"]["profile_info"])

    def test_sqlite_busy_is_separate_from_business_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_connect, old_router, old_recorder = sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER
            connections = []
            try:
                probe.install_sql_timing(root)
                first = sqlite3.connect(root / "small.db", timeout=0)
                second = sqlite3.connect(root / "small.db", timeout=0)
                connections.extend((first, second))
                first.execute("CREATE TABLE sample (value INTEGER)")
                first.commit()
                first.execute("BEGIN IMMEDIATE")
                row = probe.request_row(request(), 1)
                token = probe.ACTIVE.set(row)
                try:
                    with self.assertRaises(sqlite3.OperationalError):
                        second.execute("BEGIN IMMEDIATE")
                    self.assertEqual(row["sqlite_busy_locked"], 1)
                    self.assertEqual(row["sql_errors"][0]["code"] & 255, sqlite3.SQLITE_BUSY)
                    self.assertEqual(len(row["transaction_begin_ms"]), 1)
                finally:
                    probe.ACTIVE.reset(token)
            finally:
                for connection in connections:
                    connection.rollback()
                    connection.close()
                sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER = old_connect, old_router, old_recorder

    def test_preflight_plan_requires_unique_resolved_matchers_and_messages(self):
        entry = {"command": "profile", "aliases": []}
        self.assertEqual(probe.validate_plan([request()], [entry]), {"profile": entry})
        with self.assertRaisesRegex(RuntimeError, "resolved to 0"):
            probe.validate_plan([request()], [])
        with self.assertRaisesRegex(RuntimeError, "resolved to 2"):
            probe.validate_plan([request()], [entry, entry])
        with self.assertRaisesRegex(ValueError, "message IDs"):
            probe.validate_plan([{**request(), "number": 1}, {**request(), "number": 1}], [entry])

    def test_resource_stops_preserve_existing_caps(self):
        good = {"available_mb": 1024, "disk_free_bytes": 3 * 1024 ** 3,
                "max_wal_bytes": 0, "rss_mb": 200}
        self.assertIsNone(probe.resource_violation(good))
        for changes in ({"available_mb": 767}, {"disk_free_bytes": 2 * 1024 ** 3 - 1},
                        {"max_wal_bytes": 256 * 1024 ** 2 + 1}, {"rss_mb": 901}):
            self.assertIsNotNone(probe.resource_violation({**good, **changes}))

    def test_cli_keeps_arrival_rate_separate_from_inflight_cap(self):
        args = probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                                 "--concurrency", "1,5,10,20", "--arrival-rate", "50",
                                 "--preflight-only"])
        self.assertEqual(args.concurrency, [1, 5, 10, 20])
        self.assertEqual(args.arrival_rate, 50)
        self.assertTrue(args.preflight_only)
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                              "--requests-per-phase", "160", "--arrival-rate", ".5"])

    def test_rank_seek_scenario_retains_frozen_participants_and_no_bulk(self):
        args = probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                                 "--scenario", "rank-seek-v6", "--preflight-only"])
        self.assertEqual(args.scenario, "rank-seek-v6")
        self.assertEqual(args.participants, 128)
        for extra in (("--participants", "32"), ("--bulk-only",)):
            with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
                probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                                  "--scenario", "rank-seek-v6", *extra])


class MixedProbeDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def _dispatch(self, *, allowed=True, cancel=False):
        import nonebot
        from nonebot.adapters.onebot.v11 import Adapter, Bot
        from nonebot.matcher import Matcher
        from nonebot.params import Depends
        from nonebot.rule import Rule
        from tests.command_probe import make_event

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init(log_level="ERROR")
        calls, seen = [], []

        async def resource():
            calls.append("open")
            try:
                yield object()
            finally:
                calls.append("close")

        async def rule(value=Depends(resource)):
            calls.append("rule")
            seen.append(value)
            return allowed

        async def handler(value=Depends(resource)):
            calls.append("handler")
            seen.append(value)
            if cancel:
                await asyncio.sleep(30)

        matcher = Matcher.new(type_="message", rule=Rule(rule), handlers=[handler], block=True)
        row = probe.request_row(request(), 1)
        bot = Bot(Adapter(nonebot.get_driver()), "900000")
        try:
            async with AsyncExitStack() as stack:
                operation = probe.dispatch_matcher_once(
                    row, matcher, bot, make_event("profile", "900001", 1), {}, stack)
                if cancel:
                    with self.assertRaises(asyncio.TimeoutError):
                        await asyncio.wait_for(operation, timeout=.05)
                else:
                    await operation
                self.assertNotIn("close", calls)
            self.assertEqual(calls[-1], "close")
        finally:
            matcher.destroy()
        self.assertEqual(calls.count("rule"), 1)
        self.assertEqual(calls.count("open"), 1)
        self.assertLessEqual(row["matcher_check_started_at"], row["matcher_check_completed_at"])
        return row, calls, seen

    async def test_real_rule_and_handler_run_once_with_shared_dependency_lifecycle(self):
        row, calls, seen = await self._dispatch()
        self.assertEqual(calls, ["open", "rule", "handler", "close"])
        self.assertIs(seen[0], seen[1])
        self.assertEqual(row["blocked"], [])
        self.assertLessEqual(row["matcher_check_completed_at"], row["handler_started_at"])
        self.assertLessEqual(row["handler_started_at"], row["handler_completed_at"])

    async def test_real_rule_rejection_never_runs_handler(self):
        row, calls, _ = await self._dispatch(allowed=False)
        self.assertEqual(calls, ["open", "rule", "close"])
        self.assertNotIn("handler_started_at", row)
        self.assertEqual(row["blocked"], ["permission/rule rejected before handler"])

    async def test_timeout_preserves_handler_end_and_dependency_cleanup(self):
        row, calls, _ = await self._dispatch(cancel=True)
        self.assertEqual(calls, ["open", "rule", "handler", "close"])
        self.assertIn("handler_completed_at", row)


class MixedProbeOwnedTaskTests(unittest.IsolatedAsyncioTestCase):
    async def test_message_queue_drain_waits_for_actual_job_completion(self):
        jobs = queue.Queue()
        jobs.reserved_bytes = 10
        jobs.put("actual work")
        module = SimpleNamespace(_message_db_jobs=jobs, _message_db_dropped_jobs=0)

        async def finish():
            await asyncio.sleep(.01)
            jobs.get_nowait()
            with jobs.mutex:
                jobs.reserved_bytes = 0
            jobs.task_done()

        task = asyncio.create_task(finish())
        state = await probe.drain_message_writer(module)
        await task
        self.assertEqual(state["unfinished_jobs"], 0)
        self.assertEqual(state["reserved_bytes"], 0)
        self.assertGreater(state["drain_ms"], 0)
        jobs.put("undrained")
        with self.assertRaisesRegex(RuntimeError, "did not drain"):
            await probe.drain_message_writer(module, timeout=0)

    async def test_drain_only_cancels_request_owned_work(self):
        owned = asyncio.create_task(asyncio.sleep(30))
        unrelated = asyncio.create_task(asyncio.sleep(30))
        row = {"_tasks": [owned], "blocked": [], "errors": []}
        try:
            await probe.drain_owned(row, timeout=.001)
            self.assertTrue(owned.cancelled())
            self.assertFalse(unrelated.done())
            self.assertEqual(row["blocked"], ["owned background work did not drain"])
        finally:
            unrelated.cancel()
            await asyncio.gather(unrelated, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
