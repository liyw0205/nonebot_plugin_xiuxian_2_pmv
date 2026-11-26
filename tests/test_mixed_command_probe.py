"""Small deterministic contracts for the mixed-load harness, not a load run."""
import asyncio
from contextvars import copy_context
import io
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
