"""Deterministic observer accounting contracts without a fixture or load run."""
import threading
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import mixed_command_probe as probe


class MixedObserverCPUTests(unittest.TestCase):
    def test_wal_walk_covers_nested_files_once_and_preserves_symlink_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data, outside = root / "data", root / "outside"
            nested = data / "players" / "nested.db-wal"
            nested.mkdir(parents=True)
            outside.mkdir()
            (data / "main.db-wal").write_bytes(b"m" * 3)
            (nested / "player.db-wal").write_bytes(b"p" * 5)
            (outside / "other.db-wal").write_bytes(b"o" * 7)
            (data / "ignore.txt").write_bytes(b"i" * 11)
            (data / "linked-directory").symlink_to(outside, target_is_directory=True)
            (data / "linked.db-wal").symlink_to(outside / "other.db-wal")
            (data / "missing.db-wal").symlink_to(root / "missing")
            expected = sorted(path.stat().st_size for path in data.rglob("*.db-wal") if path.is_file())
            scanned, original = [], os.scandir

            def scan(path):
                scanned.append(Path(path))
                return original(path)

            with patch.object(probe.os, "scandir", side_effect=scan):
                self.assertEqual(expected, sorted(probe.wal_sizes(data)))
            self.assertEqual([3, 5, 7], expected)
            self.assertEqual({data, data / "players", nested}, set(scanned))
            self.assertEqual(3, len(scanned))

    def test_wal_walk_failure_stops_watcher_instead_of_reporting_zero(self):
        watch = probe.ResourceWatch("unavailable", threading.Event())
        with patch.object(probe.os, "scandir", side_effect=PermissionError("walk denied")):
            watch._run()
        self.assertTrue(watch.stop.is_set())
        self.assertIn("walk denied", watch.reason)
        self.assertEqual([], watch.samples)

    def test_inactive_observer_does_not_read_cpu_clock(self):
        def forbidden_clock():
            self.fail("inactive observer sampled CPU")

        observer = probe.ObserverCPU(forbidden_clock)
        with observer.measure("record"):
            pass
        self.assertEqual(observer.snapshot()["threads"], [])
        self.assertEqual(observer.snapshot()["buckets"]["record"], {"calls": 0, "cpu_ns": 0})

    def test_nested_hooks_use_exclusive_cpu_and_keep_each_call(self):
        ticks = iter((100, 110, 130, 140, 170, 200))
        observer = probe.ObserverCPU(lambda: next(ticks))
        observer.start_phase()
        with observer.measure("record"):
            with observer.measure("begin_statement"):
                pass
            with observer.measure("authorizer"):
                pass
        observer.end_phase()
        totals = observer.snapshot()["buckets"]
        self.assertEqual(totals["record"], {"calls": 1, "cpu_ns": 50})
        self.assertEqual(totals["begin_statement"], {"calls": 1, "cpu_ns": 20})
        self.assertEqual(totals["authorizer"], {"calls": 1, "cpu_ns": 30})
        self.assertEqual(sum(value["cpu_ns"] for value in totals.values()), 100)

    def test_exception_records_cpu_and_clears_nested_stack(self):
        ticks = iter((0, 10, 30, 50, 60, 70))
        observer = probe.ObserverCPU(lambda: next(ticks))
        observer.start_phase()
        with self.assertRaisesRegex(RuntimeError, "business failure"):
            with observer.measure("record"):
                with observer.measure("authorizer"):
                    raise RuntimeError("business failure")
        with observer.measure("record"):
            pass
        observer.end_phase()
        totals = observer.snapshot()["buckets"]
        self.assertEqual(totals["record"], {"calls": 2, "cpu_ns": 40})
        self.assertEqual(totals["authorizer"], {"calls": 1, "cpu_ns": 20})

    def test_phase_reset_reuses_thread_without_old_counters(self):
        ticks = iter((0, 10, 20, 45))
        observer = probe.ObserverCPU(lambda: next(ticks))
        observer.start_phase()
        with observer.measure("record"):
            pass
        observer.end_phase()
        before = observer.snapshot()
        observer.start_phase()
        with observer.measure("authorizer"):
            pass
        observer.end_phase()
        after = observer.snapshot()
        self.assertEqual(before["buckets"]["record"], {"calls": 1, "cpu_ns": 10})
        self.assertEqual(after["buckets"]["record"], {"calls": 0, "cpu_ns": 0})
        self.assertEqual(after["buckets"]["authorizer"], {"calls": 1, "cpu_ns": 25})
        self.assertEqual(len(after["threads"]), 1)
        with self.assertRaisesRegex(ValueError, "unknown observer CPU bucket"):
            with observer.measure("arbitrary-request-id"):
                pass

    def test_thread_bins_are_bounded_and_overflow_retains_all_calls(self):
        local_clock = threading.local()

        def clock():
            local_clock.tick = getattr(local_clock, "tick", 0) + 10
            return local_clock.tick

        observer = probe.ObserverCPU(clock)
        observer.start_phase()
        start = threading.Barrier(21)
        errors = []

        def worker():
            try:
                start.wait(timeout=5)
                for _ in range(25):
                    with observer.measure("record"):
                        pass
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, name=f"observer-test-{index}") for index in range(20)]
        try:
            for thread in threads:
                thread.start()
            start.wait(timeout=5)
        finally:
            for thread in threads:
                if thread.ident is not None:
                    thread.join(timeout=5)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        observer.end_phase()
        result = observer.snapshot()
        self.assertEqual(len(result["threads"]), 17)
        self.assertEqual(result["buckets"]["record"], {"calls": 500, "cpu_ns": 5000})
        overflow, = [entry for entry in result["threads"] if entry["overflow"]]
        self.assertEqual(overflow["buckets"]["record"], {"calls": 100, "cpu_ns": 1000})

    def test_record_keeps_existing_sql_cpu_sample_before_observer_clock(self):
        events = []
        ticks = iter((100, 140))

        def observer_clock():
            events.append("observer")
            return next(ticks)

        def sql_clock():
            events.append("sql")
            return 1.002

        router = probe.QueryRouter("/tmp")
        router.observer_cpu = probe.ObserverCPU(observer_clock)
        router.observer_cpu.start_phase()
        row = probe.request_row({"key": "profile", "command": "profile",
                                 "actor": "900001", "input_text": "profile"}, 1)
        connection = object()
        router.connections[id(connection)] = "fixture.db"
        token = probe.ACTIVE.set(row)
        cpu_token = probe.SQL_CPU_START.set(1.0)
        try:
            with patch.object(probe.time, "thread_time", sql_clock):
                router.record(connection, "SELECT 1", "execute", 2)
        finally:
            probe.SQL_CPU_START.reset(cpu_token)
            probe.ACTIVE.reset(token)
            router.observer_cpu.end_phase()
        self.assertEqual(events, ["sql", "observer", "observer"])
        self.assertAlmostEqual(row["sql_threads"][threading.current_thread().name]["cpu_ms"], 2)
        self.assertEqual(router.observer_cpu.snapshot()["buckets"]["record"], {"calls": 1, "cpu_ns": 40})

    def test_begin_statement_counts_early_return_without_diagnostic(self):
        ticks = iter((100, 130))
        router = probe.QueryRouter("/tmp")
        router.observer_cpu = probe.ObserverCPU(lambda: next(ticks))
        router.observer_cpu.start_phase()
        self.assertIsNone(router.begin_statement(object(), "SELECT 1"))
        router.observer_cpu.end_phase()
        self.assertEqual(router.observer_cpu.snapshot()["buckets"]["begin_statement"],
                         {"calls": 1, "cpu_ns": 30})

    def test_authorizer_counts_complete_callback_without_changing_allow_or_deny(self):
        callbacks = []
        connection = SimpleNamespace(set_authorizer=callbacks.append)
        old_connect, old_router, old_recorder = probe.sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER
        diagnostic = {"authorizer_calls": 0}
        token = probe.SQL_DIAGNOSTIC.set(diagnostic)
        try:
            with patch.object(probe.matrix, "ORIGINAL_CONNECT", return_value=connection):
                router = probe.install_sql_timing("/tmp")
                probe.sqlite3.connect(":memory:")
            ticks = iter((0, 10, 20, 30))
            router.observer_cpu = probe.ObserverCPU(lambda: next(ticks))
            router.observer_cpu.start_phase()
            authorize, = callbacks
            self.assertEqual(authorize(probe.sqlite3.SQLITE_READ, "table", "column", "main", None),
                             probe.sqlite3.SQLITE_OK)
            self.assertEqual(authorize(probe.sqlite3.SQLITE_ATTACH, "/outside-fixture.db", None, None, None),
                             probe.sqlite3.SQLITE_DENY)
            router.observer_cpu.end_phase()
            self.assertEqual(diagnostic["authorizer_calls"], 2)
            self.assertEqual(router.observer_cpu.snapshot()["buckets"]["authorizer"],
                             {"calls": 2, "cpu_ns": 20})
        finally:
            probe.SQL_DIAGNOSTIC.reset(token)
            probe.sqlite3.connect, probe.ROUTER, probe.matrix.RECORDER = old_connect, old_router, old_recorder

    def test_watcher_samples_cpu_in_watcher_thread_and_preserves_stop(self):
        ticks = iter((100, 180))
        clock_threads = []

        def clock():
            clock_threads.append(threading.get_ident())
            return next(ticks)

        observer = probe.ObserverCPU(clock)
        observer.start_phase()
        stop = threading.Event()
        watch = probe.ResourceWatch("/unused", stop, observer)
        with patch.object(probe, "resource_state", return_value={"available_mb": 0}):
            watch.thread.start()
            watch.thread.join(timeout=5)
            watch.close()
        observer.end_phase()
        self.assertFalse(watch.thread.is_alive())
        self.assertTrue(stop.is_set())
        self.assertEqual(watch.reason, "available RAM below 768 MiB")
        self.assertEqual(clock_threads, [watch.thread.ident, watch.thread.ident])
        self.assertNotEqual(watch.thread.ident, threading.get_ident())
        result = observer.snapshot()
        self.assertEqual(result["buckets"]["watch"], {"calls": 1, "cpu_ns": 80})
        self.assertEqual(result["threads"][0]["thread"], "mixed-resource-watch")


if __name__ == "__main__":
    unittest.main()
