from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event, Lock
import tempfile
import time
import unittest

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
    ProcessReadCache,
    shared_snapshot_key,
)


class ProcessReadCacheTests(unittest.TestCase):
    def test_snapshot_keys_include_database_identity_and_view_dimensions(self):
        with tempfile.TemporaryDirectory() as temp:
            first_db = Path(temp) / "first.db"
            second_db = Path(temp) / "second.db"
            first_db.touch()
            second_db.touch()
            self.assertNotEqual(
                shared_snapshot_key(first_db, "realm", 50),
                shared_snapshot_key(second_db, "realm", 50),
            )
            self.assertNotEqual(
                shared_snapshot_key(first_db, "realm", 50),
                shared_snapshot_key(first_db, "realm", 30),
            )

    def test_shared_snapshot_survives_gameplay_writes_until_expiry_or_explicit_invalidation(self):
        now = [0.0]
        cache = ProcessReadCache(clock=lambda: now[0])
        database = Path("/tmp/shared-read-cache-snapshot.db")
        calls = []

        def load(value):
            return lambda: calls.append(value) or [("snapshot", value)]

        first = cache.get_or_load(
            "realm-top-50",
            database_path=database,
            ttl=60 * 60,
            loader=load(1),
            snapshot=True,
        )
        self.assertEqual([("snapshot", 1)], first)
        self.assertEqual(
            first,
            cache.get_or_load(
                "realm-top-50",
                database_path=database,
                ttl=60 * 60,
                loader=load(2),
                snapshot=True,
            ),
        )
        cache.invalidate_database(database)
        self.assertEqual(
            first,
            cache.get_or_load(
                "realm-top-50",
                database_path=database,
                ttl=60 * 60,
                loader=load(3),
                snapshot=True,
            ),
        )
        self.assertEqual([1], calls)

        now[0] = 60 * 60 + 0.1
        refreshed = cache.get_or_load(
            "realm-top-50",
            database_path=database,
            ttl=60 * 60,
            loader=load(4),
            snapshot=True,
        )
        self.assertEqual([("snapshot", 4)], refreshed)

        cache.invalidate_snapshot_database(database)
        invalidated = cache.get_or_load(
            "realm-top-50",
            database_path=database,
            ttl=60 * 60,
            loader=load(5),
            snapshot=True,
        )
        self.assertEqual([("snapshot", 5)], invalidated)
        self.assertEqual([1, 4, 5], calls)

    def test_ttl_invalidation_and_bounded_lru(self):
        now = [0.0]
        cache = ProcessReadCache(max_entries=2, clock=lambda: now[0])
        database = Path("/tmp/shared-read-cache-game.db")
        calls = []

        def load(value):
            return lambda: calls.append(value) or {"value": value}

        first = cache.get_or_load("realm", database_path=database, ttl=30, loader=load(1))
        first["value"] = "caller mutation"
        self.assertEqual(
            {"value": 1},
            cache.get_or_load("realm", database_path=database, ttl=30, loader=load(2)),
        )
        self.assertEqual([1], calls)

        cache.get_or_load("stone", database_path=database, ttl=30, loader=load(3))
        cache.get_or_load("realm", database_path=database, ttl=30, loader=load(4))
        cache.get_or_load("power", database_path=database, ttl=30, loader=load(5))
        self.assertEqual(2, cache.stats()["entries"])
        self.assertEqual(1, cache.stats()["evictions"])

        now[0] = 31.0
        self.assertEqual(
            {"value": 6},
            cache.get_or_load("realm", database_path=database, ttl=30, loader=load(6)),
        )
        self.assertEqual(2, cache.stats()["expirations"])
        cache.invalidate_database(database)
        self.assertEqual(
            {"value": 7},
            cache.get_or_load("realm", database_path=database, ttl=30, loader=load(7)),
        )
        self.assertEqual(1, cache.stats()["invalidations"])

    def test_concurrent_miss_is_single_flight(self):
        cache = ProcessReadCache()
        database = Path("/tmp/shared-read-cache-single-flight.db")
        ready = Barrier(8)
        started = Event()
        release = Event()
        calls = 0
        calls_lock = Lock()

        def loader():
            nonlocal calls
            with calls_lock:
                calls += 1
            started.set()
            if not release.wait(timeout=5):
                raise TimeoutError("single-flight test loader was not released")
            return [("shared", 50)]

        def read():
            ready.wait(timeout=5)
            return cache.get_or_load(
                (str(database), "realm", 50),
                database_path=database,
                ttl=30,
                loader=loader,
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = [pool.submit(read) for _ in range(8)]
            self.assertTrue(started.wait(timeout=5))
            time.sleep(0.02)
            release.set()
            self.assertEqual([[('shared', 50)]] * 8, [future.result(timeout=5) for future in results])

        self.assertEqual(1, calls)
        self.assertEqual(1, cache.stats()["misses"])
        self.assertEqual(7, cache.stats()["waits"])

    def test_loader_failure_releases_waiters_without_caching_error(self):
        cache = ProcessReadCache()
        database = Path("/tmp/shared-read-cache-loader-error.db")
        started = Event()
        release = Event()
        calls = 0

        def loader():
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                release.wait(timeout=5)
                raise RuntimeError("first load failed")
            return ["recovered"]

        with ThreadPoolExecutor(max_workers=2) as pool:
            failed = pool.submit(
                cache.get_or_load,
                "realm",
                database_path=database,
                ttl=60,
                loader=loader,
            )
            self.assertTrue(started.wait(timeout=5))
            recovered = pool.submit(
                cache.get_or_load,
                "realm",
                database_path=database,
                ttl=60,
                loader=loader,
            )
            time.sleep(0.02)
            release.set()
            with self.assertRaisesRegex(RuntimeError, "first load failed"):
                failed.result(timeout=5)
            self.assertEqual(["recovered"], recovered.result(timeout=5))

        self.assertEqual(["recovered"], cache.get_or_load(
            "realm", database_path=database, ttl=60, loader=lambda: ["unexpected"]
        ))
        self.assertEqual(2, calls)
        self.assertEqual(1, cache.stats()["entries"])

    def test_invalidation_during_load_prevents_stale_entry(self):
        cache = ProcessReadCache()
        database = Path("/tmp/shared-read-cache-race.db")
        started = Event()
        release = Event()
        calls = 0

        def loader():
            nonlocal calls
            calls += 1
            if calls > 1:
                return ["fresh snapshot"]
            started.set()
            release.wait(timeout=5)
            return ["old snapshot"]

        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(
                cache.get_or_load,
                "realm",
                database_path=database,
                ttl=30,
                loader=loader,
            )
            self.assertTrue(started.wait(timeout=5))
            cache.invalidate_database(database)
            release.set()
            self.assertEqual(["fresh snapshot"], result.result(timeout=5))

        self.assertEqual(2, calls)
        self.assertEqual(["fresh snapshot"], cache.get_or_load(
            "realm", database_path=database, ttl=30, loader=lambda: ["unexpected"]
        ))
        self.assertEqual(1, cache.stats()["entries"])

    def test_snapshot_invalidation_during_load_retries_with_fresh_data(self):
        cache = ProcessReadCache()
        database = Path("/tmp/shared-read-cache-snapshot-race.db")
        started = Event()
        release = Event()
        calls = 0

        def loader():
            nonlocal calls
            calls += 1
            if calls > 1:
                return ["fresh snapshot"]
            started.set()
            release.wait(timeout=5)
            return ["old snapshot"]

        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(
                cache.get_or_load,
                "realm",
                database_path=database,
                ttl=60 * 60,
                loader=loader,
                snapshot=True,
            )
            self.assertTrue(started.wait(timeout=5))
            cache.invalidate_snapshot_database(database)
            release.set()
            self.assertEqual(["fresh snapshot"], result.result(timeout=5))

        self.assertEqual(2, calls)
        self.assertEqual(1, cache.stats()["entries"])
        self.assertLessEqual(cache.stats()["generation_paths"], 1)

    def test_dependency_invalidation_expires_snapshot_and_retries_active_load(self):
        cache = ProcessReadCache()
        with tempfile.TemporaryDirectory() as temp:
            primary = Path(temp) / "primary.db"
            dependency = Path(temp) / "display-profiles.db"
            primary.touch()
            dependency.touch()
            calls = 0
            started = Event()
            release = Event()

            def loader():
                nonlocal calls
                calls += 1
                if calls == 1:
                    started.set()
                    release.wait(timeout=5)
                    return ["stale profile"]
                return ["fresh profile"]

            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(
                    cache.get_or_load,
                    "public-rank-with-names",
                    database_path=primary,
                    ttl=60 * 60,
                    loader=loader,
                    snapshot=True,
                    dependency_databases=(dependency,),
                )
                self.assertTrue(started.wait(timeout=5))
                cache.invalidate_snapshot_database(dependency)
                release.set()
                self.assertEqual(["fresh profile"], result.result(timeout=5))

            self.assertEqual(["fresh profile"], cache.get_or_load(
                "public-rank-with-names",
                database_path=primary,
                ttl=60 * 60,
                loader=lambda: ["unexpected"],
                snapshot=True,
                dependency_databases=(dependency,),
            ))
            self.assertEqual(2, calls)
            self.assertEqual(1, cache.stats()["entries"])

    def test_generation_bookkeeping_does_not_retain_uncached_database_paths(self):
        cache = ProcessReadCache()
        with tempfile.TemporaryDirectory() as temp:
            for index in range(100):
                database = Path(temp) / f"database-{index}.db"
                cache.invalidate_database(database)
                cache.invalidate_snapshot_database(database)
        self.assertEqual(0, cache.stats()["generation_paths"])

    def test_same_public_key_is_isolated_by_database_and_snapshot_mode(self):
        cache = ProcessReadCache()
        first_db = Path("/tmp/shared-read-cache-first.db")
        second_db = Path("/tmp/shared-read-cache-second.db")
        self.assertEqual(
            ["first"],
            cache.get_or_load("realm", database_path=first_db, ttl=60, loader=lambda: ["first"]),
        )
        self.assertEqual(
            ["second"],
            cache.get_or_load("realm", database_path=second_db, ttl=60, loader=lambda: ["second"]),
        )
        self.assertEqual(
            ["snapshot"],
            cache.get_or_load(
                "realm", database_path=first_db, ttl=60, loader=lambda: ["snapshot"], snapshot=True
            ),
        )
        self.assertEqual(3, cache.stats()["entries"])

    def test_total_payload_bound_skips_oversized_entry(self):
        cache = ProcessReadCache(max_entries=8, max_bytes=256)
        database = Path("/tmp/shared-read-cache-size.db")

        result = cache.get_or_load(
            "large-view",
            database_path=database,
            ttl=60,
            loader=lambda: ["x" * 1024],
        )

        self.assertEqual(["x" * 1024], result)
        self.assertEqual(0, cache.stats()["entries"])
        self.assertEqual(0, cache.stats()["estimated_bytes"])
        self.assertEqual(1, cache.stats()["oversized"])

    def test_key_and_value_share_the_payload_bound(self):
        cache = ProcessReadCache(max_entries=8, max_bytes=256)
        database = Path("/tmp/shared-read-cache-key-size.db")

        result = cache.get_or_load(
            "x" * 512,
            database_path=database,
            ttl=60,
            loader=lambda: ["small value"],
        )

        self.assertEqual(["small value"], result)
        self.assertEqual(0, cache.stats()["entries"])
        self.assertEqual(1, cache.stats()["oversized"])

    def test_inflight_limit_waits_for_capacity_before_running_distinct_miss(self):
        cache = ProcessReadCache(max_entries=4, max_inflight=1)
        database = Path("/tmp/shared-read-cache-inflight-bound.db")
        started = Event()
        second_started = Event()
        release = Event()

        def slow_loader():
            started.set()
            if not release.wait(timeout=5):
                raise TimeoutError("bounded in-flight test loader was not released")
            return ["first"]

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                cache.get_or_load,
                "first",
                database_path=database,
                ttl=60,
                loader=slow_loader,
            )
            self.assertTrue(started.wait(timeout=5))
            second = pool.submit(
                cache.get_or_load,
                "second",
                database_path=database,
                ttl=60,
                loader=lambda: second_started.set() or ["second"],
            )
            time.sleep(0.02)
            self.assertFalse(second_started.is_set())
            self.assertEqual(1, cache.stats()["inflight"])
            self.assertGreaterEqual(cache.stats()["waits"], 1)
            release.set()
            self.assertEqual(["first"], first.result(timeout=5))
            self.assertEqual(["second"], second.result(timeout=5))

        self.assertEqual(0, cache.stats()["inflight"])
        self.assertEqual(2, cache.stats()["misses"])
        self.assertEqual(0, cache.stats()["bypasses"])
        self.assertEqual(2, cache.stats()["entries"])


if __name__ == "__main__":
    unittest.main()
