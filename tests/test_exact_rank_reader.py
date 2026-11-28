import asyncio
from contextvars import ContextVar
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.exact_rank_reader import ExactRankReader


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(.001)


class ExactRankReaderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.reader = ExactRankReader()
        self.release = threading.Event()
        self.addCleanup(self.reader.close)
        self.addAsyncCleanup(self.reader.drain)
        self.addCleanup(self.release.set)

    async def test_two_workers_overlap_without_blocking_loop_or_queueing_third(self):
        marker = ContextVar("rank_request")
        started = []
        lock = threading.Lock()

        def read(user):
            with lock:
                started.append((user, marker.get(), threading.get_ident()))
            if not self.release.wait(5):
                raise TimeoutError("test worker was not released")
            return (int(user), 1)

        manager = SimpleNamespace(get_exp_and_stone_ranks=read)
        tasks = []
        for user in ("1", "2", "3"):
            token = marker.set("request-" + user)
            tasks.append(asyncio.create_task(self.reader.read(manager, user)))
            marker.reset(token)
        await until(lambda: len(started) == 2)
        await asyncio.sleep(.02)
        self.assertEqual(len(started), 2)
        self.assertEqual(len({entry[2] for entry in started}), 2)
        self.assertNotIn(threading.get_ident(), {entry[2] for entry in started})
        self.release.set()
        self.assertEqual(await asyncio.gather(*tasks), [(1, 1), (2, 1), (3, 1)])
        self.assertEqual(sorted((user, context) for user, context, _ in started),
                         [(user, "request-" + user) for user in ("1", "2", "3")])

    async def test_repeated_cancellation_keeps_slot_and_drain_until_worker_finishes(self):
        started = []

        def read(user):
            started.append(user)
            if not self.release.wait(5):
                raise TimeoutError("test worker was not released")
            return (1, 1)

        manager = SimpleNamespace(get_exp_and_stone_ranks=read)
        first = asyncio.create_task(self.reader.read(manager, "first"))
        second = asyncio.create_task(self.reader.read(manager, "second"))
        await until(lambda: len(started) == 2)
        third = asyncio.create_task(self.reader.read(manager, "third"))
        first.cancel()
        await asyncio.sleep(.01)
        first.cancel()
        drain = asyncio.create_task(self.reader.drain())
        await asyncio.sleep(.02)
        self.assertFalse(first.done())
        self.assertFalse(drain.done())
        self.assertEqual(started, ["first", "second"])
        self.release.set()
        results = await asyncio.gather(first, second, third, return_exceptions=True)
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertEqual(results[1:], [(1, 1), (1, 1)])
        await drain

    async def test_worker_failure_and_cancelled_waiter_do_not_leak_slots(self):
        entered = []

        def fail(user):
            entered.append(user)
            self.release.wait(5)
            raise ValueError(user)

        manager = SimpleNamespace(get_exp_and_stone_ranks=fail)
        first = asyncio.create_task(self.reader.read(manager, "one"))
        second = asyncio.create_task(self.reader.read(manager, "two"))
        await until(lambda: len(entered) == 2)
        waiting = asyncio.create_task(self.reader.read(manager, "never"))
        await asyncio.sleep(.01)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        self.release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertTrue(all(isinstance(result, ValueError) for result in results))
        self.assertNotIn("never", entered)
        with self.assertRaisesRegex(ValueError, "next"):
            await self.reader.read(manager, "next")

    async def test_drain_repeated_cancellation_waits_for_running_and_waiting_reads(self):
        started = []

        def read(user):
            started.append(user)
            self.release.wait(5)
            return (1, 1)

        manager = SimpleNamespace(get_exp_and_stone_ranks=read)
        tasks = [asyncio.create_task(self.reader.read(manager, str(i))) for i in range(4)]
        await until(lambda: len(started) == 2)
        drain = asyncio.create_task(self.reader.drain())
        for _ in range(2):
            await asyncio.sleep(.01)
            drain.cancel()
        await asyncio.sleep(.01)
        self.assertFalse(drain.done())
        self.release.set()
        await asyncio.gather(*tasks)
        with self.assertRaises(asyncio.CancelledError):
            await drain
        self.assertEqual(len(started), 4)

    async def test_synchronous_close_and_reconnect_wait_for_real_worker_connection(self):
        import queue
        import tempfile
        from pathlib import Path
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import exact_rank_reader
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage

        with tempfile.TemporaryDirectory() as directory:
            for action in ("close", "reconnect"):
                with self.subTest(action=action):
                    manager = XiuxianDateManage.__new__(XiuxianDateManage)
                    manager.database_path = Path(directory) / (action + ".db")
                    manager.conn = db_backend.connect(manager.database_path, check_same_thread=False)
                    manager._conn_lock = threading.RLock()
                    manager._fast_conn_pool = queue.LifoQueue()
                    manager._fast_conn_lock = threading.Lock()
                    manager._fast_conn_count, manager._fast_conn_max = 0, 1
                    entered, released, closing = threading.Event(), threading.Event(), threading.Event()
                    worker_errors = []

                    def read(_user):
                        try:
                            with manager._repository_read_connection() as conn:
                                conn.execute("BEGIN")
                                entered.set()
                                self.release.wait(5)
                                value = tuple(conn.execute("SELECT 1,2").fetchone())
                                conn.rollback()
                            released.set()
                            return value
                        except BaseException as exc:
                            worker_errors.append(exc)
                            raise

                    def release():
                        closing.wait(3)
                        self.release.set()

                    def wait_for_worker():
                        closing.set()
                        self.reader.wait()

                    self.release.clear()
                    task = asyncio.create_task(self.reader.read(SimpleNamespace(get_exp_and_stone_ranks=read), "u"))
                    await until(entered.is_set)
                    timer = threading.Thread(target=release)
                    timer.start()
                    with (patch.object(exact_rank_reader, "wait_for_exact_rank_reads", side_effect=wait_for_worker),
                          patch.object(XiuxianDateManage, "_clear_read_cache"),
                          patch.object(XiuxianDateManage, "_check_data"),
                          patch.object(XiuxianDateManage, "_run_core_migrations")):
                        getattr(manager, action)()
                        self.assertTrue(released.is_set())
                        self.assertEqual(manager._fast_conn_count, 0)
                        self.assertTrue(manager._fast_conn_pool.empty())
                        if action == "reconnect":
                            manager.close()
                    timer.join(3)
                    self.assertEqual(await task, (1, 2))
                    self.assertEqual(worker_errors, [])


class RankReaderLoopLifetimeTests(unittest.TestCase):
    def test_sequential_event_loops_can_reuse_reader_after_drain(self):
        reader = ExactRankReader()
        self.addCleanup(reader.close)
        manager = SimpleNamespace(get_exp_and_stone_ranks=lambda user: (int(user), 2))

        async def read_all():
            values = await asyncio.gather(*(reader.read(manager, str(i)) for i in range(6)))
            await reader.drain()
            return values

        for _ in range(2):
            self.assertEqual(asyncio.run(read_all()), [(i, 2) for i in range(6)])
