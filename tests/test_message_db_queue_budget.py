from __future__ import annotations

import threading
import unittest
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import message_db


class MessageDbQueueBudgetTests(unittest.TestCase):
    def test_writer_releases_batch_budget_on_success_failure_and_disabled_recording(self) -> None:
        class StopWriter(BaseException):
            pass

        class FiniteQueue(message_db._MessageDbJobQueue):
            def get(self, block=True, timeout=None):
                if block and self.empty():
                    raise StopWriter
                return super().get(block=False)

        for scenario in ("success", "failure", "disabled"):
            with self.subTest(scenario=scenario):
                items = [("insert", {"content": str(index) * 256}) for index in (1, 2)]
                budget = sum(FiniteQueue._job_size(item) for item in items)
                jobs = FiniteQueue(maxsize=2, max_bytes=budget)
                for item in items:
                    jobs.put_nowait(item)
                connection = Mock()

                def execute(*args):
                    self.assertEqual(budget, jobs.reserved_bytes)
                    if scenario == "failure":
                        raise RuntimeError("injected writer failure")

                with (
                    patch.object(message_db, "_message_db_jobs", jobs),
                    patch.object(message_db, "_MESSAGE_DB_BATCH_SIZE", 2),
                    patch.object(message_db, "is_message_record_enabled", return_value=scenario != "disabled"),
                    patch.object(message_db.db_backend, "connect", return_value=connection),
                    patch.object(message_db, "_ensure_message_db_schema"),
                    patch.object(message_db, "_execute_message_db_job", side_effect=execute),
                    patch.object(message_db, "maybe_cleanup_message_db"),
                    patch.object(message_db.logger, "warning"),
                ):
                    with self.assertRaises(StopWriter):
                        message_db._message_db_writer_loop()
                self.assertEqual(0, jobs.unfinished_tasks)
                self.assertEqual(0, jobs.reserved_bytes)
                self.assertEqual({}, jobs._job_sizes)
                jobs.put_nowait(("insert", {"content": "next" * 64}))

    def test_in_flight_jobs_keep_their_byte_reservation(self) -> None:
        first = ("insert", {"content": "a" * 256})
        second = ("insert", {"content": "b" * 256})
        budget = message_db._MessageDbJobQueue._job_size(first)
        jobs = message_db._MessageDbJobQueue(maxsize=4, max_bytes=budget)

        jobs.put_nowait(first)
        self.assertIs(jobs.get_nowait(), first)
        with self.assertRaises(message_db.queue.Full):
            jobs.put_nowait(second)

        jobs.release(first)
        jobs.task_done()
        jobs.put_nowait(second)
        self.assertEqual(jobs.reserved_bytes, budget)
        queued = jobs.get_nowait()
        jobs.release(queued)
        jobs.task_done()
        self.assertEqual(jobs.reserved_bytes, 0)

    def test_single_job_larger_than_budget_is_rejected(self) -> None:
        jobs = message_db._MessageDbJobQueue(maxsize=10, max_bytes=128)

        with self.assertRaises(message_db.queue.Full):
            jobs.put_nowait(("insert", {"content": "x" * 1024}))

        self.assertEqual(jobs.qsize(), 0)
        self.assertEqual(jobs.reserved_bytes, 0)

    def test_concurrent_producers_cannot_overreserve_bytes(self) -> None:
        producer_count = 12
        items = [
            ("insert", {"content": str(index).zfill(32)})
            for index in range(producer_count)
        ]
        budget = message_db._MessageDbJobQueue._job_size(items[0])
        jobs = message_db._MessageDbJobQueue(maxsize=producer_count, max_bytes=budget)
        barrier = threading.Barrier(producer_count)
        accepted: list[tuple[str, dict[str, str]]] = []
        accepted_lock = threading.Lock()

        def submit(item: tuple[str, dict[str, str]]) -> None:
            barrier.wait()
            try:
                jobs.put_nowait(item)
            except message_db.queue.Full:
                return
            with accepted_lock:
                accepted.append(item)

        producers = [threading.Thread(target=submit, args=(item,)) for item in items]
        for producer in producers:
            producer.start()
        for producer in producers:
            producer.join(timeout=2)

        self.assertTrue(all(not producer.is_alive() for producer in producers))
        self.assertEqual(len(accepted), 1)
        self.assertLessEqual(jobs.reserved_bytes, jobs.max_bytes)

        queued = jobs.get_nowait()
        jobs.release(queued)
        jobs.task_done()
        self.assertEqual(jobs.reserved_bytes, 0)

    def test_oversized_production_job_uses_existing_drop_path(self) -> None:
        jobs = message_db._MessageDbJobQueue(maxsize=10, max_bytes=1024)
        with (
            patch.object(message_db, "_message_db_jobs", jobs),
            patch.object(message_db, "is_message_record_enabled", return_value=True),
            patch.object(message_db, "init_message_db"),
            patch.object(message_db, "_message_db_dropped_jobs", 0),
            patch.object(message_db, "_message_db_last_drop_log_ts", 0.0),
            patch.object(message_db.logger, "warning"),
        ):
            message_db._enqueue_message_db_job("insert", {"content": "x" * 2048})

            self.assertEqual(message_db._message_db_dropped_jobs, 1)
            self.assertEqual(jobs.qsize(), 0)
            self.assertEqual(jobs.reserved_bytes, 0)


if __name__ == "__main__":
    unittest.main()
