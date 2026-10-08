from __future__ import annotations

from datetime import datetime
import unittest

from ..application import StatusApplication
from ..process_info import ProcessInfoProvider


class FakeProcess:
    def __init__(self, pid: int, name: str, memory_bytes: int, created_at: float) -> None:
        self.pid = pid
        self._name = name
        self._memory_bytes = memory_bytes
        self._created_at = created_at

    def memory_info(self):
        class Memory:
            pass

        result = Memory()
        result.rss = self._memory_bytes
        return result

    def create_time(self) -> float:
        return self._created_at

    def name(self) -> str:
        return self._name


class ProcessInfoTests(unittest.TestCase):
    def test_processes_are_sorted_limited_and_vanished_processes_are_skipped(self) -> None:
        now = datetime(2026, 10, 8, 12, 0)
        timestamp = now.timestamp()

        class GoneProcess:
            pid = 1

            def memory_info(self):
                raise RuntimeError("process vanished")

        class FakePsutil:
            @staticmethod
            def process_iter(_attrs):
                return [
                    GoneProcess(),
                    FakeProcess(2, "small", 1024 * 1024, timestamp - 60),
                    FakeProcess(3, "large", 8 * 1024 * 1024, timestamp - 120),
                ]

        provider = ProcessInfoProvider(
            psutil_module=FakePsutil,
            now=lambda: now,
            from_timestamp=datetime.fromtimestamp,
        )
        app = StatusApplication(":memory:", process_info_provider=provider)

        self.assertTrue(app.process_info_available)
        self.assertEqual(
            app.process_info(limit=1),
            [
                {
                    "pid": 3,
                    "name": "large",
                    "memory": "8.0MB",
                    "memory_mb": 8.0,
                    "time": "0:02:00",
                }
            ],
        )

    def test_psutil_unavailable_returns_no_rows(self) -> None:
        app = StatusApplication(
            ":memory:",
            process_info_provider=ProcessInfoProvider(psutil_module=None),
        )

        self.assertFalse(app.process_info_available)
        self.assertEqual(app.process_info(), [])


if __name__ == "__main__":
    unittest.main()
