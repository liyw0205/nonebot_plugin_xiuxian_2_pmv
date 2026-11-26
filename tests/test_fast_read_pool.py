import queue
import threading
import unittest
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import xiuxian2_handle as handle


class ObservedQueue(queue.LifoQueue):
    def __init__(self):
        super().__init__()
        self.waiting = threading.Event()

    def get(self, block=True, timeout=None):
        if block:
            self.waiting.set()
        return super().get(block, timeout)


class FastReadPoolTests(unittest.TestCase):
    def test_waiter_recovers_discarded_capacity_and_connect_failure(self):
        for discard, fail in ((False, False), (True, False), (True, True)):
            with self.subTest(discard=discard, fail=fail):
                manager = handle.XiuxianDateManage.__new__(handle.XiuxianDateManage)
                manager._fast_conn_pool = ObservedQueue()
                manager._fast_conn_lock = threading.Lock()
                manager._fast_conn_count = 0
                manager._fast_conn_max = 1
                manager.database_path = "synthetic-only"
                original, replacement = Mock(), Mock()
                outcome = []

                def borrow():
                    try:
                        outcome.append(manager._borrow_fast_conn())
                    except RuntimeError as exc:
                        outcome.append(exc)

                effects = [original, RuntimeError("connect failed"), replacement] if fail else [original, replacement]
                with patch.object(handle.db_backend, "connect", side_effect=effects) as connect:
                    self.assertIs(manager._borrow_fast_conn(), original)
                    thread = threading.Thread(target=borrow, daemon=True)
                    thread.start()
                    try:
                        self.assertTrue(manager._fast_conn_pool.waiting.wait(2))
                        manager._return_fast_conn(original, reusable=not discard)
                        thread.join(2)
                        self.assertFalse(thread.is_alive(), "pool waiter lost the discarded slot")
                        if fail:
                            self.assertIsInstance(outcome[0], RuntimeError)
                            self.assertEqual(manager._fast_conn_count, 0)
                            self.assertIs(manager._borrow_fast_conn(), replacement)
                        else:
                            self.assertIs(outcome[0], replacement if discard else original)
                            self.assertEqual(connect.call_count, 2 if discard else 1)
                        self.assertEqual(manager._fast_conn_count, 1)
                        self.assertEqual(original.close.call_count, int(discard))
                    finally:
                        manager._fast_conn_pool.put(replacement)
                        thread.join(2)
