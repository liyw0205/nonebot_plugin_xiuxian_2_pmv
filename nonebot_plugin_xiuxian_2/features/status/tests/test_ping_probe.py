from __future__ import annotations

import asyncio
from collections import Counter
import threading
import time
import unittest

from ..application import StatusApplication
from ..ping_probe import PING_TARGETS, PingProbe, PingResult, PingSnapshot, PingTarget


class PingProbeTests(unittest.TestCase):
    def test_linux_command_and_posix_average_are_preserved(self) -> None:
        calls: list[tuple[tuple[str, ...], float]] = []

        def runner(command, timeout):
            calls.append((tuple(command), timeout))
            return "rtt min/avg/max/mdev = 1.000/12.500/20.000/2.000 ms"

        probe = PingProbe(
            runner=runner,
            system=lambda: "Linux",
            targets=(PingTarget("测试", "example.test", "国内"),),
        )
        snapshot = asyncio.run(probe.probe_all())

        self.assertEqual(calls, [(('ping', '-c', '4', 'example.test'), 10.0)])
        self.assertEqual(snapshot.results[0].delay_ms, 12.5)
        self.assertFalse(snapshot.results[0].is_timeout)
        self.assertEqual(snapshot.results[0].emoji, "🚀")

    def test_windows_command_and_average_are_preserved(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, timeout):
            commands.append(tuple(command))
            return "平均 = 125ms"

        probe = PingProbe(
            runner=runner,
            system=lambda: "Windows",
            targets=(PingTarget("测试", "example.test", "国外"),),
        )
        result = asyncio.run(probe.probe_all()).results[0]

        self.assertEqual(commands, [("ping", "-n", "4", "example.test")])
        self.assertEqual(result.delay_ms, 125.0)
        self.assertFalse(result.is_timeout)
        self.assertEqual(result.emoji, "🐢")

    def test_timeout_exception_and_unparseable_output_are_fail_closed(self) -> None:
        calls = Counter()

        def runner(command, timeout):
            calls[command[-1]] += 1
            if command[-1] == "timeout.test":
                raise TimeoutError("probe timed out")
            return "unrecognised output"

        probe = PingProbe(
            runner=runner,
            targets=(
                PingTarget("超时", "timeout.test", "国内"),
                PingTarget("异常", "invalid.test", "国外"),
            ),
        )
        results = asyncio.run(probe.probe_all()).results

        self.assertEqual(calls, Counter({"timeout.test": 1, "invalid.test": 1}))
        self.assertTrue(all(result.is_timeout for result in results))
        self.assertTrue(all(result.delay_ms == 0 for result in results))
        self.assertTrue(all(result.emoji == "💀" for result in results))

    def test_all_fixed_sites_run_concurrently_and_keep_order(self) -> None:
        lock = threading.Lock()
        active = 0
        maximum = 0
        commands: list[tuple[str, ...]] = []

        def runner(command, timeout):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                commands.append(tuple(command))
            time.sleep(0.02)
            with lock:
                active -= 1
            return "rtt min/avg/max/mdev = 1/2/3/4 ms"

        snapshot = asyncio.run(PingProbe(runner=runner).probe_all())

        self.assertEqual([result.name for result in snapshot.results], [target.name for target in PING_TARGETS])
        self.assertEqual(len(commands), 8)
        self.assertGreater(maximum, 1)

    def test_render_keeps_legacy_grouping_and_timeout_text(self) -> None:
        snapshot = PingSnapshot(
            (
                PingResult("百度", "www.baidu.com", "国内", 12.345, False, "🚀"),
                PingResult("谷歌", "www.google.com", "国外", 0, True, "💀"),
            )
        )

        self.assertEqual(
            snapshot.render(),
            "网络延迟测试\n\n【国内站点】\n🚀 百度: 12.345ms\n\n【国外站点】\n💀 谷歌: 超时(0ms)\n",
        )

    def test_status_application_delegates_without_ledger(self) -> None:
        class FakeProbe:
            async def probe_all(self):
                return PingSnapshot(())

        app = StatusApplication(":memory:", ping_probe=FakeProbe())
        self.assertEqual(asyncio.run(app.ping_test()), "网络延迟测试\n\n【国内站点】\n\n【国外站点】\n")


if __name__ == "__main__":
    unittest.main()
