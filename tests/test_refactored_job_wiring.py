from __future__ import annotations

import asyncio
import importlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.compatibility.legacy_jobs import legacy_job_handler
from nonebot_plugin_xiuxian_2.plugin import shutdown, startup


class RefactoredJobWiringTests(unittest.TestCase):
    def test_legacy_job_warns_and_records_hit_when_executed(self) -> None:
        handler = legacy_job_handler("daily_reset_stone_limits")
        module = SimpleNamespace(daily_reset_stone_limits_job=lambda: "ok")
        real_import = importlib.import_module

        def import_target(name: str, package: str | None = None):
            if name == "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_scheduler":
                return module
            return real_import(name, package)

        with patch("nonebot_plugin_xiuxian_2.compatibility.legacy_jobs.importlib.import_module", side_effect=import_target):
            with patch("nonebot_plugin_xiuxian_2.compatibility.commands.record_compatibility_hit") as record:
                with self.assertWarnsRegex(DeprecationWarning, "legacy job daily_reset_stone_limits"):
                    self.assertEqual(handler(), "ok")
        record.assert_called_once_with("job:daily_reset_stone_limits")

    def test_legacy_manifest_jobs_have_lazy_executable_handlers(self) -> None:
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                state, _readiness, context, lifecycle = await startup(build_runtime_context(data_dir=directory))
                try:
                    # Legacy optional startup resources may be unavailable in
                    # a unit-test process; job registration itself is still
                    # expected before readiness is reported.
                    self.assertIn(state.phase.value, {"ready", "not_ready"})
                    legacy = [job for job in context.jobs.list() if job.owner == "compatibility"]
                    self.assertTrue(legacy)
                    self.assertTrue(all("unwired" not in job.handler.__qualname__ for job in legacy))
                finally:
                    await shutdown(lifecycle)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
