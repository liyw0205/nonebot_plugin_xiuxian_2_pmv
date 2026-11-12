from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.bootstrap import Lifecycle, LifecyclePhase
from nonebot_plugin_xiuxian_2.plugin import install_driver_hooks


class StartupFailureVisibilityTests(unittest.TestCase):
    def test_repository_error_is_retained_and_reraised_with_original_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing_resource = Path(directory) / "功法" / "功法概率设置.json"
            original = FileNotFoundError(2, "No such file or directory", str(missing_resource))
            lifecycle = Lifecycle()

            def ensure_repositories() -> None:
                raise original

            lifecycle.register(LifecyclePhase.REPOSITORIES, ensure_repositories)
            with patch("nonebot_plugin_xiuxian_2.bootstrap.lifecycle.emit") as emit:
                state = asyncio.run(lifecycle.start())

            self.assertIs(state.phase, LifecyclePhase.NOT_READY)
            self.assertIs(state.failed_phase, LifecyclePhase.REPOSITORIES)
            self.assertIn(str(missing_resource), state.error or "")
            self.assertEqual(state.error_type, "FileNotFoundError")
            self.assertIn(str(missing_resource), state.error_message or "")
            self.assertIn("ensure_repositories", state.error_traceback or "")
            emit.assert_called_once()
            self.assertEqual(emit.call_args.kwargs["failed_phase"], "repositories")
            self.assertEqual(emit.call_args.kwargs["error_type"], "FileNotFoundError")
            self.assertIn(str(missing_resource), emit.call_args.args[1])
            exported = lifecycle.export()
            self.assertEqual(exported["failed_phase"], "repositories")
            self.assertEqual(exported["error_type"], "FileNotFoundError")
            self.assertIn(str(missing_resource), exported["error_traceback"] or "")

            with self.assertRaises(FileNotFoundError) as raised:
                lifecycle.raise_startup_error()
            self.assertIs(raised.exception, original)
            self.assertIn(str(missing_resource), str(raised.exception))

    def test_driver_hook_reraises_before_bank_clock_and_optional_matchers(self) -> None:
        lifecycle = Lifecycle()
        original = FileNotFoundError("resource missing from isolated data directory")

        def ensure_repositories() -> None:
            raise original

        lifecycle.register(LifecyclePhase.REPOSITORIES, ensure_repositories)
        state = asyncio.run(lifecycle.start())
        context = SimpleNamespace(
            message_gateway=SimpleNamespace(sender=object()),
            services=None,
            clock=object(),
        )
        driver = SimpleNamespace(
            config=SimpleNamespace(xiuxian_data_dir="/isolated/data"),
            on_startup=lambda callback: None,
            on_shutdown=lambda callback: None,
        )

        with (
            patch("nonebot_plugin_xiuxian_2.adapters.nonebot.register_migrated_matchers") as register_migrated,
            patch("nonebot_plugin_xiuxian_2.adapters.nonebot.register_bank_first_use_matcher") as register_first_use,
            patch("nonebot_plugin_xiuxian_2.adapters.nonebot.register_bank_first_use_extended_matchers") as register_extended,
            patch("nonebot_plugin_xiuxian_2.features.bank.clock.set_bank_clock") as set_bank_clock,
            patch("nonebot_plugin_xiuxian_2.plugin.build_runtime_context", return_value=context),
            patch("nonebot_plugin_xiuxian_2.plugin.startup", new=AsyncMock(return_value=(state, object(), context, lifecycle))),
        ):
            on_startup, _on_shutdown = install_driver_hooks(driver)
            with self.assertRaises(FileNotFoundError) as raised:
                asyncio.run(on_startup())

        self.assertIs(raised.exception, original)
        self.assertTrue(register_migrated.called)
        register_first_use.assert_not_called()
        register_extended.assert_not_called()
        set_bank_clock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
