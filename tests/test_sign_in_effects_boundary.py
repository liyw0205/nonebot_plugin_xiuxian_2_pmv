from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from nonebot_plugin_xiuxian_2.features.sign_in.application import SignInApplication
from nonebot_plugin_xiuxian_2.features.sign_in.migrations import apply_sign_in
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class FixedRandom:
    @staticmethod
    def randint(lower: int, upper: int) -> int:
        return lower


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class SignInEffectsBoundaryTests(unittest.TestCase):
    def test_effects_run_after_success_and_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "sign.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, is_sign INTEGER, stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?)", ("u1", 0, 0))
                apply_platform_schema(uow)
                apply_sign_in(uow)
            effects = Mock()
            app = SignInApplication(database, random_source=FixedRandom(), clock=FixedClock(), effects=effects)

            first = app.claim(user_id="u1", operation_id="sign-1", lower_limit=10, upper_limit=20)
            replay = app.claim(user_id="u1", operation_id="sign-1", lower_limit=10, upper_limit=20)

            self.assertTrue(first.ok)
            self.assertEqual(replay.status, "replayed")
            # The legacy operation projection is also replayed after the
            # ledger replay; the injected adapter owns dedupe for its
            # non-idempotent side effects.
            self.assertGreaterEqual(effects.on_signed.call_count, 2)
            self.assertEqual(effects.on_signed.call_args_list[0].kwargs["replayed"], False)
            self.assertTrue(any(call.kwargs["replayed"] for call in effects.on_signed.call_args_list[1:]))

    def test_rejected_claim_does_not_run_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "sign.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, is_sign INTEGER, stone INTEGER)")
                apply_platform_schema(uow)
                apply_sign_in(uow)
            effects = Mock()
            app = SignInApplication(database, random_source=FixedRandom(), clock=FixedClock(), effects=effects)

            result = app.claim(user_id="missing", operation_id="sign-missing")

            self.assertEqual(result.code, "user_missing")
            effects.on_signed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
