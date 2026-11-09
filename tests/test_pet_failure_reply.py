"""Regression coverage for silent pet-command failures.

Log evidence (``/root/xiu2/logs/xiu2.log-20261103-033001.gz``, 2026-11-01, seven
occurrences) -- the only plugin-owned failure shape in the 10-day window that
still had no user-visible answer after ``d64ce230``::

    11-01 20:20:25 [ERROR] nonebot | Running Matcher(type='message',
        module=...xiuxian.xiuxian_pet, lineno=93) failed.

``lineno=93`` is ``pet_egg = on_command("砸蛋", ...)``: the matcher raised and the
player received nothing at all.  Every one of the ten pet command handlers that
call ``pet_application.*`` had the same hole, so the fix is a guard that answers
once, logs the exception, and never leaks a traceback into the reply.
"""

from __future__ import annotations

import ast
import inspect
import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot.params import CommandArg

from nonebot_plugin_xiuxian_2.core.errors import ConflictError, DomainError, ValidationError
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_hatch
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.failure_reply import (
    BUSY_FAILURE_TEXT,
    CONFLICT_FAILURE_TEXT,
    GENERIC_FAILURE_TEXT,
    VALIDATION_FAILURE_TEXT,
    failure_reply,
    failure_text,
    guard_pet_handler,
)

PET_MODULE = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_pet"
    / "__init__.py"
)


class _Recorder:
    """Stand-in for ``handle_send`` that records every reply."""

    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[object, object, str]] = []
        self.fail = fail

    async def __call__(self, bot, event, msg, *args, **kwargs):
        self.calls.append((bot, event, msg))
        if self.fail:
            raise RuntimeError("delivery down")


class GuardUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_failure_sends_exactly_one_leak_free_reply(self) -> None:
        recorder = _Recorder()

        @guard_pet_handler("砸蛋", send=recorder)
        async def handler(bot, event):
            raise sqlite3.IntegrityError(
                "UNIQUE constraint failed: player_pet_item.user_id, /srv/data/player.db"
            )

        result = await handler("bot", "event")

        self.assertIsNone(result)
        self.assertEqual(len(recorder.calls), 1)
        bot, event, msg = recorder.calls[0]
        self.assertEqual((bot, event), ("bot", "event"))
        self.assertIn("砸蛋", msg)
        self.assertIn(GENERIC_FAILURE_TEXT, msg)
        for leak in ("sqlite3", "UNIQUE", ".db", "Traceback", "player_pet_item"):
            self.assertNotIn(leak, msg)

    async def test_busy_database_gets_a_retry_hint(self) -> None:
        recorder = _Recorder()

        @guard_pet_handler("宠物喂食", send=recorder)
        async def handler(bot, event):
            raise sqlite3.OperationalError("database is locked")

        await handler("bot", "event")
        self.assertEqual(len(recorder.calls), 1)
        self.assertIn(BUSY_FAILURE_TEXT, recorder.calls[0][2])

    async def test_conflict_and_validation_texts(self) -> None:
        self.assertEqual(failure_text(ConflictError("busy")), CONFLICT_FAILURE_TEXT)
        self.assertEqual(failure_text(ValidationError("bad input")), VALIDATION_FAILURE_TEXT)
        self.assertEqual(failure_text(DomainError("domain_error", "domain")), GENERIC_FAILURE_TEXT)
        self.assertEqual(failure_text(ValueError("oops")), GENERIC_FAILURE_TEXT)

    async def test_success_path_sends_nothing_extra(self) -> None:
        recorder = _Recorder()

        @guard_pet_handler("放生宠物", send=recorder)
        async def handler(bot, event):
            return "done"

        self.assertEqual(await handler("bot", "event"), "done")
        self.assertEqual(recorder.calls, [])

    async def test_delivery_failure_does_not_mask_the_original(self) -> None:
        recorder = _Recorder(fail=True)

        @guard_pet_handler("宠物融合", send=recorder)
        async def handler(bot, event):
            raise ValueError("boom")

        # The guard must swallow the original exception and also survive a
        # failing send, so the player-facing contract never becomes an exception.
        self.assertIsNone(await handler("bot", "event"))
        self.assertEqual(len(recorder.calls), 1)

    async def test_wrapper_keeps_the_nonebot_signature(self) -> None:
        """functools.wraps must keep Bot/Event/CommandArg injection intact."""

        async def handler(bot, event, args: str = CommandArg()):
            return args

        wrapped = guard_pet_handler("宠物游历")(handler)
        self.assertEqual(inspect.signature(wrapped), inspect.signature(handler))
        params = list(inspect.signature(wrapped).parameters)
        self.assertEqual(params, ["bot", "event", "args"])

    async def test_failure_reply_is_short_and_action_scoped(self) -> None:
        text = failure_reply("宠物技能洗练", sqlite3.OperationalError("no such table: x"))
        self.assertTrue(text.startswith("宠物技能洗练未完成："))
        self.assertLess(len(text), 80)


class GuardEndToEndTests(unittest.IsolatedAsyncioTestCase):
    """The real repository exception path must reach the player once."""

    async def test_real_hatch_failure_reaches_the_player(self) -> None:
        from nonebot_plugin_xiuxian_2.features.pet.repository import PetHatchSqlRepository

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            game, player = root / "game.db", root / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_pet_hatch(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u', 100)")
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, "
                    "egg_pity_count INTEGER, egg_pity_no_mythic_count INTEGER, travel TEXT)"
                )
                uow.execute("INSERT INTO player_pet VALUES('u', '', 0, 0, NULL)")
                # No player_pet_item table: the hatch INSERT raises and must not
                # leave the player without an answer.
            recorder = _Recorder()
            repository = PetHatchSqlRepository(game, player)

            @guard_pet_handler("砸蛋", send=recorder)
            async def handler(bot, event):
                return repository.hatch(
                    "op-e2e",
                    "u",
                    100,
                    10,
                    ["", 0, 0, None],
                    [({"uid": "p1", "pet_id": "1", "stars": 1, "exp": 0, "total_exp": 0, "skill": {}}, True)],
                    ["p1", 1, 0],
                    10,
                )

            self.assertIsNone(await handler("bot", "event"))
            self.assertEqual(len(recorder.calls), 1)
            self.assertIn("砸蛋未完成", recorder.calls[0][2])
            with DatabaseUnitOfWork(game) as uow:
                self.assertEqual(
                    uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 100
                )


class GuardCoverageContractTests(unittest.TestCase):
    """No pet command may regress into silence."""

    def test_every_pet_application_call_site_is_guarded(self) -> None:
        source = PET_MODULE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        guarded_handlers = 0
        call_sites = 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            body = ast.get_source_segment(source, node) or ""
            if "pet_application." not in body:
                continue
            call_sites += body.count("pet_application.")
            names = []
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    target = decorator.func
                    if isinstance(target, ast.Name):
                        names.append(target.id)
                    elif isinstance(target, ast.Attribute):
                        names.append(target.attr)
                elif isinstance(decorator, ast.Name):
                    names.append(decorator.id)
            self.assertIn(
                "guard_pet_handler",
                names,
                f"handler at line {node.lineno} calls pet_application without a guard",
            )
            guarded_handlers += 1
        self.assertEqual(guarded_handlers, 10)
        self.assertEqual(call_sites, 12)


if __name__ == "__main__":
    unittest.main()
