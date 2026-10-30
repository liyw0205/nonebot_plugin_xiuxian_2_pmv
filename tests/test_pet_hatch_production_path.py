"""Production-shaped evidence for the ``砸蛋`` (pet egg) command contract.

The defect this suite pins down needed the REAL ``PetApplication`` (not a stub):

* ``xiuxian_pet/__init__.py``'s ``pet_egg`` handler pre-replay check called
  ``pet_application.hatch_result(operation_id)`` positionally, while the
  application declares ``def hatch_result(self, *, operation_id: str)``.
  The call sits BEFORE the stone check, the bag-capacity check, the hatch and
  the persistence, and the handler has no ``try``/``except``, so **every**
  ``砸蛋`` raised ``TypeError`` and the player received no reply at all --
  the whole hatching feature was dead.

The pre-existing ``features/pet/tests/test_pet_application.py`` stub declared
``def hatch_result(self, *args, **kwargs)``, which accepts positional arguments,
so the broken call shape stayed green locally.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.pet.application import PetApplication
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_hatch
from nonebot_plugin_xiuxian_2.features.pet.repository import PetHatchResult
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class PetHatchProductionPathTests(unittest.TestCase):
    """Drive the replay precheck the way the ``pet_egg`` handler drives it."""

    def build(self, temp: str, *, with_schema: bool = True, stone: int = 0) -> tuple[Path, Path]:
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        if with_schema:
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                apply_pet_hatch(uow)
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, "
                    "exp INTEGER, hp INTEGER, mp INTEGER)"
                )
                uow.execute(
                    "INSERT INTO user_xiuxian(user_id,stone,exp,hp,mp) VALUES('u',?,0,100,100)",
                    (stone,),
                )
        else:
            game.write_bytes(b"")
        with DatabaseUnitOfWork(player) as uow:
            uow.execute(
                "CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, "
                "egg_pity_count INTEGER DEFAULT 0, egg_pity_no_mythic_count INTEGER DEFAULT 0, "
                "travel TEXT)"
            )
            uow.execute(
                "CREATE TABLE player_pet_item(id TEXT PRIMARY KEY, user_id TEXT, uid TEXT, "
                "is_active INTEGER, pet_id TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, "
                "skill_id TEXT, created_at INTEGER, updated_at INTEGER)"
            )
        return game, player

    # -------------------------------------------------------- call contract
    def test_positional_hatch_result_raises_type_error(self) -> None:
        """The exact broken call shape the handler used to make."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            application = PetApplication(game, player)
            with self.assertRaises(TypeError):
                application.hatch_result("pet-hatch:x:u")  # type: ignore[misc]

    def test_keyword_hatch_result_call_works(self) -> None:
        """The correct shape; must survive the real application."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            application = PetApplication(game, player)
            self.assertIsNone(application.hatch_result(operation_id="pet-hatch:x:u"))

    def test_missing_schema_returns_none_instead_of_creating_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, with_schema=False)
            application = PetApplication(game, player)
            with self.assertRaises(Exception):
                # The feature repository performs no request-time DDL: it either
                # reports the missing schema or raises, but never creates it.
                application.hatch_result(operation_id="pet-hatch:x:u")
            with DatabaseUnitOfWork(game) as uow:
                created = uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                    ("pet_hatch_operations",),
                )
            self.assertIsNone(created)

    # ------------------------------------------------------------ replay
    def test_recorded_hatch_replays_as_duplicate_with_same_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=5000)
            application = PetApplication(game, player)
            pets = [({"uid": "p1", "name": "灵狐"}, True)]
            updated_meta = ["p1", 1, 2]
            outcome = application.hatch(
                operation_id="pet-hatch:1:u",
                user_id="u",
                expected_stone=5000,
                cost=1000,
                expected_meta=["", 0, 0, None],
                pets=pets,
                updated_meta=updated_meta,
                bag_limit=99,
            )
            self.assertTrue(outcome.ok)
            # 砸蛋's pre-replay check must now find that receipt.
            prior = application.hatch_result(operation_id="pet-hatch:1:u")
            self.assertIsInstance(prior, PetHatchResult)
            self.assertEqual(prior.status, "duplicate")
            self.assertTrue(prior.succeeded)
            self.assertEqual(prior.bag_limit, 99)
            self.assertEqual(list(prior.updated_meta), updated_meta)
            self.assertEqual(prior.pets, tuple((dict(pet), True) for pet, _ in pets))
            # The read-back receipt does not carry the charged cost; the handler
            # falls back to the freshly computed total (`prior.cost or total_cost`).
            self.assertEqual(prior.cost, 0)

    # ------------------------------------------------------ handler contract
    def test_handler_source_passes_operation_id_by_keyword(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_pet"
            / "__init__.py"
        )
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "hatch_result"
        ]
        self.assertGreaterEqual(len(calls), 2)
        for call in calls:
            keyword_names = {kw.arg for kw in call.keywords}
            self.assertIn(
                "operation_id",
                keyword_names,
                msg=f"hatch_result call at line {call.lineno} must pass operation_id=",
            )
            self.assertEqual(
                call.args,
                [],
                msg=f"hatch_result call at line {call.lineno} must not be positional",
            )


if __name__ == "__main__":
    unittest.main()
