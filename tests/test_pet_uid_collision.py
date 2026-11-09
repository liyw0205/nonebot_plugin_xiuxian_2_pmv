"""Regression coverage for the pet-UID collision that aborted ``砸蛋`` in production.

Log evidence (``/root/xiu2/logs/xiu2.log-20261103-033001.gz``, 2026-11-01, seven
occurrences for ``砸蛋10``)::

    File ".../xiuxian/xiuxian_pet/transaction_service.py", line 461, in hatch
        conn.execute(
    File ".../xiuxian/xiuxian_utils/db_backend.py", line 249, in execute
    sqlite3.IntegrityError: UNIQUE constraint failed: player_pet_item.user_id,
    player_pet_item.uid

Root cause: every new pet got ``pet_<epoch_seconds>_<randint(1000, 9999)>``, so a
ten-egg batch rolled inside one second collided on roughly 0.5% of batches.  The
colliding INSERT aborted the whole cross-database transaction, so the player saw
a bare failure with no explanation.

Fix under test:

* ``new_pet_uid`` (``xiuxian_utils/pet_system.py``) keeps the ``pet_<digits>_<digits>``
  shape but allocates from a per-process token plus a per-second sequence, so two
  pets created in the same second can never share a UID.
* every writer of ``player_pet_item`` turns a residual ``UNIQUE`` violation into
  the typed ``uid_conflict`` status and rolls back, so nothing is charged,
  no egg is consumed, and no receipt is written.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.compatibility.legacy_pet_transactions import PetHatchService
from nonebot_plugin_xiuxian_2.features.back.pet_egg_repository import PetEggUseSqlRepository
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_hatch
from nonebot_plugin_xiuxian_2.features.pet.repository import PetHatchSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import pet_system


class _FixedClock:
    """Stand-in for the ``time`` module inside ``pet_system``."""

    def __init__(self, value: float) -> None:
        self.value = value

    def time(self) -> float:  # noqa: D401 - mirrors time.time
        return self.value


def _old_uid_scheme() -> str:
    """The exact production formula that collided."""

    import random

    return f"pet_{int(pet_system.time.time())}_{random.randint(1000, 9999)}"


class PetUidAllocationTests(unittest.TestCase):
    def test_frozen_second_no_longer_collides(self) -> None:
        """Ten eggs in one second used to collide; 5000 allocations must not."""

        with mock.patch.object(pet_system, "time", _FixedClock(1762000000.0)):
            uids = [pet_system.new_pet_uid() for _ in range(5000)]
        self.assertEqual(len(set(uids)), len(uids))
        for uid in uids[:5]:
            self.assertRegex(uid, r"^pet_\d+_\d+$")
            self.assertTrue(uid.startswith("pet_1762000000_"))

    def test_old_scheme_reproduces_the_production_collision(self) -> None:
        """Pin the defect shape: the previous allocator repeats under one clock."""

        with mock.patch.object(pet_system, "time", _FixedClock(1762000000.0)):
            uids = [_old_uid_scheme() for _ in range(5000)]
        self.assertLess(len(set(uids)), len(uids))


class PetHatchUidConflictTests(unittest.TestCase):
    """Residual UID clashes must fail closed instead of raising."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            apply_pet_hatch(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u', 100)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, "
                "egg_pity_count INTEGER, egg_pity_no_mythic_count INTEGER, travel TEXT)"
            )
            uow.execute("INSERT INTO player_pet VALUES('u', 'taken', 0, 0, NULL)")
            uow.execute(
                "CREATE TABLE player_pet_item(id TEXT, user_id TEXT, uid TEXT, "
                "is_active INTEGER, pet_id TEXT, stars INTEGER, exp INTEGER, "
                "total_exp INTEGER, skill_id TEXT, created_at INTEGER, updated_at INTEGER)"
            )
            # Production shape: UNIQUE(user_id, uid) is what raised.
            uow.execute(
                "CREATE UNIQUE INDEX idx_player_pet_item_user_uid "
                "ON player_pet_item(user_id, uid)"
            )
            uow.execute(
                "INSERT INTO player_pet_item(id, user_id, uid, is_active, pet_id, stars, "
                "exp, total_exp) VALUES('u:taken', 'u', 'taken', 0, '1', 1, 0, 0)"
            )
        self.pet = {"uid": "taken", "pet_id": "1", "stars": 1, "exp": 0, "total_exp": 0, "skill": {}}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_modern_repository_reports_uid_conflict_without_charging(self) -> None:
        repository = PetHatchSqlRepository(self.game, self.player)
        outcome = repository.hatch(
            "op-uid", "u", 100, 10, ["taken", 0, 0, None], [(self.pet, False)], ["taken", 1, 0], 10
        )
        self.assertEqual(outcome.status, "uid_conflict")
        self.assertFalse(outcome.succeeded)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 100)
            self.assertIsNone(
                uow.query_one("SELECT operation_id FROM pet_hatch_operations WHERE operation_id='op-uid'")
            )
        with DatabaseUnitOfWork(self.player) as uow:
            rows = uow.query_all("SELECT uid FROM player_pet_item WHERE user_id='u'")
            self.assertEqual([row["uid"] for row in rows], ["taken"])

    def test_legacy_service_reports_uid_conflict_without_charging(self) -> None:
        service = PetHatchService(self.game, self.player)
        outcome = service.hatch(
            "op-legacy", "u", 100, 10, ["taken", 0, 0, None], [(self.pet, False)], ["taken", 1, 0], 10
        )
        self.assertEqual(outcome.status, "uid_conflict")
        self.assertFalse(outcome.succeeded)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 100)
            self.assertIsNone(
                uow.query_one("SELECT operation_id FROM pet_hatch_operations WHERE operation_id='op-legacy'")
            )


    def test_ten_eggs_in_one_second_all_persist(self) -> None:
        """The production batch shape (砸蛋10 in a single second) now succeeds."""

        with mock.patch.object(pet_system, "time", _FixedClock(1762000000.0)):
            pets = [
                ({"uid": pet_system.new_pet_uid(), "pet_id": "1", "stars": 1,
                  "exp": 0, "total_exp": 0, "skill": {}}, False)
                for _ in range(10)
            ]
        repository = PetHatchSqlRepository(self.game, self.player)
        outcome = repository.hatch(
            "op-ten", "u", 100, 10, ["taken", 0, 0, None], pets, ["taken", 10, 0], 20
        )
        self.assertEqual(outcome.status, "applied")
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 90)
        with DatabaseUnitOfWork(self.player) as uow:
            rows = uow.query_all("SELECT uid FROM player_pet_item WHERE user_id='u'")
        self.assertEqual(len(rows), 11)  # one pre-existing + ten hatched


class BatchPetEggUidConflictTests(unittest.TestCase):
    def test_uid_conflict_keeps_the_egg_and_writes_no_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            game, player = root / "game.db", root / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER)")
                uow.execute("INSERT INTO back VALUES('u', 5001, 3)")
                # Production creates this receipt table at startup; do the same so
                # the rollback assertion checks "no receipt row" rather than
                # "the CREATE TABLE itself rolled back".
                uow.execute(
                    "CREATE TABLE batch_pet_egg_use_operations("
                    "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
                    "quantity INTEGER NOT NULL,pets_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, "
                    "egg_pity_count INTEGER, egg_pity_no_mythic_count INTEGER, travel TEXT)"
                )
                uow.execute("INSERT INTO player_pet VALUES('u', 'taken', 0, 0, NULL)")
                uow.execute(
                    "CREATE TABLE player_pet_item(id TEXT, user_id TEXT, uid TEXT, "
                    "is_active INTEGER, pet_id TEXT, stars INTEGER, exp INTEGER, "
                    "total_exp INTEGER, skill_id TEXT, created_at INTEGER, updated_at INTEGER)"
                )
                uow.execute(
                    "CREATE UNIQUE INDEX idx_player_pet_item_user_uid "
                    "ON player_pet_item(user_id, uid)"
                )
                uow.execute(
                    "INSERT INTO player_pet_item(id, user_id, uid, is_active, pet_id, stars, "
                    "exp, total_exp) VALUES('u:taken', 'u', 'taken', 1, '1', 1, 0, 0)"
                )
            repository = PetEggUseSqlRepository(game, player, ensure_schema=True)
            outcome = repository.use_pet_eggs(
                operation_id="op-egg",
                user_id="u",
                item_id=5001,
                quantity=1,
                expected_active_uid="taken",
                expected_pet_uids=["taken"],
                pets=[({"uid": "taken", "pet_id": "1", "stars": 1, "exp": 0, "total_exp": 0,
                        "skill": {}}, "bag")],
                bag_limit=10,
            )
            self.assertEqual(outcome.status, "uid_conflict")
            with DatabaseUnitOfWork(game) as uow:
                self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id='u'")["goods_num"], 3)
                self.assertIsNone(
                    uow.query_one(
                        "SELECT operation_id FROM batch_pet_egg_use_operations WHERE operation_id='op-egg'"
                    )
                )
            with DatabaseUnitOfWork(player) as uow:
                rows = uow.query_all("SELECT uid FROM player_pet_item WHERE user_id='u'")
                self.assertEqual([row["uid"] for row in rows], ["taken"])


if __name__ == "__main__":
    unittest.main()
