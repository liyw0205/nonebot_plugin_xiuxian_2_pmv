from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..accessory_repository import AdminAccessorySqlRepository
from ..application import AdminAssetApplication
from ..migrations import apply_admin_accessory_operations, apply_admin_stone_adjustment
from tests.test_db_backend import db_backend


class AdminAccessoryRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.game = self.root / "game.db"
        self.player = self.root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
        with DatabaseUnitOfWork(self.game) as uow:
            apply_admin_stone_adjustment(uow)
            apply_admin_accessory_operations(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
        self.repo = AdminAccessorySqlRepository(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _accessory(uid: str, *, item_id: int = 7, quality: int = 3, name: str = "灵玉") -> dict:
        return {"uid": uid, "item_id": item_id, "quality": quality, "name": name}

    @staticmethod
    def _factory(start: int = 1):
        next_uid = start

        def create() -> dict:
            nonlocal next_uid
            result = AdminAccessoryRepositoryTests._accessory(f"new-{next_uid}")
            next_uid += 1
            return result

        return create

    def test_grant_destroy_partial_quantity_replay_and_audit(self) -> None:
        snapshot = self.repo.snapshot("u")
        self.assertEqual(snapshot.status, "ok")
        factory = self._factory()
        granted = self.repo.grant(
            "grant-1", "admin", "u", 7, "灵玉", 3, 2,
            snapshot.equipped, snapshot.bag, 10, factory, target_name="目标",
        )
        replay = self.repo.grant(
            "grant-1", "admin", "u", 7, "灵玉", 3, 2,
            snapshot.equipped, snapshot.bag, 10, factory, target_name="目标",
        )
        conflict = self.repo.grant(
            "grant-1", "admin", "u", 7, "灵玉", 3, 1,
            snapshot.equipped, snapshot.bag, 10, factory, target_name="目标",
        )

        self.assertEqual((granted.status, granted.affected_quantity), ("granted", 2))
        self.assertEqual((replay.status, replay.accessories), ("duplicate", granted.accessories))
        self.assertEqual(conflict.status, "operation_conflict")
        current = self.repo.snapshot("u")
        destroyed = self.repo.destroy(
            "destroy-1", "admin", "u", 7, "灵玉", 5,
            current.equipped, current.bag, target_name="目标",
        )
        destroy_replay = self.repo.destroy(
            "destroy-1", "admin", "u", 7, "灵玉", 5,
            current.equipped, current.bag, target_name="目标",
        )

        self.assertEqual((destroyed.status, destroyed.affected_quantity), ("destroyed", 2))
        self.assertEqual((destroy_replay.status, destroy_replay.affected_quantity), ("duplicate", 2))
        self.assertEqual(self.repo.snapshot("u").bag, [])
        with db_backend.connection(self.game) as conn:
            rows = conn.execute(
                "SELECT action,item_delta,detail,trace_id FROM economy_log ORDER BY id"
            ).fetchall()
        self.assertEqual([row[0] for row in rows], ["admin_accessory_add", "admin_accessory_cost"])
        self.assertEqual([json.loads(row[1])[0]["amount"] for row in rows], [2, -2])
        self.assertEqual([row[3] for row in rows], ["grant-1", "destroy-1"])
        self.assertEqual(json.loads(rows[0][2])["accessory_uids"], ["new-1", "new-2"])

    def test_cas_capacity_invalid_state_user_and_factory_uid(self) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "INSERT INTO player_accessory(user_id,equipped,bag) VALUES(?,?,?)",
                ("u", "{}", json.dumps([self._accessory("owned-1"), self._accessory("owned-2")])),
            )
        stale = self.repo.snapshot("u")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE player_accessory SET bag=? WHERE user_id='u'",
                (json.dumps([self._accessory("owned-1"), self._accessory("owned-2"), self._accessory("owned-3")]),),
            )
        stale_result = self.repo.grant(
            "stale", "admin", "u", 7, "灵玉", 3, 1,
            stale.equipped, stale.bag, 10, self._factory(),
        )
        self.assertEqual(stale_result.status, "state_changed")

        current = self.repo.snapshot("u")
        full = self.repo.grant(
            "full", "admin", "u", 7, "灵玉", 3, 1,
            current.equipped, current.bag, 3, self._factory(),
        )
        self.assertEqual(full.status, "inventory_full")
        self.assertEqual(
            self.repo.grant(
                "missing-user", "admin", "absent", 7, "灵玉", 3, 1,
                {}, [], 10, self._factory(),
            ).status,
            "user_missing",
        )
        invalid_factory_result = self.repo.grant(
            "invalid-factory", "admin", "u", 7, "灵玉", 3, 1,
            current.equipped, current.bag, 10, lambda: self._accessory("owned-1"),
        )
        self.assertEqual(invalid_factory_result.status, "invalid_plan")

        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE player_accessory SET bag=? WHERE user_id='u'",
                (json.dumps([self._accessory("duplicate"), self._accessory("duplicate")]),),
            )
        invalid_snapshot = self.repo.snapshot("u")
        invalid_result = self.repo.grant(
            "invalid-state", "admin", "u", 7, "灵玉", 3, 1,
            invalid_snapshot.equipped, invalid_snapshot.bag, 10, self._factory(),
        )
        self.assertEqual(invalid_result.status, "invalid_state")

    def test_legacy_receipt_payload_and_result_are_replayed(self) -> None:
        payload = {
            "operator_id": "admin",
            "user_id": "u",
            "item_id": 7,
            "item_name": "灵玉",
            "quality": 3,
            "quantity": 1,
            "max_accessories": 10,
            "target_name": "目标",
        }
        accessory = self._accessory("legacy-uid")
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        result_json = json.dumps(
            {
                "action": "grant",
                "user_id": "u",
                "requested_quantity": 1,
                "affected_quantity": 1,
                "accessories": [accessory],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO admin_accessory_operations(operation_id,action,payload,result_json) "
                "VALUES(?,?,?,?)",
                ("legacy", "grant", payload_json, result_json),
            )

        replay = self.repo.grant(
            "legacy", "admin", "u", 7, "灵玉", 3, 1, {}, [], 10,
            lambda: self.fail("legacy receipt must replay before invoking the factory"),
            target_name="目标",
        )

        self.assertEqual((replay.status, replay.accessories), ("duplicate", (accessory,)))
        self.assertEqual(self.repo.snapshot("u").bag, [])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 0)

    def test_missing_schema_does_not_create_or_repair_databases(self) -> None:
        missing_game = self.root / "missing-game.db"
        missing_player = self.root / "missing-player.db"
        repo = AdminAccessorySqlRepository(missing_game, missing_player)
        self.assertEqual(repo.snapshot("u").status, "schema_missing")
        result = repo.grant(
            "not-ready", "admin", "u", 7, "灵玉", 3, 1, {}, [], 10, self._factory()
        )
        self.assertEqual(result.status, "schema_missing")
        self.assertFalse(missing_game.exists())
        self.assertFalse(missing_player.exists())

        incomplete_player = self.root / "incomplete-player.db"
        with DatabaseUnitOfWork(incomplete_player) as uow:
            uow.execute("CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY)")
        self.assertEqual(AdminAccessorySqlRepository(self.game, incomplete_player).snapshot("u").status, "schema_missing")
        self.assertEqual(AdminAccessorySqlRepository(self.game, incomplete_player).grant(
            "incomplete", "admin", "u", 7, "灵玉", 3, 1, {}, [], 10, self._factory()
        ).status, "schema_missing")
        with DatabaseUnitOfWork(incomplete_player, read_only=True) as uow:
            columns = {row["name"] for row in uow.query_all('PRAGMA table_info("player_accessory")')}
        self.assertEqual(columns, {"user_id"})

    def test_late_receipt_failure_rolls_back_player_change_and_audit(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER reject_accessory_receipt BEFORE INSERT "
                "ON admin_accessory_operations BEGIN SELECT RAISE(ABORT,'reject receipt'); END"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "reject receipt"):
            self.repo.grant("late", "admin", "u", 7, "灵玉", 3, 1, {}, [], 10, self._factory())

        self.assertEqual(self.repo.snapshot("u").bag, [])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM admin_accessory_operations").fetchone()[0], 0)

    def test_application_passes_quality_capacity_and_factory_to_repository(self) -> None:
        app = AdminAssetApplication(self.game)
        outcome = app.adjust_accessory(
            operation_id="application-grant",
            operator_id="admin",
            user_id="u",
            action="grant",
            item_id=7,
            item_name="灵玉",
            quantity=1,
            player_database=self.player,
            quality=5,
            max_accessories=9,
            create_accessory=lambda: self._accessory("application-uid", quality=5),
        )

        self.assertEqual((outcome.status, outcome.ok), ("granted", True))
        self.assertEqual(outcome.data["accessories"][0]["quality"], 5)
        self.assertEqual(self.repo.snapshot("u").bag[0]["uid"], "application-uid")


if __name__ == "__main__":
    unittest.main()
