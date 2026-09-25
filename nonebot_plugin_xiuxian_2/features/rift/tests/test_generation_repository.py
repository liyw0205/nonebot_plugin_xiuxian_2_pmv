from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema, build_migrations, migrations_for_database
from ..application import RiftApplication
from ..generation_repository import RiftGenerationSqlRepository
from ..migrations import apply_rift_world_generation


class TestRiftGenerationRepository(unittest.TestCase):
    @staticmethod
    def _prepare(database: Path, *, migration: bool = True) -> None:
        with DatabaseUnitOfWork(database) as uow:
            apply_platform_schema(uow)
            if migration:
                apply_rift_world_generation(uow)

    @staticmethod
    def _plan(name: str = "东玄域", *, participants: list[str] | None = None) -> dict:
        return {
            "name": name,
            "rank": 1,
            "time": 60,
            "target_nodes": [{"node_id": "trial-1", "node_name": "问心台"}],
            "l_user_id": participants or [],
        }

    def test_default_application_generates_and_replays_without_legacy_repository(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            application = RiftApplication(database, Path(temp) / "player.db")
            application.legacy_repository.invoke = lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("legacy repository used")
            )
            plan = self._plan(participants=["u", "u"])

            first = application.generate(
                operation_id="generation-1",
                rift_key="global",
                rift_plan=plan,
            )
            replay = application.generate(
                operation_id="generation-1",
                rift_key="global",
                rift_plan=plan,
            )
            second = application.generate(
                operation_id="generation-2",
                rift_key="global",
                rift_plan=self._plan("西玄域"),
            )

            self.assertIsNone(application.repository)
            self.assertTrue(first.ok)
            self.assertEqual(first.data["status"], "applied")
            self.assertEqual(first.data["state"]["participants"], ())
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.data["state"]["revision"], 1)
            self.assertTrue(second.ok)
            self.assertEqual(second.data["state"]["revision"], 2)
            self.assertEqual(application.current_world(rift_key="global")["rift_data"]["name"], "西玄域")

    def test_existing_application_ledger_payload_replays_without_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)

            class LegacyGenerationRepository:
                def invoke(self, action, operation_id, user_id, **kwargs):
                    return {
                        "status": "applied",
                        "state": {
                            "rift_key": kwargs["rift_key"],
                            "generation_id": operation_id,
                            "rift_data": kwargs["rift_plan"],
                            "participants": (),
                            "revision": 1,
                        },
                    }

            plan = self._plan()
            old_application = RiftApplication(
                database,
                Path(temp) / "player.db",
                repository=LegacyGenerationRepository(),
            )
            old_result = old_application.generate(
                operation_id="legacy-web-generation",
                rift_key="global",
                rift_plan=plan,
            )
            current_application = RiftApplication(database, Path(temp) / "player.db")
            replay = current_application.generate(
                operation_id="legacy-web-generation",
                rift_key="global",
                rift_plan=plan,
            )

            self.assertTrue(old_result.ok)
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.data["state"]["generation_id"], "legacy-web-generation")

    def test_generate_accepts_legacy_web_user_scope(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            application = RiftApplication(database, Path(temp) / "player.db")

            result = application.generate(
                operation_id="web-generation",
                user_id="web-user",
                rift_key="global",
                rift_plan=self._plan(),
            )

            self.assertTrue(result.ok)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                ledger = uow.query_one(
                    "SELECT request_hash FROM operation_ledger "
                    "WHERE operation_id='web-generation' AND action='rift.generate'"
                )
            self.assertIsNotNone(ledger)

    def test_repository_preserves_revision_conflict_and_superseded_statuses(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            repository = RiftGenerationSqlRepository(database)

            first = repository.generate("generation-1", "global", self._plan())
            duplicate = repository.generate("generation-1", "global", self._plan())
            conflict = repository.generate(
                "generation-1", "global", self._plan("西玄域")
            )
            repository.generate("generation-2", "global", self._plan("西玄域"))
            superseded = repository.generate("generation-1", "global", self._plan())

            self.assertEqual((first.status, first.state["revision"]), ("applied", 1))
            self.assertEqual(duplicate.status, "duplicate")
            self.assertEqual(conflict.status, "state_changed")
            self.assertEqual(superseded.status, "superseded")
            self.assertEqual(superseded.state["revision"], 2)

    def test_replays_legacy_generation_payload_semantically(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            plan = self._plan()
            legacy_data = {key: value for key, value in plan.items() if key != "l_user_id"}
            legacy_payload = json.dumps(
                ["global", legacy_data], ensure_ascii=False, sort_keys=True
            )
            snapshot = json.dumps(legacy_data, ensure_ascii=False, sort_keys=True)
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "INSERT INTO rift_world_state"
                    "(rift_key,generation_id,rift_data,participants,revision) "
                    "VALUES(?,?,?,?,7)",
                    ("global", "legacy-generation", snapshot, "[]"),
                )
                uow.execute(
                    "INSERT INTO rift_generation_operations"
                    "(operation_id,payload,rift_key,generation_id,rift_data,revision) "
                    "VALUES(?,?,?,?,?,7)",
                    (
                        "legacy-generation",
                        legacy_payload,
                        "global",
                        "legacy-generation",
                        snapshot,
                    ),
                )

            result = RiftGenerationSqlRepository(database).generate(
                "legacy-generation", "global", plan
            )

            self.assertEqual(result.status, "duplicate")
            self.assertEqual(result.state["revision"], 7)

    def test_bootstrap_matches_legacy_snapshot_identity_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            repository = RiftGenerationSqlRepository(database)
            snapshot = self._plan(participants=["u", "u", 7])

            first = repository.bootstrap("global", snapshot)
            replay = repository.bootstrap("global", self._plan("ignored"))
            normalized = {key: value for key, value in snapshot.items() if key != "l_user_id"}
            canonical = json.dumps(
                ["global", normalized, ("u", "7")],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            expected_legacy_id = (
                "legacy:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
            )

            self.assertEqual(first["generation_id"], expected_legacy_id)
            self.assertEqual(first["participants"], ("u", "7"))
            self.assertEqual(first["revision"], 1)
            self.assertEqual(replay, first)

    def test_missing_migration_rejects_writes_and_reads_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database, migration=False)
            application = RiftApplication(database, Path(temp) / "player.db")

            result = application.generate(
                operation_id="missing-schema",
                rift_key="global",
                rift_plan=self._plan(),
            )
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "schema_missing")
            self.assertIsNone(application.current_world(rift_key="global"))
            self.assertIsNone(
                application.bootstrap_world(
                    rift_key="global", legacy_snapshot=self._plan()
                )
            )
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                tables = {
                    row["name"]
                    for row in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertNotIn("rift_world_state", tables)
            self.assertNotIn("rift_generation_operations", tables)

    def test_generation_failure_rolls_back_world_and_operation(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            repository = RiftGenerationSqlRepository(database)
            original = repository.generate("generation-1", "global", self._plan())
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TRIGGER reject_generation BEFORE INSERT ON "
                    "rift_generation_operations WHEN NEW.operation_id='generation-2' "
                    "BEGIN SELECT RAISE(ABORT,'reject generation'); END"
                )

            with self.assertRaises(sqlite3.IntegrityError):
                repository.generate("generation-2", "global", self._plan("西玄域"))

            self.assertEqual(repository.get_current("global")["generation_id"], "generation-1")
            self.assertEqual(repository.get_current("global")["revision"], original.state["revision"])
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM rift_generation_operations"
                    )["count"],
                    1,
                )

    def test_registered_manual_scheduled_and_startup_paths_use_application(self):
        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py"
        ).read_text(encoding="utf-8")
        manual = source[source.index("async def create_rift(") : source.index("@explore_rift.handle")]
        scheduled_dispatch = source[
            source.index("async def scheduled_rift_generation(") : source.index(
                "async def generate_rift_for_group("
            )
        ]
        scheduled = source[
            source.index("async def generate_rift_for_group(") : source.index("def _rift_progress_snapshot")
        ]
        startup = source[
            source.index("async def read_rift_(") : source.index("# 定时任务生成秘境")
        ]
        assert "rift_application.generate(" in manual
        assert "await generate_rift_for_group()" in scheduled_dispatch
        assert "rift_application.generate(" in scheduled
        assert "rift_application.current_world(" in startup
        assert "rift_application.bootstrap_world(" in startup
        assert "_rift_entry_service()" not in source
        assert "RiftEntryService" not in source

    def test_world_generation_migration_is_game_only(self):
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }

        self.assertIn("rift.005", routed["game_db"])
        for database_key in ("player_db", "trade_db", "impart_db", "message_db"):
            self.assertNotIn("rift.005", routed[database_key])
