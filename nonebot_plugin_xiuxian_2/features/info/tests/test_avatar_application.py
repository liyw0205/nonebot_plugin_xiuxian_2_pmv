from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ....core.errors import OperationConflictError
from ....infrastructure.database import DatabaseUnitOfWork
from ....infrastructure.database import request_hash
from ..avatar_application import PlayerAvatarApplication
from ..avatar_repository import AvatarStateSqlRepository
from ..migrations import apply_avatar_identity_player, apply_avatar_initialization_player


class PlayerAvatarApplicationTests(unittest.TestCase):
    @staticmethod
    def _database(path: Path) -> None:
        with DatabaseUnitOfWork(path) as uow:
            apply_avatar_identity_player(uow)
            apply_avatar_initialization_player(uow)
            uow.execute(
                "INSERT INTO avatar(user_id, main_id, avatar_id, active_id, create_time) "
                "VALUES('main', 'main', 'avatar', 'main', '2026-10-02 00:00:00')"
            )

    def test_toggle_replay_stale_snapshot_and_restore(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-state-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            self._database(database)
            application = PlayerAvatarApplication(database)

            toggled = application.toggle_active(
                operation_id="toggle-1", user_id="main", expected_active_id="main"
            )
            self.assertTrue(toggled.ok)
            self.assertEqual((toggled.active_id, toggled.role), ("avatar", "avatar"))

            replayed = application.toggle_active(
                operation_id="toggle-1", user_id="main", expected_active_id="avatar"
            )
            self.assertTrue(replayed.ok)
            self.assertTrue(replayed.replayed)
            self.assertEqual(replayed.active_id, "avatar")

            stale = application.toggle_active(
                operation_id="toggle-stale", user_id="main", expected_active_id="main"
            )
            self.assertEqual(stale.status, "stale_snapshot")
            self.assertEqual(application.get_active_user_id("main"), "avatar")

            toggled_back = application.toggle_active(
                operation_id="toggle-2", user_id="main", expected_active_id="avatar"
            )
            self.assertEqual((toggled_back.active_id, toggled_back.role), ("main", "main"))
            restored = application.restore_active(operation_id="restore-1", user_id="main")
            self.assertEqual(restored.active_id, "main")

    def test_same_operation_with_different_user_conflicts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-conflict-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            self._database(database)
            application = PlayerAvatarApplication(database)
            application.toggle_active(
                operation_id="toggle-1", user_id="main", expected_active_id="main"
            )

            with self.assertRaises(OperationConflictError):
                application.toggle_active(
                    operation_id="toggle-1", user_id="other", expected_active_id="other"
                )

    def test_state_and_receipt_roll_back_together_and_retry_applies_once(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-rollback-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            self._database(database)
            repository = AvatarStateSqlRepository(database)
            application = PlayerAvatarApplication(database, repository=repository)

            with patch.object(repository, "_record_receipt", side_effect=RuntimeError("injected")):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    application.toggle_active(
                        operation_id="toggle-1", user_id="main", expected_active_id="main"
                    )

            self.assertEqual(application.get_active_user_id("main"), "main")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_operation_receipts")["n"], 0
                )

            retried = application.toggle_active(
                operation_id="toggle-1", user_id="main", expected_active_id="main"
            )
            self.assertEqual(retried.active_id, "avatar")

    def test_initialization_replays_frozen_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-init-replay-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_avatar_identity_player(uow)
                apply_avatar_initialization_player(uow)
            application = PlayerAvatarApplication(database)

            first = application.initialize(
                operation_id="init-1",
                user_id="main",
                proposed_avatar_id="avatar-1",
                proposed_create_time="2026-10-02 00:00:00",
            )
            replayed = application.initialize(
                operation_id="init-1",
                user_id="main",
                proposed_avatar_id="avatar-2",
                proposed_create_time="2026-10-03 00:00:00",
            )

            self.assertEqual(first.status, "applied")
            self.assertEqual((first.avatar_id, first.create_time), ("avatar-1", "2026-10-02 00:00:00"))
            self.assertTrue(replayed.replayed)
            self.assertEqual((replayed.avatar_id, replayed.create_time), ("avatar-1", "2026-10-02 00:00:00"))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                row = uow.query_one("SELECT * FROM avatar WHERE user_id='main'")
                self.assertEqual(row["avatar_id"], "avatar-1")
                self.assertEqual(row["create_time"], "2026-10-02 00:00:00")
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_operation_receipts")["n"], 1
                )
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_initialization_plans")["n"], 0
                )

    def test_initialization_retry_uses_plan_after_receipt_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-init-recovery-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_avatar_identity_player(uow)
                apply_avatar_initialization_player(uow)
            repository = AvatarStateSqlRepository(database)
            application = PlayerAvatarApplication(database, repository=repository)

            with patch.object(repository, "_record_receipt", side_effect=RuntimeError("injected")):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    application.initialize(
                        operation_id="init-1",
                        user_id="main",
                        proposed_avatar_id="avatar-1",
                        proposed_create_time="2026-10-02 00:00:00",
                    )

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(uow.query_one("SELECT user_id FROM avatar WHERE user_id='main'"))
                plan = uow.query_one(
                    "SELECT avatar_id, create_time FROM avatar_initialization_plans "
                    "WHERE operation_id='init-1'"
                )
                self.assertEqual((plan["avatar_id"], plan["create_time"]), ("avatar-1", "2026-10-02 00:00:00"))
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_operation_receipts")["n"], 0
                )

            retried = application.initialize(
                operation_id="init-1",
                user_id="main",
                proposed_avatar_id="avatar-2",
                proposed_create_time="2026-10-03 00:00:00",
            )
            self.assertEqual((retried.avatar_id, retried.create_time), ("avatar-1", "2026-10-02 00:00:00"))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_initialization_plans")["n"], 0
                )

    def test_initialization_requires_prebuilt_plan_schema(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-init-no-schema-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_avatar_identity_player(uow)

            result = PlayerAvatarApplication(database).initialize(
                operation_id="init-1",
                user_id="main",
                proposed_avatar_id="avatar-1",
                proposed_create_time="2026-10-02 00:00:00",
            )

            self.assertEqual(result.status, "schema_missing")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master WHERE name='avatar_initialization_plans'"
                    )
                )

    def test_initialization_preserves_existing_legacy_avatar_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-init-legacy-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE avatar(user_id TEXT PRIMARY KEY, main_id TEXT, avatar_id TEXT)"
                )
                uow.execute(
                    "INSERT INTO avatar(user_id,main_id,avatar_id) VALUES('main','main','legacy-avatar')"
                )
                apply_avatar_identity_player(uow)
                apply_avatar_initialization_player(uow)

            result = PlayerAvatarApplication(database).initialize(
                operation_id="init-legacy",
                user_id="main",
                proposed_avatar_id="new-avatar",
                proposed_create_time="2026-10-02 00:00:00",
            )

            self.assertEqual(result.avatar_id, "legacy-avatar")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                row = uow.query_one("SELECT main_id, avatar_id FROM avatar WHERE user_id='main'")
                self.assertEqual((row["main_id"], row["avatar_id"]), ("main", "legacy-avatar"))

    def test_distinct_initialization_operations_converge_on_first_committed_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-init-race-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_avatar_identity_player(uow)
                apply_avatar_initialization_player(uow)
            repository = AvatarStateSqlRepository(database)
            digest = request_hash({"action": "avatar_init", "user_id": "main"})
            first_plan = repository._reserve_initialization(
                operation_id="init-1",
                user_id="main",
                proposed_avatar_id="avatar-1",
                proposed_create_time="2026-10-02 00:00:00",
                digest=digest,
            )
            second_plan = repository._reserve_initialization(
                operation_id="init-2",
                user_id="main",
                proposed_avatar_id="avatar-2",
                proposed_create_time="2026-10-03 00:00:00",
                digest=digest,
            )

            first = repository._complete_initialization("init-1", first_plan, digest)
            second = repository._complete_initialization("init-2", second_plan, digest)

            self.assertEqual((first.status, second.status), ("applied", "applied"))
            self.assertEqual((first.avatar_id, second.avatar_id), ("avatar-1", "avatar-1"))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_operation_receipts")["n"], 2
                )
                self.assertEqual(
                    uow.query_one("SELECT COUNT(*) AS n FROM avatar_initialization_plans")["n"], 0
                )

    def test_missing_schema_fails_closed_without_request_ddl(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-no-schema-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with sqlite3.connect(database):
                pass
            result = PlayerAvatarApplication(database).toggle_active(
                operation_id="toggle-1", user_id="main", expected_active_id="main"
            )

            self.assertEqual(result.status, "schema_missing")
            with sqlite3.connect(database) as connection:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertEqual(tables, set())

    def test_player_migration_preserves_legacy_avatar_row(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-migration-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE avatar(user_id TEXT PRIMARY KEY, main_id TEXT, avatar_id TEXT, active_id TEXT)"
                )
                uow.execute(
                    "INSERT INTO avatar(user_id,main_id,avatar_id,active_id) "
                    "VALUES('main','main','avatar','avatar')"
                )
            with DatabaseUnitOfWork(database) as uow:
                apply_avatar_identity_player(uow)
                apply_avatar_identity_player(uow)
                self.assertIsNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master WHERE name='avatar_initialization_plans'"
                    )
                )
                apply_avatar_initialization_player(uow)
                apply_avatar_initialization_player(uow)
                row = uow.query_one(
                    "SELECT user_id, main_id, avatar_id, active_id, create_time FROM avatar"
                )
                self.assertEqual(row["main_id"], "main")
                self.assertEqual(row["avatar_id"], "avatar")
                self.assertEqual(row["active_id"], "avatar")
                self.assertIsNone(row["create_time"])
                self.assertIsNotNone(
                    uow.query_one(
                        "SELECT name FROM sqlite_master WHERE name='avatar_operation_receipts'"
                    )
                )

    def test_migration_adds_active_id_to_older_avatar_rows_without_resetting_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="avatar-migration-old-") as temp_dir:
            database = Path(temp_dir) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE avatar(user_id TEXT PRIMARY KEY, main_id TEXT, avatar_id TEXT)"
                )
                uow.execute(
                    "INSERT INTO avatar(user_id,main_id,avatar_id) VALUES('main','main','avatar')"
                )
            with DatabaseUnitOfWork(database) as uow:
                apply_avatar_identity_player(uow)
                row = uow.query_one(
                    "SELECT user_id, main_id, avatar_id, active_id FROM avatar"
                )
                self.assertEqual(row["main_id"], "main")
                self.assertEqual(row["avatar_id"], "avatar")
                self.assertIsNone(row["active_id"])

    def test_migration_is_player_db_only(self) -> None:
        from ....plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertNotIn("info.avatar.001", game)
        self.assertIn("info.avatar.001", player)
        self.assertNotIn("info.avatar.002", game)
        self.assertIn("info.avatar.002", player)


class AvatarIdentityResolutionTests(unittest.TestCase):
    def test_check_user_keeps_impersonation_above_avatar_identity(self) -> None:
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        from ....xiuxian.xiuxian_utils import utils

        class AvatarApplication:
            resolved: list[str] = []

            def get_active_user_id(self, user_id: str) -> str:
                self.resolved.append(str(user_id))
                return "avatar-id" if user_id == "real-id" else user_id

        queried: list[str] = []

        def get_profile(user_id: str):
            queried.append(str(user_id))
            return {"user_id": str(user_id), "is_ban": 0}

        class SqlMessage:
            def get_user_cd(self, user_id: str):
                queried.append(str(user_id))
                return {"type": 1}

        with patch.object(utils, "_player_avatar", return_value=AvatarApplication()):
            with patch.object(utils, "get_user_profile", side_effect=get_profile):
                with patch.object(utils, "_sql_message", return_value=SqlMessage()):
                    with patch(
                        "nonebot_plugin_xiuxian_2.xiuxian.blackhouse.is_user_blackhoused",
                        return_value=False,
                    ):
                        utils._impersonating_users["real-id"] = "impersonated-id"
                        try:
                            is_user, profile, message = utils.check_user("real-id")
                            state_matches, _ = utils.check_user_type("real-id", 1)
                        finally:
                            utils._impersonating_users.pop("real-id", None)

        self.assertTrue(is_user)
        self.assertEqual(message, "")
        self.assertEqual(queried, ["impersonated-id", "impersonated-id"])
        self.assertEqual(AvatarApplication.resolved, ["real-id", "real-id"])
        self.assertTrue(state_matches)
        self.assertEqual(profile["user_id"], "impersonated-id")

    def test_check_user_routes_to_avatar_when_not_impersonating(self) -> None:
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        from ....xiuxian.xiuxian_utils import utils

        class AvatarApplication:
            def get_active_user_id(self, _user_id: str) -> str:
                return "avatar-id"

        with patch.object(utils, "_player_avatar", return_value=AvatarApplication()):
            with patch.object(
                utils,
                "get_user_profile",
                return_value={"user_id": "avatar-id", "is_ban": 0},
            ):
                with patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.blackhouse.is_user_blackhoused",
                    return_value=False,
                ):
                    is_user, profile, message = utils.check_user("real-id")

        self.assertTrue(is_user)
        self.assertEqual(message, "")
        self.assertEqual(profile["user_id"], "avatar-id")

    def test_runtime_facades_do_not_write_or_read_active_id_through_legacy_manager(self) -> None:
        root = Path(__file__).parents[4]
        avatar_source = (
            root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/avatar.py"
        ).read_text(encoding="utf-8")
        utility_source = (
            root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/utils.py"
        ).read_text(encoding="utf-8")
        base_source = (
            root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/__init__.py"
        ).read_text(encoding="utf-8")

        self.assertIn("_player_avatar_application().toggle_active(", avatar_source)
        self.assertIn("_player_avatar_application().restore_active(", avatar_source)
        self.assertNotIn(
            '_player_data_manager().update_or_write_data(main_id, "avatar", "active_id"',
            avatar_source,
        )
        self.assertIn("_player_avatar().get_active_user_id(", utility_source)
        self.assertIn("user_id = get_active_user_id(real_user_id)", base_source)


if __name__ == "__main__":
    unittest.main()
