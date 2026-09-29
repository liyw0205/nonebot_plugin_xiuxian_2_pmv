import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import apply_activity_pass_claim
from nonebot_plugin_xiuxian_2.features.activity_reward.pass_claim_application import ActivityPassClaimApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, ReconcileService
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import ActivityPassClaimService
from tests.test_db_backend import db_backend


class ActivityPassClaimTransactionTests(unittest.TestCase):
    def test_activity_service_defers_pass_claim_application_construction(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service

        self.assertIsNone(service._activity_pass_claim_application_instance)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.activity = root / "activity.db"
        self.game = root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TABLE activity_pass_balance(activity_key TEXT,user_id TEXT,exp INTEGER,"
                "total_exp INTEGER,level INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id))"
            )
            conn.execute(
                "CREATE TABLE activity_pass_reward_claim(activity_key TEXT,user_id TEXT,level INTEGER,"
                "create_time TEXT,PRIMARY KEY(activity_key,user_id,level))"
            )
            conn.execute("INSERT INTO activity_pass_balance VALUES('festival','u',20,220,2,'')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_activity_pass_claim(uow)
        self.application = ActivityPassClaimApplication(self.game, self.activity)
        self.service = ActivityPassClaimService(self.activity, self.game)
        self.rewards = (
            {
                "level": 1,
                "name": "初入庆典",
                "reward": "灵石x50",
                "reward_items": ({"type": "stone", "quantity": 50},),
            },
            {
                "level": 2,
                "name": "勤修补给",
                "reward": "战令牌x2",
                "reward_items": (
                    {"id": 101, "name": "战令牌", "type": "道具", "quantity": 2},
                ),
            },
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_claims_all_levels_and_replays_idempotently(self):
        result = self.service.claim("op", "u", "festival", 2, self.rewards, 100)
        self.assertEqual("applied", result.status)
        replay = self.service.claim("op", "u", "festival", 2, self.rewards, 100)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(result.rewards, replay.rewards)
        self.assertEqual(result.rewards, self.service.get_result("op", "u").rewards)
        self.assertEqual("operation_conflict", self.service.get_result("op", "other").status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(
                [1, 2],
                [row[0] for row in conn.execute("SELECT level FROM activity_pass_reward_claim ORDER BY level")],
            )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM activity_pass_reward_claim_operations WHERE operation_id='op' AND status='applied'"
            ).fetchone()[0])
            self.assertEqual(2, conn.execute(
                "SELECT COUNT(*) FROM activity_pass_reward_claim_reservations WHERE operation_id='op'"
            ).fetchone()[0])

    def test_operation_conflict_does_not_mutate_state(self):
        self.service.claim("op", "u", "festival", 2, self.rewards, 100)
        changed = (dict(self.rewards[0]),)
        changed[0]["name"] = "不同奖励"
        self.assertEqual(
            "operation_conflict",
            self.service.claim("op", "u", "festival", 2, changed, 100).status,
        )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rechecks_level_and_claimed_state(self):
        self.assertEqual(
            "state_changed",
            self.service.claim("stale-level", "u", "festival", 1, self.rewards[:1], 100).status,
        )
        with db_backend.transaction(self.activity) as conn:
            conn.execute("INSERT INTO activity_pass_reward_claim VALUES('festival','u',1,'')")
        self.assertEqual(
            "state_changed",
            self.service.claim("claimed", "u", "festival", 2, self.rewards, 100).status,
        )

    def test_inventory_limit_rejects_without_granting_or_claiming(self):
        self.assertEqual(
            "inventory_full",
            self.service.claim("full", "u", "festival", 2, self.rewards, 1).status,
        )
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_pass_reward_claim").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])
            self.assertEqual(0, conn.execute(
                "SELECT COUNT(*) FROM activity_pass_reward_claim_reservations WHERE operation_id='full'"
            ).fetchone()[0])

    def test_projection_failure_reconciles_without_repeating_assets(self):
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TRIGGER fail_pass_projection BEFORE INSERT ON activity_pass_reward_claim "
                "BEGIN SELECT RAISE(ABORT,'projection unavailable'); END"
            )
        with self.assertRaises(Exception):
            self.service.claim("projection-retry", "u", "festival", 2, self.rewards, 100)
        with db_backend.connection(self.game) as conn:
            self.assertEqual("granted", conn.execute(
                "SELECT status FROM activity_pass_reward_claim_operations WHERE operation_id='projection-retry'"
            ).fetchone()[0])
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])
        with db_backend.transaction(self.activity) as conn:
            conn.execute("DROP TRIGGER fail_pass_projection")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers={self.application.action: self.application.reconcile},
            )
        self.assertTrue(report.clean)
        self.assertEqual("duplicate", self.service.claim(
            "projection-retry", "u", "festival", 2, self.rewards, 100
        ).status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])

    def test_retry_resumes_even_when_projection_already_committed(self):
        finalize = self.application.repository._finalize_legacy_state

        def finalize_then_lose_acknowledgement(request):
            finalize(request)
            raise RuntimeError("lost projection acknowledgement")

        self.application.repository._finalize_legacy_state = finalize_then_lose_acknowledgement
        self.service._application = self.application
        with self.assertRaisesRegex(RuntimeError, "lost projection acknowledgement"):
            self.service.claim("ack-lost", "u", "festival", 2, self.rewards, 100)
        self.application.repository._finalize_legacy_state = finalize

        resumed = self.service.resume_pending("ack-lost", "u")
        self.assertEqual("duplicate", resumed.status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM activity_pass_reward_claim").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])
            self.assertEqual("applied", conn.execute(
                "SELECT status FROM operation_ledger WHERE operation_id='ack-lost' AND action=?",
                (self.application.action,),
            ).fetchone()[0])

    def test_default_claim_command_resumes_the_stable_child_operation_id(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service as activity_service

        finalize = self.application.repository._finalize_legacy_state

        def finalize_then_lose_acknowledgement(request):
            finalize(request)
            raise RuntimeError("lost projection acknowledgement")

        self.application.repository._finalize_legacy_state = finalize_then_lose_acknowledgement
        with (
            patch.object(activity_service, "_activity_pass_claim_application", return_value=self.application),
            patch.object(activity_service, "load_config", return_value={}),
            patch.object(activity_service, "activity_runtime_state", return_value={"ok": True, "features": ["claim"]}),
            patch.object(activity_service, "_activity_pass_config", return_value={
                "enabled": True,
                "level_rewards": [
                    {"level": 1, "name": "初入庆典", "reward": "stone"},
                    {"level": 2, "name": "勤修补给", "reward": "token"},
                ],
            }),
            patch.object(activity_service, "_activity_config_key", return_value="festival"),
            patch.object(activity_service, "_get_pass_balance", return_value={"level": 2}),
            patch.object(activity_service, "ensure_activity_files"),
            patch.object(activity_service, "DB_PATH", str(self.activity)),
            patch.object(activity_service, "parse_reward", side_effect=[
                ({"type": "stone", "quantity": 50},),
                ({"id": 101, "name": "战令牌", "type": "道具", "quantity": 2},),
            ]),
            patch.object(activity_service, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
        ):
            with self.assertRaisesRegex(RuntimeError, "lost projection acknowledgement"):
                activity_service.claim_activity_pass_rewards("u", operation_id="claim-all:pass-child")

        self.application.repository._finalize_legacy_state = finalize
        with patch.object(activity_service, "_activity_pass_claim_application", return_value=self.application):
            ok, message = activity_service.claim_activity_pass_rewards(
                "u", operation_id="claim-all:pass-child"
            )
        self.assertTrue(ok)
        self.assertIn("Lv.1", message)
        self.assertIn("Lv.2", message)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])

    def test_request_rejects_when_startup_schema_is_missing_without_creating_it(self):
        missing_game = Path(self.tmp.name) / "missing-game.db"
        with db_backend.transaction(missing_game):
            pass
        with self.assertRaisesRegex(RuntimeError, "activity_reward.006 schema_missing"):
            ActivityPassClaimApplication(missing_game, self.activity).claim(
                "missing-schema", "u", "festival", 2, self.rewards, 100
            )
        with db_backend.connection(missing_game) as conn:
            self.assertEqual(0, conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'activity_pass_reward_claim_%'"
            ).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
