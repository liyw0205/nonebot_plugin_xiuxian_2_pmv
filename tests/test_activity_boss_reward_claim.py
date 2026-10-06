import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import BossRewardClaimService
from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_boss_milestone_claim,
    apply_activity_boss_rank_claim,
)
from nonebot_plugin_xiuxian_2.features.activity_reward.boss_milestone_claim_application import ActivityBossMilestoneClaimApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.boss_rank_claim_application import ActivityBossRankClaimApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, ReconcileService
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


class ActivityBossRewardClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        (root / "activity").mkdir()
        self.activity, self.game = root / "activity" / "activity.db", root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_boss_milestone(activity_key TEXT,milestone_key TEXT,unlocked_time TEXT,PRIMARY KEY(activity_key,milestone_key))")
            conn.execute("CREATE TABLE activity_boss_milestone_claim(activity_key TEXT,user_id TEXT,milestone_key TEXT,create_time TEXT,PRIMARY KEY(activity_key,user_id,milestone_key))")
            conn.execute("CREATE TABLE activity_boss_damage(activity_key TEXT,user_id TEXT,total_damage INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id))")
            conn.execute("CREATE TABLE activity_boss_rank_claim(activity_key TEXT,user_id TEXT,tier_key TEXT,create_time TEXT,PRIMARY KEY(activity_key,user_id,tier_key))")
            conn.execute("INSERT INTO activity_boss_milestone VALUES('a','m1','')")
            conn.execute("INSERT INTO activity_boss_damage VALUES('a','u',100,'')")
            conn.execute("INSERT INTO activity_boss_damage VALUES('a','v',50,'')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',0)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_activity_boss_milestone_claim(uow)
            apply_activity_boss_rank_claim(uow)
        self.application = ActivityBossMilestoneClaimApplication(self.game, self.activity)
        self.rank_application = ActivityBossRankClaimApplication(self.game, self.activity)
        self.service = BossRewardClaimService(self.activity, self.game, max_goods_num=100)

    def tearDown(self): self.tmp.cleanup()

    def test_composite_claim_uses_stable_child_operation_ids(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_boss

        with (
            patch.object(activity_boss, "claim_boss_milestone_reward", return_value=(True, "进度奖励")) as milestone,
            patch.object(activity_boss, "claim_boss_rank_reward", return_value=(True, "排行奖励")) as rank,
        ):
            ok, message = activity_boss.claim_boss_rewards(
                "u", operation_id="activity:boss-claim:u:message-1"
            )

        self.assertTrue(ok)
        self.assertEqual("进度奖励\n排行奖励", message)
        milestone.assert_called_once_with("u", "", "activity:boss-claim:u:message-1:milestone")
        rank.assert_called_once_with("u", "", "activity:boss-claim:u:message-1:rank")

    def test_targeted_claim_uses_matching_stable_child_operation_id(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_boss

        with patch.object(activity_boss, "claim_boss_rank_reward", return_value=(True, "排行奖励")) as rank:
            self.assertEqual(
                (True, "排行奖励"),
                activity_boss.claim_boss_rewards(
                    "u", "排行", "activity:boss-claim:u:message-2"
                ),
            )
        rank.assert_called_once_with("u", "", "activity:boss-claim:u:message-2:rank")

    def test_milestone_assets_receipt_and_reservation_commit_together(self):
        rows = [{"key": "m1", "name": "进度奖", "reward": "灵石x50"}]
        self.assertEqual("applied", self.service.claim_milestones("u", "a", rows, "milestone-op").status)
        self.assertEqual("duplicate", self.service.claim_milestones("u", "a", rows, "milestone-op").status)
        self.assertEqual("duplicate", self.service.get_result("milestone-op", "u").status)
        self.assertEqual("operation_conflict", self.service.get_result("milestone-op", "other").status)
        with db_backend.connection(self.game) as conn: self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual("applied", conn.execute(
                "SELECT status FROM activity_boss_milestone_claim_operations WHERE operation_id='milestone-op'"
            ).fetchone()[0])
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM activity_boss_milestone_claim_reservations WHERE operation_id='milestone-op'"
            ).fetchone()[0])

    def test_unlocked_and_inventory_rejections_do_not_grant(self):
        rows = [{"key": "missing", "name": "尚未解锁", "reward": "灵石x20"}]
        self.assertEqual("already_claimed", self.service.claim_milestones("u", "a", rows, "locked").status)
        rows = [{"key": "m1", "name": "进度奖", "reward": "令牌x2"}]
        limited = ActivityBossMilestoneClaimApplication(self.game, self.activity)
        self.assertEqual("inventory_full", limited.claim("u", "a", [
            {"key": "m1", "name": "进度奖", "reward": "令牌x2", "reward_items": [
                {"id": 101, "name": "令牌", "type": "道具", "quantity": 2},
            ]},
        ], 1, "limited").status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(0, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(0, conn.execute(
                "SELECT COUNT(*) FROM activity_boss_milestone_claim_reservations "
                "WHERE operation_id IN ('locked','limited')"
            ).fetchone()[0])

    def test_claims_only_unlocked_milestones_and_later_claims_new_unlocks(self):
        rows = [
            {"key": "m1", "name": "已解锁奖励", "reward": "灵石x20"},
            {"key": "m2", "name": "未解锁奖励", "reward": "灵石x80"},
        ]
        first = self.service.claim_milestones("u", "a", rows)
        self.assertEqual(("applied", ("已解锁奖励",)), (first.status, first.names))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(20, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM activity_boss_milestone_claim_reservations"
            ).fetchone()[0])
        with db_backend.transaction(self.activity) as conn:
            conn.execute("INSERT INTO activity_boss_milestone VALUES('a','m2','')")
        second = self.service.claim_milestones("u", "a", rows)
        self.assertEqual(("applied", ("未解锁奖励",)), (second.status, second.names))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(100, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_fresh_request_can_claim_after_rejection(self):
        rows = [{"key": "m1", "reward": "stone", "reward_items": [
            {"type": "stone", "quantity": 50},
        ]}]
        self.assertEqual("not_unlocked", self.application.claim("u", "new", rows, 100).status)
        with db_backend.transaction(self.activity) as conn:
            conn.execute("INSERT INTO activity_boss_milestone VALUES('new','m1','')")
        self.assertEqual("applied", self.application.claim("u", "new", rows, 100).status)
        self.assertEqual("already_claimed", self.application.claim("u", "new", rows, 100).status)

    def test_empty_configuration_is_a_rejection_not_an_exception(self):
        self.assertEqual("already_claimed", self.application.claim("u", "a", [], 100).status)
        self.assertEqual("not_unlocked", self.application.claim("u", "missing", [], 100).status)

    def test_items_and_stone_roll_back_together_on_asset_write_failure(self):
        rows = [{"key": "m1", "reward": "bundle", "reward_items": [
            {"type": "stone", "quantity": 50},
            {"id": 101, "name": "token", "type": "item", "quantity": 2},
        ]}]
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_item BEFORE INSERT ON back BEGIN SELECT RAISE(ABORT,'item unavailable'); END")
        with self.assertRaisesRegex(Exception, "item unavailable"):
            self.application.claim("u", "a", rows, 100, "asset-retry")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(0, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual("started", conn.execute(
                "SELECT status FROM activity_boss_milestone_claim_operations WHERE operation_id='asset-retry'"
            ).fetchone()[0])
        with db_backend.transaction(self.game) as conn:
            conn.execute("DROP TRIGGER fail_item")
        self.assertTrue(self.application.resume_pending("asset-retry", "u").succeeded)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual((2, 2), tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()))

    def test_prepare_failure_does_not_leave_an_orphan_ledger_record(self):
        with patch.object(self.application.repository, "_select_claimable", side_effect=RuntimeError("unavailable")):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                self.application.claim("u", "a", [{"key": "m1"}], 100, "prepare-failed")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM operation_ledger").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_boss_milestone_claim_operations").fetchone()[0])

    def test_concurrent_same_request_does_not_repeat_assets(self):
        rows = [{"key": "m1", "reward": "stone", "reward_items": [
            {"type": "stone", "quantity": 50},
        ]}]
        def claim(_):
            application = ActivityBossMilestoneClaimApplication(self.game, self.activity)
            return application.claim("u", "a", rows, 100, "concurrent").status

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(claim, range(2)))
        self.assertTrue(all(status in {"applied", "duplicate"} for status in statuses), statuses)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_boss_milestone_claim_reservations").fetchone()[0])

    def test_applied_partial_selection_recovers_a_failed_ledger_finish(self):
        rows = [{"key": "m1", "reward": "stone", "reward_items": [
            {"type": "stone", "quantity": 50},
        ]}, {"key": "m2", "reward": "locked"}]
        with patch.object(self.application.ledger, "finish", side_effect=RuntimeError("ledger unavailable")):
            with self.assertRaisesRegex(RuntimeError, "ledger unavailable"):
                self.application.claim("u", "a", rows, 100, "ledger-retry")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(uow, operation_handlers={self.application.action: self.application.reconcile})
        self.assertTrue(report.clean)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual("applied", conn.execute("SELECT status FROM operation_ledger WHERE operation_id='ledger-retry'").fetchone()[0])

    def test_request_without_startup_schema_does_not_create_it(self):
        database = Path(self.tmp.name) / "empty-game.db"
        with DatabaseUnitOfWork(database):
            pass
        with self.assertRaisesRegex(RuntimeError, "activity_reward.008 schema_missing"):
            ActivityBossMilestoneClaimApplication(database, self.activity).claim("u", "a", [{"key": "m1"}], 100)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertEqual([], uow.query_all("SELECT name FROM sqlite_master WHERE type='table'"))

    def test_projection_failure_reconciles_without_repeating_assets(self):
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TRIGGER fail_milestone_projection BEFORE INSERT ON activity_boss_milestone_claim "
                "BEGIN SELECT RAISE(ABORT,'projection unavailable'); END"
            )
        rows = [{"key": "m1", "name": "进度奖", "reward": "灵石x50"}]
        with self.assertRaises(Exception):
            self.service.claim_milestones("u", "a", rows, "milestone-retry")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual("granted", conn.execute(
                "SELECT status FROM activity_boss_milestone_claim_operations WHERE operation_id='milestone-retry'"
            ).fetchone()[0])
        with db_backend.transaction(self.activity) as conn:
            conn.execute("DROP TRIGGER fail_milestone_projection")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers={self.application.action: self.application.reconcile},
            )
        self.assertTrue(report.clean)
        self.assertEqual("duplicate", self.service.claim_milestones(
            "u", "a", rows, "milestone-retry"
        ).status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_same_operation_recovers_after_projection_acknowledgement_loss(self):
        finalize = self.application.repository._finalize_legacy_state

        def finalize_then_fail(request):
            finalize(request)
            raise RuntimeError("lost projection acknowledgement")

        self.application.repository._finalize_legacy_state = finalize_then_fail
        self.service._milestone_application = self.application
        rows = [{"key": "m1", "name": "进度奖", "reward": "灵石x50"}]
        with self.assertRaisesRegex(RuntimeError, "lost projection acknowledgement"):
            self.service.claim_milestones("u", "a", rows, "milestone-ack-lost")
        self.application.repository._finalize_legacy_state = finalize
        self.assertEqual("duplicate", self.application.resume_pending("milestone-ack-lost", "u").status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_boss_milestone_claim").fetchone()[0])

    def test_cli_reconcile_dispatches_pending_milestone_claim(self):
        from nonebot_plugin_xiuxian_2 import cli

        with patch.object(self.application.repository, "_finalize_legacy_state", side_effect=RuntimeError("unavailable")):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                self.application.claim("u", "a", [{"key": "m1", "reward_items": [
                    {"type": "stone", "quantity": 50},
                ]}], 100, "cli-retry")
        root = Path(self.tmp.name)
        context = SimpleNamespace(
            paths=SimpleNamespace(data=root, players=root / "players"), clock=self.application.clock,
            database=SimpleNamespace(path=lambda key: self.game if key == "game_db" else root / "player.db"),
        )
        with patch.object(cli, "build_runtime_context", return_value=context), redirect_stdout(StringIO()):
            self.assertEqual(0, cli.main(["reconcile", "--apply", "--data-dir", str(root)]))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual("applied", conn.execute("SELECT status FROM operation_ledger WHERE operation_id='cli-retry'").fetchone()[0])

    def test_default_claim_command_resumes_the_stable_child_operation_id(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_boss

        finalize = self.application.repository._finalize_legacy_state

        def finalize_then_lose_acknowledgement(request):
            finalize(request)
            raise RuntimeError("lost projection acknowledgement")

        self.application.repository._finalize_legacy_state = finalize_then_lose_acknowledgement
        with (
            patch.object(activity_boss, "_boss_milestone_claim_application", return_value=self.application),
            patch.object(activity_boss, "_runtime_gate", return_value=(True, "", None)),
            patch.object(activity_boss, "_find_boss_activity", return_value={
                "key": "a",
                "server_milestones": [
                    {"key": "m1", "name": "进度奖", "reward": "灵石x50"},
                    {"key": "m2", "name": "后续奖励", "reward": "灵石x80"},
                ],
            }),
            patch.object(activity_boss, "ensure_activity_files"),
            patch.object(activity_boss, "parse_reward", side_effect=lambda reward: [
                {"type": "stone", "quantity": int(reward.removeprefix("灵石x"))},
            ]),
            patch.object(activity_boss, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
        ):
            with self.assertRaisesRegex(RuntimeError, "lost projection acknowledgement"):
                activity_boss.claim_boss_milestone_reward("u", operation_id="claim-all:boss-milestone-child")

            self.application.repository._finalize_legacy_state = finalize
            ok, message = activity_boss.claim_boss_milestone_reward(
                "u", operation_id="claim-all:boss-milestone-child"
            )

        self.assertTrue(ok)
        self.assertIn("进度奖", message)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rank_claim_and_failure_rollback(self):
        tiers = [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward": "灵石x80"}]
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TRIGGER fail_rank BEFORE INSERT ON activity_boss_rank_claim BEGIN SELECT RAISE(ABORT,'projection unavailable'); END")
        with self.assertRaisesRegex(Exception, "projection unavailable"):
            self.service.claim_rank("u", "a", tiers, "rank-projection")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(80, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual("granted", conn.execute(
                "SELECT status FROM activity_boss_rank_claim_operations WHERE operation_id='rank-projection'"
            ).fetchone()[0])
        with db_backend.transaction(self.activity) as conn:
            conn.execute("DROP TRIGGER fail_rank")
        self.assertEqual("duplicate", self.rank_application.resume_pending("rank-projection", "u").status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_boss_rank_claim WHERE tier_key='1-1'").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(80, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rank_claim_preserves_tier_reclaim_after_rank_changes(self):
        tiers = [
            {"rank_min": 1, "rank_max": 1, "name": "第一名", "reward": "灵石x80"},
            {"rank_min": 2, "rank_max": 2, "name": "第二名", "reward": "灵石x40"},
        ]
        first = self.service.claim_rank("u", "a", tiers, "rank-first")
        self.assertEqual(("applied", 1, ("第一名",)), (first.status, first.rank, first.names))
        with db_backend.transaction(self.activity) as conn:
            conn.execute("UPDATE activity_boss_damage SET total_damage=40 WHERE activity_key='a' AND user_id='u'")
        second = self.service.claim_rank("u", "a", tiers, "rank-second")
        self.assertEqual(("applied", 2, ("第二名",)), (second.status, second.rank, second.names))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(120, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(["1-1", "2-2"], [row[0] for row in conn.execute(
                "SELECT tier_key FROM activity_boss_rank_claim ORDER BY tier_key"
            )])

    def test_rank_snapshots_reject_rank_changes_before_asset_grant(self):
        tiers = [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward": "灵石x80"}]
        claim = self.rank_application.repository.claim

        def change_rank_then_claim(*args, **kwargs):
            with db_backend.transaction(self.activity) as conn:
                conn.execute("UPDATE activity_boss_damage SET total_damage=10 WHERE activity_key='a' AND user_id='u'")
            return claim(*args, **kwargs)

        with patch.object(self.rank_application.repository, "claim", side_effect=change_rank_then_claim):
            result = self.rank_application.claim("u", "a", tiers, 100, "rank-state-changed")
        self.assertEqual("state_changed", result.status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(0, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rank_inventory_and_asset_failures_do_not_partially_grant(self):
        tier = [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward": "令牌x2"}]
        limited = ActivityBossRankClaimApplication(self.game, self.activity)
        self.assertEqual("inventory_full", limited.claim("u", "a", [
            {**tier[0], "reward_items": [{"id": 101, "name": "令牌", "type": "道具", "quantity": 2}]},
        ], 1, "rank-limited").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_rank_item BEFORE INSERT ON back BEGIN SELECT RAISE(ABORT,'item unavailable'); END")
        with self.assertRaisesRegex(Exception, "item unavailable"):
            limited.claim("u", "a", [
                {**tier[0], "reward_items": [
                    {"type": "stone", "quantity": 50},
                    {"id": 101, "name": "令牌", "type": "道具", "quantity": 2},
                ]},
            ], 100, "rank-asset-failure")
        with db_backend.connection(self.game) as conn:
            self.assertEqual(0, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual("started", conn.execute(
                "SELECT status FROM activity_boss_rank_claim_operations WHERE operation_id='rank-asset-failure'"
            ).fetchone()[0])
        with db_backend.transaction(self.game) as conn:
            conn.execute("DROP TRIGGER fail_rank_item")
        self.assertTrue(self.rank_application.resume_pending("rank-asset-failure", "u").succeeded)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])

    def test_default_rank_command_resumes_stable_child_id(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_boss

        finalize = self.rank_application.repository._finalize_legacy_state

        def finalize_then_lose_acknowledgement(request, uow=None):
            finalize(request, uow)
            raise RuntimeError("lost projection acknowledgement")

        self.rank_application.repository._finalize_legacy_state = finalize_then_lose_acknowledgement
        with (
            patch.object(activity_boss, "_boss_rank_claim_application", return_value=self.rank_application),
            patch.object(activity_boss, "_runtime_gate", return_value=(True, "", None)),
            patch.object(activity_boss, "_find_boss_activity", return_value={
                "key": "a", "rank_rewards": [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward": "灵石x80"}],
            }),
            patch.object(activity_boss, "ensure_activity_files"),
            patch.object(activity_boss, "parse_reward", return_value=[{"type": "stone", "quantity": 80}]),
            patch.object(activity_boss, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
            patch.object(activity_boss, "resolve_daohao", return_value="道号"),
        ):
            with self.assertRaisesRegex(RuntimeError, "lost projection acknowledgement"):
                activity_boss.claim_boss_rank_reward("u", operation_id="claim-all:boss-rank-child")
            self.rank_application.repository._finalize_legacy_state = finalize
            ok, message = activity_boss.claim_boss_rank_reward("u", operation_id="claim-all:boss-rank-child")
        self.assertTrue(ok)
        self.assertIn("第一名", message)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(80, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rank_result_recovers_from_cli_reconcile(self):
        from nonebot_plugin_xiuxian_2 import cli

        finalize = self.rank_application.repository._finalize_legacy_state
        def finalize_then_fail(request, uow=None):
            finalize(request, uow)
            raise RuntimeError("projection unavailable")
        self.rank_application.repository._finalize_legacy_state = finalize_then_fail
        with self.assertRaisesRegex(RuntimeError, "projection unavailable"):
            self.rank_application.claim("u", "a", [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward_items": [
                {"type": "stone", "quantity": 80},
            ]}], 100, "rank-cli-retry")
        self.rank_application.repository._finalize_legacy_state = finalize
        root = Path(self.tmp.name)
        context = SimpleNamespace(
            paths=SimpleNamespace(data=root, players=root / "players"), clock=self.rank_application.clock,
            database=SimpleNamespace(path=lambda key: self.game if key == "game_db" else root / "player.db"),
        )
        with patch.object(cli, "build_runtime_context", return_value=context), redirect_stdout(StringIO()):
            self.assertEqual(0, cli.main(["reconcile", "--apply", "--data-dir", str(root)]))
        with db_backend.connection(self.game) as conn:
            self.assertEqual("applied", conn.execute("SELECT status FROM operation_ledger WHERE operation_id='rank-cli-retry'").fetchone()[0])
            self.assertEqual(80, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rank_result_recovers_failed_ledger_finish_via_cli_reconcile(self):
        from nonebot_plugin_xiuxian_2 import cli

        tiers = [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward_items": [
            {"type": "stone", "quantity": 80},
        ]}]
        with patch.object(self.rank_application.ledger, "finish", side_effect=RuntimeError("ledger unavailable")):
            with self.assertRaisesRegex(RuntimeError, "ledger unavailable"):
                self.rank_application.claim("u", "a", tiers, 100, "rank-ledger-retry")
        root = Path(self.tmp.name)
        context = SimpleNamespace(
            paths=SimpleNamespace(data=root, players=root / "players"), clock=self.rank_application.clock,
            database=SimpleNamespace(path=lambda key: self.game if key == "game_db" else root / "player.db"),
        )
        with patch.object(cli, "build_runtime_context", return_value=context), redirect_stdout(StringIO()):
            self.assertEqual(0, cli.main(["reconcile", "--apply", "--data-dir", str(root)]))
        with db_backend.connection(self.game) as conn:
            self.assertEqual("applied", conn.execute(
                "SELECT status FROM operation_ledger WHERE operation_id='rank-ledger-retry'"
            ).fetchone()[0])
            self.assertEqual(80, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0])


if __name__ == "__main__": unittest.main()
