from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.player_reasons import REASONS, reason_text
from nonebot_plugin_xiuxian_2.features.bank.application import BankApplication
from nonebot_plugin_xiuxian_2.features.bank.command_replies import render_bank_reply
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.features.sect.creation_repository import SectCreationSqlRepository
from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import failure_reply, result_failure
from nonebot_plugin_xiuxian_2.features.admin_asset.application import AdminAssetApplication
from nonebot_plugin_xiuxian_2.features.admin_asset.migrations import apply_admin_stone_adjustment
from nonebot_plugin_xiuxian_2.features.activity_reward.boss_rank_claim_repository import ActivityBossRankClaimRepository
from nonebot_plugin_xiuxian_2.features.activity_reward.boss_rank_claim_application import ActivityBossRankClaimApplication
from nonebot_plugin_xiuxian_2.features.activity_reward.boss_milestone_claim_repository import ActivityBossMilestoneClaimRepository
from nonebot_plugin_xiuxian_2.features.activity_reward.boss_milestone_claim_application import ActivityBossMilestoneClaimApplication
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_boss
from tests import test_accessory_upgrade_application as _accessory


class AccessoryMaterialReasonTests(unittest.TestCase):
    setUp = _accessory.AccessoryUpgradeApplicationTests.setUp
    tearDown = _accessory.AccessoryUpgradeApplicationTests.tearDown
    state = _accessory.AccessoryUpgradeApplicationTests.state

    def assert_material_reason(self, bag, uids, reason, *, repository=True):
        with sqlite3.connect(self.player) as conn:
            conn.execute("UPDATE player_accessory SET bag=? WHERE user_id='u'", (json.dumps(bag),))
        equipped, bag = self.state()
        result = self.application.upgrade("reason", "u", "戒指", equipped, bag, uids)
        self.assertEqual(result.reason, reason)
        message = result_failure("饰品升品", result)
        self.assertIn(reason_text(reason), message)
        self.assertNotIn("或", message)
        self.assertNotIn("定位号", message)
        if repository:
            stored = self.application.repository.upgrade("reason", "u", "戒指", equipped, bag, uids, self.main)
            self.assertEqual((stored.status, stored.reason), (result.status, result.reason))
        self.assertEqual(self.state(), (equipped, bag))
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM accessory_transaction_operations").fetchone()[0], 0)

    def test_count_duplicate_choice_and_ambiguous_material_are_distinct(self):
        bag = [self.material_one, self.material_two, self.other]
        self.assert_material_reason(bag, ("material-1",), "accessory_material_count")
        self.assert_material_reason(bag, ("material-1", "material-1"), "accessory_material_duplicate_selection", repository=False)
        self.assert_material_reason(bag, ("missing", "material-2"), "accessory_material_missing")
        self.assert_material_reason(bag + [dict(self.material_one)], ("material-1", "material-2"), "accessory_material_uid_duplicate")

    def test_each_material_attribute_reaches_the_reply_separately(self):
        for field, value in (("item_id", 999), ("part", "手镯"), ("set_type", "寒月"), ("quality", 2)):
            with self.subTest(field=field):
                bag = [dict(self.material_one, **{field: value}), self.material_two, self.other]
                self.assert_material_reason(bag, ("material-1", "material-2"), "accessory_material_" + field)


class SectCreationRollbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "game.db"
        with sqlite3.connect(self.database) as conn:
            conn.executescript(
                "CREATE TABLE user_xiuxian(user_id TEXT,sect_id INTEGER,sect_position INTEGER,stone INTEGER);"
                "INSERT INTO user_xiuxian VALUES('u',NULL,NULL,1000);"
                "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,"
                "sect_scale INTEGER,sect_used_stone INTEGER,join_open INTEGER,closed INTEGER,combat_power INTEGER);"
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sect_operations(uow)
        self.repository = SectCreationSqlRepository(self.database)

    def snapshot(self):
        with sqlite3.connect(self.database) as conn:
            return tuple(conn.iterdump())

    def assert_rejected_without_changes(self):
        before = self.snapshot()
        with self.assertRaises(DomainError) as caught:
            self.repository.create("create", "u", "test-sect", 300, 0)
        self.assertEqual(caught.exception.code, "sect_member_write_rejected")
        self.assertIn("成员记录", failure_reply("创建宗门", caught.exception))
        self.assertEqual(self.snapshot(), before)

    def test_duplicate_historical_users_roll_back_balance_membership_sect_and_receipt(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("INSERT INTO user_xiuxian VALUES('u',NULL,NULL,1000)")
        self.assert_rejected_without_changes()

    def test_ignored_membership_update_rolls_back_inserted_sect(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("CREATE TRIGGER skip_member BEFORE UPDATE ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END")
        self.assert_rejected_without_changes()

    def test_receipt_insert_failure_rolls_back_balance_membership_and_sect(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON sect_creation_operations BEGIN SELECT RAISE(ABORT,'fixture'); END")
        before = self.snapshot()
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.create("create", "u", "test-sect", 300, 0)
        self.assertEqual(self.snapshot(), before)


class FinalReplyTests(unittest.TestCase):
    def test_unexplained_fresh_rejection_is_never_labeled_as_legacy(self):
        with patch("nonebot_plugin_xiuxian_2.xiuxian.command_guard.report_exception", return_value="unmapped") as report:
            self.assertEqual(result_failure("操作", {"status": "state_changed"}), "unmapped")
        self.assertIn("state_changed", str(report.call_args.args[1]))
        self.assertNotIn("state_changed", REASONS)

    def test_legacy_evidence_and_specific_reason_have_distinct_replies(self):
        legacy = result_failure("操作", {"status": "state_changed", "reason": "legacy_rejection"})
        current = result_failure("操作", {"status": "state_changed", "reason": "exp_changed", "subject": "target"})
        self.assertIn("旧请求回执", legacy)
        self.assertIn("目标方", current)
        self.assertIn("修为", current)
        self.assertNotIn("邀请", current)

    def test_write_rejections_do_not_expose_implementation_or_guess_causes(self):
        for code, message in REASONS.items():
            if code.endswith("write_rejected"):
                with self.subTest(reason=code):
                    self.assertNotIn("行数", message)
                    self.assertNotIn("SQL", message)
                    self.assertNotIn("或", message)
                    self.assertIn("管理员", message)


class BankReasonChainTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "game.db"
        with sqlite3.connect(self.database) as conn:
            conn.executescript("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER); INSERT INTO user_xiuxian VALUES('u',1000);")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_bank_accounts(uow)
            apply_platform_schema(uow)
            uow.execute("INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES('u',100,'1','before')")
        self.application = BankApplication(self.database)

    def assert_chain(self, method, reason, **changes):
        request = dict(operation_id=method + reason, user_id="u", expected_saved_stone=100, expected_saved_at="before", bank_level="1", interest=0, settled_at="after")
        if method in {"deposit", "withdraw"}:
            request["amount"] = 10
        if method == "deposit":
            request["save_limit"] = 1000
        request.update(changes)
        first = getattr(self.application, method)(**request)
        replay = getattr(self.application, method)(**request)
        self.assertEqual((first.code, first.data["reason"]), ("state_changed", reason))
        self.assertEqual(replay.data, first.data)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.message, reason_text(reason))
        self.assertEqual(render_bank_reply(first.data, {}), reason_text(reason))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone(), (1000,))
            self.assertEqual(conn.execute("SELECT saved_stone,bank_level,updated_at FROM bank_accounts").fetchone(), (100, "1", "before"))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bank_account_operations").fetchone(), (0,))


    def test_deposit_withdraw_interest_preserve_each_snapshot_difference_and_replay(self):
        for method in ("deposit", "withdraw", "settle_interest"):
            for reason, changes in (("bank_saved_changed", {"expected_saved_stone": 101}), ("bank_saved_at_changed", {"expected_saved_at": "stale"}), ("bank_level_changed", {"bank_level": "2"})):
                with self.subTest(method=method, reason=reason):
                    self.assert_chain(method, reason, **changes)

    def test_upgrade_level_rejection_is_a_result_with_reason(self):
        result = self.application.upgrade(operation_id="upgrade", user_id="u", expected_level="2", next_level="3", cost=10)
        self.assertEqual((result.code, result.data["reason"]), ("state_changed", "bank_level_changed"))

    def test_partial_withdrawal_write_rolls_back_wallet_and_receipts(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("CREATE TRIGGER skip_bank BEFORE UPDATE ON bank_accounts BEGIN SELECT RAISE(IGNORE); END")
        with self.assertRaises(DomainError) as caught:
            self.application.withdraw(operation_id="partial", user_id="u", amount=10, expected_saved_stone=100, expected_saved_at="before", bank_level="1", interest=0, settled_at="after")
        self.assertEqual(caught.exception.code, "bank_account_write_rejected")
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone(), (1000,))
            self.assertEqual(conn.execute("SELECT saved_stone FROM bank_accounts").fetchone(), (100,))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bank_account_operations").fetchone(), (0,))


class RemainingBoundaryReasonTests(unittest.TestCase):
    def test_admin_stone_reason_survives_normalization_ledger_replay_and_player_reply(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
                apply_platform_schema(uow)
                apply_admin_stone_adjustment(uow)
            app = AdminAssetApplication(database)
            request = dict(operation_id="admin-stale", operator_id="admin", user_id="u", expected_stone=101, requested_delta=1)
            result = app.adjust_stone(**request)
            replay = app.adjust_stone(**request)
            self.assertEqual(result.data["reason"], "stone_changed")
            self.assertEqual(result.message, reason_text("stone_changed"))
            self.assertEqual(replay.data, result.data)
            self.assertIn("灵石余额", result_failure("管理员调整", replay))

    def test_boss_reward_reasons_survive_feature_receipt_ledger_and_command(self):
        cases = (
            ("rank", ActivityBossRankClaimRepository, ActivityBossRankClaimApplication, "boss_reward_rank_changed", "首领伤害排名"),
            ("rank", ActivityBossRankClaimRepository, ActivityBossRankClaimApplication, "boss_reward_items_changed", "奖励物品"),
            ("milestone", ActivityBossMilestoneClaimRepository, ActivityBossMilestoneClaimApplication, "boss_milestone_claimed", "已领取"),
            ("milestone", ActivityBossMilestoneClaimRepository, ActivityBossMilestoneClaimApplication, "boss_milestone_unlock_changed", "尚未解锁"),
        )
        for kind, repository, application, reason, expected in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                database = Path(directory) / "game.db"
                with DatabaseUnitOfWork(database) as uow:
                    table = "activity_boss_" + kind + "_claim_operations"
                    reservations = "activity_boss_" + kind + "_claim_reservations"
                    uow.execute(f"CREATE TABLE {table}(operation_id TEXT PRIMARY KEY,status TEXT,result_status TEXT,result_json TEXT,updated_at TEXT)")
                    uow.execute(f"CREATE TABLE {reservations}(operation_id TEXT)")
                    uow.execute(f"INSERT INTO {table} VALUES('claim','started','','',NULL)")
                    repository._reject(uow, "claim", reason)
                    decoded = repository._decode(uow.query_one(f"SELECT * FROM {table}"))
                outcome = application._outcome("claim", decoded)
                replay = application._from_outcome(outcome.replay())
                self.assertEqual((decoded.status, replay.status), ("state_changed", "state_changed"))
                self.assertEqual((decoded.reason, replay.reason), (reason, reason))
                fake = type("Application", (), {"resume_pending": lambda *args: replay})()
                with patch.object(activity_boss, "_boss_" + kind + "_claim_application", return_value=fake):
                    claim = getattr(activity_boss, "claim_boss_" + kind + "_reward")
                    ok, message = claim("u", operation_id="claim")
                self.assertFalse(ok)
                self.assertIn(expected, message)
                self.assertNotIn("状态已变化", message)
