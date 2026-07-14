import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner_cultivation_service import PartnerCultivationService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner_invite_service import PartnerInviteService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner_protection_service import PartnerProtectionService
from tests.test_db_backend import db_backend


class PartnerInviteSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,level_up_rate INTEGER)")
            conn.executemany("INSERT INTO user_xiuxian VALUES(%s,%s,0,1,1,1,0)", [("a", 1000), ("b", 2000)])
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,affection INTEGER)")
            conn.executemany("INSERT INTO partner VALUES(%s,NULL,0)", [("a",), ("b",)])
            conn.execute("CREATE TABLE partner_two_exp_usage(user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL)")
            conn.executemany("INSERT INTO partner_two_exp_usage VALUES(%s,0)", [("a",), ("b",)])
            PartnerProtectionService.ensure_schema(conn)
            PartnerInviteService.ensure_schema(conn)
        self.invites = PartnerInviteService(self.player)
        self.settlement = PartnerCultivationService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def settle(self, operation="op"):
        return self.settlement.apply(
            operation, "a", "b", expected_exp_1=1000, expected_exp_2=2000,
            exp_1=100, exp_2=200, used_count=2, power_1=1, power_2=2,
            hp_1=3, mp_1=4, atk_1=5, hp_2=6, mp_2=7, atk_2=8,
            invite_id="i1", expected_used_count_1=0, expected_used_count_2=0,
        )

    def test_accept_consumes_invite_rewards_and_usage_together(self):
        self.invites.create("i1", "a", "b", 2, now=100, ttl_seconds=9999999999)
        self.assertEqual("applied", self.settle().status)
        with db_backend.connection(self.player) as conn:
            self.assertEqual("accepted", conn.execute("SELECT status FROM partner_cultivation_invites WHERE invite_id='i1'").fetchone()[0])
            self.assertEqual([2, 2], [row[0] for row in conn.execute("SELECT used_count FROM partner_two_exp_usage ORDER BY user_id")])
        with db_backend.connection(self.game) as conn:
            self.assertEqual([1100, 2200], [row[0] for row in conn.execute("SELECT exp FROM user_xiuxian ORDER BY user_id")])

    def test_reject_expire_and_failed_settlement_keep_rewards_unchanged(self):
        self.invites.create("i1", "a", "b", 2, now=100, ttl_seconds=60)
        self.assertEqual("applied", self.invites.resolve("i1", "b", "rejected", now=120).status)
        self.assertEqual("invitation_changed", self.settle().status)
        self.invites.create("i2", "a", "b", 1, now=200, ttl_seconds=10)
        self.assertIsNone(self.invites.pending_for_target("b", now=211))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(1000, conn.execute("SELECT exp FROM user_xiuxian WHERE user_id='a'").fetchone()[0])

    def test_protected_invite_requires_the_on_snapshot(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO status(user_id,two_exp_protect) VALUES(%s,%s)",
                ("b", "on"),
            )

        result = self.invites.create(
            "i1", "a", "b", 2, now=100,
            expected_target_protection="on",
        )

        self.assertEqual("applied", result.status)
        self.assertEqual("pending", result.invite.status)

    def test_changed_protection_does_not_leave_an_invite(self):
        for status in ("off", "refusal"):
            with self.subTest(status=status):
                with db_backend.transaction(self.player) as conn:
                    conn.execute("DELETE FROM partner_cultivation_invites")
                    conn.execute("DELETE FROM status WHERE user_id='b'")
                    conn.execute(
                        "INSERT INTO status(user_id,two_exp_protect) VALUES(%s,%s)",
                        ("b", status),
                    )

                result = self.invites.create(
                    f"invite-{status}", "a", "b", 2, now=100,
                    expected_target_protection="on",
                )

                self.assertEqual("protection_changed", result.status)
                with db_backend.connection(self.player) as conn:
                    self.assertEqual(0, conn.execute(
                        "SELECT COUNT(*) FROM partner_cultivation_invites"
                    ).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
