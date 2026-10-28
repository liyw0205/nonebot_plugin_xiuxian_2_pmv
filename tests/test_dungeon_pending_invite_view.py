from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.dungeon.team_application import (
    DungeonTeamApplication,
)
from nonebot_plugin_xiuxian_2.features.dungeon.team_presentation import (
    TeamPendingInviteView,
    render_pending_invite,
)
from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import (
    DungeonTeamRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class DungeonPendingInviteViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE dungeon_team_invites("
                "invite_id TEXT PRIMARY KEY,team_id TEXT NOT NULL,"
                "inviter_id TEXT NOT NULL,invitee_id TEXT NOT NULL,"
                "group_id TEXT NOT NULL,created_at REAL NOT NULL,"
                "expires_at REAL NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
                "consumed_at TIMESTAMP DEFAULT NULL)"
            )
        self.repository = DungeonTeamRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def insert(
        self,
        *,
        invite_id: str = "invite-1",
        invitee: str = "member",
        expires_at: float = 200.0,
        status: str = "pending",
        consumed_at: str | None = None,
    ) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO dungeon_team_invites("
                "invite_id,team_id,inviter_id,invitee_id,group_id,created_at,expires_at,status,consumed_at"
                ") VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    invite_id,
                    "team-1",
                    "leader",
                    invitee,
                    "group-1",
                    100.0,
                    expires_at,
                    status,
                    consumed_at,
                ),
            )

    def test_pending_invite_returns_only_live_invite_for_user(self) -> None:
        self.insert()

        invite = self.repository.pending_invite("member", 150.0)

        self.assertIsNotNone(invite)
        assert invite is not None
        self.assertEqual(
            (invite.invite_id, invite.team_id, invite.inviter_id, invite.group_id),
            ("invite-1", "team-1", "leader", "group-1"),
        )

    def test_expired_joined_consumed_and_other_user_are_hidden(self) -> None:
        self.insert(invite_id="expired", expires_at=150.0)
        self.insert(invite_id="joined", status="joined")
        self.insert(invite_id="consumed", consumed_at="2026-10-29 00:00:00")
        self.insert(invite_id="other", invitee="someone-else")

        self.assertIsNone(self.repository.pending_invite("member", 200.0))

    def test_missing_table_and_missing_database_return_none_without_ddl(self) -> None:
        missing_table = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(missing_table, immediate=True) as uow:
            uow.execute("CREATE TABLE filler (id INTEGER)")
        missing_file = Path(self.temp.name) / "missing.db"

        self.assertIsNone(DungeonTeamRepository(missing_table).pending_invite("member", 100.0))
        self.assertIsNone(DungeonTeamRepository(missing_file).pending_invite("member", 100.0))
        self.assertFalse(missing_file.exists())
        with DatabaseUnitOfWork(missing_table) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='dungeon_team_invites'"
                )
            )

    def test_application_delegates_pending_invite(self) -> None:
        calls = []

        class FakeRepository:
            def pending_invite(self, user_id, now_timestamp):
                calls.append((user_id, now_timestamp))
                return "sentinel"

        application = DungeonTeamApplication(self.database, repository=FakeRepository())

        self.assertEqual("sentinel", application.pending_invite("member", 123.0))
        self.assertEqual([("member", 123.0)], calls)

    def test_renderer_shows_team_inviter_group_and_countdown(self) -> None:
        view = render_pending_invite(
            team_name="青云队",
            inviter_name="张三",
            group_id="123456",
            remaining_seconds=61,
        )

        self.assertIsInstance(view, TeamPendingInviteView)
        self.assertIn("【待处理组队邀请】", view.message)
        self.assertIn("队伍：青云队", view.message)
        self.assertIn("邀请人：张三", view.message)
        self.assertIn("发起群：123456", view.message)
        self.assertIn("剩余：2分钟", view.message)
        self.assertIn("同意组队", view.message)
        self.assertIn("拒绝组队", view.message)

    def test_renderer_falls_back_for_blank_names_and_expiry(self) -> None:
        blank = render_pending_invite(
            team_name="", inviter_name="", group_id="", remaining_seconds=1
        )
        expired = render_pending_invite(
            team_name="team", inviter_name="leader", group_id="group", remaining_seconds=0
        )

        self.assertIn("队伍：未知队伍", blank.message)
        self.assertIn("邀请人：未知用户", blank.message)
        self.assertIn("发起群：未知群", blank.message)
        self.assertIn("已过期", expired.message)
        self.assertNotIn("待处理组队邀请", expired.message)

    def test_view_team_handler_wires_pending_invite_and_keeps_fallback(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_dungeon"
            / "__init__.py"
        )
        source = source_path.read_text(encoding="utf-8")
        start = source.index("async def view_team_handler")
        handler = source[start : source.index("async def transfer_team_handler", start)]
        helper_start = source.index("def _team_invite_or_no_team_view")
        helper = source[helper_start:]

        self.assertIn("_team_invite_or_no_team_view(user_id)", handler)
        self.assertIn("pending_invite(user_id", helper)
        self.assertIn("render_pending_invite(", helper)
        self.assertIn("你不在任何队伍中", helper)
        self.assertIn('"同意组队"', helper)
        self.assertIn('"拒绝组队"', helper)


if __name__ == "__main__":
    unittest.main()
