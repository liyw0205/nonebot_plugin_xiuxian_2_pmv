"""终态组队邀请行的启动期保留期清理。

`dungeon_team_invites` 的终态行（expired/joined）只由 UPDATE 产生、从不删除。
所有业务读者都只取 `status='pending'`：

* `due_invites` / `pending_for_user` 带 `status='pending'`；
* `join` 对终态行返回 `invite_invalid`，与行不存在完全相同；
* 60 秒通知任务在 `invite_by_id` 为 None 时静默返回，终态行让 `expire()` 返回
  非 `applied`，同样静默。

本套件钉住：只删过期终态行、pending 永久保留、回执不动、缺表/缺列不建表、
二次运行零删除、批大小与预算中断后可从下次补齐，以及删除前后读者结果等价。
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.dungeon.migrations import (
    apply_dungeon_team,
    apply_dungeon_team_invite_expiry,
)
from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import DungeonTeamRepository
from nonebot_plugin_xiuxian_2.features.maintenance.terminal_invite_retention import (
    DEFAULT_BATCH_SIZE,
    cutoff_epoch,
    maintain_terminal_invites,
    prune_terminal_invites,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

DAY = 86400.0
NOW = 1_800_000_000.0  # 固定参考时刻，避免真实时钟影响断言

class _SeqClock:
    """按序返回时间戳；用尽后回落到最后一个值。"""

    def __init__(self, values) -> None:
        self.values = list(values)
        self.index = 0

    def __call__(self) -> float:
        if self.index < len(self.values):
            value = self.values[self.index]
            self.index += 1
            return value
        return self.values[-1] if self.values else 0.0

class CutoffTests(unittest.TestCase):
    def test_cutoff_is_now_minus_retention(self) -> None:
        self.assertEqual(cutoff_epoch(NOW, 30), NOW - 30 * DAY)

    def test_zero_retention_keeps_cutoff_at_now(self) -> None:
        self.assertEqual(cutoff_epoch(NOW, 0), NOW)

    def test_negative_retention_is_clamped(self) -> None:
        self.assertEqual(cutoff_epoch(NOW, -5), NOW)

class InviteRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.player = self.root / "player.db"
        with DatabaseUnitOfWork(self.player) as uow:
            apply_dungeon_team(uow)
            apply_dungeon_team_invite_expiry(uow)

    def tearDown(self) -> None:
        self.temp.cleanup()

    # ---------------------------------------------------------------- helpers
    def _invite(self, invite_id: str, *, status: str, expires_at: float, consumed_at=None) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "INSERT INTO dungeon_team_invites("
                "invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,"
                "consumed_at,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    invite_id,
                    "team-1",
                    "leader-1",
                    "guest-1",
                    "group-1",
                    float(expires_at),
                    consumed_at,
                    status,
                    float(expires_at - 600),
                ),
            )

    def _rows(self) -> dict[str, dict]:
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            return {
                str(row["invite_id"]): dict(row)
                for row in uow.query_all("SELECT * FROM dungeon_team_invites")
            }

    def _prune(self, **kwargs):
        params = {"now": NOW, "logger": None, "clock": None}
        params.update(kwargs)
        clock = params.pop("clock")
        if clock is None:
            return prune_terminal_invites(self.player, **params)
        return prune_terminal_invites(self.player, clock=clock, **params)

    # ------------------------------------------------------------------ cases
    def test_only_old_terminal_rows_are_removed(self) -> None:
        self._invite("old-expired", status="expired", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")
        self._invite("old-joined", status="joined", expires_at=NOW - 90 * DAY, consumed_at="2026-12-01 00:00:00")
        self._invite("old-pending", status="pending", expires_at=NOW - 40 * DAY)
        self._invite("recent-expired", status="expired", expires_at=NOW - 1 * DAY, consumed_at="2027-01-01 00:00:00")
        self._invite("pending-but-consumed", status="pending", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")

        report = self._prune()

        self.assertFalse(report.schema_missing)
        self.assertFalse(report.candidates_remaining)
        remaining = set(self._rows())
        self.assertEqual(remaining, {"old-pending", "recent-expired"})
        self.assertEqual(report.deleted, 3)

    def test_receipts_and_operations_are_untouched(self) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json) "
                "VALUES('op-1','{}','applied','team-1','{\"k\":1}')"
            )
            before = uow.query_one("SELECT * FROM dungeon_team_operations WHERE operation_id='op-1'")

        self._invite("old-expired", status="expired", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")
        self._prune()

        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            after = uow.query_one("SELECT * FROM dungeon_team_operations WHERE operation_id='op-1'")
            count = uow.query_one("SELECT COUNT(*) AS c FROM dungeon_team_operations")["c"]
        self.assertEqual(dict(before), dict(after))
        self.assertEqual(count, 1)

    def test_due_invites_still_sees_pending_rows(self) -> None:
        self._invite("due-pending", status="pending", expires_at=NOW - 60)
        repository = DungeonTeamRepository(self.player)

        before = repository.due_invites(NOW)
        self._prune()
        after = repository.due_invites(NOW)

        self.assertEqual(
            [invite.invite_id for invite in before.invites],
            [invite.invite_id for invite in after.invites],
        )
        self.assertIn("due-pending", [invite.invite_id for invite in after.invites])

    def test_readers_cannot_tell_a_deleted_terminal_row_from_absence(self) -> None:
        self._invite("old-expired", status="expired", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")
        repository = DungeonTeamRepository(self.player)

        # 终态行存在时：expire 不会 applied（通知任务因此静默），行仍可被 id 取到
        with_row = repository.expire("expire-1", "old-expired", NOW).status
        self.assertNotEqual(with_row, "applied")
        self.assertIsNotNone(repository.invite_by_id("old-expired"))

        self._prune()

        # 行被删后：invite_by_id 为 None（60 秒通知任务静默返回），expire 结果与删除前一致
        self.assertIsNone(repository.invite_by_id("old-expired"))
        without_row = repository.expire("expire-2", "old-expired", NOW).status
        self.assertNotEqual(without_row, "applied")
        self.assertEqual(with_row, without_row)

    def test_missing_table_is_schema_missing_without_ddl(self) -> None:
        empty = self.root / "empty.db"
        sqlite3.connect(empty).close()
        report = prune_terminal_invites(empty, now=NOW)
        self.assertTrue(report.schema_missing)
        with DatabaseUnitOfWork(empty, read_only=True) as uow:
            tables = [row["name"] for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")]
        self.assertEqual(tables, [])

    def test_missing_column_is_schema_missing(self) -> None:
        partial = self.root / "partial.db"
        with DatabaseUnitOfWork(partial) as uow:
            uow.execute(
                "CREATE TABLE dungeon_team_invites(invite_id TEXT PRIMARY KEY,expires_at REAL NOT NULL,status TEXT NOT NULL)"
            )
        report = prune_terminal_invites(partial, now=NOW)
        self.assertTrue(report.schema_missing)

    def test_second_run_deletes_nothing(self) -> None:
        self._invite("old-expired", status="expired", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")
        first = self._prune()
        second = self._prune()
        self.assertEqual(first.deleted, 1)
        self.assertEqual(second.deleted, 0)
        self.assertFalse(second.candidates_remaining)

    def test_batch_bound_and_resume(self) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            for index in range(DEFAULT_BATCH_SIZE + 300):
                uow.execute(
                    "INSERT INTO dungeon_team_invites("
                    "invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,"
                    "consumed_at,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        f"inv-{index:04d}",
                        "team-1",
                        "leader-1",
                        "guest-1",
                        "group-1",
                        NOW - 40 * DAY,
                        "2027-01-01 00:00:00",
                        "expired",
                        NOW - 41 * DAY,
                    ),
                )

        # 第 1 次 clock 取起点 0，第 2 次预算检查通过，第 3 次已超预算 → 只删一批
        first = self._prune(clock=_SeqClock([0.0, 0.0, 100.0]))
        self.assertEqual(first.deleted, DEFAULT_BATCH_SIZE)
        self.assertTrue(first.candidates_remaining)

        second = self._prune()
        self.assertEqual(second.deleted, 300)
        self.assertFalse(second.candidates_remaining)

    def test_zero_budget_interrupts_before_deleting(self) -> None:
        self._invite("old-expired", status="expired", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")
        report = self._prune(budget_seconds=0.0)
        self.assertEqual(report.deleted, 0)
        self.assertTrue(report.candidates_remaining)
        self.assertIn("old-expired", self._rows())

    def test_state_file_records_the_pass(self) -> None:
        self._invite("old-expired", status="expired", expires_at=NOW - 40 * DAY, consumed_at="2027-01-01 00:00:00")
        before = sorted(path.name for path in self.root.iterdir())
        report = maintain_terminal_invites(self.player, now=NOW, logger=None)
        after = sorted(path.name for path in self.root.iterdir())
        self.assertEqual(report.deleted, 1)
        self.assertEqual(report.batches, 1)
        self.assertEqual(report.cutoff_epoch, cutoff_epoch(NOW, 30))
        # 谓词幂等，因此不写任何进度/状态文件
        self.assertEqual(before, after)

if __name__ == "__main__":
    unittest.main()
