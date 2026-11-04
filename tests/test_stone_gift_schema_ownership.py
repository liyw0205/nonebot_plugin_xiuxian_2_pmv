"""灵石赠送请求路径的 DDL 收口与降级合同。

`stone_gift_operations` / `stone_gift_limits` 由 `stone_gift.001` / `stone_gift.002`
创建；`StoneGiftRepository` 不再在 `limits()` / `transfer()` 里重复建表。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.stone_gift import migrations as gift_migrations
from nonebot_plugin_xiuxian_2.features.stone_gift.application import StoneGiftApplication
from nonebot_plugin_xiuxian_2.features.stone_gift.domain import StoneGiftRecord
from nonebot_plugin_xiuxian_2.features.stone_gift.repository import StoneGiftRepository
from nonebot_plugin_xiuxian_2.features.stone_gift.schema import (
    LIMITS_COLUMNS,
    OPERATIONS_COLUMNS,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FEATURE = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "stone_gift"


class GiftFixture:
    def __init__(self, root: Path) -> None:
        self.database = root / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('sender', 1000)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('recipient', 100)")

    def migrate(self, *, limits: bool = True) -> "GiftFixture":
        with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
            apply_platform_schema(uow)
            gift_migrations.apply_stone_gift(uow)
            if limits:
                gift_migrations.apply_stone_gift_limits(uow)
        return self

    def balances(self) -> tuple[int, int]:
        with db_backend.connection(self.database) as conn:
            rows = dict(conn.execute("SELECT user_id, stone FROM user_xiuxian").fetchall())
        return int(rows["sender"]), int(rows["recipient"])

    def tables(self) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }

    def columns(self, table: str) -> set[str]:
        with db_backend.connection(self.database) as conn:
            return {
                str(row[1])
                for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
            }


RECORD = StoneGiftRecord(
    operation_id="gift-direct",
    sender_id="sender",
    recipient_id="recipient",
    gross_amount=500,
    net_amount=450,
    fee_amount=50,
)


class StoneGiftSchemaOwnershipTests(unittest.TestCase):
    def test_migrations_create_both_tables_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = GiftFixture(Path(temp))
            for _ in range(2):
                with DatabaseUnitOfWork(str(fixture.database), immediate=True) as uow:
                    gift_migrations.apply_stone_gift(uow)
                    gift_migrations.apply_stone_gift_limits(uow)
            self.assertEqual(fixture.columns("stone_gift_operations"), set(OPERATIONS_COLUMNS))
            self.assertEqual(fixture.columns("stone_gift_limits"), set(LIMITS_COLUMNS))

    def test_read_paths_degrade_without_the_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = GiftFixture(Path(temp))
            repository = StoneGiftRepository()
            with DatabaseUnitOfWork(str(fixture.database)) as uow:
                self.assertIsNone(repository.operation(uow, "missing"))
                limits = repository.limits(uow, "2026-01-01", ("sender", "recipient"))
            self.assertEqual(
                limits,
                {"sender": {"sent": 0, "received": 0}, "recipient": {"sent": 0, "received": 0}},
            )
            self.assertNotIn("stone_gift_operations", fixture.tables())
            self.assertNotIn("stone_gift_limits", fixture.tables())

    def test_transfer_fails_closed_without_the_limits_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = GiftFixture(Path(temp)).migrate(limits=False)
            with DatabaseUnitOfWork(str(fixture.database), immediate=True) as uow:
                status = StoneGiftRepository().transfer(
                    uow,
                    RECORD,
                    transfer_date="2026-01-01",
                    send_limit=1000,
                    receive_limit=1000,
                )
            self.assertEqual(status, "schema_missing")
            self.assertEqual(fixture.balances(), (1000, 100))
            self.assertNotIn("stone_gift_limits", fixture.tables())
            with db_backend.connection(fixture.database) as conn:
                receipts = conn.execute(
                    "SELECT COUNT(*) FROM stone_gift_operations"
                ).fetchone()[0]
            self.assertEqual(int(receipts), 0)

    def test_application_maps_schema_missing_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = GiftFixture(Path(temp)).migrate(limits=False)
            outcome = StoneGiftApplication(str(fixture.database)).transfer(
                operation_id="gift-unmigrated",
                sender_id="sender",
                recipient_id="recipient",
                gross_amount=500,
            )
            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "schema_missing")
            self.assertIn("启动迁移", outcome.message)
            self.assertEqual(fixture.balances(), (1000, 100))
            self.assertNotIn("stone_gift_limits", fixture.tables())

    def test_transfer_and_replay_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = GiftFixture(Path(temp)).migrate()
            application = StoneGiftApplication(str(fixture.database))
            first = application.transfer(
                operation_id="gift-ok",
                sender_id="sender",
                recipient_id="recipient",
                gross_amount=500,
            )
            replay = application.transfer(
                operation_id="gift-ok",
                sender_id="sender",
                recipient_id="recipient",
                gross_amount=500,
            )
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(fixture.balances(), (500, 550))
            with db_backend.connection(fixture.database) as conn:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM stone_gift_operations").fetchone()[0], 1
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM stone_gift_limits").fetchone()[0], 2
                )

    def test_request_path_has_no_ddl(self) -> None:
        source = (FEATURE / "repository.py").read_text(encoding="utf-8")
        self.assertNotIn("CREATE TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertIn("table_ready", source)
        migrations = (FEATURE / "migrations.py").read_text(encoding="utf-8")
        self.assertIn("OPERATIONS_DDL", migrations)
        self.assertIn("LIMITS_DDL", migrations)


if __name__ == "__main__":
    unittest.main()
