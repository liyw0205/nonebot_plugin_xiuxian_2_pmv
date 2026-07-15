from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.transaction_service import (
    LotterySettlementService,
)
from tests.test_db_backend import db_backend


class LotterySettlementServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "lottery.db"
        self.legacy_path = Path(self.temp.name) / "lottery_pool.json"
        self.write_legacy()
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,user_name TEXT NOT NULL,stone INTEGER NOT NULL)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s)",
                (
                    ("u1", "道友一", 100),
                    ("u2", "道友二", 200),
                    ("u3", "道友三", 300),
                ),
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_legacy(self, *, pool=0, participants=(), last_winner=None) -> None:
        self.legacy_path.write_text(
            json.dumps(
                {
                    "pool": pool,
                    "participants": list(participants),
                    "last_winner": last_winner,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def service(self, numbers) -> LotterySettlementService:
        number_iter = iter(numbers)
        return LotterySettlementService(
            self.database,
            self.legacy_path,
            randint=lambda lower, upper: next(number_iter),
        )

    def settle(
        self,
        service,
        operation_id,
        user_id="u1",
        business_date="2026-07-14",
        deposit=1_000_000,
    ):
        return service.settle(
            operation_id,
            user_id,
            f"fallback-{user_id}",
            business_date,
            deposit=deposit,
            occurred_at=f"{business_date} 08:30:00",
        )

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_migrates_legacy_json_once_and_queries_database_snapshot(self) -> None:
        self.write_legacy(
            pool=8_765_432,
            participants=("legacy-1", "legacy-2", "legacy-1"),
            last_winner={
                "user_id": "legacy-winner",
                "name": "旧中奖者",
                "time": "2026-07-13 21:00:00",
                "amount": 123_456,
                "lottery_number": 666,
            },
        )
        service = self.service(())

        snapshot = service.get_snapshot(
            "2026-07-14", occurred_at="2026-07-14 00:01:00"
        )

        self.assertEqual((snapshot.pool, snapshot.participants), (8_765_432, 2))
        self.assertEqual(snapshot.last_winner.user_id, "legacy-winner")
        self.assertEqual(snapshot.last_winner.user_name, "旧中奖者")
        self.assertEqual(snapshot.last_winner.amount, 123_456)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_legacy_migrations"), 1)

        self.write_legacy(pool=99, participants=("changed",))
        restarted = self.service(())
        next_day = restarted.get_snapshot(
            "2026-07-15", occurred_at="2026-07-15 00:01:00"
        )
        self.assertEqual((next_day.pool, next_day.participants), (8_765_432, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_legacy_migrations"), 1)

    def test_non_winner_deposits_once_and_duplicate_reuses_frozen_draw(self) -> None:
        service = self.service((12345,))

        first = self.settle(service, "lottery:sign:1")
        duplicate = self.settle(service, "lottery:sign:1")

        self.assertEqual(
            (
                first.status,
                first.lottery_number,
                first.prize_tier,
                first.pool_before,
                first.pool_after,
                first.participants,
            ),
            ("settled", 12345, "none", 0, 1_000_000, 1),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(
            (
                duplicate.lottery_number,
                duplicate.prize_tier,
                duplicate.pool_after,
                duplicate.participants,
            ),
            (
                first.lottery_number,
                first.prize_tier,
                first.pool_after,
                first.participants,
            ),
        )
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='u1'"), 100)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_settlement_operations"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_winner_history"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM economy_log"), 0)

    def test_grand_prize_atomically_empties_pool_and_pays_winner(self) -> None:
        self.write_legacy(pool=500)
        service = self.service((66,))

        result = self.settle(service, "lottery:grand")

        self.assertEqual(
            (result.prize_tier, result.prize, result.pool_before, result.pool_after),
            ("grand", 1_000_500, 500, 0),
        )
        self.assertEqual(result.user_name, "道友一")
        self.assertEqual(result.wallet_stone, 1_000_600)
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='u1'"), 1_000_600)
        self.assertEqual(self.scalar("SELECT prize_amount FROM lottery_winner_history"), 1_000_500)
        self.assertEqual(self.scalar("SELECT stone_delta FROM economy_log"), 1_000_500)
        self.assertEqual(self.scalar("SELECT trace_id FROM economy_log"), "lottery:grand")

    def test_percentage_prize_tiers_use_the_funded_pool_snapshot(self) -> None:
        service = self.service((16662, 16620, 12346))

        first = self.settle(service, "lottery:first", "u1")
        second = self.settle(service, "lottery:second", "u2")
        third = self.settle(service, "lottery:third", "u3")

        self.assertEqual((first.prize_tier, first.prize, first.pool_after), ("first", 100_000, 900_000))
        self.assertEqual((second.prize_tier, second.prize, second.pool_after), ("second", 19_000, 1_881_000))
        self.assertEqual((third.prize_tier, third.prize, third.pool_after), ("third", 2_881, 2_878_119))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_winner_history"), 3)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM economy_log"), 3)

    def test_same_date_rejects_second_participation_without_new_deposit(self) -> None:
        service = self.service((12345, 12345))

        first = self.settle(service, "lottery:first-event")
        repeated = self.settle(service, "lottery:other-event")
        next_day = self.settle(
            service, "lottery:next-day", business_date="2026-07-15"
        )

        self.assertEqual(first.status, "settled")
        self.assertEqual(repeated.status, "already_participated")
        self.assertEqual(repeated.operation_id, "lottery:first-event")
        self.assertEqual(next_day.status, "settled")
        self.assertEqual(next_day.pool_after, 2_000_000)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_settlement_operations"), 2)

    def test_reused_operation_freezes_date_and_rejects_changed_payload(self) -> None:
        service = self.service((12345,))
        self.settle(service, "lottery:conflict")

        changed_user = self.settle(service, "lottery:conflict", user_id="u2")
        next_day_replay = self.settle(
            service, "lottery:conflict", business_date="2026-07-15"
        )
        changed_deposit = self.settle(
            service, "lottery:conflict", deposit=2_000_000
        )

        self.assertEqual(changed_user.status, "operation_conflict")
        self.assertEqual(next_day_replay.status, "duplicate")
        self.assertEqual(next_day_replay.business_date, "2026-07-14")
        self.assertEqual(changed_deposit.status, "operation_conflict")
        self.assertEqual(self.scalar("SELECT pool_amount FROM lottery_pool_state"), 1_000_000)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_participants"), 1)

    def test_operation_failure_rolls_back_pool_payout_history_and_audit(self) -> None:
        service = self.service((6,))
        service.get_snapshot(
            "2026-07-14", occurred_at="2026-07-14 00:01:00"
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_lottery_operation "
                "BEFORE INSERT ON lottery_settlement_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.settle(service, "lottery:failed")

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='u1'"), 100)
        self.assertEqual(self.scalar("SELECT pool_amount FROM lottery_pool_state"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_participants"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM lottery_winner_history"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM economy_log"), 0)

    def test_production_entry_uses_stable_transactional_lottery_operation(self) -> None:
        base_path = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base"
        )
        source = (base_path / "__init__.py").read_text(encoding="utf-8")
        sign_handler = source.split("async def sign_in_", 1)[1].split(
            "async def hongyun_", 1
        )[0]
        lottery_handler = source.split("async def handle_lottery", 1)[1].split(
            "@help_in.handle", 1
        )[0]

        self.assertIn("_lottery_operation_id(sign_operation_id)", sign_handler)
        self.assertIn("sign_result.succeeded", sign_handler)
        self.assertIn("lottery_settlement_service.settle(", lottery_handler)
        self.assertNotIn("random.randint", lottery_handler)
        self.assertNotIn("update_ls", lottery_handler)
        self.assertNotIn("lottery_pool", sign_handler + lottery_handler)
        self.assertFalse((base_path / "lottery_pool.py").exists())


if __name__ == "__main__":
    unittest.main()
