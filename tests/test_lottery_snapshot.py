import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sign_in.lottery_application import LotteryApplication
from nonebot_plugin_xiuxian_2.features.sign_in.lottery_repository import LotteryRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class LotterySnapshotTests(unittest.TestCase):
    def test_snapshot_reads_pool_participants_and_latest_winner(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                LotteryRepository.ensure_schema(uow)
                uow.execute("UPDATE lottery_pool_state SET pool_amount=42 WHERE state_id=1")
                uow.execute("INSERT INTO lottery_participants(business_date,user_id,operation_id,participated_at) VALUES(?,?,?,?)", ("2026-09-14", "u1", "op-1", "2026-09-14 00:00:00"))
                uow.execute("INSERT INTO lottery_winner_history(operation_id,user_id,user_name,prize_tier,lottery_number,prize_amount,won_at) VALUES(?,?,?,?,?,?,?)", ("op-w", "u2", "乙", "first", 1666, 100000, "2026-09-14 00:01:00"))
            snapshot = LotteryApplication(str(database)).snapshot("2026-09-14")
            self.assertEqual((snapshot.pool, snapshot.participants), (42, 1))
            self.assertIsNotNone(snapshot.last_winner)
            assert snapshot.last_winner is not None
            self.assertEqual((snapshot.last_winner.user_id, snapshot.last_winner.prize), ("u2", 100000))


if __name__ == "__main__":
    unittest.main()
