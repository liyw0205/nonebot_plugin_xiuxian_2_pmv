from __future__ import annotations

import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ....plugin import apply_platform_schema
from ..application import DufangApplication
from ..migrations import (
    apply_dufang_bet_payout,
    apply_dufang_player_receipts,
    apply_dufang_resolution,
    apply_dufang_share_player,
)


WIN_PLAN = {
    "entity": {"name": "sealed", "desc": "sealed"},
    "process": "open",
    "result_type": "success",
    "event": {"title": "found", "desc": "found", "outcome": "win"},
    "payout_outcome": "win",
    "gain": 50,
    "requested_loss": 0,
    "sharing": None,
}


def prepare_dufang_databases(game: str, player: str) -> None:
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
        OperationLedger().ensure_schema(uow)
        apply_dufang_bet_payout(uow)
        apply_dufang_resolution(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_dufang_share_player(uow)
        apply_dufang_player_receipts(uow)


class DufangApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = DufangApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)

    def test_bet_freezes_plan_and_payout_resumes_from_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = f"{directory}/game.db"
            player = f"{directory}/player.db"
            prepare_dufang_databases(game, player)
            app = DufangApplication(game, player)
            draws = []
            planned = app.plan_for_bet("bet-1", draw=lambda: (draws.append(1), WIN_PLAN)[1])
            first_bet = app.bet(
                operation_id="bet-1",
                user_id="u",
                cost=20,
                placed_at="now",
                resolution=planned.resolution,
            )
            replay_plan = app.plan_for_bet(
                "bet-1", draw=lambda: self.fail("a replay must not draw a new result")
            )
            replayed_bet = app.bet(
                operation_id="bet-1",
                user_id="u",
                cost=20,
                placed_at="later",
                resolution=replay_plan.resolution,
            )
            payout = app.payout(
                operation_id="pay-1", user_id="u", bet_id="bet-1", settled_at="end"
            )
            replayed_payout = app.payout(
                operation_id="pay-1", user_id="u", bet_id="bet-1", settled_at="later"
            )
            self.assertEqual(draws, [1])
            self.assertEqual(first_bet.data["resolution"], replayed_bet.data["resolution"])
            self.assertTrue(replayed_bet.replayed)
            self.assertEqual((payout.data["wallet_stone"], replayed_payout.data["wallet_stone"]), (130, 130))

    def test_started_bet_ledger_recovers_committed_or_uncommitted_repository_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = f"{directory}/game.db"
            player = f"{directory}/player.db"
            prepare_dufang_databases(game, player)
            app = DufangApplication(game, player)
            with DatabaseUnitOfWork(game, immediate=True) as uow:
                app.ledger.begin(uow, "bet-1", "dufang.bet", {"cost": 20, "user_id": "u"})
            result = app.bet(
                operation_id="bet-1",
                user_id="u",
                cost=20,
                placed_at="now",
                resolution=WIN_PLAN,
            )
            self.assertTrue(result.ok)
            self.assertEqual(result.data["status"], "applied")
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                row = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")
                count = uow.query_one("SELECT COUNT(*) AS count FROM dufang_bets")
            self.assertEqual((row["stone"], count["count"]), (80, 1))

    def test_pending_reconciliation_is_capped_at_five_bets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = f"{directory}/game.db"
            player = f"{directory}/player.db"
            prepare_dufang_databases(game, player)
            app = DufangApplication(game, player)
            for index in range(6):
                app.bet(
                    operation_id=f"bet-{index}",
                    user_id="u",
                    cost=1,
                    placed_at=f"now-{index}",
                    resolution={**WIN_PLAN, "gain": 1},
                )
            result = app.reconcile_pending(limit=100, settled_at="end")
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                statuses = uow.query_all(
                    "SELECT status,COUNT(*) AS count FROM dufang_bets GROUP BY status"
                )
            counts = {str(row["status"]): int(row["count"]) for row in statuses}
            self.assertEqual(result["settled"], 5)
            self.assertEqual(result["pending"], 1)
            self.assertEqual(counts, {"pending": 1, "win": 5})


if __name__ == "__main__":
    unittest.main()
