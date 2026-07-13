"""Join-transaction coverage is implemented in test_dungeon_team_transaction_service."""

from tests.test_dungeon_team_transaction_service import DungeonTeamTransactionServiceTests


class DungeonTeamJoinTransactionTests(DungeonTeamTransactionServiceTests):
    """Runs the shared suite as the independently tracked join deliverable."""

    __test__ = True
