"""Join-transaction coverage is implemented in test_dungeon_team_transaction_service."""

from tests import test_dungeon_team_transaction_service as _base


class DungeonTeamJoinTransactionTests(_base.DungeonTeamTransactionServiceTests):
    """Runs the shared suite as the independently tracked join deliverable."""

    __test__ = True
