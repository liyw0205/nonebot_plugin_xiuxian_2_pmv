import unittest
from tests import test_guishi_parameter_dispatch as dispatch


class AuctionParameterDispatchTests(unittest.TestCase):
    check_group = dispatch.GuishiParameterDispatchTests.check_group

    def test_auction_queue_handoff_recovery_and_boundaries(self):
        self.check_group('market-auction-queue', 2, {'success': 2})

    def test_auction_lifecycle(self):
        self.check_group('market-auction-lifecycle', 2, {'success': 2})

    def test_auction_start_write_recovery(self):
        self.check_group('market-auction-start-write', 1, {'success': 1})

    def test_auction_settlement_write_recovery(self):
        self.check_group('market-auction-settlement-write', 2, {'success': 2})

    def test_auction_pending_claim_lifecycle(self):
        self.check_group('market-auction-claim', 1, {'success': 1})
