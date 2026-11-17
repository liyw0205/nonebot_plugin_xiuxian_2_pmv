import unittest
from tests import test_guishi_parameter_dispatch as dispatch


class AuctionParameterDispatchTests(unittest.TestCase):
    check_group = dispatch.GuishiParameterDispatchTests.check_group

    def test_auction_lifecycle(self):
        self.check_group('market-auction-lifecycle', 2, {'success': 2})
