"""Independent final-settlement regression suite."""

from tests import test_auction_settlement as _base


class AuctionFinalSettlementTransactionTests(_base.AuctionSettlementTests):
    __test__ = True
