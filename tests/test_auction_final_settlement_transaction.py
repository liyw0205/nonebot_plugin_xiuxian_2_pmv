"""Independent final-settlement regression suite."""

from tests.test_auction_settlement import AuctionSettlementTests


class AuctionFinalSettlementTransactionTests(AuctionSettlementTests):
    __test__ = True
