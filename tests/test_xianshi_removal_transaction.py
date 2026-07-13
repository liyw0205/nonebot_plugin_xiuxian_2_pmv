"""Independent xianshi removal coverage lives in the repository regression suite."""
from tests.test_trade_purchase import TradePurchaseTests
class XianshiRemovalTransactionTests(TradePurchaseTests):
    __test__ = True
