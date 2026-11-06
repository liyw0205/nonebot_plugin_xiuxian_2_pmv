"""Independent xianshi removal coverage lives in the repository regression suite."""
from tests import test_trade_purchase as _base
class XianshiRemovalTransactionTests(_base.TradePurchaseTests):
    __test__ = True
