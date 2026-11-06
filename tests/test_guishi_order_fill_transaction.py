"""Independent guishi fill coverage lives in the matching regression suite."""
from tests import test_guishi_order_matching as _base
class GuishiOrderFillTransactionTests(_base.GuishiOrderMatchingTests):
    __test__ = True
