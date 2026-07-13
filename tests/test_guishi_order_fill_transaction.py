"""Independent guishi fill coverage lives in the matching regression suite."""
from tests.test_guishi_order_matching import GuishiOrderMatchingTests
class GuishiOrderFillTransactionTests(GuishiOrderMatchingTests):
    __test__ = True
