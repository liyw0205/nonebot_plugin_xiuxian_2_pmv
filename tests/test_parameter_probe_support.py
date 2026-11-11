import unittest
from types import SimpleNamespace

from tests.parameter_exception_watch import watch_handler_catches


def swallowed():
    try:
        raise ValueError("injected hidden failure")
    except Exception:
        return "fallback"


def handled_finish():
    from nonebot.exception import FinishedException
    try:
        raise FinishedException
    except Exception:
        return "normal flow"


class ParameterExceptionWatchTests(unittest.TestCase):
    def entry(self, function):
        return {"module": __name__, "command": "fixture", "matcher": SimpleNamespace(handlers=[SimpleNamespace(call=function)])}

    def test_swallowed_error_retains_context_and_traceback(self):
        with watch_handler_catches(self.entry(swallowed)) as caught:
            self.assertEqual(swallowed(), "fallback")
        self.assertEqual(len(caught), 1)
        self.assertEqual(caught[0]["command"], "fixture")
        self.assertIn("ValueError: injected hidden failure", caught[0]["traceback"])

    def test_nonebot_finish_is_normal_control_flow(self):
        with watch_handler_catches(self.entry(handled_finish)) as caught:
            handled_finish()
        self.assertEqual(caught, [])


class ParameterNumberDisplayTests(unittest.TestCase):
    def test_normal_and_oversized_asset_display(self):
        import nonebot
        nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to
        for value, expected in ((0, "0"), (9999, "9999"), (10000, "1万"),
                                (12345, "1.23万"), (-100000000, "-1亿")):
            with self.subTest(value=value):
                self.assertEqual(number_to(value), expected)
        positive = number_to(10 ** 350 + 123)
        self.assertTrue(positive.endswith("极无量大"))
        self.assertEqual(number_to(-(10 ** 350 + 123)), "-" + positive)
        self.assertEqual(number_to("inf"), "0")


if __name__ == "__main__":
    unittest.main()
