from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod.guess_number_puzzle import (
    _encourage,
    _make_answer,
)


class FixedRandom:
    def __init__(self, values):
        self.values = iter(values)

    def choice(self, values):
        value = next(self.values)
        return values[value] if isinstance(value, int) else value


def test_make_answer_uses_injected_random_source():
    assert _make_answer(4, FixedRandom(["7", "0", "1", "2"])) == "7012"


def test_encourage_uses_injected_random_source():
    assert _encourage(0, 4, FixedRandom([1])).startswith("暂时空枪")
