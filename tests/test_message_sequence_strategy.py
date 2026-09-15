from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.messaging.reliability import MessageSequenceStrategy


class FixedRandom:
    def __init__(self, values):
        self.values = iter(values)

    def randint(self, start, end):
        return next(self.values)


def test_sequence_strategy_uses_injected_random_source():
    strategy = MessageSequenceStrategy(FixedRandom([1000, 2]))

    assert strategy.next(SimpleNamespace(self_id="bot"), "group", "user") == 1002


def test_sequence_strategy_wraps_using_injected_random_source():
    strategy = MessageSequenceStrategy(FixedRandom([3, 7777]))
    key = ("bot", "group", "user")
    strategy._values[key] = 999999

    assert strategy.next(SimpleNamespace(self_id="bot"), "group", "user") == 7777
