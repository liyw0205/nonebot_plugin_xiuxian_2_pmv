from __future__ import annotations

import unittest

from ..application import RiftApplication
from ..domain import RiftDamageEventResolver


class FixedRandom:
    def __init__(self, draw: int):
        self.draw = draw

    def randint(self, start: int, end: int) -> int:
        assert start <= self.draw <= end
        return self.draw

    def choice(self, values):
        return values[0]


def _resolver(exp_reward=lambda current, percent, level, **_: int(current * percent)):
    return RiftDamageEventResolver(
        battle_config={
            "掉血事件": {
                "desc": ["遭遇事件：{}"],
                "cost": {
                    "exp": {"type_rate": 10, "value": [0.003]},
                    "hp": {"type_rate": 70, "value": [0.3]},
                    "stone": {"type_rate": 20, "value": [3000000]},
                },
            }
        },
        exp_reward=exp_reward,
        format_number=str,
    )


class RiftDamageEventTests(unittest.TestCase):
    def test_hp_damage_is_a_pure_outcome(self):
        user = {"exp": 100, "hp": 100, "mp": 50, "level": "练气境"}
        event = _resolver().roll("掉血事件", user, random_source=FixedRandom(11))
        self.assertEqual({"hp": -15}, event.delta)
        self.assertEqual("遭遇事件：气血减少了：15点！", event.message)
        self.assertEqual(100, user["hp"])

    def test_exp_damage_uses_the_injected_reward_formula(self):
        user = {"exp": 100, "hp": 100, "mp": 50, "level": "练气境"}
        event = _resolver(lambda *_args, **_kwargs: 7).roll(
            "掉血事件", user, random_source=FixedRandom(1)
        )
        self.assertEqual({"exp": -7, "hp": -4, "mp": -7}, event.delta)
        self.assertEqual("遭遇事件：修为减少了：7点！", event.message)

    def test_application_returns_feature_outcome(self):
        app = RiftApplication(
            "/tmp/rift-damage-game.db",
            "/tmp/rift-damage-player.db",
            damage_event_resolver=_resolver(),
        )
        result = app.roll_damage_event(
            "掉血事件",
            {"exp": 100, "hp": 100, "mp": 50, "level": "练气境"},
            random_source=FixedRandom(81),
        )
        self.assertEqual({"stone": -3000000}, result["delta"])
        self.assertEqual("遭遇事件：灵石减少了：3000000枚！", result["message"])


if __name__ == "__main__":
    unittest.main()
