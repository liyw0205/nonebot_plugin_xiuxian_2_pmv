from __future__ import annotations

import inspect
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.application import BackApplication
from nonebot_plugin_xiuxian_2.features.base.application import BaseApplication
from nonebot_plugin_xiuxian_2.features.buff.application import BuffApplication
from nonebot_plugin_xiuxian_2.features.map.application import MapApplication
from nonebot_plugin_xiuxian_2.features.natal_treasure.application import NatalTreasureApplication
from nonebot_plugin_xiuxian_2.features.rift.application import RiftApplication
from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication


class _Repository:
    def __init__(self, status: str = "applied") -> None:
        self.status = status

    def invoke(self, action: str, *args, **kwargs):
        if self.status == "error":
            raise RuntimeError("repository failure")
        return {"status": self.status, "action": action}

    def __getattr__(self, name: str):
        def call(*args, **kwargs):
            if self.status == "error":
                raise RuntimeError("repository failure")
            return {"status": self.status, "action": name}

        return call


class LegacyApplicationContractTests(unittest.TestCase):
    applications = (
        BackApplication,
        BaseApplication,
        BuffApplication,
        MapApplication,
        NatalTreasureApplication,
        RiftApplication,
        TradeApplication,
    )

    @staticmethod
    def _build(application, directory: str, repository: _Repository):
        game = Path(directory) / "game.db"
        player = Path(directory) / "player.db"
        trade = Path(directory) / "trade.db"
        if application is BackApplication:
            return application(game, player, repository=repository)
        if application is TradeApplication:
            return application(game, trade, repository=repository)
        if application is NatalTreasureApplication:
            return application(player, game, repository=repository)
        return application(game, player, repository=repository)

    def test_every_legacy_action_has_success_replay_rejection_and_failure(self) -> None:
        for application in self.applications:
            actions = [
                method
                for method, value in inspect.getmembers(application, inspect.isfunction)
                if not method.startswith("_") and method not in {"reply", "snapshot"}
            ]
            self.assertTrue(actions, application.__name__)
            with tempfile.TemporaryDirectory() as directory:
                repository = _Repository()
                service = self._build(application, directory, repository)
                for index, action in enumerate(actions):
                    first = getattr(service, action)(operation_id=f"{application.__name__}-{index}", user_id="user")
                    self.assertTrue(first.ok, (application.__name__, action, first))
                    replay = getattr(service, action)(operation_id=f"{application.__name__}-{index}", user_id="user")
                    self.assertEqual(replay.status, "replayed", (application.__name__, action, replay))

                repository.status = "rejected"
                rejected = getattr(service, actions[0])(operation_id="rejected", user_id="user")
                self.assertFalse(rejected.ok)

                repository.status = "error"
                with self.assertRaises(RuntimeError):
                    getattr(service, actions[0])(operation_id="failed", user_id="user")


if __name__ == "__main__":
    unittest.main()
