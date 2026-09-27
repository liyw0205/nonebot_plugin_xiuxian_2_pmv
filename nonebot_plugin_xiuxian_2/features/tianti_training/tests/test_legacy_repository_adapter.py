import unittest
from datetime import datetime
from unittest.mock import patch

import nonebot

nonebot.init()

from ..repository import LegacyTiantiTrainingRepository


class LegacyTiantiTrainingRepositoryTests(unittest.TestCase):
    def test_each_compatibility_operation_constructs_only_its_service(self):
        repository = LegacyTiantiTrainingRepository("game.db", "player.db")
        module = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.transaction_service"
        with (
            patch(f"{module}.StoneTrainingService") as stone,
            patch(f"{module}.MedicineBathService") as bath,
            patch(f"{module}.TiantiBreakthroughService") as breakthrough,
            patch(f"{module}.QiaoxueService") as qiaoxue,
        ):
            services = {
                "train": (stone, "train"),
                "apply_bath": (bath, "apply"),
                "breakthrough": (breakthrough, "attempt"),
                "open_qiaoxue": (qiaoxue, "open"),
            }
            calls = (
                ("train", lambda: repository.train("op-1", "u", 20)),
                (
                    "apply_bath",
                    lambda: repository.apply_bath(
                        "op-2", "u", ({"item_id": 1, "name": "herb", "amount": 1},),
                        1.5, "slot", datetime(2026, 9, 14), 60,
                    ),
                ),
                (
                    "breakthrough",
                    lambda: repository.breakthrough(
                        "op-3", "u", cultivation_rank=1, roll_success=True,
                    ),
                ),
                ("open_qiaoxue", lambda: repository.open_qiaoxue("op-4", "u", 3)),
            )
            for operation, invoke in calls:
                service, method = services[operation]
                expected_result = object()
                getattr(service.return_value, method).return_value = expected_result
                self.assertIs(invoke(), expected_result)

            for service, _ in services.values():
                service.assert_called_once()


if __name__ == "__main__":
    unittest.main()
