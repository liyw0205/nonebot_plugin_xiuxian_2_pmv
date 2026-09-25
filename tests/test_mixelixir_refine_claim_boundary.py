from __future__ import annotations

import asyncio
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.features.mixelixir.application import MixelixirApplication
from nonebot_plugin_xiuxian_2.features.mixelixir.migrations import (
    apply_mixelixir_refine_claim,
    apply_mixelixir_refine_claim_player,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


class MixelixirRefineClaimBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_registered_recipe_matcher_settles_through_feature_application(self) -> None:
        module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir"
        )
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u',0)")
                uow.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                    "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
                )
                for goods_id, name in ((1, "草"), (2, "引"), (3, "辅")):
                    uow.execute(
                        "INSERT INTO back VALUES('u',?,?, '药材',2,0)", (goods_id, name)
                    )
                uow.execute("INSERT INTO back VALUES('u',4,'炉','炼丹炉',1,0)")
                apply_mixelixir_refine_claim(uow)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,丹药控火 TEXT,"
                    "炼丹记录 TEXT,炼丹经验 TEXT)"
                )
                uow.execute("INSERT INTO mix_elixir_info VALUES('u','0','{}','0')")
                apply_mixelixir_refine_claim_player(uow)

            application = MixelixirApplication(game, player)
            fake_items = SimpleNamespace(
                get_data_by_item_id=lambda goods_id: {
                    1: {"主药": {"type": "阳", "power": 1, "h_a_c": 1}},
                    2: {"药引": {"h_a_c": 1}},
                    3: {"辅药": {"type": "阴", "power": 1}},
                    20: {"name": "试炼丹", "mix_all": 10, "mix_exp": 5},
                }[int(goods_id)]
            )
            fake_event = SimpleNamespace(message_id="refine-message-1")
            fake_bot = SimpleNamespace(self_id="bot")
            fake_buff = SimpleNamespace(get_user_main_buff_data=lambda: None)
            registered_handler = next(
                handler for handler in module.mix_make.handlers
                if handler.call is module.mix_make_
            )

            with (
                patch.object(module, "mixelixir_application", application),
                patch.object(module, "assign_bot", new=AsyncMock(return_value=(fake_bot, None))),
                patch.object(
                    module,
                    "check_user",
                    return_value=(True, {"user_id": "u", "mixelixir_num": 0}, ""),
                ),
                patch.object(
                    module,
                    "check_yaocai_name_in_back",
                    new=AsyncMock(side_effect=[(True, 1), (True, 2), (True, 3)]),
                ),
                patch.object(
                    module,
                    "check_ldl_name_in_back",
                    new=AsyncMock(return_value=(True, {"id": 4, "buff": 0})),
                ),
                patch.object(module, "Items", return_value=fake_items),
                patch.object(module, "tiaohe", new=AsyncMock(return_value=False)),
                patch.object(module, "check_mix", new=AsyncMock(return_value=(True, 20))),
                patch.object(
                    module,
                    "get_player_info",
                    return_value={"丹药控火": 0, "炼丹记录": {}, "炼丹经验": 0},
                ),
                patch.object(
                    module.xiuxian_impart,
                    "get_user_impart_info_with_id",
                    return_value=None,
                ),
                patch.object(module, "UserBuffDate", return_value=fake_buff),
                patch.object(module, "XiuConfig", return_value=SimpleNamespace(max_goods_num=99)),
                patch.object(module, "handle_send", new=AsyncMock()) as send,
                patch.object(
                    module,
                    "_mixelixir_refine_cost_service",
                    side_effect=AssertionError("legacy refine cost service was called"),
                ),
                patch.object(
                    module,
                    "_mixelixir_refine_reward_service",
                    side_effect=AssertionError("legacy refine reward service was called"),
                ),
                patch.object(application, "refine_cost", wraps=application.refine_cost) as refine_cost,
                patch.object(application, "refine_reward", wraps=application.refine_reward) as refine_reward,
            ):
                with self.assertRaises(FinishedException):
                    asyncio.run(
                        registered_handler.call(
                            fake_bot,
                            fake_event,
                            mode="主药草1药引引1辅药辅1丹炉炉",
                        )
                    )

            self.assertEqual(refine_cost.call_count, 1)
            self.assertEqual(refine_reward.call_count, 1)
            self.assertEqual(send.await_count, 1)
            with db_backend.transaction(game) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='u'"
                    ).fetchone()[0],
                    1,
                )
            with db_backend.transaction(player) as conn:
                mix_state = conn.execute(
                    "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='u'"
                ).fetchone()
                self.assertEqual(json.loads(mix_state[0]), {"20": {"name": "试炼丹", "num": 1}})
                self.assertEqual(mix_state[1], "5")
                self.assertEqual(
                    conn.execute('SELECT "炼丹次数" FROM statistics WHERE user_id=\'u\'').fetchone()[0],
                    1,
                )


if __name__ == "__main__":
    unittest.main()
