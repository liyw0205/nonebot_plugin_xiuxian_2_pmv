from pathlib import Path
import unittest
from unittest.mock import patch


class PlayerFightLazyWriterTests(unittest.TestCase):
    def test_player_fight_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_fight.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().update_user_hp_mp(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)

    def test_battle_status_writes_through_player_state_application(self):
        class Writer:
            def __init__(self):
                self.calls = []

            def update_vitals(self, *args, **kwargs):
                self.calls.append((args, kwargs))

        writer = Writer()
        statuses = [{"player": {"user_id": "u", "hp": 35, "mp": 12}}]
        import nonebot

        try:
            nonebot.init()
        except Exception:
            pass
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_fight

        with patch.object(player_fight, "_player_state", return_value=writer):
            player_fight.update_all_user_status(statuses, bot_id="bot")

        assert writer.calls == [
            (("u", 35, 12), {"fallback": unittest.mock.ANY})
        ]


if __name__ == "__main__":
    unittest.main()
