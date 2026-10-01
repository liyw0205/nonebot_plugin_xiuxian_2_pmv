import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.player_state.application import PlayerStateApplication


class PlayerFightLazyWriterTests(unittest.TestCase):
    @staticmethod
    def _player_fight():
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_fight

        return player_fight

    def test_player_fight_has_no_legacy_sql_manager_or_vital_writer(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_fight.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("_sql_message", source)
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("update_user_hp_mp(", source)

    def test_battle_status_writes_through_player_state_application(self):
        class Writer:
            def __init__(self):
                self.calls = []

            def update_vitals(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return SimpleNamespace(status="applied")

        writer = Writer()
        statuses = [{"player": {"user_id": "u", "hp": 35, "mp": 12}}]
        player_fight = self._player_fight()

        with patch.object(player_fight, "_player_state", return_value=writer):
            player_fight.update_all_user_status(statuses, bot_id="bot")

        self.assertEqual(writer.calls, [(('u', 35, 12), {})])

    def test_battle_status_updates_isolated_player_database(self):
        with tempfile.TemporaryDirectory(prefix="player-fight-vitals-") as directory:
            database = Path(directory) / "player.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT,hp INTEGER,mp INTEGER)"
                )
                connection.execute(
                    "INSERT INTO user_xiuxian VALUES('u',80,70)"
                )

            player_fight = self._player_fight()

            with patch.object(
                player_fight,
                "_player_state",
                return_value=PlayerStateApplication(database),
            ):
                player_fight.update_all_user_status(
                    [{"player": {"user_id": "u", "hp": 35, "mp": 12}}],
                    bot_id="bot",
                )

            with sqlite3.connect(database) as connection:
                state = connection.execute(
                    "SELECT hp,mp FROM user_xiuxian WHERE user_id='u'"
                ).fetchone()
            self.assertEqual(state, (35, 12))

    def test_battle_status_does_not_create_database_for_missing_schema(self):
        with tempfile.TemporaryDirectory(prefix="player-fight-missing-") as directory:
            database = Path(directory) / "missing.sqlite3"
            player_fight = self._player_fight()

            with patch.object(
                player_fight,
                "_player_state",
                return_value=PlayerStateApplication(database),
            ):
                player_fight.update_all_user_status(
                    [{"player": {"user_id": "u", "hp": 35, "mp": 12}}],
                    bot_id="bot",
                )

            self.assertFalse(database.exists())


if __name__ == "__main__":
    unittest.main()
