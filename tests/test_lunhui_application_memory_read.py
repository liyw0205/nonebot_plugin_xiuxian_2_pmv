from __future__ import annotations

import tempfile
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.lunhui.application import LunhuiApplication
from tests.test_db_backend import db_backend


def test_memory_read_is_normalized_by_the_application_without_schema_writes():
    with tempfile.TemporaryDirectory(prefix="lunhui-memory-") as directory:
        game = Path(directory) / "game.db"
        player = Path(directory) / "player.db"
        with db_backend.transaction(player) as conn:
            conn.execute(
                "CREATE TABLE reincarnation_memory(user_id TEXT PRIMARY KEY,main_buff TEXT,"
                "memory_level TEXT,retrieved_main TEXT)"
            )
            conn.execute(
                "INSERT INTO reincarnation_memory VALUES(%s,%s,%s,%s)",
                ("u", "42", "破虚境圆满", "1"),
            )

        memory = LunhuiApplication(game, player).get_reincarnation_memory("u")

        assert memory == {
            "main_buff": 42,
            "sub_buff": 0,
            "sec_buff": 0,
            "effect1_buff": 0,
            "effect2_buff": 0,
            "memory_level": "破虚境圆满",
            "retrieved": {
                "main": True,
                "sub": False,
                "sec": False,
                "effect1": False,
                "effect2": False,
            },
        }
        with db_backend.connection(player) as conn:
            assert conn.column_names("reincarnation_memory") == [
                "user_id", "main_buff", "memory_level", "retrieved_main"
            ]


def test_memory_read_fails_closed_for_missing_storage_without_creating_database():
    with tempfile.TemporaryDirectory(prefix="lunhui-memory-") as directory:
        game = Path(directory) / "game.db"
        player = Path(directory) / "missing-player.db"

        assert LunhuiApplication(game, player).get_reincarnation_memory("u") is None
        assert not player.exists()
