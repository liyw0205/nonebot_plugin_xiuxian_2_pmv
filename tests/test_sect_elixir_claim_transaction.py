import sqlite3

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.elixir_claim_service import SectElixirClaimService


def make_database(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER, "
            "sect_contribution INTEGER, sect_elixir_get INTEGER);"
            "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, elixir_room_level INTEGER, sect_materials INTEGER);"
            "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, "
            "create_time TEXT, update_time TEXT, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id));"
            "INSERT INTO user_xiuxian VALUES ('u1', 7, 1, 500, 0);"
            "INSERT INTO sects VALUES (7, 2, 500);"
        )


def test_claim_grants_batch_and_marks_once(tmp_path):
    database = tmp_path / "game.db"
    make_database(database)
    service = SectElixirClaimService(database)
    rewards = [(1999, "渡厄丹", "丹药", 1), (2001, "聚气丹", "丹药", 2)]
    assert service.claim("claim-1", "u1", 7, 100, 100, rewards, 999).status == "applied"
    replay = service.claim("claim-1", "u1", 7, 100, 100, [(1999, "渡厄丹", "丹药", 9)], 999)
    assert replay.status == "duplicate"
    assert replay.rewards == tuple(rewards)
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT sect_elixir_get FROM user_xiuxian").fetchone()[0] == 1
        assert conn.execute("SELECT goods_id, goods_num, bind_num FROM back ORDER BY goods_id").fetchall() == [(1999, 1, 1), (2001, 2, 2)]


def test_already_claimed_does_not_grant_rewards(tmp_path):
    database = tmp_path / "game.db"
    make_database(database)
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE user_xiuxian SET sect_elixir_get=1")
    result = SectElixirClaimService(database).claim("claim-2", "u1", 7, 100, 100, [(1999, "渡厄丹", "丹药", 1)], 999)
    assert result.status == "already_claimed"
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM back").fetchone()[0] == 0


def test_elixir_handler_has_no_legacy_split_writes():
    source = open("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py", encoding="utf-8").read()
    handler = source[source.index("async def sect_elixir_get_"):source.index("@sect_buff_info.handle")]
    assert "sect_elixir_claim_service.claim(" in handler
    assert "sql_message.send_back(" not in handler
    assert "sql_message.update_user_sect_elixir_get_num(" not in handler
