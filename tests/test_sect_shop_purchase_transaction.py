import sqlite3

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import SectShopPurchaseService


def make_database(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_contribution INTEGER);"
            "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_materials INTEGER, closed INTEGER);"
            "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, "
            "create_time TEXT, update_time TEXT, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id));"
            "INSERT INTO user_xiuxian VALUES ('u1', 7, 1000);"
            "INSERT INTO sects VALUES (7, 1000, 0);"
        )


def test_purchase_updates_all_assets_and_is_idempotent(tmp_path):
    database = tmp_path / "game.db"
    make_database(database)
    service = SectShopPurchaseService(database)
    result = service.purchase("op-1", "u1", 7, 200, "丹药", "丹药", 2, 100, 3, 0, 999)
    duplicate = service.purchase("op-1", "u1", 7, 200, "丹药", "丹药", 2, 100, 3, 0, 999)
    assert result.status == "applied"
    assert duplicate.status == "duplicate"
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT sect_contribution FROM user_xiuxian").fetchone()[0] == 800
        assert conn.execute("SELECT sect_materials FROM sects").fetchone()[0] == 800
        assert conn.execute("SELECT goods_num, bind_num FROM back").fetchone() == (2, 2)
        assert conn.execute("SELECT quantity FROM sect_shop_weekly_purchases").fetchone()[0] == 2


def test_purchase_limit_failure_rolls_back_everything(tmp_path):
    database = tmp_path / "game.db"
    make_database(database)
    result = SectShopPurchaseService(database).purchase("op-2", "u1", 7, 200, "丹药", "丹药", 2, 100, 3, 2, 999)
    assert result.status == "limit_reached"
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT sect_contribution FROM user_xiuxian").fetchone()[0] == 1000
        assert conn.execute("SELECT sect_materials FROM sects").fetchone()[0] == 1000
        assert conn.execute("SELECT COUNT(*) FROM back").fetchone()[0] == 0


def test_shop_handler_has_no_legacy_split_writes():
    source = open("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py", encoding="utf-8").read()
    handler = source[source.index("async def _(bot: Bot", source.index("@sect_buy.handle")):]
    assert "sect_shop_purchase_service.purchase(" in handler
    assert "sql_message.update_sect_materials(" not in handler[:handler.index("await sect_buy.finish()", handler.index("成功兑换"))]
    assert "sql_message.deduct_sect_contribution(" not in handler
    assert "update_sect_weekly_purchase(" not in handler
