import json
import sqlite3

import pytest

from nonebot_plugin_xiuxian_2.features.back.accessory_decompose_application import AccessoryDecomposeApplication
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


@pytest.mark.parametrize(
    "database,event,fault,reason",
    [
        ("game", "BEFORE UPDATE OF goods_num ON back", "SELECT RAISE(IGNORE)",
         "accessory_decompose_stone_write_rejected"),
        ("game", "AFTER UPDATE OF goods_num ON back",
         "UPDATE back SET goods_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id",
         "accessory_decompose_stone_projection_mismatch"),
        ("player", "BEFORE UPDATE OF equipped ON player_accessory", "SELECT RAISE(IGNORE)",
         "accessory_decompose_state_write_rejected"),
        ("player", "AFTER UPDATE OF equipped ON player_accessory",
         "UPDATE player_accessory SET bag='[]' WHERE user_id=NEW.user_id",
         "accessory_decompose_state_projection_mismatch"),
        ("game", "BEFORE INSERT ON accessory_transaction_operations", "SELECT RAISE(IGNORE)",
         "accessory_decompose_receipt_write_rejected"),
        ("game", "AFTER INSERT ON accessory_transaction_operations",
         "UPDATE accessory_transaction_operations SET payload='{}' WHERE operation_id=NEW.operation_id",
         "accessory_decompose_receipt_projection_mismatch"),
        ("game", "AFTER INSERT ON accessory_transaction_operations",
         "UPDATE back SET goods_num=0 WHERE user_id='u' AND goods_id=20023",
         "accessory_decompose_stone_projection_mismatch"),
    ],
)
def test_single_decompose_known_write_rejections_roll_back_and_recover(tmp_path, database, event, fault, reason):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    target = {"uid": "acc-1", "name": "测试戒指", "quality": 4, "affixes": []}
    other = dict(target, uid="acc-2")
    with sqlite3.connect(game) as connection:
        connection.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))"
        )
        connection.execute("INSERT INTO back VALUES('u',20023,'洗练石','特殊道具',5,2)")
    with sqlite3.connect(player) as connection:
        connection.execute("CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)")
        connection.execute("INSERT INTO player_accessory VALUES(?,?,?)",
                           ("u", "{}", json.dumps([target, other], ensure_ascii=False)))
    with DatabaseUnitOfWork(game) as uow:
        apply_accessory_affix_operations(uow)
    application = AccessoryDecomposeApplication(game, player)

    def snapshot():
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            stones = uow.query_all("SELECT * FROM back")
            receipts = uow.query_all("SELECT * FROM accessory_transaction_operations")
        with DatabaseUnitOfWork(player, read_only=True) as uow:
            state = uow.query_all("SELECT * FROM player_accessory")
        return stones, receipts, state

    selected_database = game if database == "game" else player
    with sqlite3.connect(selected_database) as connection:
        connection.execute("CREATE TABLE isolated_effect(value INTEGER)")
        connection.execute(f"CREATE TRIGGER reject_decompose {event} "
                           f"BEGIN INSERT INTO isolated_effect VALUES(1); {fault}; END")
    before = snapshot()
    result = application.decompose("op", "u", "acc-1", target, 20023, "洗练石", 3, 100)
    assert (result.status, result.reason, getattr(result, "subject", "")) == ("failed", reason, "acc-1")
    assert snapshot() == before
    with sqlite3.connect(selected_database) as connection:
        assert connection.execute("SELECT * FROM isolated_effect").fetchall() == []
        connection.execute("DROP TRIGGER reject_decompose")
    recovered = application.decompose("op", "u", "acc-1", target, 20023, "洗练石", 3, 100)
    assert recovered.status == "applied"
    after = snapshot()
    assert after[0][0]["goods_num"] == 8 and after[0][0]["bind_num"] == 5
    assert json.loads(after[2][0]["bag"]) == [other]
    assert application.decompose("op", "u", "acc-1", target, 20023, "洗练石", 3, 100).status == "duplicate"
    assert snapshot() == after
