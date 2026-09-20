import sqlite3
import importlib

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.transaction_service import (
    AdminRootChangeService,
)


def test_admin_facade_defers_root_change_service_construction():
    admin = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin"
    )
    assert admin._admin_root_change_service_instance is None

def test_admin_root_change_uses_feature_application():
    from pathlib import Path
    source = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
    text = source.read_text(encoding="utf-8")
    handler = text[text.index("async def gmm_command_"):text.index("@hmll.handle")]
    assert "_adjust_admin_root(" in handler
    helper = text[text.index("def _adjust_admin_root("):text.index("def _grant_admin_accessory(")]
    assert "admin_asset_application.execute_legacy_call(" in helper


OLD = ("金灵根", "天灵根", 0, "练气境圆满", 10000, 31200, "青云")


def create_service(tmp_path):
    database = tmp_path / "game.db"
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root TEXT,root_type TEXT,"
        "root_level INTEGER,level TEXT,exp INTEGER,power INTEGER,user_name TEXT)"
    )
    conn.execute("INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?,?)", ("u", *OLD))
    conn.commit()
    conn.close()
    return database, AdminRootChangeService(database)


def change(service, operation_id="root-op", expected=OLD, root_id=8, rate=7.0):
    return service.change(operation_id, "admin", "u", expected, root_id, 2.6, rate)


def test_root_change_updates_root_and_power_and_replays(tmp_path):
    database, service = create_service(tmp_path)
    result = change(service)
    assert result.status == "applied"
    assert result.root_type == "永恒道果"
    assert result.power == 182000
    assert change(service).status == "duplicate"
    assert change(
        service,
        expected=(
            "轮回无尽不灭，只为触及永恒之境", "永恒道果", 0,
            "练气境圆满", 10000, 182000, "青云",
        ),
    ).status == "duplicate"
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT root,root_type,power FROM user_xiuxian").fetchone() == (
        "轮回无尽不灭，只为触及永恒之境", "永恒道果", 182000,
    )
    assert conn.execute("SELECT COUNT(*) FROM admin_root_change_operations").fetchone()[0] == 1
    conn.close()


def test_root_change_rechecks_snapshot_and_operation_payload(tmp_path):
    database, service = create_service(tmp_path)
    assert change(service, expected=("木灵根", *OLD[1:])).status == "state_changed"
    assert change(service).status == "applied"
    assert change(service, root_id=7, rate=5.0).status == "operation_conflict"
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT COUNT(*) FROM admin_root_change_operations").fetchone()[0] == 1
    conn.close()


def test_fate_root_uses_snapshot_name_and_operation_failure_rolls_back(tmp_path):
    database, service = create_service(tmp_path)
    conn = sqlite3.connect(database)
    service._ensure_schema(conn)
    conn.execute(
        "CREATE TRIGGER fail_root_operation BEFORE INSERT ON admin_root_change_operations "
        "BEGIN SELECT RAISE(ABORT,'failed'); END"
    )
    conn.commit()
    conn.close()
    with pytest.raises(Exception):
        change(service, root_id=9, rate=7.0)
    conn = sqlite3.connect(database)
    assert conn.execute(
        "SELECT root,root_type,root_level,level,exp,power,user_name FROM user_xiuxian"
    ).fetchone() == OLD
    assert conn.execute("SELECT COUNT(*) FROM admin_root_change_operations").fetchone()[0] == 0
    conn.close()


def test_admin_root_change_entry_uses_lazy_service():
    source = open(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py",
        encoding="utf-8",
    ).read()
    handler = source[source.index("async def gmm_command_"):source.index("@cz.handle", source.index("async def gmm_command_"))]
    assert "_admin_root_change_service().root_values(" in handler
    assert "_adjust_admin_root(" in handler
    assert "_admin_root_change_service_instance = None" in source
    assert "def _admin_root_change_service(" in source
    assert handler.index("_admin_root_change_service().root_values(") < handler.index("_adjust_admin_root(")
    assert "_admin_operation_id(event, \"root-change\", str(user_id))" in source
