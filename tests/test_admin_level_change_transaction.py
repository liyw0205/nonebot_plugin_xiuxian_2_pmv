import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.level_change_service import (
    AdminLevelChangeService,
)


OLD = ("练气境初期", 6000, 3000, 6000, 600, 22800, "混沌灵根", 0)


def create_service(tmp_path):
    database = tmp_path / "game.db"
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,"
        "hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER,root_type TEXT,root_level INTEGER)"
    )
    conn.execute("INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?,?,?)", ("u", *OLD))
    conn.commit()
    conn.close()
    return database, AdminLevelChangeService(database)


def change(service, operation_id="level-op", expected=OLD, level="练气境圆满"):
    return service.change(operation_id, "admin", "u", expected, level, 10000, 2.6, 1.9)


def test_level_change_updates_all_derived_state_and_replays(tmp_path):
    database, service = create_service(tmp_path)
    result = change(service)
    assert result.status == "applied"
    assert (result.level, result.exp, result.hp, result.mp, result.atk, result.power) == (
        "练气境圆满", 10000, 5000, 10000, 1000, 49400,
    )
    assert change(service).status == "duplicate"
    assert change(service, expected=("练气境圆满", 10000, 5000, 10000, 1000, 49400, "混沌灵根", 0)).status == "duplicate"
    conn = sqlite3.connect(database)
    assert conn.execute(
        "SELECT level,exp,hp,mp,atk,power FROM user_xiuxian"
    ).fetchone() == ("练气境圆满", 10000, 5000, 10000, 1000, 49400)
    assert conn.execute("SELECT COUNT(*) FROM admin_level_change_operations").fetchone()[0] == 1
    conn.close()


def test_level_change_rechecks_snapshot_and_rejects_operation_conflict(tmp_path):
    database, service = create_service(tmp_path)
    assert change(service, expected=("练气境中期", *OLD[1:])).status == "state_changed"
    assert change(service).status == "applied"
    assert change(service, level="筑基境圆满").status == "operation_conflict"
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT COUNT(*) FROM admin_level_change_operations").fetchone()[0] == 1
    conn.close()


def test_level_change_operation_failure_rolls_back_every_field(tmp_path):
    database, service = create_service(tmp_path)
    conn = sqlite3.connect(database)
    service._ensure_schema(conn)
    conn.execute(
        "CREATE TRIGGER fail_level_operation BEFORE INSERT ON admin_level_change_operations "
        "BEGIN SELECT RAISE(ABORT,'failed'); END"
    )
    conn.commit()
    conn.close()
    with pytest.raises(Exception):
        change(service)
    conn = sqlite3.connect(database)
    assert conn.execute(
        "SELECT level,exp,hp,mp,atk,power,root_type,root_level FROM user_xiuxian"
    ).fetchone() == OLD
    assert conn.execute("SELECT COUNT(*) FROM admin_level_change_operations").fetchone()[0] == 0
    conn.close()
