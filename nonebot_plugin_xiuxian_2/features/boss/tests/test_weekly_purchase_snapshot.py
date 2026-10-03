from __future__ import annotations

import json
from datetime import date

from ..application import BossApplication
from ..migrations import apply_boss_player_schema
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


def _player_database(path):
    with DatabaseUnitOfWork(path) as uow:
        apply_boss_player_schema(uow)
    return path


def test_weekly_snapshot_is_read_only_and_resets_only_the_returned_old_value(tmp_path):
    player = _player_database(tmp_path / "player.db")
    previous = json.dumps({"_last_reset": "2026-09-14", "7": 2})
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("ALTER TABLE boss ADD COLUMN weekly_purchases TEXT")
        uow.execute(
            "INSERT INTO boss(user_id,weekly_purchases) VALUES('u',?)",
            (previous,),
        )

    application = BossApplication(tmp_path / "game.db", player)
    current_week = application.weekly_purchases("u", date(2026, 9, 17))
    following_week = application.weekly_purchases("u", date(2026, 9, 21))

    assert current_week == {"_last_reset": "2026-09-14", "7": 2}
    assert following_week == {"_last_reset": "2026-09-21"}
    with DatabaseUnitOfWork(player, read_only=True) as uow:
        row = uow.query_one(
            "SELECT weekly_purchases FROM boss WHERE user_id='u'"
        )
    assert row["weekly_purchases"] == previous


def test_weekly_snapshot_uses_migrated_table_when_legacy_column_is_absent(tmp_path):
    player = _player_database(tmp_path / "player.db")
    stored = json.dumps({"_last_reset": "2026-09-14", "9": 1})
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(
            "INSERT INTO boss_weekly_purchases(user_id,weekly_purchases) VALUES('u',?)",
            (stored,),
        )

    snapshot = BossApplication(tmp_path / "game.db", player).weekly_purchases(
        "u", date(2026, 9, 17)
    )

    assert snapshot == {"_last_reset": "2026-09-14", "9": 1}


def test_weekly_snapshot_fails_closed_without_creating_database_or_schema(tmp_path):
    missing = tmp_path / "missing.db"

    snapshot = BossApplication(tmp_path / "game.db", missing).weekly_purchases(
        "u", date(2026, 9, 17)
    )

    assert snapshot is None
    assert not missing.exists()


def test_purchase_initializes_legacy_weekly_row_only_on_success(tmp_path):
    game = tmp_path / "game.db"
    player = _player_database(tmp_path / "player.db")
    with DatabaseUnitOfWork(game) as uow:
        OperationLedger().ensure_schema(uow)
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
        uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('u')")
        uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('poor')")
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
            "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
            "bind_num INTEGER,UNIQUE(user_id,goods_id))"
        )
        uow.execute(
            "CREATE TABLE boss_purchase_operations(operation_id TEXT PRIMARY KEY,"
            "payload TEXT,quantity INTEGER,cost INTEGER,integral INTEGER,"
            "purchased INTEGER,inventory INTEGER)"
        )
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("ALTER TABLE boss ADD COLUMN weekly_purchases TEXT")
        uow.execute("INSERT INTO boss_limit(user_id,integral) VALUES('u',100)")
        uow.execute("INSERT INTO boss_limit(user_id,integral) VALUES('poor',0)")

    application = BossApplication(game, player)
    weekly = application.weekly_purchases("u", date(2026, 9, 17))
    assert weekly == {"_last_reset": "2026-09-17"}
    assert application.purchase(
        operation_id="boss-purchase-first-row",
        user_id="u",
        item_id=1,
        item_name="灵草",
        item_type="药材",
        quantity=1,
        unit_cost=10,
        weekly_limit=2,
        expected_integral=100,
        expected_weekly_purchases=weekly,
        max_goods_num=99,
        today=date(2026, 9, 17),
    ).ok

    with DatabaseUnitOfWork(player, read_only=True) as uow:
        row = uow.query_one(
            "SELECT weekly_purchases FROM boss WHERE user_id='u'"
        )
    assert json.loads(row["weekly_purchases"]) == {
        "_last_reset": "2026-09-17",
        "1": 1,
    }

    rejected_weekly = application.weekly_purchases("poor", date(2026, 9, 17))
    rejected = application.purchase(
        operation_id="boss-purchase-insufficient-integral",
        user_id="poor",
        item_id=1,
        item_name="灵草",
        item_type="药材",
        quantity=1,
        unit_cost=10,
        weekly_limit=2,
        expected_integral=0,
        expected_weekly_purchases=rejected_weekly,
        max_goods_num=99,
        today=date(2026, 9, 17),
    )
    assert not rejected.ok
    with DatabaseUnitOfWork(player, read_only=True) as uow:
        row = uow.query_one("SELECT user_id FROM boss WHERE user_id='poor'")
    assert row is None
