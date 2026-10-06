from __future__ import annotations

import sqlite3

import pytest

from ..boss_settlement_repository import ActivityBossSettlementSqlRepository


def _database(tmp_path, *, inventory: int | None = None):
    database = tmp_path / "game.db"
    with sqlite3.connect(database) as conn:
        conn.executescript(
            """
            CREATE TABLE activity_boss_state(
                activity_key TEXT PRIMARY KEY,hp_left INTEGER,max_hp INTEGER,update_time TEXT
            );
            CREATE TABLE activity_boss_damage(
                activity_key TEXT,user_id TEXT,total_damage INTEGER,update_time TEXT,
                PRIMARY KEY(activity_key,user_id)
            );
            CREATE TABLE activity_boss_fight_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT,user_id TEXT,
                damage INTEGER,fight_date TEXT,source TEXT,create_time TEXT
            );
            CREATE TABLE activity_boss_milestone(
                activity_key TEXT,milestone_key TEXT,unlocked_time TEXT,
                PRIMARY KEY(activity_key,milestone_key)
            );
            CREATE TABLE activity_boss_settlement_operations(
                operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,damage INTEGER NOT NULL,
                hp_left INTEGER NOT NULL,max_hp INTEGER NOT NULL,fight_count INTEGER NOT NULL,
                inventory INTEGER,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE activity_item_inventory(
                activity_key TEXT,user_id TEXT,item_id TEXT,count INTEGER,update_time TEXT,
                PRIMARY KEY(activity_key,user_id,item_id)
            );
            INSERT INTO activity_boss_state VALUES('boss',1000,1000,'');
            """
        )
        if inventory is not None:
            conn.execute(
                "INSERT INTO activity_item_inventory VALUES('boss','u','firework',?,'')",
                (inventory,),
            )
    return database


def _coop(repo, operation_id="coop-1", *, expected_hp=1000, damage=250):
    return repo.settle_cooperative(
        operation_id=operation_id,
        user_id="u",
        activity_key="boss",
        expected_hp=expected_hp,
        expected_max_hp=1000,
        expected_fight_count=0,
        daily_limit=3,
        fixed_damage=damage,
        fight_date="2026-10-06",
        timestamp="2026-10-06 10:00:00",
        milestones=({"key": "p80", "hp_percent": 80},),
    )


def test_cooperative_settlement_is_atomic_and_replayable(tmp_path):
    database = _database(tmp_path)
    repo = ActivityBossSettlementSqlRepository(database)

    result = _coop(repo)
    assert (result.status, result.damage, result.hp_left, result.fight_count) == (
        "applied",
        250,
        750,
        1,
    )
    assert _coop(repo).status == "duplicate"
    assert _coop(repo, damage=251).status == "operation_conflict"

    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT total_damage FROM activity_boss_damage").fetchone()[0] == 250
        assert conn.execute("SELECT COUNT(*) FROM activity_boss_fight_log").fetchone()[0] == 1
        assert conn.execute("SELECT milestone_key FROM activity_boss_milestone").fetchone()[0] == "p80"


def test_item_settlement_rolls_back_inventory_and_boss_state_on_receipt_failure(tmp_path):
    database = _database(tmp_path, inventory=4)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_receipt BEFORE INSERT ON activity_boss_settlement_operations "
            "BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
        )

    repo = ActivityBossSettlementSqlRepository(database)
    with pytest.raises(sqlite3.IntegrityError, match="receipt failed"):
        repo.settle_item(
            operation_id="item-1",
            user_id="u",
            activity_key="boss",
            item_id="firework",
            expected_inventory=4,
            item_cost=2,
            expected_hp=1000,
            expected_max_hp=1000,
            expected_fight_count=0,
            daily_limit=3,
            fixed_damage=180,
            fight_date="2026-10-06",
            timestamp="2026-10-06 10:00:00",
        )

    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT count FROM activity_item_inventory").fetchone()[0] == 4
        assert conn.execute("SELECT hp_left FROM activity_boss_state").fetchone()[0] == 1000
        assert conn.execute("SELECT COUNT(*) FROM activity_boss_damage").fetchone()[0] == 0


def test_cooperative_path_does_not_require_item_inventory_table(tmp_path):
    database = _database(tmp_path)
    with sqlite3.connect(database) as conn:
        conn.execute("DROP TABLE activity_item_inventory")

    result = _coop(ActivityBossSettlementSqlRepository(database))
    assert result.status == "applied"


def test_settlement_normalizes_a_changed_configured_max_hp_in_transaction(tmp_path):
    database = _database(tmp_path)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE activity_boss_state SET hp_left=250,max_hp=500 WHERE activity_key='boss'"
        )

    repo = ActivityBossSettlementSqlRepository(database)
    result = repo.settle_cooperative(
        operation_id="scaled-1",
        user_id="u",
        activity_key="boss",
        expected_hp=500,
        expected_max_hp=1000,
        expected_fight_count=0,
        daily_limit=3,
        fixed_damage=100,
        fight_date="2026-10-06",
        timestamp="2026-10-06 10:00:00",
    )
    assert (result.status, result.hp_left, result.max_hp) == ("applied", 400, 1000)
