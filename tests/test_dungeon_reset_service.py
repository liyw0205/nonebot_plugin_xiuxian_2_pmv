from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.transaction_service import (
    DungeonResetResult,
    DungeonResetService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.dungeon_manager import (
    DungeonManager,
    DungeonTemplate,
)


def create_database(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE dungeon_global_state("
            "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,date TEXT)"
        )
        conn.execute(
            "INSERT INTO dungeon_global_state VALUES('0','old','Old','2026-07-13')"
        )
        conn.execute(
            "CREATE TABLE player_dungeon_status("
            "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,"
            "dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,"
            "last_reset_date TEXT)"
        )
        conn.executemany(
            "INSERT INTO player_dungeon_status VALUES(?,?,?,?,?,?,?)",
            (
                ("u1", "old", "Old", "exploring", 3, 4, "2026-07-13"),
                ("u2", "old", "Old", "completed", 4, 4, "2026-07-13"),
            ),
        )
    return database


def dungeon(number=1):
    return {
        "dungeon_id": f"d{number}",
        "dungeon_name": f"Dungeon {number}",
        "total_layers": number + 4,
        "dungeon_type": "explore",
        "description": f"snapshot {number}",
    }


def full_dungeon():
    return {
        "dungeon_id": "published",
        "dungeon_name": "Published Dungeon",
        "total_layers": 7,
        "dungeon_type": "challenge",
        "description": "durable snapshot",
        "events": [
            {
                "event_id": "monster",
                "weight": 3,
                "description": "Published monster",
                "battle": {"monster_templates": ["elite"]},
            },
            {
                "event_id": "spirit_stone",
                "weight": 1,
                "reward": {"spirit_stone": [11, 22]},
            },
        ],
        "monster_templates": {
            "elite": {
                "name_prefix": ["Ancient"],
                "base_names": ["Guard"],
                "hp_range": [2, 3],
                "reward": {"stone": [5, 9]},
            }
        },
        "boss": {
            "name": "Snapshot Boss",
            "hp_range": [8, 9],
            "skills": [14501],
            "reward": {"stone": [90, 100]},
        },
    }


def state(database):
    with sqlite3.connect(database) as conn:
        global_state = conn.execute(
            "SELECT user_id,dungeon_id,dungeon_name,date "
            "FROM dungeon_global_state WHERE user_id='0'"
        ).fetchone()
        players = conn.execute(
            "SELECT user_id,dungeon_id,dungeon_name,dungeon_status,current_layer,"
            "total_layers,last_reset_date FROM player_dungeon_status ORDER BY user_id"
        ).fetchall()
        operations = conn.execute(
            "SELECT operation_id,business_date,generation,source,dungeon_snapshot,"
            "result_json,status FROM dungeon_reset_operations ORDER BY generation"
        ).fetchall() if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='dungeon_reset_operations'"
        ).fetchone() else []
    return global_state, players, operations


def published_global_state(database):
    columns = (
        "dungeon_id",
        "dungeon_name",
        "date",
        "total_layers",
        "dungeon_type",
        "description",
        "reset_generation",
        "reset_operation_id",
    )
    with sqlite3.connect(database) as conn:
        row = conn.execute(
            "SELECT " + ",".join(columns)
            + " FROM dungeon_global_state WHERE user_id='0'"
        ).fetchone()
    return dict(zip(columns, row))


def test_reset_publishes_snapshot_and_replays_without_reroll(tmp_path):
    database = create_database(tmp_path)
    service = DungeonResetService(database)
    calls = []

    def factory():
        calls.append("called")
        return dungeon(1)

    first = service.reset(
        "daily-1", "2026-07-14", "daily", factory,
        updated_at="2026-07-14 00:01:00",
    )
    duplicate = service.reset(
        "daily-1", "2026-07-14", "crossday",
        lambda: (_ for _ in ()).throw(AssertionError("must not reroll")),
    )

    assert (first.status, first.generation, first.reset_players) == (
        "applied", 1, 2,
    )
    assert duplicate == DungeonResetService(database).reset(
        "daily-1", "2026-07-14", "daily",
        lambda: (_ for _ in ()).throw(AssertionError("must not reroll")),
    )
    assert duplicate.status == "duplicate"
    assert duplicate.dungeon_snapshot == dungeon(1)
    assert calls == ["called"]

    global_state, players, operations = state(database)
    assert global_state == ("0", "d1", "Dungeon 1", "2026-07-14")
    assert players == [
        ("u1", "d1", "Dungeon 1", "not_started", 0, 5, "2026-07-14"),
        ("u2", "d1", "Dungeon 1", "not_started", 0, 5, "2026-07-14"),
    ]
    assert len(operations) == 1
    assert operations[0][:4] == ("daily-1", "2026-07-14", 1, "daily")
    assert operations[0][6] == "completed"


def test_daily_and_crossday_share_one_publication_even_with_different_ids(tmp_path):
    database = create_database(tmp_path)
    first = DungeonResetService(database)
    second = DungeonResetService(database)
    calls = []

    def publish(service, operation_id, source, number):
        return service.reset(
            operation_id,
            "2026-07-14",
            source,
            lambda: calls.append(number) or dungeon(number),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            future.result()
            for future in (
                executor.submit(publish, first, "scheduled", "daily", 1),
                executor.submit(publish, second, "lazy", "crossday", 2),
            )
        )

    assert sorted(result.status for result in results) == ["applied", "duplicate"]
    assert len(calls) == 1
    assert len(state(database)[2]) == 1
    assert first.automatic_operation_id("2026-07-14") == (
        second.automatic_operation_id("2026-07-14")
    )


def test_manual_operations_create_new_generations_on_same_day(tmp_path):
    database = create_database(tmp_path)
    service = DungeonResetService(database)
    daily = service.reset("auto", "2026-07-14", "daily", lambda: dungeon(1))
    manual_one = service.reset(
        "manual-1", "2026-07-14", "manual", lambda: dungeon(2)
    )
    manual_two = service.reset(
        "manual-2", "2026-07-14", "manual", lambda: dungeon(3)
    )
    duplicate = service.reset(
        "manual-1", "2026-07-14", "manual",
        lambda: (_ for _ in ()).throw(AssertionError("must not reroll")),
    )
    conflict = service.reset(
        "manual-1", "2026-07-15", "manual",
        lambda: (_ for _ in ()).throw(AssertionError("must not reroll")),
    )

    assert [daily.generation, manual_one.generation, manual_two.generation] == [1, 2, 3]
    assert (duplicate.status, duplicate.generation) == ("duplicate", 2)
    assert (conflict.status, conflict.generation) == ("operation_conflict", 2)
    assert state(database)[0] == ("0", "d3", "Dungeon 3", "2026-07-14")
    assert len(state(database)[2]) == 3


def test_player_initialization_uses_published_generation_atomically(tmp_path):
    database = create_database(tmp_path)
    service = DungeonResetService(database)
    published = service.reset(
        "manual-generation", "2026-07-14", "manual", lambda: dungeon(2)
    )
    with sqlite3.connect(database) as conn:
        conn.execute("DELETE FROM player_dungeon_status WHERE user_id='u2'")

    initialized = service.ensure_player_status("u2", dungeon(2))
    again = service.ensure_player_status("u2", dungeon(2))

    assert initialized == again
    assert initialized == {
        "dungeon_id": "d2",
        "dungeon_name": "Dungeon 2",
        "dungeon_status": "not_started",
        "current_layer": 0,
        "total_layers": 6,
        "last_reset_date": "2026-07-14",
        "reset_generation": published.generation,
        "reset_operation_id": "manual-generation",
    }
    with sqlite3.connect(database) as conn:
        global_generation = conn.execute(
            "SELECT reset_generation,reset_operation_id FROM dungeon_global_state "
            "WHERE user_id='0'"
        ).fetchone()
        player_generation = conn.execute(
            "SELECT reset_generation,reset_operation_id FROM player_dungeon_status "
            "WHERE user_id='u2'"
        ).fetchone()
    assert global_generation == player_generation == (1, "manual-generation")


def test_player_or_operation_failure_rolls_back_everything(tmp_path):
    for trigger_sql in (
        "CREATE TRIGGER fail_player BEFORE UPDATE ON player_dungeon_status "
        "BEGIN SELECT RAISE(ABORT,'player failed'); END",
        "CREATE TRIGGER fail_operation BEFORE INSERT ON dungeon_reset_operations "
        "BEGIN SELECT RAISE(ABORT,'operation failed'); END",
    ):
        database = create_database(tmp_path / trigger_sql.split()[2])
        service = DungeonResetService(database)
        service.reset("schema", "2026-07-13", "manual", lambda: dungeon(1))
        with sqlite3.connect(database) as conn:
            conn.execute("DELETE FROM dungeon_reset_operations")
            conn.execute(
                "UPDATE dungeon_global_state SET dungeon_id='old',dungeon_name='Old',"
                "date='2026-07-13' WHERE user_id='0'"
            )
            conn.execute(
                "UPDATE player_dungeon_status SET dungeon_id='old',dungeon_name='Old',"
                "dungeon_status='exploring',current_layer=3,total_layers=4,"
                "last_reset_date='2026-07-13'"
            )
            conn.execute(trigger_sql)
        before = state(database)

        try:
            service.reset("failed", "2026-07-14", "daily", lambda: dungeon(2))
        except sqlite3.IntegrityError as exc:
            assert "failed" in str(exc)
        else:
            raise AssertionError("fault injection must fail")
        assert state(database) == before


def test_global_failure_rolls_back_and_same_operation_can_retry(tmp_path):
    database = create_database(tmp_path)
    service = DungeonResetService(database)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER fail_global BEFORE UPDATE ON dungeon_global_state "
            "BEGIN SELECT RAISE(ABORT,'global failed'); END"
        )
    before_global, before_players, _ = state(database)
    try:
        service.reset("retry", "2026-07-14", "daily", lambda: dungeon(1))
    except sqlite3.IntegrityError as exc:
        assert "global failed" in str(exc)
    else:
        raise AssertionError("fault injection must fail")

    with sqlite3.connect(database) as conn:
        operation_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='dungeon_reset_operations'"
        ).fetchone()
        if operation_table:
            assert conn.execute(
                "SELECT COUNT(*) FROM dungeon_reset_operations"
            ).fetchone()[0] == 0
        conn.execute("DROP TRIGGER fail_global")
    assert state(database)[:2] == (before_global, before_players)
    assert service.reset(
        "retry", "2026-07-14", "daily", lambda: dungeon(1)
    ).status == "applied"


def test_legacy_tables_and_operation_columns_are_migrated(tmp_path):
    database = tmp_path / "legacy.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE dungeon_global_state(user_id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO dungeon_global_state VALUES('0')")
        conn.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO player_dungeon_status VALUES('u')")
        conn.execute(
            "CREATE TABLE dungeon_reset_operations(operation_id TEXT PRIMARY KEY)"
        )

    result = DungeonResetService(database).reset(
        "migrated", "2026-07-14", "manual", lambda: dungeon(4)
    )
    assert (result.status, result.generation, result.reset_players) == (
        "applied", 1, 1,
    )
    with sqlite3.connect(database) as conn:
        global_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(dungeon_global_state)")
        }
        player_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(player_dungeon_status)")
        }
        operation_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(dungeon_reset_operations)")
        }
    assert set(DungeonResetService._GLOBAL_COLUMNS).issubset(global_columns)
    assert set(DungeonResetService._PLAYER_COLUMNS).issubset(player_columns)
    assert set(DungeonResetService._OPERATION_COLUMNS).issubset(operation_columns)


def test_manager_restores_removed_config_from_published_operation_snapshot(tmp_path):
    database = create_database(tmp_path)
    service = DungeonResetService(database)
    operation_id = service.automatic_operation_id("2026-07-14")
    published = service.reset(
        operation_id,
        "2026-07-14",
        "daily",
        full_dungeon,
    )
    assert published.generation == 1
    assert service.operation_result(operation_id).dungeon_snapshot == full_dungeon()

    reset_calls = []
    manager = object.__new__(DungeonManager)
    manager._lock = RLock()
    manager.dungeon_templates = []
    manager.current_dungeon = None
    manager._get_current_date = lambda: "2026-07-14"
    manager._get_global_state = lambda: published_global_state(database)
    manager.reset_service = SimpleNamespace(
        operation_result=service.operation_result,
        reset=lambda *args, **kwargs: reset_calls.append((args, kwargs)),
    )

    manager._load_or_init_today_dungeon()

    assert manager._template_snapshot(manager.current_dungeon) == full_dungeon()
    assert manager.current_dungeon.get_event_map()["monster"].battle == {
        "monster_templates": ["elite"]
    }
    assert manager.current_dungeon.monster_templates == full_dungeon()[
        "monster_templates"
    ]
    assert manager.current_dungeon.boss_config == full_dungeon()["boss"]
    assert reset_calls == []
    assert published_global_state(database)["reset_generation"] == 1
    assert len(state(database)[2]) == 1


def test_manager_legacy_snapshot_uses_config_for_missing_runtime_fields():
    complete = full_dungeon()
    template_data = {
        "id": complete["dungeon_id"],
        "name": complete["dungeon_name"],
        "total_layers": complete["total_layers"],
        "type": complete["dungeon_type"],
        "description": complete["description"],
        "events": complete["events"],
        "monster_templates": complete["monster_templates"],
        "boss": complete["boss"],
    }
    legacy_snapshot = {
        "dungeon_id": complete["dungeon_id"],
        "dungeon_name": complete["dungeon_name"],
        "total_layers": complete["total_layers"],
        "dungeon_type": complete["dungeon_type"],
        "description": complete["description"],
    }
    manager = object.__new__(DungeonManager)
    manager.dungeon_templates = [DungeonTemplate(template_data)]
    manager.reset_service = SimpleNamespace(
        operation_result=lambda operation_id: DungeonResetResult(
            status="duplicate",
            operation_id=operation_id,
            business_date="2026-07-14",
            generation=1,
            source="daily",
            dungeon_snapshot=legacy_snapshot,
            operation_status="completed",
        )
    )

    restored = manager._published_template(
        {
            "dungeon_id": complete["dungeon_id"],
            "reset_operation_id": "legacy-operation",
            "dungeon_name": None,
            "total_layers": None,
            "dungeon_type": None,
            "description": None,
        }
    )

    assert manager._template_snapshot(restored) == complete
    assert restored.name != "None"
    assert restored.type != "None"
    assert restored.description != "None"


def test_manager_none_global_fields_do_not_replace_durable_snapshot():
    complete = full_dungeon()
    manager = object.__new__(DungeonManager)
    manager.dungeon_templates = []
    manager.reset_service = SimpleNamespace(
        operation_result=lambda operation_id: DungeonResetResult(
            status="duplicate",
            operation_id=operation_id,
            business_date="2026-07-14",
            generation=1,
            source="daily",
            dungeon_snapshot=complete,
            operation_status="completed",
        )
    )

    restored = manager._published_template(
        {
            "dungeon_id": complete["dungeon_id"],
            "reset_operation_id": "published-operation",
            "dungeon_name": None,
            "total_layers": None,
            "dungeon_type": None,
            "description": None,
        }
    )

    assert manager._template_snapshot(restored) == complete


def test_manager_replaying_old_reset_keeps_current_publication_in_memory():
    old = SimpleNamespace(
        id="old", name="Old", total_layers=5, type="explore", description="old"
    )
    current = SimpleNamespace(
        id="current",
        name="Current",
        total_layers=6,
        type="explore",
        description="current",
    )
    manager = object.__new__(DungeonManager)
    manager.dungeon_templates = [old, current]
    manager.current_dungeon = current
    manager._get_current_date = lambda: "2026-07-14"
    manager._get_global_state = lambda: {"dungeon_id": "current"}
    manager.reset_service = SimpleNamespace(
        reset=lambda *args, **kwargs: DungeonResetResult(
            status="duplicate",
            operation_id="old-operation",
            business_date="2026-07-14",
            generation=1,
            source="manual",
            dungeon_snapshot={
                "dungeon_id": "old",
                "dungeon_name": "Old",
                "total_layers": 5,
            },
            operation_status="completed",
        )
    )

    result = manager.reset_dungeon("old-operation", source="manual")

    assert result.dungeon_snapshot["dungeon_id"] == "old"
    assert manager.current_dungeon is current
