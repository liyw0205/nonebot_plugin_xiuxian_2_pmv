import json
import sqlite3
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.world_events.attack_application import DemonAttackApplication
from nonebot_plugin_xiuxian_2.features.world_events.domain import DemonAttackSettlementResult
from nonebot_plugin_xiuxian_2.features.world_events.migrations import apply_world_events_player
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


def create_db(path):
    boss = {
        "wave": 1,
        "boss_hp": 1000,
        "boss_max_hp": 1000,
        "battle_hp": 100,
        "battle_max_hp": 100,
        "reward_multiplier": 1.0,
    }
    with DatabaseUnitOfWork(path, immediate=True) as uow:
        apply_world_events_player(uow)
        uow.execute(
            "INSERT INTO world_event_state(user_id,status,event_id,bosses,participants,claimed) "
            "VALUES(?,?,?,?,?,?)",
            ("global", "active", "event-1", json.dumps({"练气境": boss}), "{}", "{}"),
        )
    return boss


def settle(app, boss, operation_id="op-1", participants=None, **kwargs):
    return app.settle(
        operation_id=operation_id,
        event_key="global",
        user_id="10001",
        user_name="测试道友",
        realm="练气境",
        total_damage=1,
        expected_event={"status": "active", "event_id": "event-1"},
        expected_boss=boss,
        expected_participants=participants or {},
        attack_limit=kwargs.get("attack_limit", 3),
        real_hp_multiplier=kwargs.get("real_hp_multiplier", 100),
        max_damage_ratio=kwargs.get("max_damage_ratio", 0.2),
        max_pursuit_ratio=kwargs.get("max_pursuit_ratio", 0.1),
    )


def test_application_settlement_atomically_updates_state_stats_and_operation(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    result = settle(DemonAttackApplication(db), boss)
    assert isinstance(result, DemonAttackSettlementResult)
    assert (result.status, result.real_damage, result.boss_now_hp) == ("applied", 100, 900)

    with sqlite3.connect(db) as conn:
        bosses, participants = map(
            json.loads,
            conn.execute("SELECT bosses,participants FROM world_event_state WHERE user_id='global'").fetchone(),
        )
        assert bosses["练气境"]["boss_hp"] == 900
        assert participants["练气境:1:10001"]["damage"] == 100
        assert participants["练气境:1:10001"]["attacks"] == 1
        assert conn.execute(
            'SELECT "魔修入侵参与","魔修入侵伤害" FROM statistics WHERE user_id=?',
            ("10001",),
        ).fetchone() == (1, 100)
        assert conn.execute("SELECT COUNT(*) FROM demon_attack_settlement_operations").fetchone()[0] == 1


def test_application_replay_is_idempotent_and_returns_settlement_fields(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    app = DemonAttackApplication(db)
    first = settle(app, boss)
    replay = app.get_result("op-1")
    second = settle(app, boss)

    assert first.status == "applied"
    assert replay is not None and replay.status == "duplicate"
    assert replay.real_damage == 100
    assert second.status == "duplicate"
    assert (first.real_damage, first.boss_now_hp) == (second.real_damage, second.boss_now_hp)
    with sqlite3.connect(db) as conn:
        participants = json.loads(conn.execute("SELECT participants FROM world_event_state").fetchone()[0])
        assert participants["练气境:1:10001"]["attacks"] == 1


def test_operation_id_conflict_and_changed_snapshot_are_rejected(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    app = DemonAttackApplication(db)
    assert settle(app, boss).status == "applied"

    conflict = app.settle(
        operation_id="op-1",
        user_id="another-user",
        event_key="global",
        user_name="其他道友",
        realm="练气境",
        total_damage=1,
        expected_event={"status": "active", "event_id": "event-1"},
        expected_boss=boss,
        expected_participants={},
        attack_limit=3,
        real_hp_multiplier=100,
        max_damage_ratio=0.2,
        max_pursuit_ratio=0.1,
    )
    changed_boss = dict(boss, boss_hp=999)
    changed = settle(app, changed_boss, operation_id="op-2")
    assert conflict.status == "operation_conflict"
    assert changed.status == "state_changed"


def test_attack_limit_is_checked_inside_the_atomic_settlement(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    app = DemonAttackApplication(db)
    assert settle(app, boss, attack_limit=1).status == "applied"

    with sqlite3.connect(db) as conn:
        next_boss = json.loads(conn.execute("SELECT bosses FROM world_event_state").fetchone()[0])["练气境"]
        participants = json.loads(conn.execute("SELECT participants FROM world_event_state").fetchone()[0])
    result = settle(app, next_boss, operation_id="op-2", participants=participants, attack_limit=1)
    assert result.status == "already_settled"


@pytest.mark.parametrize(("boss_hp", "expected_killed", "expected_pursuit"), [(50, True, False), (0, False, True)])
def test_kill_and_pursuit_unlock_wave_rewards(tmp_path, boss_hp, expected_killed, expected_pursuit):
    db = tmp_path / "player.db"
    create_db(db)
    boss = {"wave": 1, "boss_hp": boss_hp, "boss_max_hp": 1000, "battle_hp": 100, "battle_max_hp": 100, "reward_multiplier": 1.0}
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE world_event_state SET bosses=? WHERE user_id='global'", (json.dumps({"练气境": boss}),))

    result = settle(DemonAttackApplication(db), boss)
    with sqlite3.connect(db) as conn:
        bosses, participants = map(
            json.loads,
            conn.execute("SELECT bosses,participants FROM world_event_state WHERE user_id='global'").fetchone(),
        )
        record = participants["练气境:1:10001"]
        assert record["reward_ready"] == 1
        assert bosses["练气境"]["boss_hp"] == result.boss_now_hp
        if expected_killed:
            assert conn.execute('SELECT "魔修入侵击退" FROM statistics WHERE user_id=?', ("10001",)).fetchone() == (1,)

    assert result.status == "applied"
    assert result.killed is expected_killed
    assert result.pursuit_mode is expected_pursuit


def test_large_damage_statistics_use_overflow_safe_sqlite_binding(tmp_path):
    db = tmp_path / "player.db"
    create_db(db)
    boss = {
        "wave": 1,
        "boss_hp": 10**30,
        "boss_max_hp": 10**30,
        "battle_hp": 100,
        "battle_max_hp": 100,
        "reward_multiplier": 1.0,
    }
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE world_event_state SET bosses=? WHERE user_id='global'",
            (json.dumps({"练气境": boss}),),
        )

    result = settle(
        DemonAttackApplication(db),
        boss,
        real_hp_multiplier=10**30,
    )
    with sqlite3.connect(db) as conn:
        damage_stat = conn.execute(
            'SELECT "魔修入侵伤害" FROM statistics WHERE user_id=?',
            ("10001",),
        ).fetchone()[0]
    assert result.status == "applied"
    assert result.real_damage > 2**63 - 1
    assert float(damage_stat) > 2**63 - 1


def test_failure_rolls_back_state_stats_and_operation(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TRIGGER reject_demon_operation BEFORE INSERT ON demon_attack_settlement_operations "
            "BEGIN SELECT RAISE(ABORT, 'reject operation'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="reject operation"):
        settle(DemonAttackApplication(db), boss)

    with sqlite3.connect(db) as conn:
        bosses, participants = conn.execute(
            "SELECT bosses,participants FROM world_event_state"
        ).fetchone()
        assert json.loads(bosses)["练气境"]["boss_hp"] == 1000
        assert participants == "{}"
        assert conn.execute("SELECT COUNT(*) FROM statistics").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM demon_attack_settlement_operations").fetchone()[0] == 0


def test_real_entry_uses_feature_owned_replay_and_settlement():
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py").read_text(
        encoding="utf-8"
    )
    start = source.index("async def attack_demon_invasion_")
    handler = source[start:source.index("async def claim_demon_reward_", start)]
    assert "demon_attack_application.get_result(" in handler
    assert "demon_attack_application.settle(" in handler
    assert "_demon_attack_settlement_service" not in handler
    assert "from .transaction_service import DemonAttackSettlementService" not in source

    application = Path("nonebot_plugin_xiuxian_2/features/world_events/attack_application.py").read_text(
        encoding="utf-8"
    )
    repository = Path("nonebot_plugin_xiuxian_2/features/world_events/attack_repository.py").read_text(
        encoding="utf-8"
    )
    assert "DemonAttackSettlementSqlRepository" in application
    assert "DemonAttackSettlementService" not in application
    assert "DatabaseUnitOfWork(self.player_database, immediate=True)" in repository


def test_player_attack_migration_is_scoped_to_player_database():
    migrations = build_migrations()
    game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
    player_versions = {item.version for item in migrations_for_database(migrations, "player_db")}
    assert "world_events.001" in game_versions
    assert "world_events.002" not in game_versions
    assert "world_events.002" in player_versions


def test_player_attack_migration_adds_columns_without_replacing_legacy_state(tmp_path):
    db = tmp_path / "legacy-player.db"
    with DatabaseUnitOfWork(db, immediate=True) as uow:
        uow.execute(
            "CREATE TABLE world_event_state ("
            "user_id TEXT PRIMARY KEY,status TEXT,event_id TEXT,bosses TEXT,participants TEXT,claimed TEXT)"
        )
        uow.execute(
            "INSERT INTO world_event_state VALUES(?,?,?,?,?,?)",
            ("global", "active", "event-old", '{"realm":{}}', "{}", "{}"),
        )
        apply_world_events_player(uow)

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT status,event_id,bosses,participants,claimed FROM world_event_state WHERE user_id='global'"
        ).fetchone()
        columns = {item[1] for item in conn.execute("PRAGMA table_info(world_event_state)")}
        assert row == ("active", "event-old", '{"realm":{}}', "{}", "{}")
        assert {"active", "manual", "started_at", "last_result"} <= columns
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='demon_attack_settlement_operations'"
        ).fetchone()[0] == 1
