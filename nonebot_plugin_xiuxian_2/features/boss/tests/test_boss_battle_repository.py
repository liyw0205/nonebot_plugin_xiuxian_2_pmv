import sqlite3

import pytest

from ....core.errors import DomainError
from ..migrations import apply_boss_player_schema, apply_boss_settlement
from ..application import BossApplication
from ..repository import BossPurchaseSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork

from tests.test_world_boss_battle_settlement import create_databases, settle


def _battle_snapshot(game, player):
    with sqlite3.connect(game) as connection:
        game_state = (
            connection.execute("SELECT * FROM user_xiuxian ORDER BY user_id").fetchall(),
            connection.execute("SELECT * FROM user_cd ORDER BY user_id").fetchall(),
            connection.execute("SELECT * FROM back ORDER BY user_id,goods_id").fetchall(),
            connection.execute("SELECT * FROM world_boss_battle_operations ORDER BY operation_id").fetchall(),
            connection.execute("SELECT * FROM isolated_effect ORDER BY value").fetchall(),
        )
    with sqlite3.connect(player) as connection:
        player_state = (
            connection.execute("SELECT * FROM boss ORDER BY user_id").fetchall(),
            connection.execute("SELECT * FROM boss_limit ORDER BY user_id").fetchall(),
            connection.execute("SELECT * FROM world_boss_state ORDER BY state_key").fetchall(),
            connection.execute('SELECT * FROM statistics ORDER BY user_id').fetchall(),
            connection.execute('SELECT * FROM xiuxian_tasks ORDER BY user_id').fetchall(),
            connection.execute("SELECT * FROM isolated_effect ORDER BY value").fetchall(),
        )
    return game_state, player_state


_SILENT_BATTLE_WRITE_FAULTS = [
    pytest.param(
        "daily",
        "player",
        "CREATE TRIGGER battle_fault BEFORE UPDATE OF boss_stone ON boss "
        "BEGIN INSERT INTO isolated_effect VALUES('daily'); SELECT RAISE(IGNORE); END",
        None,
        id="daily-allowance-ignore",
    ),
    pytest.param(
        "inventory",
        "game",
        "CREATE TRIGGER battle_fault BEFORE INSERT ON back "
        "BEGIN INSERT INTO isolated_effect VALUES('inventory'); SELECT RAISE(IGNORE); END",
        {"id": 9001, "name": "首领遗物", "type": "神物", "quantity": 1, "bind": True},
        id="inventory-ignore",
    ),
    pytest.param(
        "statistics",
        "player",
        "CREATE TRIGGER battle_fault BEFORE INSERT ON statistics "
        "BEGIN INSERT INTO isolated_effect VALUES('statistics'); SELECT RAISE(IGNORE); END",
        None,
        id="statistics-ignore",
    ),
    pytest.param(
        "tasks",
        "player",
        "CREATE TRIGGER battle_fault BEFORE INSERT ON xiuxian_tasks "
        "BEGIN INSERT INTO isolated_effect VALUES('tasks'); SELECT RAISE(IGNORE); END",
        None,
        id="task-progress-ignore",
    ),
    pytest.param(
        "stamina",
        "game",
        "CREATE TRIGGER battle_fault AFTER UPDATE OF user_stamina ON user_xiuxian "
        "BEGIN INSERT INTO isolated_effect VALUES('stamina'); "
        "UPDATE user_xiuxian SET user_stamina=89 WHERE user_id=NEW.user_id; END",
        None,
        id="player-state-rewrite",
    ),
    pytest.param(
        "state",
        "player",
        "CREATE TRIGGER battle_fault AFTER UPDATE OF bosses ON world_boss_state "
        "BEGIN INSERT INTO isolated_effect VALUES('state'); "
        "UPDATE world_boss_state SET bosses='[]' WHERE state_key=NEW.state_key; END",
        None,
        id="boss-session-rewrite",
    ),
    pytest.param(
        "receipt",
        "game",
        "CREATE TRIGGER battle_fault BEFORE INSERT ON world_boss_battle_operations "
        "BEGIN INSERT INTO isolated_effect VALUES('receipt'); SELECT RAISE(IGNORE); END",
        None,
        id="permanent-receipt-ignore",
    ),
]


@pytest.mark.parametrize("stage,database,trigger,item", _SILENT_BATTLE_WRITE_FAULTS)
def test_silent_battle_settlement_write_is_a_known_atomic_rejection(
    tmp_path, stage, database, trigger, item
):
    game, player, activity = create_databases(tmp_path)
    with DatabaseUnitOfWork(player) as uow:
        apply_boss_player_schema(uow)
        uow.execute("CREATE TABLE isolated_effect(value TEXT)")
    with DatabaseUnitOfWork(game) as uow:
        apply_boss_settlement(uow)
        uow.execute("CREATE TABLE isolated_effect(value TEXT)")
    target_database = player if database == "player" else game
    with DatabaseUnitOfWork(target_database) as uow:
        uow.execute(trigger)

    before = _battle_snapshot(game, player)
    repository = BossPurchaseSqlRepository(game, player, activity)
    try:
        result = settle(repository, item=item)
    except DomainError as exc:
        assert exc.code == f"boss_battle_{stage}_write_rejected"
        assert exc.details.get("reason") == exc.code
        assert exc.details.get("subject") == "10001"
    else:
        pytest.fail(
            f"{stage} write was reported {result.status!r}; "
            f"before={before!r}; after={_battle_snapshot(game, player)!r}"
        )

    assert _battle_snapshot(game, player) == before


def test_default_boss_repository_settlement_is_feature_owned(tmp_path):
    game, player, activity = create_databases(tmp_path)
    with DatabaseUnitOfWork(player) as uow:
        apply_boss_player_schema(uow)
    with sqlite3.connect(game) as connection:
        connection.execute(
            "CREATE TABLE world_boss_battle_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,boss_hp INTEGER NOT NULL,"
            "stamina INTEGER NOT NULL,battle_count INTEGER NOT NULL,stone INTEGER NOT NULL,"
            "exp INTEGER NOT NULL,integral INTEGER NOT NULL,activity_lines TEXT NOT NULL)"
        )

    repository = BossPurchaseSqlRepository(game, player, activity)
    result = settle(repository)

    assert result.status == "applied"
    assert repository._settlement.__class__.__module__.endswith("features.boss.battle_repository")
    assert settle(repository).status == "duplicate"


def test_settlement_replay_does_not_create_missing_schema(tmp_path):
    game = tmp_path / "game.db"
    sqlite3.connect(game).close()
    repository = BossPurchaseSqlRepository(game, tmp_path / "player.db")

    assert repository.settlement_result("missing") is None
    with sqlite3.connect(game) as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='world_boss_battle_operations'"
        ).fetchone() is None


def test_daily_limit_snapshot_is_read_only_and_defaults_missing_rows(tmp_path):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with sqlite3.connect(player) as connection:
        connection.execute(
            "CREATE TABLE boss(user_id TEXT PRIMARY KEY,boss_integral INTEGER,boss_stone INTEGER,boss_battle_count INTEGER)"
        )
        connection.execute("INSERT INTO boss VALUES('existing',12,34,5)")

    application = BossApplication(game, player)
    missing = application.daily_limit_snapshot("missing")
    existing = application.daily_limit_snapshot("existing")

    assert (missing.battle_count, missing.integral, missing.stone) == (0, 0, 0)
    assert (existing.battle_count, existing.integral, existing.stone) == (5, 12, 34)
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT COUNT(*) FROM boss").fetchone()[0] == 1


def test_daily_limit_snapshot_missing_database_and_schema_do_not_create_them(tmp_path):
    player = tmp_path / "missing-player.db"
    repository = BossPurchaseSqlRepository(tmp_path / "game.db", player)

    snapshot = repository.daily_limit_snapshot("u")
    assert (snapshot.battle_count, snapshot.integral, snapshot.stone) == (0, 0, 0)
    assert not player.exists()

    sqlite3.connect(player).close()
    snapshot = repository.daily_limit_snapshot("u")
    assert (snapshot.battle_count, snapshot.integral, snapshot.stone) == (0, 0, 0)
    with sqlite3.connect(player) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='boss'"
        ).fetchone()[0] == 0


def test_daily_limit_snapshot_tolerates_legacy_partial_columns(tmp_path):
    player = tmp_path / "player.db"
    with sqlite3.connect(player) as connection:
        connection.execute("CREATE TABLE boss(user_id TEXT PRIMARY KEY,boss_stone INTEGER)")
        connection.execute("INSERT INTO boss VALUES('u',34)")

    snapshot = BossPurchaseSqlRepository(tmp_path / "game.db", player).daily_limit_snapshot("u")

    assert (snapshot.battle_count, snapshot.integral, snapshot.stone) == (0, 0, 34)


def test_clean_player_schema_uses_separate_weekly_purchase_projection(tmp_path):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with sqlite3.connect(game) as connection:
        connection.executescript(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY);"
            "INSERT INTO user_xiuxian VALUES('u');"
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
            "UNIQUE(user_id,goods_id));"
            "CREATE TABLE boss_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
            "quantity INTEGER,cost INTEGER,integral INTEGER,purchased INTEGER,inventory INTEGER);"
        )
    with DatabaseUnitOfWork(player) as uow:
        apply_boss_player_schema(uow)
        uow.execute("INSERT INTO boss(user_id) VALUES('u')")
        uow.execute("INSERT INTO boss_limit(user_id,integral) VALUES('u',100)")

    result = BossPurchaseSqlRepository(game, player).purchase(
        "purchase-clean", "u", 1, "灵草", "药材", 1, 10, 2, 100, {}, 99
    )

    assert result["status"] == "applied"
    with sqlite3.connect(player) as connection:
        weekly = connection.execute(
            "SELECT weekly_purchases FROM boss_weekly_purchases WHERE user_id='u'"
        ).fetchone()[0]
    assert '"1": 1' in weekly
