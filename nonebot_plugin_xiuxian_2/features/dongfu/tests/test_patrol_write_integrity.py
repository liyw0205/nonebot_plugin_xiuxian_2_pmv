from __future__ import annotations

import sqlite3

import pytest

from ....core.errors import DomainError
from ....core.numeric import require_exact_integer
from ....infrastructure.database import DatabaseUnitOfWork
from ..patrol_repository import DongfuPatrolSqlRepository
from . import install_operation_schema


@pytest.fixture
def patrol(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER,stone INTEGER,user_name TEXT)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',100,1000000,'actor')")
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                    "goods_num INTEGER,bind_num INTEGER,create_time TEXT,PRIMARY KEY(user_id,goods_id))")
        uow.execute("INSERT INTO back VALUES('u',21005,'dew','special',3,2,'old')")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,patrol_date TEXT,"
                    "patrol_count INTEGER,patrol_guard INTEGER,plant_slots TEXT)")
        uow.execute("INSERT INTO dongfu_status VALUES('u',1,'2026-11-22',0,0,'preserved')")
    install_operation_schema(game)
    return game, player, DongfuPatrolSqlRepository(game, player)


def _apply(repo, *, day="2026-11-22", stone=120000, reward=(21005, "dew", 1)):
    return repo.patrol("same", "u", day, 8, 3, stone, reward, 99)


def _projection(game, player):
    result = []
    for path, tables in ((game, ("user_xiuxian", "back", "dongfu_patrol_operations")), (player, ("dongfu_status",))):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            result.extend(tuple(tuple(row.values()) for row in uow.query_all(f"SELECT * FROM {table}")) for table in tables)
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


FAULTS = (
    ("game", "BEFORE UPDATE ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END", "player_write_rejected", "actor", False),
    ("game", "AFTER UPDATE ON user_xiuxian BEGIN UPDATE user_xiuxian SET user_stamina=99; END", "player_projection_mismatch", "actor", False),
    ("game", "AFTER UPDATE ON user_xiuxian BEGIN UPDATE user_xiuxian SET stone=0; END", "player_projection_mismatch", "actor", False),
    ("player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "patrol", False),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET patrol_guard=0; END", "state_projection_mismatch", "patrol", False),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_slots='lost'; END", "state_projection_mismatch", "patrol", False),
    ("game", "BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "item:21005", True),
    ("game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "item:21005", False),
    ("game", "AFTER INSERT ON back BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21005", True),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "item:21005", False),
    ("game", "BEFORE INSERT ON dongfu_patrol_operations BEGIN SELECT RAISE(IGNORE); END", "receipt_write_rejected", "receipt", False),
    ("game", "AFTER INSERT ON dongfu_patrol_operations BEGIN UPDATE dongfu_patrol_operations SET payload='changed'; END", "receipt_projection_mismatch", "receipt", False),
    ("game", "AFTER INSERT ON dongfu_patrol_operations BEGIN UPDATE dongfu_patrol_operations SET patrol_count=99; END", "receipt_projection_mismatch", "receipt", False),
    ("game", "AFTER INSERT ON dongfu_patrol_operations BEGIN UPDATE user_xiuxian SET user_stamina=99; END", "player_projection_mismatch", "actor", False),
    ("game", "AFTER INSERT ON dongfu_patrol_operations BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "item:21005", False),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN DELETE FROM dongfu_status; END", "state_projection_mismatch", "patrol", False),
)


@pytest.mark.parametrize("database,trigger,suffix,subject,empty", FAULTS)
def test_write_rejection_has_reason_complete_rollback_and_recovery(patrol, database, trigger, suffix, subject, empty):
    game, player, repo = patrol
    target = game if database == "game" else player
    if empty:
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("DELETE FROM back")
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER patrol_fault " + trigger)
    with pytest.raises(DomainError) as caught:
        _apply(repo)
    reason = "dongfu_patrol_" + suffix
    assert caught.value.code == reason
    assert caught.value.details == {"reason": reason, "subject": subject}
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER patrol_fault")
    assert _apply(repo).status == "patrolled"
    committed = _projection(game, player)
    assert _apply(repo, stone=0, reward=None).status == "duplicate"
    assert _projection(game, player) == committed


@pytest.mark.parametrize("database,operation", (
    ("game", "BEFORE UPDATE ON user_xiuxian"), ("player", "BEFORE UPDATE ON dongfu_status"),
    ("game", "BEFORE INSERT ON back"), ("game", "BEFORE INSERT ON dongfu_patrol_operations"),
))
def test_abort_preserves_original_exception_and_rolls_back(patrol, database, operation):
    game, player, repo = patrol
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER patrol_fault " + operation + " BEGIN SELECT RAISE(ABORT,'patrol refused'); END")
    with pytest.raises(sqlite3.IntegrityError, match="patrol refused"):
        _apply(repo)
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER patrol_fault")
    assert _apply(repo).status == "patrolled"


@pytest.mark.parametrize("stone", (1000000, 2**63 - 1, 10**70))
def test_exact_stone_gain_and_existing_item_metadata_are_preserved(patrol, stone):
    game, player, repo = patrol
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET stone=xiuxian_int_store(?)", (str(stone),))
    assert _apply(repo).status == "patrolled"
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        user = uow.query_one("SELECT user_stamina,stone FROM user_xiuxian")
        assert user["user_stamina"] == 92
        assert require_exact_integer(user["stone"], field="stone", subject="actor") == stone + 120000
        assert tuple(uow.query_one("SELECT goods_num,bind_num,goods_name,goods_type,create_time FROM back").values()) == (4, 3, "dew", "special", "old")


def test_no_material_outcome_leaves_inventory_unchanged(patrol):
    game, player, repo = patrol
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        before = uow.query_all("SELECT * FROM back")
    assert _apply(repo, reward=None).status == "patrolled"
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        assert uow.query_all("SELECT * FROM back") == before
