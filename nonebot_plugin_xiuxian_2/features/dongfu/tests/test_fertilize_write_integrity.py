from __future__ import annotations

import json
import sqlite3

import pytest

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..fertilize_repository import DongfuFertilizeSqlRepository
from . import install_operation_schema


@pytest.fixture
def fertilize(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    slots = [{"slot": 1, "seed_id": 21001, "fertilizer": 0}]
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,goods_name TEXT,PRIMARY KEY(user_id,goods_id))")
        uow.execute("INSERT INTO back VALUES('u',21006,3,2,'fertilizer')")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,"
                    "plant_slots TEXT,harvest_settlement TEXT)")
        uow.execute("INSERT INTO dongfu_status VALUES('u',1,?,'pending-harvest')", (json.dumps(slots),))
    install_operation_schema(game)
    return game, player, slots, DongfuFertilizeSqlRepository(game, player)


def _projection(game, player):
    result = []
    for path, tables in ((game, ("back", "dongfu_fertilize_operations")), (player, ("dongfu_status",))):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            result.extend(tuple(tuple(row.values()) for row in uow.query_all(f"SELECT * FROM {table}"))
                          for table in tables)
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


FAULTS = (
    ("game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "item:21006"),
    ("player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "plot:1"),
    ("game", "BEFORE INSERT ON dongfu_fertilize_operations BEGIN SELECT RAISE(IGNORE); END", "receipt_write_rejected", "receipt"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21006"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "item:21006"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_slots='[]'; END", "state_projection_mismatch", "plot:1"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET harvest_settlement='lost'; END", "state_projection_mismatch", "plot:1"),
    ("game", "AFTER INSERT ON dongfu_fertilize_operations BEGIN UPDATE dongfu_fertilize_operations SET payload='changed'; END", "receipt_projection_mismatch", "receipt"),
    ("game", "AFTER INSERT ON dongfu_fertilize_operations BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21006"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN DELETE FROM dongfu_status; END", "state_projection_mismatch", "plot:1"),
)


@pytest.mark.parametrize("database,trigger,suffix,subject", FAULTS)
def test_known_write_rejection_rolls_back_and_original_message_recovers(fertilize, database, trigger, suffix, subject):
    game, player, slots, repo = fertilize
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER fertilize_fault " + trigger)
    with pytest.raises(DomainError) as caught:
        repo.fertilize("same", "u", json.dumps(slots), 1, 21006, 3)
    assert caught.value.code == "dongfu_fertilize_" + suffix
    assert caught.value.details == {"reason": "dongfu_fertilize_" + suffix, "subject": subject}
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER fertilize_fault")
    assert repo.fertilize("same", "u", json.dumps(slots), 1, 21006, 3).status == "fertilized"
    committed = _projection(game, player)
    assert repo.fertilize("same", "u", "[]", 1, 21006, 3).status == "duplicate"
    assert _projection(game, player) == committed


@pytest.mark.parametrize("database,operation", (
    ("game", "BEFORE UPDATE ON back"),
    ("player", "BEFORE UPDATE ON dongfu_status"),
    ("game", "BEFORE INSERT ON dongfu_fertilize_operations"),
))
def test_abort_retains_original_error_and_rolls_back(fertilize, database, operation):
    game, player, slots, repo = fertilize
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER fertilize_fault " + operation + " BEGIN SELECT RAISE(ABORT,'fertilizer refused'); END")
    with pytest.raises(sqlite3.IntegrityError, match="fertilizer refused"):
        repo.fertilize("same", "u", json.dumps(slots), 1, 21006, 3)
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER fertilize_fault")
    assert repo.fertilize("same", "u", json.dumps(slots), 1, 21006, 3).succeeded


@pytest.mark.parametrize("quantity,bound,remaining_bound", ((1, 1, 0), (3, 2, 1), (3, 0, 0)))
def test_binding_is_consumed_before_unbound_inventory(fertilize, quantity, bound, remaining_bound):
    game, player, slots, repo = fertilize
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE back SET goods_num=?,bind_num=?", (quantity, bound))
    assert repo.fertilize("binding", "u", json.dumps(slots), 1, 21006, 3).succeeded
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        assert tuple(uow.query_one("SELECT goods_num,bind_num FROM back").values()) == (quantity - 1, remaining_bound)
