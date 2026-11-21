from __future__ import annotations

import json
import sqlite3

import pytest

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..plant_repository import DongfuPlantSqlRepository
from . import install_operation_schema


@pytest.fixture
def planting(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    slots = [{"slot": 1, "seed_id": 0, "seed_name": "", "plant_start": "", "plant_finish": "", "fertilizer": 0}]
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,goods_name TEXT,PRIMARY KEY(user_id,goods_id))")
        uow.execute("INSERT INTO back VALUES('u',21001,3,2,'seed')")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,"
                    "planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT,harvest_settlement TEXT)")
        uow.execute("INSERT INTO dongfu_status VALUES('u',1,?,0,0,'','','harvest-history')", (json.dumps(slots),))
    install_operation_schema(game)
    return game, player, slots, DongfuPlantSqlRepository(game, player)


def _apply(repo, slots):
    return repo.plant("same", "u", json.dumps(slots), 1, 21001, "seed", "2026-11-22 12:00:00", "2026-11-22 13:00:00")


def _projection(game, player):
    result = []
    for path, tables in ((game, ("back", "dongfu_plant_operations")), (player, ("dongfu_status",))):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            result.extend(tuple(tuple(row.values()) for row in uow.query_all(f"SELECT * FROM {table}"))
                          for table in tables)
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


FAULTS = (
    ("game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "item:21001"),
    ("player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "plot:1"),
    ("game", "BEFORE INSERT ON dongfu_plant_operations BEGIN SELECT RAISE(IGNORE); END", "receipt_write_rejected", "receipt"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21001"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "item:21001"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_slots='[]'; END", "state_projection_mismatch", "plot:1"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_finish='lost'; END", "state_projection_mismatch", "plot:1"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET harvest_settlement='lost'; END", "state_projection_mismatch", "plot:1"),
    ("game", "AFTER INSERT ON dongfu_plant_operations BEGIN UPDATE dongfu_plant_operations SET payload='changed'; END", "receipt_projection_mismatch", "receipt"),
    ("game", "AFTER INSERT ON dongfu_plant_operations BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21001"),
)


@pytest.mark.parametrize("database,trigger,suffix,subject", FAULTS)
def test_rejected_write_keeps_specific_reason_rolls_back_and_recovers(planting, database, trigger, suffix, subject):
    game, player, slots, repo = planting
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER plant_fault " + trigger)
    with pytest.raises(DomainError) as caught:
        _apply(repo, slots)
    assert caught.value.code == "dongfu_plant_" + suffix
    assert caught.value.details == {"reason": "dongfu_plant_" + suffix, "subject": subject}
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER plant_fault")
    assert _apply(repo, slots).status == "planted"
    committed = _projection(game, player)
    assert _apply(repo, []).status == "duplicate"
    assert _projection(game, player) == committed


@pytest.mark.parametrize("database,operation", (
    ("game", "BEFORE UPDATE ON back"), ("player", "BEFORE UPDATE ON dongfu_status"),
    ("game", "BEFORE INSERT ON dongfu_plant_operations"),
))
def test_abort_preserves_traceable_error_and_rolls_back(planting, database, operation):
    game, player, slots, repo = planting
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER plant_fault " + operation + " BEGIN SELECT RAISE(ABORT,'planting refused'); END")
    with pytest.raises(sqlite3.IntegrityError, match="planting refused"):
        _apply(repo, slots)
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER plant_fault")
    assert _apply(repo, slots).succeeded


@pytest.mark.parametrize("quantity,bound,remaining_bound", ((1, 1, 0), (3, 2, 1), (3, 0, 0)))
def test_seed_binding_is_consumed_before_unbound_stock(planting, quantity, bound, remaining_bound):
    game, player, slots, repo = planting
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE back SET goods_num=?,bind_num=?", (quantity, bound))
    assert _apply(repo, slots).status == "planted"
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        assert tuple(uow.query_one("SELECT goods_num,bind_num FROM back").values()) == (quantity - 1, remaining_bound)


def test_occupied_plot_reports_specific_snapshot_difference_without_writes(planting):
    game, player, slots, repo = planting
    changed = [dict(slots[0], seed_id=21002)]
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE dongfu_status SET plant_slots=?", (json.dumps(changed),))
    before = _projection(game, player)
    stale = _apply(repo, slots)
    assert (stale.status, stale.reason, stale.subject) == ("state_changed", "plant_seed_id_changed", "plot:1")
    assert _apply(repo, changed).status == "plot_occupied"
    assert _projection(game, player) == before
