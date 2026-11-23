from __future__ import annotations

import json
import sqlite3

import pytest

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..accelerate_repository import DongfuAccelerateSqlRepository
from . import install_operation_schema


@pytest.fixture
def accelerate(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    slots = [{"slot": 1, "seed_id": 21001, "fertilizer": 2,
              "plant_start": "2026-11-22 10:00:00", "plant_finish": "2026-11-22 14:00:00"}]
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,goods_name TEXT,PRIMARY KEY(user_id,goods_id))")
        uow.execute("INSERT INTO back VALUES('u',21005,3,2,'accelerator')")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,"
                    "planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT,harvest_settlement TEXT)")
        uow.execute("INSERT INTO dongfu_status VALUES('u',1,?,1,21001,?,?,'pending-harvest')",
                    (json.dumps(slots), slots[0]["plant_start"], slots[0]["plant_finish"]))
    install_operation_schema(game)
    return game, player, slots, DongfuAccelerateSqlRepository(game, player)


def _apply(repo, slots, *, now="2026-11-22 12:00:00", finish="2026-11-22 13:00:00", slot_no=1):
    return repo.accelerate("same", "u", json.dumps(slots), slot_no, 21005, now, finish)


def _projection(game, player):
    result = []
    for path, tables in ((game, ("back", "dongfu_accelerate_operations")), (player, ("dongfu_status",))):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            result.extend(tuple(tuple(row.values()) for row in uow.query_all(f"SELECT * FROM {table}"))
                          for table in tables)
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


FAULTS = (
    ("game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "item:21005"),
    ("player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "plot:1"),
    ("game", "BEFORE INSERT ON dongfu_accelerate_operations BEGIN SELECT RAISE(IGNORE); END", "receipt_write_rejected", "receipt"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21005"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "item:21005"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_slots='[]'; END", "state_projection_mismatch", "plot:1"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_finish='lost'; END", "state_projection_mismatch", "plot:1"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET harvest_settlement='lost'; END", "state_projection_mismatch", "plot:1"),
    ("game", "AFTER INSERT ON dongfu_accelerate_operations BEGIN UPDATE dongfu_accelerate_operations SET payload='changed'; END", "receipt_projection_mismatch", "receipt"),
    ("game", "AFTER INSERT ON dongfu_accelerate_operations BEGIN UPDATE back SET goods_num=99; END", "inventory_projection_mismatch", "item:21005"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN DELETE FROM dongfu_status; END", "state_projection_mismatch", "plot:1"),
)


@pytest.mark.parametrize("database,trigger,suffix,subject", FAULTS)
def test_known_rejection_rolls_back_both_databases_and_recovers(accelerate, database, trigger, suffix, subject):
    game, player, slots, repo = accelerate
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER accelerate_fault " + trigger)
    with pytest.raises(DomainError) as caught:
        _apply(repo, slots)
    assert caught.value.code == "dongfu_accelerate_" + suffix
    assert caught.value.details == {"reason": "dongfu_accelerate_" + suffix, "subject": subject}
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER accelerate_fault")
    assert _apply(repo, slots).status == "accelerated"
    committed = _projection(game, player)
    assert _apply(repo, [], now="later", finish="later").status == "duplicate"
    assert _projection(game, player) == committed


@pytest.mark.parametrize("database,operation", (
    ("game", "BEFORE UPDATE ON back"), ("player", "BEFORE UPDATE ON dongfu_status"),
    ("game", "BEFORE INSERT ON dongfu_accelerate_operations"),
))
def test_abort_keeps_original_exception_and_complete_rollback(accelerate, database, operation):
    game, player, slots, repo = accelerate
    target = game if database == "game" else player
    before = _projection(game, player)
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER accelerate_fault " + operation + " BEGIN SELECT RAISE(ABORT,'acceleration refused'); END")
    with pytest.raises(sqlite3.IntegrityError, match="acceleration refused"):
        _apply(repo, slots)
    assert _projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER accelerate_fault")
    assert _apply(repo, slots).succeeded


@pytest.mark.parametrize("quantity,bound,remaining_bound", ((1, 1, 0), (3, 2, 1), (3, 0, 0)))
def test_consumes_binding_before_unbound_material(accelerate, quantity, bound, remaining_bound):
    game, player, slots, repo = accelerate
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE back SET goods_num=?,bind_num=?", (quantity, bound))
    assert _apply(repo, slots).succeeded
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        assert tuple(uow.query_one("SELECT goods_num,bind_num FROM back").values()) == (quantity - 1, remaining_bound)


@pytest.mark.parametrize("finish,now,status", (
    ("2026-11-22 12:00:00.000000", "2026-11-22 12:00:00", "already_mature"),
    ("2026-11-22 12:00:00", "2026-11-22 12:00:00.000000", "already_mature"),
    ("2026-11-22 12:00:00.500000", "2026-11-22 12:00:00.500000", "already_mature"),
    ("2026-11-22 12:00:00.500001", "2026-11-22 12:00:00.500000", "accelerated"),
))
def test_maturity_compares_actual_time_instead_of_text(accelerate, finish, now, status):
    game, player, slots, repo = accelerate
    slots[0]["plant_finish"] = finish
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE dongfu_status SET plant_slots=?,plant_finish=?", (json.dumps(slots), finish))
    before = _projection(game, player)
    assert _apply(repo, slots, now=now, finish=now).status == status
    if status == "already_mature":
        assert _projection(game, player) == before


def test_stale_finish_and_message_conflict_do_not_write(accelerate):
    game, player, slots, repo = accelerate
    changed = [dict(slots[0], plant_finish="2026-11-22 15:00:00")]
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE dongfu_status SET plant_slots=?", (json.dumps(changed),))
    before = _projection(game, player)
    stale = _apply(repo, slots)
    assert (stale.status, stale.reason, stale.subject) == ("state_changed", "plant_plant_finish_changed", "plot:1")
    assert _projection(game, player) == before
    assert _apply(repo, changed).status == "accelerated"
    committed = _projection(game, player)
    conflict = _apply(repo, [], slot_no=2)
    assert (conflict.status, conflict.reason) == ("state_changed", "operation_conflict")
    assert _projection(game, player) == committed
