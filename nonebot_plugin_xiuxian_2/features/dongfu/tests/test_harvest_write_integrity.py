import json
import sqlite3
from copy import deepcopy

import pytest

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..harvest_repository import DongfuHarvestSqlRepository
from ..plant_slots import canonical_plant_slots, empty_plant_slot
from . import install_operation_schema


@pytest.fixture
def harvest(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    slots = [empty_plant_slot(index) for index in range(1, 4)]
    slots[0].update(seed_id=21001, seed_name="seed", plant_start="2026-11-21 09:00:00",
                    plant_finish="2026-11-21 10:00:00")
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u')")
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                    "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                    "PRIMARY KEY(user_id,goods_id))")
        uow.execute("INSERT INTO back VALUES('u',1,'herb','herb',10,'old','old',3)")
    snapshot = dict(expected_slots=slots, slot_numbers=[1], failed_slots=[], operation_id="harvest",
                    items=[dict(id=1, name="herb", type="herb", amount=2),
                           dict(id=2, name="seed", type="seed", amount=1)])
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plot_count INTEGER,"
                    "plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,"
                    "plant_finish TEXT,harvest_settlement TEXT)")
        uow.execute("INSERT INTO dongfu_status VALUES('u',1,3,?,1,21001,?,?,?)",
                    (canonical_plant_slots(slots), slots[0]["plant_start"], slots[0]["plant_finish"],
                     json.dumps(snapshot)))
    install_operation_schema(game)
    return game, player, DongfuHarvestSqlRepository(game, player), snapshot


def projection(game, player):
    result = []
    for database in (game, player):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = uow.query_all("SELECT name FROM sqlite_master WHERE type='table' "
                                   "AND name NOT LIKE 'sqlite_%' ORDER BY name")
            result.append({row["name"]: uow.query_all('SELECT * FROM "' + row["name"] + '"')
                           for row in tables})
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


def settle(repo, snapshot):
    return repo.harvest("harvest", "u", snapshot["expected_slots"], snapshot["slot_numbers"],
                        snapshot["items"], 99, "2026-11-21 12:00:00")


# Only the harvest transaction is exercised; preparation remains a durable retry plan.
FAULTS = [
    ("player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END",
     "dongfu_harvest_state_write_rejected", "plant_slots"),
    ("player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET planting=1; END",
     "dongfu_harvest_state_projection_mismatch", "plant_slots"),
    ("game", "BEFORE INSERT ON back WHEN NEW.goods_id=2 BEGIN SELECT RAISE(IGNORE); END",
     "dongfu_harvest_inventory_write_rejected", "item:2"),
    ("game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END",
     "dongfu_harvest_inventory_write_rejected", "item:1"),
    ("game", "AFTER INSERT ON back WHEN NEW.goods_id=2 BEGIN UPDATE back SET bind_num=0 WHERE goods_id=2; END",
     "dongfu_harvest_inventory_projection_mismatch", "item:2"),
    ("game", "AFTER UPDATE ON back BEGIN UPDATE back SET goods_num=99 WHERE goods_id=1; END",
     "dongfu_harvest_inventory_projection_mismatch", "item:1"),
    ("game", "BEFORE INSERT ON dongfu_harvest_operations BEGIN SELECT RAISE(IGNORE); END",
     "dongfu_harvest_receipt_write_rejected", "receipt"),
    ("game", "AFTER INSERT ON dongfu_harvest_operations BEGIN UPDATE dongfu_harvest_operations SET rewards='[]'; END",
     "dongfu_harvest_receipt_projection_mismatch", "receipt"),
    ("game", "BEFORE INSERT ON domain_outbox BEGIN SELECT RAISE(IGNORE); END",
     "dongfu_harvest_outbox_write_rejected", "outbox"),
    ("game", "AFTER INSERT ON domain_outbox BEGIN UPDATE domain_outbox SET status='sent'; END",
     "dongfu_harvest_outbox_projection_mismatch", "outbox"),
    ("game", "AFTER INSERT ON domain_outbox BEGIN UPDATE back SET goods_num=99 WHERE goods_id=1; END",
     "dongfu_harvest_inventory_projection_mismatch", "item:1"),
    ("player", "AFTER UPDATE ON dongfu_status WHEN NEW.harvest_settlement='' "
     "BEGIN UPDATE dongfu_status SET plant_slots='[]'; END",
     "dongfu_harvest_state_projection_mismatch", "plant_slots"),
]


@pytest.mark.parametrize("database,trigger,reason,subject", FAULTS)
def test_refused_harvest_rolls_back_all_business_writes_and_can_retry(harvest, database, trigger, reason, subject):
    game, player, repo, snapshot = harvest
    target = player if database == "player" else game
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("CREATE TRIGGER fault " + trigger)
    before = projection(game, player)
    with pytest.raises(DomainError) as caught:
        settle(repo, snapshot)
    assert caught.value.code == reason
    assert caught.value.details["subject"] == subject
    assert projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER fault")
    assert settle(repo, snapshot).status == "harvested"
    after = projection(game, player)
    assert settle(repo, snapshot).status == "duplicate"
    assert projection(game, player) == after
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        assert [(row["goods_id"], row["goods_num"], row["bind_num"]) for row in
                uow.query_all("SELECT * FROM back ORDER BY goods_id")] == [(1, 12, 5), (2, 1, 1)]


@pytest.mark.parametrize("database,table,action", [
    ("player", "dongfu_status", "UPDATE"), ("game", "back", "INSERT"),
    ("game", "dongfu_harvest_operations", "INSERT"), ("game", "domain_outbox", "INSERT"),
])
def test_sqlite_abort_preserves_original_error_and_retry_plan(harvest, database, table, action):
    game, player, repo, snapshot = harvest
    target = player if database == "player" else game
    with DatabaseUnitOfWork(target) as uow:
        uow.execute(f"CREATE TRIGGER fault BEFORE {action} ON {table} "
                    "BEGIN SELECT RAISE(ABORT,'harvest refused'); END")
    before = projection(game, player)
    with pytest.raises(sqlite3.IntegrityError, match="harvest refused"):
        settle(repo, snapshot)
    assert projection(game, player) == before
    with DatabaseUnitOfWork(target) as uow:
        uow.execute("DROP TRIGGER fault")
    assert settle(repo, snapshot).status == "harvested"


@pytest.mark.parametrize("body,reason", [
    ("BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "harvest_snapshot_write_rejected"),
    ("AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET harvest_settlement='{}'; END",
     "harvest_snapshot_projection_mismatch"),
])
def test_snapshot_write_checks_persisted_plan_and_rolls_back(harvest, body, reason):
    game, player, repo, snapshot = harvest
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE dongfu_status SET harvest_settlement=''")
        uow.execute("CREATE TRIGGER fault " + body)
    before = projection(game, player)
    with pytest.raises(DomainError) as caught:
        repo.prepare_snapshot("harvest", "u", snapshot["expected_slots"], deepcopy(snapshot), seed_names={21001: "seed"})
    assert caught.value.code == reason
    assert caught.value.details["subject"] == "harvest_snapshot"
    assert projection(game, player) == before
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("DROP TRIGGER fault")
    assert repo.prepare_snapshot("harvest", "u", snapshot["expected_slots"], snapshot, seed_names={21001: "seed"}).status == "prepared"


@pytest.mark.parametrize("finish,expected", [
    ("2026-11-21 12:00:00", "harvested"),
    ("2026-11-21 12:00:00.000000", "harvested"),
    ("2026-11-21 12:00:00.000001", "not_mature"),
])
def test_maturity_compares_business_time_instead_of_timestamp_text(harvest, finish, expected):
    game, player, repo, snapshot = harvest
    snapshot["expected_slots"][0]["plant_finish"] = finish
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE dongfu_status SET plant_slots=?,plant_finish=?",
                    (json.dumps(snapshot["expected_slots"]), finish))
    before = projection(game, player)
    assert settle(repo, snapshot).status == expected
    if expected == "not_mature":
        assert projection(game, player) == before
