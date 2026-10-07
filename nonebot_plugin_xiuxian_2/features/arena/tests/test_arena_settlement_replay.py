import json
import sqlite3

import pytest

from ..repository import ArenaChallengePurchaseSqlRepository


@pytest.fixture
def repository(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with sqlite3.connect(game) as connection:
        connection.execute("CREATE TABLE arena_challenge_settlement_operations(operation_id TEXT PRIMARY KEY,challenger_id TEXT,payload TEXT,result_json TEXT)")
    return ArenaChallengePurchaseSqlRepository(game, player)


def test_replay_missing_success_conflict(repository):
    assert repository.settlement_result("operation", "user") is None
    with sqlite3.connect(repository.game_database) as connection:
        connection.execute("INSERT INTO arena_challenge_settlement_operations VALUES(?,?,?,?)",
                           ("operation", "user", "[]", json.dumps({"status": "applied", "outcome": "win", "score_delta": 20})))
    replay = repository.settlement_result("operation", "user")
    assert (replay["status"], replay["outcome"]) == ("duplicate", "win")
    assert repository.settlement_result("operation", "other")["status"] == "operation_conflict"


@pytest.mark.parametrize("encoded,status", [
    ('{"status":"state_changed"}', "state_changed"),
    ('{"status":"limit_reached"}', "limit_reached"),
    ('{"outcome":"win","score_delta":20}', "operation_conflict"),
    ('{"status":null}', "operation_conflict"),
    ('{}', "operation_conflict"), ('[]', "operation_conflict"),
    ('null', "operation_conflict"), ('{', "operation_conflict"),
])
def test_rejected_or_ambiguous_receipt_is_never_success(repository, encoded, status):
    with sqlite3.connect(repository.game_database) as connection:
        connection.execute("INSERT INTO arena_challenge_settlement_operations VALUES(?,?,?,?)",
                           ("operation", "user", "[]", encoded))
    assert repository.settlement_result("operation", "user")["status"] == status


def test_receipt_lookup_does_not_create_missing_databases(tmp_path):
    game, player = tmp_path / "missing-game.db", tmp_path / "missing-player.db"
    repository = ArenaChallengePurchaseSqlRepository(game, player)
    with pytest.raises(sqlite3.OperationalError):
        repository.settlement_result("operation", "user")
    with pytest.raises(sqlite3.OperationalError):
        repository.purchase_result("operation", "user", 1, 1)
    assert not game.exists() and not player.exists()
