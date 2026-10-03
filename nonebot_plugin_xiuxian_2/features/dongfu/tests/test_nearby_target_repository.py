from pathlib import Path
import shutil
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import DongfuApplication
from ..nearby_target_repository import DongfuNearbyTargetSqlQueryRepository


@pytest.fixture
def nearby(tmp_path):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE map_status (user_id TEXT,realm TEXT,heaven TEXT,node_id TEXT)")
        uow.executemany(
            "INSERT INTO map_status VALUES (?,?,?,?)",
            [("me", "realm", "heaven", "node"), ("first", "realm", "heaven", "node"),
             ("second", "realm", "heaven", "node"), ("missing", "realm", "heaven", "node")],
        )
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES (?,?)",
            [("me", "my-name"), ("first", "target"), ("second", "target")],
        )
    return DongfuNearbyTargetSqlQueryRepository(game, player), player, game


def test_same_name_selects_first_map_row_without_dongfu_eligibility_filter(nearby):
    repository, _, _ = nearby
    assert repository.get("me", "target") == {"user_id": "first", "user_name": "target"}
    assert repository.get("me", "my-name") == {"user_id": "me", "user_name": "my-name"}


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
def test_all_position_dimensions_must_match(nearby, field):
    repository, player, _ = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(f"UPDATE map_status SET {field}='elsewhere' WHERE user_id='first'")
    assert repository.get("me", "target")["user_id"] == "second"


def test_missing_profiles_are_skipped_and_game_first_row_is_selected_before_name(nearby):
    repository, _, game = nearby
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name='other-name' WHERE user_id='first'")
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','target')")
        uow.execute("DELETE FROM user_xiuxian WHERE user_id='second'")
    assert repository.get("me", "target") is None
    assert repository.get("me", "other-name")["user_id"] == "first"


def test_duplicate_map_rows_use_canonical_actor_first_row(nearby):
    repository, player, _ = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("INSERT INTO map_status VALUES ('me','other','heaven','node')")
    assert repository.get("me", "target")["user_id"] == "first"


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
@pytest.mark.parametrize("value", (None, ""))
def test_incomplete_actor_position_has_no_target(nearby, field, value):
    repository, player, _ = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(f"UPDATE map_status SET {field}=? WHERE user_id='me'", (value,))
    assert repository.get("me", "target") is None


@pytest.mark.parametrize("value", (0, "0", "null", "false", "[]"))
def test_legacy_decoded_false_position_has_no_target(nearby, value):
    repository, player, _ = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE map_status SET realm=?", (value,))
    assert repository.get("me", "target") is None


def test_legacy_json_encoded_actor_position_uses_decoded_equality(nearby):
    repository, player, _ = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE map_status SET realm=? WHERE user_id='me'", ('"realm"',))
    assert repository.get("me", "target")["user_id"] == "first"


@pytest.mark.parametrize("user_id,user_name", (("", "target"), ("absent", "target"), ("me", ""), ("me", "absent")))
def test_empty_or_missing_actor_and_name_return_none(nearby, user_id, user_name):
    repository, _, _ = nearby
    assert repository.get(user_id, user_name) is None


def test_quoted_name_is_bound_not_interpolated(nearby):
    repository, _, game = nearby
    name = "' OR 1=1 --"
    assert repository.get("me", name) is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name=? WHERE user_id='second'", (name,))
    assert repository.get("me", name) == {"user_id": "second", "user_name": name}


def test_profile_lookup_uses_legacy_string_id_not_numeric_column_coercion(tmp_path):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE map_status (user_id INTEGER,realm TEXT,heaven TEXT,node_id TEXT)")
        uow.executemany("INSERT INTO map_status VALUES (?,'r','h','n')", [(1,), (2,)])
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('02','target')")
    repository = DongfuNearbyTargetSqlQueryRepository(game, player)
    assert repository.get("1", "target") is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('2','target')")
    assert repository.get("1", "target")["user_id"] == "2"


def test_name_match_is_exact_even_with_legacy_nocase_column(nearby):
    repository, _, game = nearby
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("DROP TABLE user_xiuxian")
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT COLLATE NOCASE)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','TARGET')")
    assert repository.get("me", "target") is None
    assert repository.get("me", "TARGET")["user_id"] == "first"


def test_read_only_attachment_handles_spaces_in_paths(nearby, tmp_path):
    _, player, game = nearby
    folder = tmp_path / "db space"
    folder.mkdir()
    new_player, new_game = folder / "player db.sqlite", folder / "game db.sqlite"
    shutil.copyfile(player, new_player)
    shutil.copyfile(game, new_game)
    assert DongfuNearbyTargetSqlQueryRepository(new_game, new_player).get("me", "target")["user_id"] == "first"


def test_corrupted_game_database_returns_no_target_without_repair(nearby):
    repository, _, game = nearby
    game.write_bytes(b"not a database")
    assert repository.get("me", "target") is None
    assert game.read_bytes() == b"not a database"


@pytest.mark.parametrize("missing", ("game", "player", "both"))
def test_missing_databases_are_not_created(tmp_path, missing):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    for label, path in (("game", game), ("player", player)):
        if missing not in (label, "both"):
            with DatabaseUnitOfWork(path):
                pass
    before = set(tmp_path.iterdir())
    assert DongfuNearbyTargetSqlQueryRepository(game, player).get("me", "target") is None
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize("broken", ("map_table", "map_column", "profile_table", "profile_column"))
def test_missing_schema_is_not_repaired(nearby, broken):
    repository, player, game = nearby
    path = player if broken.startswith("map") else game
    table = "map_status" if broken.startswith("map") else "user_xiuxian"
    with DatabaseUnitOfWork(path) as uow:
        uow.execute(f"DROP TABLE {table}")
        if broken.endswith("column"):
            uow.execute(f"CREATE TABLE {table} (user_id TEXT)")
    assert repository.get("me", "target") is None
    with DatabaseUnitOfWork(path, read_only=True) as uow:
        exists = uow.query_one("SELECT name FROM sqlite_master WHERE name=?", (table,))
        assert bool(exists) == broken.endswith("column")
        if exists:
            assert [row["name"] for row in uow.query_all(f"PRAGMA table_info({table})")] == ["user_id"]


def test_reads_use_only_single_row_queries_and_read_only_attachment(nearby, monkeypatch):
    repository, _, _ = nearby
    statements = []
    execute = DatabaseUnitOfWork.execute

    def tracked(uow, sql, params=()):
        assert uow.read_only
        statements.append((sql, params))
        return execute(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", tracked)
    monkeypatch.setattr(DatabaseUnitOfWork, "query_all", Mock(side_effect=AssertionError("no bulk profiles")))
    assert repository.get("me", "target")["user_id"] == "first"
    selects = [sql for sql, _ in statements if sql.startswith("SELECT")]
    assert len(selects) == 2
    assert all(sql.endswith("LIMIT 1") for sql in selects)
    attachments = [params for sql, params in statements if sql.startswith("ATTACH")]
    assert len(attachments) == 1 and attachments[0][0].endswith("?mode=ro")
    assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE")) for sql, _ in statements)


def test_application_delegates_to_feature_query(tmp_path):
    target = {"user_id": "first", "user_name": "target"}
    repository = SimpleNamespace(nearby_target=Mock(return_value=target))
    app = DongfuApplication(tmp_path / "unused.db", repository=repository)
    assert app.nearby_target("me", "target") is target
    repository.nearby_target.assert_called_once_with("me", "target")
    assert not list(tmp_path.iterdir())


def test_infiltration_matcher_reads_one_target_then_checks_eligibility():
    source = (Path(__file__).resolve().parents[3] / "xiuxian/xiuxian_dongfu/__init__.py").read_text(encoding="utf-8")
    handler = source[source.index("@infiltrate_dongfu.handle"):]
    assert "dongfu_application.nearby_target(my_uid, tname)" in handler
    assert "_get_same_node_users" not in source
    assert "nearby_users" not in handler
    selection = handler.index("dongfu_application.nearby_target(")
    assert selection < handler.index("if target_uid == my_uid:") < handler.index("has, td = _has_dongfu(")
