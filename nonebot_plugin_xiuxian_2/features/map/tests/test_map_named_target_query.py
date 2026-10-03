import __future__
import ast
import asyncio
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ...combat_settlement.application import CombatSettlementApplication
from ...combat_settlement.migrations import apply_dao_battle_operations, apply_dao_battle_record
from ..application import MapApplication
from ..repository import MapNearbyPlayersSqlQueryRepository


POSITION = {"realm": "realm", "heaven": "heaven", "node_id": "node"}


@pytest.fixture
def nearby(tmp_path):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE map_status (user_id TEXT,realm TEXT,heaven TEXT,node_id TEXT)")
        uow.executemany(
            "INSERT INTO map_status VALUES (?,'realm','heaven','node')",
            [("me",), ("first",), ("second",), ("missing",)],
        )
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT,power INTEGER)")
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES (?,?,?,?)",
            [("me", "my-name", "actor-level", 10), ("first", "target", "first-level", 20),
             ("second", "target", "second-level", 30)],
        )
    return MapNearbyPlayersSqlQueryRepository(player, game), player, game


def find(repository, name="target", **kwargs):
    return repository.find(**POSITION, user_name=name, **kwargs)


def test_same_name_uses_first_map_row_and_preserves_public_fields(nearby):
    result = find(nearby[0])
    assert result.has_candidates
    assert result.target == {"user_id": "first", "user_name": "target", "level": "first-level", "power": 20}


def test_named_battle_excludes_self_but_record_lookup_allows_self(nearby):
    repository, _, game = nearby
    assert find(repository, "my-name").target["user_id"] == "me"
    result = find(repository, "my-name", exclude_user_id="me")
    assert result.has_candidates and result.target is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name='my-name' WHERE user_id='first'")
    assert find(repository, "my-name", exclude_user_id="me").target["user_id"] == "first"


def test_no_other_candidate_differs_from_missing_name_and_orphan_map_row(nearby):
    repository, _, game = nearby
    result = find(repository, "absent", exclude_user_id="me")
    assert result.has_candidates and result.target is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id<>'me'")
    result = find(repository, "my-name", exclude_user_id="me")
    assert not result.has_candidates and result.target is None


def test_later_duplicate_profile_can_match_and_ties_use_profile_rowid(nearby):
    repository, _, game = nearby
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name='other' WHERE user_id='first'")
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES ('first','target',?,?)",
            [("later-level", 40), ("last-level", 50)],
        )
    assert find(repository).target == {"user_id": "first", "user_name": "target", "level": "later-level", "power": 40}
    assert find(repository, "other").target["power"] == 20


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
def test_all_position_dimensions_match(nearby, field):
    repository, player, _ = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(f"UPDATE map_status SET {field}='elsewhere' WHERE user_id='first'")
    assert find(repository).target["user_id"] == "second"


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
@pytest.mark.parametrize("empty", (None, "", 0))
def test_incomplete_position_returns_no_candidate(nearby, field, empty):
    result = nearby[0].find(**(POSITION | {field: empty}), user_name="target")
    assert not result.has_candidates and result.target is None


def test_exact_name_binding_and_nocase_column(nearby):
    repository, _, game = nearby
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("DROP TABLE user_xiuxian")
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT COLLATE NOCASE,level TEXT,power INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','TARGET','level',1)")
    assert find(repository).target is None
    assert find(repository, "TARGET").target["user_id"] == "first"
    name = "' OR 1=1 --"
    assert find(repository, name).target is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name=?", (name,))
    assert find(repository, name).target["user_name"] == name


def test_blank_names_still_count_as_other_candidates_and_null_fields_default(nearby):
    repository, _, game = nearby
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name=NULL WHERE user_id<>'me'")
    result = find(repository, exclude_user_id="me")
    assert result.has_candidates and result.target is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name='',level=NULL,power=NULL WHERE user_id<>'me'")
    assert find(repository, exclude_user_id="me").has_candidates
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET user_name='target' WHERE user_id='first'")
    assert find(repository).target == {"user_id": "first", "user_name": "target", "level": "", "power": 0}


def test_numeric_zero_name_keeps_legacy_false_default(nearby):
    repository, _, game = nearby
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("DROP TABLE user_xiuxian")
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name,level TEXT,power INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('first',0,'level',1)")
    result = find(repository, "0")
    assert result.has_candidates and result.target is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','0','text-level',2)")
    assert find(repository, "0").target["level"] == "text-level"


def test_large_text_power_uses_python_integer_not_sql_cast(nearby):
    repository, _, game = nearby
    power = 10 ** 80
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("DROP TABLE user_xiuxian")
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT,power TEXT)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','target','level',?)", (str(power),))
    assert find(repository).target["power"] == power


def test_cast_id_equality_keeps_distinct_leading_zero_ids(tmp_path):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE map_status (user_id INTEGER,realm TEXT,heaven TEXT,node_id TEXT)")
        uow.execute("INSERT INTO map_status VALUES (2,'realm','heaven','node')")
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT,power INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('02','target','level',1)")
    repository = MapNearbyPlayersSqlQueryRepository(player, game)
    assert not find(repository).has_candidates
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('2','target','level',1)")
    assert find(repository).target["user_id"] == "2"
    assert not find(repository, exclude_user_id="2").has_candidates
    assert find(repository, exclude_user_id="02").target["user_id"] == "2"


def test_self_exclusion_is_python_exact_even_with_nocase_uid(nearby):
    repository, player, game = nearby
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("DROP TABLE map_status")
        uow.execute("CREATE TABLE map_status (user_id TEXT COLLATE NOCASE,realm TEXT,heaven TEXT,node_id TEXT)")
        uow.execute("INSERT INTO map_status VALUES ('ME','realm','heaven','node')")
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('ME','target','level',1)")
    assert find(repository, exclude_user_id="me").target["user_id"] == "ME"
    assert not find(repository, exclude_user_id="ME").has_candidates


@pytest.mark.parametrize("name", (None, ""))
def test_empty_requested_name_does_not_open_database(tmp_path, name):
    repository = MapNearbyPlayersSqlQueryRepository(tmp_path / "player.db", tmp_path / "game.db")
    result = find(repository, name)
    assert result.target is None and not result.has_candidates
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("missing", ("game", "player", "both"))
def test_missing_databases_are_not_created(tmp_path, missing):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    for label, path in (("game", game), ("player", player)):
        if missing not in (label, "both"):
            with sqlite3.connect(path):
                pass
    before = set(tmp_path.iterdir())
    result = find(MapNearbyPlayersSqlQueryRepository(player, game))
    assert result.target is None and not result.has_candidates
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize("broken", ("map_table", "map_column", "profile_table", "profile_column"))
def test_missing_schema_fails_closed_without_repairs(nearby, broken):
    repository, player, game = nearby
    path, table = (player, "map_status") if broken.startswith("map") else (game, "user_xiuxian")
    with DatabaseUnitOfWork(path) as uow:
        uow.execute(f"DROP TABLE {table}")
        if broken.endswith("column"):
            uow.execute(f"CREATE TABLE {table} (user_id TEXT)")
    before = path.read_bytes()
    result = find(repository)
    assert result.target is None and not result.has_candidates
    assert path.read_bytes() == before


def test_corrupted_game_database_is_not_repaired(nearby):
    repository, _, game = nearby
    game.write_bytes(b"not a database")
    result = find(repository)
    assert result.target is None and not result.has_candidates
    assert game.read_bytes() == b"not a database"


@pytest.mark.parametrize("failure", ("presence", "power"))
def test_query_or_selected_power_errors_close_all_connections(nearby, monkeypatch, failure):
    repository, _, game = nearby
    if failure == "power":
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("UPDATE user_xiuxian SET power='broken' WHERE user_id='first'")
    opened = []
    enter, execute = DatabaseUnitOfWork.__enter__, DatabaseUnitOfWork.execute

    def tracked_enter(uow):
        value = enter(uow)
        opened.append(uow)
        return value

    def tracked_execute(uow, sql, params=()):
        if failure == "presence" and sql.startswith("SELECT 1 AS present"):
            raise RuntimeError("presence unavailable")
        return execute(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "__enter__", tracked_enter)
    monkeypatch.setattr(DatabaseUnitOfWork, "execute", tracked_execute)
    result = find(repository, "absent" if failure == "presence" else "target")
    assert result.target is None and not result.has_candidates
    assert len(opened) == 1 and opened[0].read_only and opened[0].connection is None


def test_read_only_single_row_queries_and_paths_with_spaces(nearby, tmp_path, monkeypatch):
    _, player, game = nearby
    folder = tmp_path / "db space"
    folder.mkdir()
    new_player, new_game = folder / "player db.sqlite", folder / "game db.sqlite"
    shutil.copyfile(player, new_player)
    shutil.copyfile(game, new_game)
    repository = MapNearbyPlayersSqlQueryRepository(new_player, new_game)
    statements = []
    execute = DatabaseUnitOfWork.execute

    def tracked(uow, sql, params=()):
        assert uow.read_only
        statements.append((sql, params))
        return execute(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", tracked)
    monkeypatch.setattr(DatabaseUnitOfWork, "query_all", Mock(side_effect=AssertionError("no full candidate list")))
    assert find(repository).target["user_id"] == "first"
    assert find(repository, "absent", exclude_user_id="me").has_candidates
    selects = [sql for sql, _ in statements if sql.startswith("SELECT")]
    assert len(selects) == 3 and all(sql.endswith("LIMIT 1") for sql in selects)
    attach = [params[0] for sql, params in statements if sql.startswith("ATTACH")]
    assert all(uri.endswith("?mode=ro") and "%20" in uri for uri in attach)
    assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE")) for sql, _ in statements)


def test_application_delegates_to_bounded_find(nearby, monkeypatch):
    _, player, game = nearby
    value = SimpleNamespace(target={"user_id": "target"}, has_candidates=True)
    query = Mock(return_value=value)
    monkeypatch.setattr(MapNearbyPlayersSqlQueryRepository, "find", query)
    application = MapApplication(game, player)
    assert application.nearby_target(**POSITION, user_name="name", exclude_user_id="me") is value
    query.assert_called_once_with(**POSITION, user_name="name", exclude_user_id="me")


def load_handler(command, result):
    source = (Path(__file__).resolve().parents[3] / "xiuxian/xiuxian_map/__init__.py").read_text(encoding="utf-8")
    function = next(
        node for node in ast.parse(source).body
        if isinstance(node, ast.AsyncFunctionDef) and any(
            isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
            and isinstance(decorator.func.value, ast.Name) and decorator.func.value.id == command
            for decorator in node.decorator_list
        )
    )
    function.decorator_list = []
    application = SimpleNamespace(nearby_target=Mock(return_value=result))
    battle = SimpleNamespace(
        settle_dao_battle=Mock(return_value=SimpleNamespace(ok=True)),
        get_dao_record=Mock(return_value={"total": 1, "win": 1, "lose": 0}),
    )
    user = {"user_id": "me", "user_name": "my-name", "power": 10}
    environment = dict(
        CommandArg=lambda: None, assign_bot=AsyncMock(return_value=("bot", None)),
        check_user=Mock(return_value=(True, user, "")), _load_map_data=Mock(return_value={}),
        _get_player_map_status=Mock(return_value=POSITION), map_application=application,
        _get_all_in_same_node=Mock(side_effect=AssertionError("named command must not read a list")),
        handle_send=AsyncMock(), dao_battle_application=battle,
        _map_operation_id=Mock(return_value="operation"), runtime_random=SimpleNamespace(random=Mock(return_value=0)),
    )
    exec(compile(ast.Module(body=[function], type_ignores=[]), "map-handler", "exec", flags=__future__.annotations.compiler_flag), environment)
    return environment


@pytest.mark.parametrize("command,exclude", (("dao_qc", "me"), ("dao_view", None)))
def test_named_handlers_call_query_not_list_and_keep_settlement_boundary(command, exclude):
    target = {"user_id": "first", "user_name": "target", "power": 20}
    env = load_handler(command, SimpleNamespace(target=target, has_candidates=True))
    asyncio.run(env["_"]("bot", object(), SimpleNamespace(extract_plain_text=lambda: "target")))
    env["map_application"].nearby_target.assert_called_once_with(**POSITION, user_name="target", exclude_user_id=exclude)
    env["_get_all_in_same_node"].assert_not_called()
    battle = env["dao_battle_application"]
    if command == "dao_qc":
        request = battle.settle_dao_battle.call_args.kwargs
        assert request["target_id"] == "first" and request["challenger_id"] == "me"
        assert request["expected_position"] is POSITION and request["operation_id"] == "operation"
    else:
        battle.get_dao_record.assert_called_once_with("first")


@pytest.mark.parametrize("has_candidates", (False, True))
def test_named_battle_retains_empty_vs_missing_name_messages(has_candidates):
    env = load_handler("dao_qc", SimpleNamespace(target=None, has_candidates=has_candidates))
    asyncio.run(env["_"]("bot", object(), SimpleNamespace(extract_plain_text=lambda: "absent")))
    message = env["handle_send"].await_args.args[2]
    assert message == ("附近未找到道友【absent】" if has_candidates else "附近无可切磋道友。")
    env["dao_battle_application"].settle_dao_battle.assert_not_called()


def test_record_lookup_without_name_does_not_query_nearby():
    env = load_handler("dao_view", None)
    asyncio.run(env["_"]("bot", object(), SimpleNamespace(extract_plain_text=lambda: "")))
    env["map_application"].nearby_target.assert_not_called()
    env["_get_all_in_same_node"].assert_not_called()
    env["dao_battle_application"].get_dao_record.assert_called_once_with("me")


def test_record_missing_name_does_not_read_record():
    env = load_handler("dao_view", SimpleNamespace(target=None, has_candidates=False))
    asyncio.run(env["_"]("bot", object(), SimpleNamespace(extract_plain_text=lambda: "absent")))
    env["dao_battle_application"].get_dao_record.assert_not_called()
    assert env["handle_send"].await_args.args[2] == "附近未找到道友【absent】"


def test_random_battle_and_nearby_display_keep_original_list_paths():
    target = {"user_id": "first", "user_name": "target", "level": "level", "power": 20}
    for command in ("dao_qc", "nearby_users_cmd"):
        env = load_handler(command, None)
        env["_get_all_in_same_node"] = Mock(return_value=[target])
        env["runtime_random"].choice = Mock(return_value=target)
        args = ["bot", object()]
        if command == "dao_qc":
            args.append(SimpleNamespace(extract_plain_text=lambda: ""))
        asyncio.run(env["_"](*args))
        env["map_application"].nearby_target.assert_not_called()
        env["_get_all_in_same_node"].assert_called_once_with("realm", "heaven", "node")
        if command == "dao_qc":
            env["runtime_random"].choice.assert_called_once_with([target])


@pytest.mark.parametrize("drift", (False, True))
def test_named_handler_real_query_settlement_replay_and_position_recheck(nearby, drift):
    _, player, game = nearby
    with DatabaseUnitOfWork(player) as uow:
        apply_platform_schema(uow)
        apply_dao_battle_operations(uow)
        apply_dao_battle_record(uow)
    env = load_handler("dao_qc", None)
    application = MapApplication(game, player)
    env["map_application"] = application
    env["dao_battle_application"] = CombatSettlementApplication(game, player)
    if drift:
        query = application.nearby_target

        def moved(**kwargs):
            result = query(**kwargs)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("UPDATE map_status SET node_id='elsewhere' WHERE user_id='first'")
            return result

        application.nearby_target = moved
    arguments = ("bot", object(), SimpleNamespace(extract_plain_text=lambda: "target"))
    asyncio.run(env["_"](*arguments))
    if not drift:
        asyncio.run(env["_"](*arguments))
    with DatabaseUnitOfWork(player, read_only=True) as uow:
        records = uow.query_all("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id")
        operations = uow.query_one("SELECT COUNT(*) AS count FROM map_dao_battle_operations")
    if drift:
        assert not records and operations["count"] == 0
        assert env["handle_send"].await_args.args[2] == "对方已离开当前节点，论道未结算。"
    else:
        assert records == [
            {"user_id": "first", "total": 1, "win": 0, "lose": 1},
            {"user_id": "me", "total": 1, "win": 1, "lose": 0},
        ]
        assert operations["count"] == 1
