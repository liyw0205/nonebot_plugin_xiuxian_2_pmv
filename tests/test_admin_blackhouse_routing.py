from __future__ import annotations

import __future__
import ast
import asyncio
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.admin.application import AdminApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_admin_blackhouse_status import prepare_database


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
ADMIN = ROOT / "xiuxian_admin/__init__.py"


def _load_functions(path, names, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and node.name in names]
    assert {node.name for node in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


@pytest.fixture
def application(tmp_path):
    database = tmp_path / "game.db"
    prepare_database(database)
    return AdminApplication(database)


def _facade(application):
    return _load_functions(ROOT / "blackhouse.py", (
        "_application", "_normalize_user_id", "is_user_blackhoused",
        "list_blackhoused_users", "ban_user", "unban_user",
    ), {
        "AdminApplication": AdminApplication,
        "get_paths": lambda: SimpleNamespace(game_db=application.database),
        "logger": Mock(), "uuid4": uuid4,
    })


def _route(application, *, lookup=None):
    admin = type("AdminMatcher", (), {"module_name": "plugin.xiuxian_admin"})
    game = type("GameMatcher", (), {"module_name": "plugin.xiuxian_base"})
    unregistered = type("UnregisteredMatcher", (), {"module_name": "other.plugin"})
    namespace = _load_functions(ROOT / "on_compat.py", (
        "_event_user_id", "_filter_blackhoused_matchers",
        "_matcher_exempt_command_disable", "_xiuxian_submodule_from_matcher",
    ), {
        "_MATCHER_ROUTES": {admin: object(), game: object()},
        "_COMMAND_SUBMODULES": {}, "_COMMAND_DISABLE_EXEMPT_SUBMODULE": "xiuxian_admin",
        "is_user_blackhoused": lookup or _facade(application)["is_user_blackhoused"],
        "logger": Mock(),
    })
    return namespace["_filter_blackhoused_matchers"], (admin, game, unregistered)


def _handler(application, name, *, message_id="request"):
    output = []

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def handle_send(bot, event, message):
        output.append(message)

    def find_user(field, value):
        with DatabaseUnitOfWork(application.database, read_only=True) as uow:
            return uow.query_one(
                f"SELECT user_id,user_name FROM user_xiuxian WHERE {field}=?", (value,)
            )

    namespace = _load_functions(ADMIN, (
        name, "_resolve_blackhouse_target", "_blackhouse_storage_error",
    ), {
        "re": re, "logger": Mock(), "CommandArg": lambda: None,
        "admin_application": application,
        "assign_bot": assign_bot, "handle_send": handle_send,
        "get_at_user_id": lambda args: args.at,
        "get_user_id": lambda event: event.get_user_id(),
        "_admin_operation_id": lambda event, action, uid: f"{message_id}:{action}:{uid}",
        "_sql_message": lambda: SimpleNamespace(
            get_user_info_with_id=lambda uid: find_user("user_id", uid),
            get_user_info_with_name=lambda name: find_user("user_name", name),
        ),
    })
    event = SimpleNamespace(get_user_id=lambda: "admin")

    def run(text="", *, at=None):
        args = SimpleNamespace(extract_plain_text=lambda: text, at=at)
        parameters = (object(), event) if name == "view_blackhouse_" else (object(), event, args)
        asyncio.run(namespace[name](*parameters))
        return output[-1]

    return run


@pytest.mark.parametrize("user_id", ["free", "guest"])
def test_real_handlers_and_router_share_registered_and_unregistered_state(application, user_id):
    route, candidates = _route(application)
    event = SimpleNamespace(get_user_id=lambda: user_id)
    assert route(list(candidates), event) == list(candidates)

    message = _handler(application, "blackhouse_")(at=user_id)
    assert "已被关入小黑屋" in message
    assert route(list(candidates), event) == [candidates[0], candidates[2]]
    assert user_id in _handler(application, "view_blackhouse_")()

    restarted = AdminApplication(application.database)
    restarted_route, restarted_candidates = _route(restarted)
    assert restarted.is_user_blackhoused(user_id)
    assert restarted_route(list(restarted_candidates), event) == [
        restarted_candidates[0], restarted_candidates[2]
    ]

    assert "已从小黑屋释放" in _handler(restarted, "unblackhouse_")(at=user_id)
    assert route(list(candidates), event) == list(candidates)
    assert user_id not in _handler(application, "view_blackhouse_")()
    with DatabaseUnitOfWork(application.database, read_only=True) as uow:
        player = uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id=?", (user_id,))
    assert player == ({"is_ban": 0} if user_id == "free" else None)


def test_replayed_ban_after_unban_neither_rebans_nor_claims_current_ban(application):
    ban = _handler(application, "blackhouse_", message_id="ban-once")
    assert "已被关入小黑屋" in ban("free")
    assert "已从小黑屋释放" in _handler(application, "unblackhouse_", message_id="release")("free")
    duplicate = ban("free")
    assert "已处理" in duplicate
    assert "已在小黑屋中" not in duplicate and "已被关入" not in duplicate
    assert not application.is_user_blackhoused("free")
    assert "free" not in _handler(application, "view_blackhouse_")()


def test_replayed_unban_after_ban_neither_releases_nor_claims_current_freedom(application):
    unban = _handler(application, "unblackhouse_", message_id="unban-once")
    assert "已从小黑屋释放" in unban("banned")
    assert "已被关入小黑屋" in _handler(application, "blackhouse_", message_id="reban")("banned")
    duplicate = unban("banned")
    assert "已处理" in duplicate
    assert "当前未被封禁" not in duplicate and "已从小黑屋释放" not in duplicate
    assert application.is_user_blackhoused("banned")


def test_name_resolution_and_invalid_targets_preserve_existing_handler_behavior(application, monkeypatch):
    assert "Free 已被关入" in _handler(application, "blackhouse_")("Free")
    assert application.is_user_blackhoused("free")
    snapshot = Mock(wraps=application.blackhouse_snapshot)
    monkeypatch.setattr(application, "blackhouse_snapshot", snapshot)
    assert "未找到目标用户" in _handler(application, "blackhouse_")("bad!")
    snapshot.assert_not_called()


@pytest.mark.parametrize("name", ["blackhouse_", "unblackhouse_"])
@pytest.mark.parametrize("status", ["failed", "state_changed", "operation_conflict", "schema_missing"])
def test_failed_write_results_are_never_reported_as_success(application, monkeypatch, name, status):
    failed = SimpleNamespace(
        status=status, succeeded=False, changed=False,
        action="ban" if name == "blackhouse_" else "unban",
        previous_banned=False, final_banned=False,
    )
    monkeypatch.setattr(application, "set_blackhouse_status", Mock(return_value=failed))
    message = _handler(application, name)("free")
    assert message
    for success in ("已被关入", "已在小黑屋中", "已从小黑屋释放", "当前未被封禁"):
        assert success not in message
    assert not application.is_user_blackhoused("free")


@pytest.mark.parametrize("name", ["blackhouse_", "unblackhouse_", "view_blackhouse_"])
@pytest.mark.parametrize("error", [RuntimeError("schema_missing"), OSError("storage")])
def test_storage_errors_produce_explicit_failure_without_success(application, monkeypatch, name, error):
    method = "list_blackhoused_users" if name == "view_blackhouse_" else "blackhouse_snapshot"
    monkeypatch.setattr(application, method, Mock(side_effect=error))
    message = _handler(application, name)("free")
    assert "未就绪" in message if str(error) == "schema_missing" else "异常" in message
    for success in ("已被关入", "已从小黑屋释放", "当前未被封禁", "空空如也"):
        assert success not in message


def test_admin_and_unrouted_candidates_skip_lookup_and_remain_available(application):
    lookup = Mock(side_effect=AssertionError("unnecessary database read"))
    route, (admin, game, unregistered) = _route(application, lookup=lookup)
    event = SimpleNamespace(get_user_id=lambda: "banned")
    for candidates in ([], [admin], [unregistered], [admin, unregistered]):
        assert route(candidates, event) == candidates
    assert route([game], SimpleNamespace(get_user_id=lambda: "")) == [game]
    lookup.assert_not_called()


def test_storage_failure_blocks_only_routed_nonadmin_matchers(tmp_path):
    database = tmp_path / "missing.db"
    route, candidates = _route(AdminApplication(database))
    assert route(list(candidates), SimpleNamespace(get_user_id=lambda: "free")) == [
        candidates[0], candidates[2]
    ]
    assert not database.exists()


def test_legacy_facade_writes_are_visible_to_application_and_route(application):
    facade = _facade(application)
    assert facade["ban_user"]("guest", name="Guest", reason="test") == "banned"
    assert application.is_user_blackhoused("guest")
    assert facade["list_blackhoused_users"]() == application.list_blackhoused_users()
    route, candidates = _route(application)
    event = SimpleNamespace(get_user_id=lambda: "guest")
    assert route(list(candidates), event) == [candidates[0], candidates[2]]
    assert facade["unban_user"]("guest") == "unbanned"
    assert route(list(candidates), event) == list(candidates)
    assert facade["is_user_blackhoused"](None) is False
