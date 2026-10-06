from __future__ import annotations

import __future__
import ast
import asyncio
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.admin.command_control_application import (
    AdminCommandControlApplication,
)
from nonebot_plugin_xiuxian_2.features.admin import command_control_application
from nonebot_plugin_xiuxian_2.features.admin.command_control_repository import (
    AdminCommandControlRepository,
)


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
CONTROLS = ROOT / "xiuxian_admin/command_controls.py"
REGISTRY = {"灵石": "xiuxian_base", "修仙签到": "xiuxian_base", "指令禁用": "xiuxian_admin"}
ALIASES = {"钱包": "灵石", "签到": "修仙签到", "管理禁用": "指令禁用"}


def _functions(path, namespace, names=None):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and (names is None or node.name in names)]
    if names is not None:
        assert {node.name for node in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


@pytest.fixture
def repository(tmp_path):
    repository = AdminCommandControlRepository(tmp_path / "command_disable.json")
    repository.sync_command_registry(REGISTRY)
    repository.rebuild_alias_index(ALIASES)
    return repository


def _compat(repository):
    return _functions(ROOT / "command_disable.py", {
        "AdminCommandControlApplication": lambda *args, **kwargs: AdminCommandControlApplication(repository),
        "COMMAND_DISABLE_EXEMPT_MODULE": "xiuxian_admin", "logger": Mock(),
        "get_paths": lambda: SimpleNamespace(data=repository.path.parent),
    })


class Finished(Exception):
    pass


def _handler(repository, name):
    output = []
    rebuild = Mock(side_effect=AssertionError("a flag change must not rebuild route indexes"))

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def send(bot, event, message, **kwargs):
        output.append((message, kwargs))

    async def finish():
        raise Finished

    namespace = _functions(CONTROLS, {
        **_compat(repository), "re": re, "logger": Mock(),
        "CommandArg": lambda: None, "assign_bot": assign_bot,
        "handle_send": send, "send_help_message": send,
        "build_pagination_buttons": lambda command, page, total: {"pagination": (command, page, total)},
        "rebuild_on_compat_index": rebuild,
        "cmd_disable": SimpleNamespace(finish=finish),
        "cmd_enable": SimpleNamespace(finish=finish),
        "cmd_list": SimpleNamespace(finish=finish),
    }, (name, "_parse_command_list_args"))

    def run(raw=""):
        args = SimpleNamespace(extract_plain_text=lambda: raw)
        try:
            asyncio.run(namespace[name](object(), object(), args))
        except Finished:
            pass
        rebuild.assert_not_called()
        assert len(output) == 1
        return output[0]

    return run


def _router(repository):
    admin = type("AdminMatcher", (), {"module_name": "plugin.xiuxian_admin"})
    game = type("GameMatcher", (), {"module_name": "plugin.xiuxian_base"})
    outside = type("OutsideMatcher", (), {"module_name": "other.plugin"})
    namespace = _functions(ROOT / "on_compat.py", {
        **_compat(repository), "logger": Mock(),
        "_COMMAND_DISABLE_EXEMPT_SUBMODULE": "xiuxian_admin",
        "_COMMAND_SUBMODULES": {admin: "xiuxian_admin", game: "xiuxian_base"},
        "_PRIMARY_COMMAND_NAMES": {admin: "指令禁用", game: "灵石"},
        "_MATCHER_ROUTES": {
            admin: SimpleNamespace(commands=(("指令禁用",), ("管理禁用",)), fullmatches=(), prefixes=()),
            game: SimpleNamespace(commands=(("灵石",), ("钱包",)), fullmatches=(), prefixes=()),
        },
    }, ("_filter_disabled_matchers", "_matcher_is_disabled",
        "_matcher_exempt_command_disable", "_xiuxian_submodule_from_matcher"))
    return namespace["_filter_disabled_matchers"], [admin, game, outside]


def test_default_application_and_compatibility_facade_share_configured_owner(repository, monkeypatch):
    monkeypatch.setattr(command_control_application, "get_paths", lambda: SimpleNamespace(data=repository.path.parent))
    facade = _functions(ROOT / "command_disable.py", {
        "AdminCommandControlApplication": AdminCommandControlApplication,
        "COMMAND_DISABLE_EXEMPT_MODULE": "xiuxian_admin",
    })
    assert facade["set_command_disabled"]("钱包", disabled=True) == (True, "")
    assert AdminCommandControlApplication().is_command_disabled("灵石")
    assert repository.is_command_disabled("钱包")


def test_real_handlers_deduplicate_primary_alias_and_module_and_apply_to_router_immediately(repository):
    route, candidates = _router(repository)
    assert route(candidates, set(candidates), ("钱包",), "钱包") == candidates
    with patch.object(repository, "_persist", wraps=repository._persist) as persist:
        message, _ = _handler(repository, "cmd_disable_")("灵石,钱包，xiuxian_base,签到,missing")
        assert message.count("灵石") == 1 and message.count("修仙签到") == 1
        assert "已禁用" in message and "未处理" in message and "missing" in message
        assert persist.call_count == 1
        assert route(candidates, set(candidates), ("钱包",), "钱包") == [candidates[0], candidates[2]]
        assert _compat(repository)["is_command_disabled"]("签到")

        _handler(repository, "cmd_disable_")("钱包,xiuxian_base")
        assert persist.call_count == 1
        enabled, _ = _handler(repository, "cmd_enable_")("钱包")
        assert "已解禁" in enabled
        assert persist.call_count == 2
        assert route(candidates, set(candidates), ("钱包",), "钱包") == candidates
    assert AdminCommandControlRepository(repository.path).is_command_disabled("签到")


@pytest.mark.parametrize("target", ["指令禁用", "管理禁用", "xiuxian_admin"])
def test_real_handler_cannot_disable_admin_by_name_alias_or_module(repository, target):
    before = repository.path.read_bytes()
    message, _ = _handler(repository, "cmd_disable_")(target)
    assert "已禁用：" not in message and "未处理" in message
    assert repository.path.read_bytes() == before
    assert not repository.is_command_disabled("指令禁用")


@pytest.mark.parametrize("target", ["指令禁用", "管理禁用"])
def test_web_compatible_single_setter_cannot_disable_admin(repository, target):
    ok, error = _compat(repository)["set_command_disabled"](target, disabled=True)
    assert not ok and error
    assert not repository.is_command_disabled("管理禁用")


def test_real_list_handler_filters_disabled_rows_and_clamps_page(repository):
    registry = {**REGISTRY, **{f"entry{index:02}": "xiuxian_test" for index in range(35)}}
    repository.sync_command_registry(registry)
    repository.apply_disable_targets("xiuxian_test", disabled=True)
    message, buttons = _handler(repository, "cmd_list_")("禁用 xiuxian_test 999")
    assert "第 2/2 页" in message and "共 35 条" in message
    assert "entry30" in message and "entry00" not in message
    assert "指令禁用" not in message and "修仙签到" not in message
    assert buttons["pagination"] == ("指令列表 禁用 xiuxian_test", 2, 2)
    missing, _ = _handler(repository, "cmd_list_")("not-a-command")
    assert "无匹配指令" in missing


def test_real_list_handler_handles_overlong_numeric_page(repository):
    message, buttons = _handler(repository, "cmd_list_")("9" * 5000)
    assert "第 1/1 页" in message
    assert buttons["pagination"] == ("指令列表", 1, 1)


def test_real_write_failure_preserves_file_cache_and_route_state(repository):
    original = repository.path.read_bytes()
    route, candidates = _router(repository)
    with patch("nonebot_plugin_xiuxian_2.infrastructure.filesystem.atomic.os.replace", side_effect=OSError("disk full")):
        message, _ = _handler(repository, "cmd_disable_")("xiuxian_base")
    assert "失败" in message and "已禁用：" not in message
    assert repository.path.read_bytes() == original
    assert not repository.is_command_disabled("灵石")
    assert not repository.is_command_disabled("修仙签到")
    assert route(candidates, set(candidates), ("钱包",), "钱包") == candidates
    assert list(repository.path.parent.glob(".*.tmp")) == []


@pytest.mark.parametrize("bad", ["{broken", "[]", '{"commands":[]}'])
def test_corrupt_file_is_not_replaced_and_router_fails_closed_except_admin_and_unrouted(repository, bad):
    repository.path.write_text(bad, encoding="utf-8")
    for handler in ("cmd_disable_", "cmd_enable_", "cmd_list_"):
        message, _ = _handler(repository, handler)("钱包")
        assert "失败" in message
        assert "已禁用：" not in message and "已解禁：" not in message
    assert repository.path.read_text(encoding="utf-8") == bad
    route, candidates = _router(repository)
    assert route(candidates, set(candidates), ("钱包",), "钱包") == [candidates[0], candidates[2]]


def test_unselected_admin_and_unrouted_candidates_do_not_read_store(repository):
    route, (admin, game, outside) = _router(repository)
    with patch.object(repository, "_document_view", side_effect=AssertionError("unnecessary read")) as read:
        for candidates, selected in (([], set()), ([admin], {admin}), ([outside], {outside}),
                                     ([admin, game, outside], set())):
            assert route(candidates, selected, ("钱包",), "钱包") == candidates
        read.assert_not_called()


def test_real_index_rebuild_preserves_flags_and_publishes_aliases_after_sync(repository):
    repository.set_command_disabled("灵石", disabled=True)
    aliases = {**ALIASES, "新钱包": "灵石"}

    class Provider:
        def __init__(self):
            self.rebuild = Mock()

    provider = Provider()
    namespace = _functions(ROOT / "on_compat.py", {
        **_compat(repository),
        "_collect_registered_command_registry": lambda: REGISTRY,
        "_collect_alias_to_primary_map": lambda: aliases,
        "matchers": SimpleNamespace(provider=provider),
        "XiuxianOnCompatProvider": Provider,
    }, ("rebuild_on_compat_index",))
    with patch.object(repository, "_persist", side_effect=AssertionError("unchanged registry write")):
        namespace["rebuild_on_compat_index"]()
    assert repository.is_command_disabled("新钱包")
    provider.rebuild.assert_called_once_with()

    aliases["未发布别名"] = "灵石"
    provider.rebuild.reset_mock()
    namespace["_collect_registered_command_registry"] = lambda: {**REGISTRY, "new": "xiuxian_test"}
    with patch.object(repository, "_persist", side_effect=OSError("disk full")):
        with pytest.raises(OSError, match="disk full"):
            namespace["rebuild_on_compat_index"]()
    assert "new" not in repository.known_commands()
    assert repository.resolve_primary_name("未发布别名") == "未发布别名"
    provider.rebuild.assert_not_called()


def test_corrupt_store_does_not_block_unselected_routed_matchers(repository):
    route, candidates = _router(repository)
    unselected = type("UnselectedMatcher", (), {"module_name": "plugin.xiuxian_base"})
    route.__globals__["_MATCHER_ROUTES"][unselected] = SimpleNamespace(
        commands=(("修仙签到",),), fullmatches=(), prefixes=()
    )
    candidates.append(unselected)
    repository.path.write_text("{broken", encoding="utf-8")
    assert route(candidates, {candidates[1]}, ("钱包",), "钱包") == [
        candidates[0], candidates[2], unselected,
    ]


def test_three_real_command_registrations_retain_superuser_permission():
    tree = ast.parse(CONTROLS.read_text(encoding="utf-8"))
    names = {"cmd_disable", "cmd_enable", "cmd_list"}
    registrations = [node for node in tree.body if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id in names for target in node.targets)]
    permission = object()
    calls = []

    def on_command(command, **kwargs):
        calls.append((command, kwargs))

    exec(compile(ast.Module(body=registrations, type_ignores=[]), str(CONTROLS), "exec"),
         {"SUPERUSER": permission, "on_command": on_command})
    assert {command for command, _ in calls} == {"指令禁用", "指令解禁", "指令列表"}
    assert all(kwargs["permission"] is permission and kwargs["block"] is True for _, kwargs in calls)
