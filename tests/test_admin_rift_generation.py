from __future__ import annotations

import __future__
import ast
import asyncio
import datetime
import hashlib
import json
import os
import random
from pathlib import Path
from threading import RLock
from types import SimpleNamespace
from unittest.mock import Mock

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.rift.application import RiftApplication
from nonebot_plugin_xiuxian_2.features.rift.generation_repository import RiftGenerationSqlRepository
from nonebot_plugin_xiuxian_2.features.rift.tests import test_generation_repository as generation_fixtures
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
RIFT = ROOT / "xiuxian_rift"


def _load_nodes(path, names, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
             and node.name in names]
    assert {node.name for node in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


@pytest.fixture
def flow(tmp_path):
    return _flow(tmp_path)


def _flow(tmp_path, *, migration=True):
    database = tmp_path / "game.db"
    generation_fixtures.TestRiftGenerationRepository._prepare(database, migration=migration)
    application = RiftApplication(database, tmp_path / "player.db")
    output = []
    logger = Mock()

    async def assign_bot(**kwargs):
        return kwargs["bot"], "group"

    async def send(bot, event, message, **kwargs):
        output.append(message)

    rift_type = _load_nodes(RIFT / "riftmake.py", ("Rift",), {})["Rift"]
    json_store = _load_nodes(ROOT / "xiuxian_utils/json_store.py", ("_path_lock", "save_json_file"), {
        "Path": Path, "os": os, "json": json, "RLock": RLock,
        "_LOCKS_GUARD": RLock(), "_PATH_LOCKS": {},
    })
    projection_namespace = _load_nodes(RIFT / "old_rift_info.py", ("OLD_RIFT_INFO", "MyEncoder"), {
        "json": json, "datetime": datetime, "save_json_file": json_store["save_json_file"],
    })
    projection_type = projection_namespace["OLD_RIFT_INFO"]
    projection = projection_type.__new__(projection_type)
    projection.data_path = tmp_path / "rift_info.json"
    projection.data = {}
    namespace = _load_nodes(RIFT / "__init__.py", (
        "create_rift", "_event_id", "_build_fixed_rift", "_rift_world_snapshot",
        "_rift_from_world_state", "_sync_world_projection", "_generation_outcome",
        "_normalise_rift_target_node", "get_rift_target_nodes", "format_rift_target_nodes",
        "assign_rift_trial_node", "build_rift_data", "build_rift_appear_msg",
    ), {
        "__package__": "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift",
        "SimpleNamespace": SimpleNamespace, "hashlib": hashlib, "random": random,
        "Rift": rift_type, "GLOBAL_RIFT_KEY": "global", "group_rift": {},
        "rift_application": application, "old_rift_info": projection,
        "runtime_ids": SimpleNamespace(new_id=Mock(return_value="generated-id")),
        "get_rift_type": lambda: "alpha",
        "config": {"rift": {"alpha": {"rank": 1, "time": 60}, "beta": {"rank": 2, "time": 90}}},
        "get_random_trial_nodes_by_realm": lambda: [{
            "realm": "realm", "heaven": "heaven", "node_id": "trial", "node_name": "Trial",
        }],
        "get_random_trial_node": lambda: None,
        "assign_bot": assign_bot, "handle_send": send, "logger": logger,
    })
    admin = _load_nodes(ROOT / "xiuxian_admin/__init__.py", ("create_new_rift_",), {
        "create_rift": namespace["create_rift"], "assign_bot": assign_bot,
    })

    def run(message_id="first"):
        output.clear()
        asyncio.run(admin["create_new_rift_"](object(), SimpleNamespace(message_id=message_id)))
        assert len(output) == 1
        return output[0]

    return SimpleNamespace(
        run=run, namespace=namespace, application=application, database=database,
        projection=projection, logger=logger,
        current=lambda: RiftGenerationSqlRepository(database).get_current("global"),
        json=lambda: json.loads(projection.data_path.read_text(encoding="utf-8")),
    )


def _generations(flow):
    with DatabaseUnitOfWork(flow.database, read_only=True) as uow:
        return uow.query_one("SELECT COUNT(*) AS count FROM rift_generation_operations")["count"]


def test_real_admin_entry_first_generation_updates_sql_memory_and_actual_json_projection(flow):
    random_state = random.getstate()
    message = flow.run()
    current = flow.current()
    assert "野生的alpha" in message
    assert current["generation_id"] == "rift-generation:manual:first"
    assert current["revision"] == 1 and current["participants"] == ()
    assert flow.namespace["group_rift"]["global"].name == "alpha"
    assert flow.json()["global"]["name"] == "alpha"
    assert flow.json()["global"]["target_nodes"][0]["node_id"] == "trial"
    assert _generations(flow) == 1
    assert random.getstate() == random_state


def test_new_operation_preserves_existing_owner_replacement_semantics(flow):
    flow.run("old")
    flow.namespace["get_rift_type"] = lambda: "beta"
    message = flow.run("new")
    assert "野生的beta" in message
    assert flow.current()["generation_id"] == "rift-generation:manual:new"
    assert flow.current()["revision"] == 2
    assert flow.json()["global"]["name"] == "beta"
    assert _generations(flow) == 2


def test_replay_uses_current_participants_instead_of_historical_ledger_snapshot(flow):
    flow.run()
    with DatabaseUnitOfWork(flow.database) as uow:
        uow.execute("UPDATE rift_world_state SET participants=?,revision=revision+1", ('["member"]',))
    message = flow.run()
    assert "未重复生成" in message
    assert flow.current()["revision"] == 2
    assert flow.json()["global"]["l_user_id"] == ["member"]
    assert flow.namespace["group_rift"]["global"].l_user_id == ["member"]
    assert _generations(flow) == 1


def test_replayed_old_generation_does_not_overwrite_new_projection(flow):
    flow.run("old")
    flow.namespace["get_rift_type"] = lambda: "beta"
    flow.run("new")
    original = flow.projection.data_path.read_bytes()
    flow.namespace["get_rift_type"] = lambda: "alpha"
    message = flow.run("old")
    assert "已更新或结束" in message and "野生的alpha" not in message
    assert flow.current()["generation_id"] == "rift-generation:manual:new"
    assert flow.namespace["group_rift"]["global"].name == "beta"
    assert flow.projection.data_path.read_bytes() == original
    assert _generations(flow) == 2


def test_replayed_generation_cannot_resurrect_terminated_projection(flow):
    flow.run()
    with DatabaseUnitOfWork(flow.database) as uow:
        uow.execute("DELETE FROM rift_world_state WHERE rift_key='global'")
    flow.namespace["group_rift"].clear()
    flow.projection.save_rift({})
    message = flow.run()
    assert "已更新或结束" in message
    assert flow.current() is None and flow.json() == {}
    assert flow.namespace["group_rift"] == {}
    assert _generations(flow) == 1


def test_conflicting_same_request_never_claims_generation_or_changes_state(flow):
    flow.run()
    original = flow.projection.data_path.read_bytes()
    flow.namespace["config"]["rift"]["alpha"]["rank"] = 3
    message = flow.run()
    assert "请求冲突" in message and "野生的" not in message
    assert flow.current()["rift_data"]["rank"] == 1
    assert flow.projection.data_path.read_bytes() == original
    assert _generations(flow) == 1


def test_missing_schema_and_its_replay_are_explicit_without_rift_request_ddl(tmp_path):
    flow = _flow(tmp_path, migration=False)
    for _ in range(2):
        message = flow.run()
        assert "尚未就绪" in message and "野生的" not in message
    assert not flow.projection.data_path.exists()
    with DatabaseUnitOfWork(flow.database, read_only=True) as uow:
        assert uow.query_all("SELECT name FROM sqlite_master WHERE name LIKE 'rift_%'") == []


def test_receipt_storage_failure_rolls_back_world_without_success_reply(flow):
    with DatabaseUnitOfWork(flow.database) as uow:
        uow.execute(
            "CREATE TRIGGER reject_generation BEFORE INSERT ON rift_generation_operations "
            "BEGIN SELECT RAISE(ABORT,'private-storage-detail'); END"
        )
    message = flow.run()
    assert "生成请求未确认" in message and "野生的" not in message
    assert "private-storage-detail" not in message
    assert flow.current() is None and _generations(flow) == 0
    assert not flow.projection.data_path.exists()
    assert "private-storage-detail" not in str(flow.logger.mock_calls)


def test_post_commit_state_read_failure_reports_uncertainty_without_claiming_rollback(flow, monkeypatch):
    monkeypatch.setattr(flow.application, "current_world", Mock(side_effect=OSError("private-storage-detail")))
    message = flow.run()
    assert "请求已提交" in message and "读取失败" in message
    assert "野生的" not in message and "回滚" not in message
    assert flow.current()["generation_id"] == "rift-generation:manual:first"
    assert _generations(flow) == 1
    assert not flow.projection.data_path.exists()


def test_json_projection_failure_preserves_old_file_reports_committed_sql_and_can_repair(flow, monkeypatch):
    flow.run("old")
    original = flow.projection.data_path.read_bytes()
    flow.namespace["get_rift_type"] = lambda: "beta"
    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "replace", Mock(side_effect=OSError("private-projection-detail")))
        message = flow.run("new")
    assert "已在数据库生成" in message and "兼容缓存同步失败" in message
    assert "野生的" not in message and "回滚" not in message
    assert flow.current()["generation_id"] == "rift-generation:manual:new"
    assert flow.projection.data_path.read_bytes() == original
    assert flow.namespace["group_rift"]["global"].name == "beta"
    assert list(flow.projection.data_path.parent.glob(".*.tmp")) == []
    assert "private-projection-detail" not in str(flow.logger.mock_calls)

    repaired = flow.run("new")
    assert "未重复生成" in repaired
    assert flow.json()["global"]["name"] == "beta"
    assert _generations(flow) == 2
