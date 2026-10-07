from __future__ import annotations

import ast
import json
import sqlite3
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.tasks.application import TaskProgressApplication
from nonebot_plugin_xiuxian_2.features.tasks.migrations import apply_task_progress
from nonebot_plugin_xiuxian_2.features.tasks.progress import TasksProgressRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


ROOT = Path(__file__).resolve().parents[1]
TASK_COMMANDS = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tasks/__init__.py"
TASK_DATA = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tasks/task_data.py"


def test_task_command_handlers_keep_one_shared_owner_boundary() -> None:
    source = TASK_COMMANDS.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(TASK_COMMANDS))
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
    }

    expected_cycles = {"task_info_": None, "daily_task_": "daily", "weekly_task_": "weekly"}
    for handler, cycle in expected_cycles.items():
        calls = {
            node.func.attr
            for node in ast.walk(functions[handler])
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "task_manager"
        }
        assert "build_status_message" in calls
        call = next(
            node for node in ast.walk(functions[handler])
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "task_manager"
            and node.func.attr == "build_status_message"
        )
        if cycle is None:
            assert len(call.args) == 1
        else:
            assert len(call.args) == 2
            assert isinstance(call.args[1], ast.Constant)
            assert call.args[1].value == cycle

    claim_calls = {
        node.func.attr
        for node in ast.walk(functions["claim_task_"])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "task_manager"
    }
    assert "claim_rewards" in claim_calls

    data_tree = ast.parse(TASK_DATA.read_text(encoding="utf-8"), filename=str(TASK_DATA))
    build_status = next(
        node for node in ast.walk(data_tree)
        if isinstance(node, ast.FunctionDef) and node.name == "build_status_message"
    )
    read_calls = {
        node.func.attr
        for node in ast.walk(build_status)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Attribute)
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "self"
        and node.func.value.attr == "progress_application"
    }
    assert "read_states" in read_calls
    assert "get_states" not in read_calls

    claim_source = TASK_DATA.read_text(encoding="utf-8")
    assert "self.claim_application.claim_rewards(" in claim_source
    assert "TaskRewardClaimService" not in claim_source


def test_task_status_read_does_not_create_database_or_projection_row(tmp_path: Path) -> None:
    database = tmp_path / "missing-player.db"
    repository = TasksProgressRepository(database)
    states = repository.read_states("u", {"daily": "2026-10-08"})

    assert states == {"daily": ({}, [], "2026-10-08")}
    assert not database.exists()


def test_task_status_read_does_not_rollover_or_write_existing_projection(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_task_progress(uow)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "INSERT INTO xiuxian_tasks(user_id,daily_period,daily_progress,daily_claimed) "
            "VALUES(?,?,?,?)",
            ("u", "2026-10-07", json.dumps({"daily_sign": 1}), json.dumps(["daily_sign"])),
        )
        conn.commit()

    application = TaskProgressApplication(database)
    assert application.read_states("u", {"daily": "2026-10-08"}) == {
        "daily": ({}, [], "2026-10-08")
    }
    with sqlite3.connect(database) as conn:
        row = conn.execute(
            "SELECT daily_period,daily_progress,daily_claimed FROM xiuxian_tasks WHERE user_id='u'"
        ).fetchone()
    assert row == (
        "2026-10-07",
        json.dumps({"daily_sign": 1}),
        json.dumps(["daily_sign"]),
    )


def test_task_status_read_fails_closed_on_partial_legacy_schema(tmp_path: Path) -> None:
    database = tmp_path / "partial-player.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE xiuxian_tasks(user_id TEXT PRIMARY KEY)")
        conn.commit()

    assert TasksProgressRepository(database).read_states(
        "u", {"daily": "2026-10-08"}
    ) == {"daily": ({}, [], "2026-10-08")}


def test_task_status_read_only_requires_columns_for_requested_period(tmp_path: Path) -> None:
    database = tmp_path / "daily-only-player.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE xiuxian_tasks("
            "user_id TEXT PRIMARY KEY, daily_period TEXT, daily_progress TEXT, daily_claimed TEXT)"
        )
        conn.execute(
            "INSERT INTO xiuxian_tasks VALUES(?,?,?,?)",
            ("u", "2026-10-08", json.dumps({"daily_sign": 1}), json.dumps([])),
        )
        conn.commit()

    assert TasksProgressRepository(database).read_states(
        "u", {"daily": "2026-10-08"}
    ) == {"daily": ({"daily_sign": 1}, [], "2026-10-08")}
