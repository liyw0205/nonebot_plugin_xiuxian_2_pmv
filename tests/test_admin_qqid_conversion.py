from __future__ import annotations

import __future__
import ast
import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.admin.application import AdminApplication
from nonebot_plugin_xiuxian_2.features.admin.id_swap_repository import DATABASE_ORDER
from nonebot_plugin_xiuxian_2.features.admin.id_update_repository import AdminIdUpdateSqlRepository
from nonebot_plugin_xiuxian_2.features.admin.migrations import apply_admin_qqid_batch
from nonebot_plugin_xiuxian_2.features.admin.qqid_application import AdminQqidApplication
from nonebot_plugin_xiuxian_2.features.admin.qqid_batch_repository import AdminQqidBatchRepository
from nonebot_plugin_xiuxian_2.features.admin.qqid_candidate_repository import AdminQqidCandidateRepository
from nonebot_plugin_xiuxian_2.features.admin.tests import test_id_update_repository as id_update_fixtures
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
ADMIN = ROOT / "xiuxian_admin/__init__.py"
COMPAT = ROOT / "xiuxian_utils/id_migration.py"


def _functions(path, namespace, names):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert {node.name for node in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


def _stack(databases, players, *, resolve_id=None, writer_type=AdminIdUpdateSqlRepository):
    resolver = resolve_id if resolve_id is not None else Mock(
        side_effect={"u1": "u3", "u2": "u2"}.__getitem__
    )
    invalidate = Mock()
    writer = writer_type(databases, players, invalidate_user_id_cache=invalidate)
    admin = AdminApplication(databases["game_db"], id_update_repository=writer)
    candidates = AdminQqidCandidateRepository(databases)
    batches = AdminQqidBatchRepository(databases["game_db"])
    application = AdminQqidApplication(candidates, batches, admin, resolve_id=resolver)
    return SimpleNamespace(
        databases=databases, players=players, resolver=resolver, writer=writer,
        admin=admin, candidates=candidates, batches=batches, application=application,
        invalidate=invalidate,
    )


def _context(tmp_path, *, batch_migrated=True, **kwargs):
    databases = id_update_fixtures._databases(tmp_path)
    if batch_migrated:
        with DatabaseUnitOfWork(databases["game_db"]) as uow:
            apply_admin_qqid_batch(uow)
    players = tmp_path / "players"
    (players / "u1").mkdir(parents=True)
    (players / "u1" / "marker").write_text("profile", encoding="utf-8")
    return _stack(databases, players, **kwargs)


def _values(context, key, query):
    return id_update_fixtures._values(context.databases[key], query)


def _tables(context):
    return {key: _values(context, key, "SELECT name,sql FROM sqlite_master ORDER BY name")
            for key in DATABASE_ORDER if context.databases[key].exists()}


def _assert_migrated(context, *, second="u2", receipt_count=1):
    assert _values(context, "game_db", "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u3",), (second,),
    ]
    assert _values(context, "game_db", "SELECT sect_owner FROM player_data") == [("u3",)]
    for key in ("impart_db", "trade_db"):
        assert _values(context, key, "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
            ("u3",), (second,),
        ]
    assert _values(context, "player_db", "SELECT * FROM user_xiuxian") == [
        ("u3", "u3", "u3", "u3", "u3"),
    ]
    assert (context.players / "u3" / "marker").read_text(encoding="utf-8") == "profile"
    assert not (context.players / "u1").exists()
    for key in DATABASE_ORDER:
        assert _values(context, key, "SELECT COUNT(*) FROM admin_id_update_step_receipts") == [
            (receipt_count,),
        ]


def _compat(application=None):
    forbidden = Mock(side_effect=AssertionError("legacy write path must not execute"))
    return _functions(COMPAT, {
        "__package__": "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils",
        "_qqid_application": application, "AdminQqidApplication": AdminQqidApplication,
        "logger": Mock(), "_ensure_id_target_columns_text": forbidden,
        "_collect_all_candidate_ids": forbidden, "_update_ids": forbidden,
    }, {"configure_qqid_application", "migrate_user_id_to_openid"})


class _Finished(Exception):
    pass


def _handler(application, *, gsk_link="configured", message_id="qqid-event"):
    messages, thread_calls = [], []

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def send(bot, event, message, **kwargs):
        messages.append(message)

    async def finish():
        raise _Finished

    async def to_thread(function, *args, **kwargs):
        thread_calls.append((function, args, kwargs))
        return await asyncio.to_thread(function, *args, **kwargs)

    namespace = _functions(ADMIN, {
        "admin_qqid_application": application, "AdminQqidApplication": AdminQqidApplication,
        "assign_bot": assign_bot, "handle_send": send,
        "asyncio": SimpleNamespace(to_thread=to_thread), "logger": Mock(),
        "XiuConfig": lambda: SimpleNamespace(gsk_link=gsk_link),
        "migrate_qqid_cmd": SimpleNamespace(finish=finish),
    }, {"migrate_qqid_cmd_", "_admin_operation_id"})
    event = SimpleNamespace(message_id=message_id, get_user_id=lambda: "admin-1")
    try:
        asyncio.run(namespace["migrate_qqid_cmd_"](object(), event))
    except _Finished:
        pass
    return messages, thread_calls


def test_real_compatibility_entry_migrates_four_databases_and_directory_once(tmp_path):
    context = _context(tmp_path)
    namespace = _compat()
    entrypoint = namespace["migrate_user_id_to_openid"]
    ok, message = entrypoint(application=context.application, operation_id="compat-1", operator_id="admin")
    assert ok and "QQID\u8f6c\u6362\u5b8c\u6210" in message
    _assert_migrated(context)
    context.invalidate.assert_called_once_with()
    assert [call.args[0] for call in context.resolver.call_args_list] == ["u1", "u2"]
    entries = context.batches.entries("compat-1")
    assert [(entry["source_id"], entry["target_id"], entry["status"]) for entry in entries] == [
        ("u1", "u3", "applied"), ("u2", "u2", "unchanged"),
    ]
    assert context.batches.get("compat-1")["status"] == "completed"
    assert context.batches.get_active() is None
    with patch.object(context.candidates, "snapshot", side_effect=AssertionError("replay scan")), \
            patch.object(context.admin, "update_user_id", side_effect=AssertionError("replay write")):
        context.resolver.side_effect = AssertionError("replay resolution")
        assert entrypoint(application=context.application, operation_id="compat-1")[0]
    assert context.batches.entries("compat-1") == entries
    _assert_migrated(context)
    context.invalidate.assert_called_once_with()


def test_default_compatibility_entry_uses_configured_owner_and_fails_closed_when_missing(tmp_path):
    context = _context(tmp_path)
    namespace = _compat()
    assert namespace["migrate_user_id_to_openid"](operation_id="missing-owner")[0] is False
    assert context.batches.get_active() is None
    namespace["configure_qqid_application"](context.application)
    assert namespace["migrate_user_id_to_openid"](operation_id="configured-owner")[0] is True
    _assert_migrated(context)


@pytest.mark.parametrize("writer_type", [
    id_update_fixtures._FailOnceAfterImpart,
    id_update_fixtures._FailAfterDirectoryRename,
])
def test_new_request_resumes_frozen_children_after_partial_database_or_directory_commit(tmp_path, writer_type):
    context = _context(tmp_path, writer_type=writer_type,
                       resolve_id=Mock(side_effect={"u1": "u3", "u2": "u4"}.__getitem__))
    result = context.application.run("interrupted", "admin")
    assert result["status"] == "pending" and result["applied"] == 0
    assert not AdminQqidApplication.format_result(result)[0]
    entries = context.batches.entries("interrupted")
    assert [(entry["source_id"], entry["target_id"], entry["status"]) for entry in entries] == [
        ("u1", "u3", "failed"), ("u2", "u4", "resolved"),
    ]
    child_ids = [entry["child_operation_id"] for entry in entries]
    assert _values(context, "game_db", "SELECT operation_id FROM admin_id_update_operations") == [
        (child_ids[0],),
    ]
    assert _values(context, "game_db", "SELECT user_id FROM user_xiuxian WHERE name='two'") == [("u2",)]

    resumed = _stack(context.databases, context.players,
                     resolve_id=Mock(side_effect=AssertionError("frozen mapping must not resolve again")))
    with patch.object(resumed.candidates, "snapshot", side_effect=AssertionError("active batch must not rescan")):
        result = resumed.application.run("new-request", "different-admin")
    assert result["status"] == "completed" and result["batch_id"] == "interrupted"
    assert result["applied"] == 2 and result["updated_cells"] == 12
    assert resumed.batches.get("new-request")["batch_id"] == "interrupted"
    assert [(entry["source_id"], entry["target_id"], entry["child_operation_id"])
            for entry in resumed.batches.entries("interrupted")] == [
        ("u1", "u3", child_ids[0]), ("u2", "u4", child_ids[1]),
    ]
    _assert_migrated(resumed, second="u4", receipt_count=2)


def test_original_and_recovery_events_replay_their_completed_batch_without_touching_a_later_active_batch(tmp_path):
    context = _context(tmp_path, writer_type=id_update_fixtures._FailOnceAfterImpart)
    assert context.application.run("original-event", "admin")["status"] == "pending"
    resumed = _stack(context.databases, context.players,
                     resolve_id=Mock(side_effect=AssertionError("frozen batch resolution")))
    result = resumed.application.run("recovery-event", "admin")
    assert result["status"] == "completed" and result["batch_id"] == "original-event"
    assert resumed.batches.get("recovery-event")["batch_id"] == "original-event"
    _assert_migrated(resumed)

    later = _stack(context.databases, context.players,
                   writer_type=id_update_fixtures._FailOnceAfterImpart,
                   resolve_id=Mock(side_effect={"u2": "u2", "u3": "u5"}.__getitem__))
    assert later.application.run("later-event", "admin")["status"] == "pending"
    active = later.batches.get_active()
    assert active["batch_id"] == "later-event"
    entries = later.batches.entries("later-event")
    original_entries = later.batches.entries("original-event")
    receipt_counts = {key: _values(later, key, "SELECT COUNT(*) FROM admin_id_update_step_receipts")
                      for key in DATABASE_ORDER}
    with patch.object(later.candidates, "snapshot", side_effect=AssertionError("event replay scan")), \
            patch.object(later.admin, "update_user_id", side_effect=AssertionError("event replay touched later batch")):
        later.application.resolve_id = Mock(side_effect=AssertionError("event replay resolution"))
        for event_id in ("original-event", "recovery-event"):
            replayed = later.application.run(event_id, "different-admin")
            assert replayed["status"] == "completed" and replayed["batch_id"] == "original-event"
            assert replayed["applied"] == 1 and replayed["updated_cells"] == 9
    assert later.batches.get_active() == active
    assert later.batches.entries("later-event") == entries
    assert later.batches.entries("original-event") == original_entries
    assert {key: _values(later, key, "SELECT COUNT(*) FROM admin_id_update_step_receipts")
            for key in DATABASE_ORDER} == receipt_counts


def test_child_success_before_batch_checkpoint_failure_replays_receipt_without_losing_count(tmp_path):
    context = _context(tmp_path)
    with patch.object(context.batches, "record_result", side_effect=OSError("checkpoint unavailable")):
        result = context.application.run("checkpoint", "admin")
    assert result["status"] == "pending" and result["applied"] == 0
    assert not AdminQqidApplication.format_result(result)[0]
    _assert_migrated(context)
    entry = context.batches.entries("checkpoint")[0]
    child_id = entry["child_operation_id"]
    assert entry["status"] == "resolved"
    resumed = _stack(context.databases, context.players,
                     resolve_id=Mock(side_effect=AssertionError("checkpoint replay must not resolve")))
    with patch.object(resumed.candidates, "snapshot", side_effect=AssertionError("checkpoint replay must not scan")):
        result = resumed.application.run("checkpoint-retry", "admin")
    assert result["status"] == "completed" and result["applied"] == 1
    assert result["unchanged"] == 1 and result["updated_cells"] == 9
    entry = resumed.batches.entries("checkpoint")[0]
    assert entry["source_id"] == "u1" and entry["target_id"] == "u3"
    assert entry["child_operation_id"] == child_id and entry["result"]["replayed"] is True
    resumed.invalidate.assert_not_called()
    _assert_migrated(resumed)


@pytest.mark.parametrize("mapping,conflicts,unchanged", [
    ({"u1": "u3", "u2": "u3"}, 2, 0),
    ({"u1": "u2", "u2": "u2"}, 2, 0),
    ({"u1": "u1", "u2": "u2"}, 0, 2),
])
def test_frozen_duplicate_or_existing_targets_do_not_merge_and_unchanged_ids_do_not_write(
    tmp_path, mapping, conflicts, unchanged,
):
    context = _context(tmp_path, resolve_id=Mock(side_effect=mapping.__getitem__))
    with patch.object(context.admin, "update_user_id", side_effect=AssertionError("no child should write")):
        result = context.application.run("conflicts", "admin")
    assert result["applied"] == 0 and result["updated_cells"] == 0
    assert result["conflict"] == conflicts and result["unchanged"] == unchanged
    assert result["status"] == ("partial" if conflicts else "completed")
    assert AdminQqidApplication.format_result(result)[0] is (not conflicts)
    assert _values(context, "game_db", "SELECT user_id FROM user_xiuxian ORDER BY name") == [("u1",), ("u2",)]
    assert _values(context, "game_db", "SELECT COUNT(*) FROM admin_id_update_operations") == [(0,)]
    assert (context.players / "u1" / "marker").read_text(encoding="utf-8") == "profile"
    assert not (context.players / "u3").exists()


def test_existing_destination_directory_is_not_overwritten_by_real_writer(tmp_path):
    context = _context(tmp_path)
    (context.players / "u3").mkdir()
    (context.players / "u3" / "marker").write_text("existing", encoding="utf-8")
    result = context.application.run("directory-conflict", "admin")
    assert result["status"] == "partial" and result["conflict"] == 1
    assert result["applied"] == 0 and result["unchanged"] == 1
    assert context.batches.entries("directory-conflict")[0]["result"]["code"] == "user_id_conflict"
    assert (context.players / "u3" / "marker").read_text(encoding="utf-8") == "existing"
    assert (context.players / "u1" / "marker").read_text(encoding="utf-8") == "profile"
    for key in DATABASE_ORDER:
        assert _values(context, key, "SELECT COUNT(*) FROM admin_id_update_step_receipts") == [(0,)]


def test_terminal_reconcile_pending_rejection_uses_new_attempt_after_external_recovery(tmp_path):
    context = _context(tmp_path, writer_type=id_update_fixtures._FailOnceAfterImpart,
                       resolve_id=Mock(side_effect={"u1": "u3", "u2": "u2", "u4": "u4"}.__getitem__))
    assert context.admin.update_user_id("external-pending", "u2", "u4").code == "needs_reconcile"
    result = context.application.run("blocked-by-external", "admin")
    assert result["status"] == "pending" and result["applied"] == 0
    rejected = context.batches.entries("blocked-by-external")[0]
    assert rejected["status"] == "failed"
    assert rejected["result"]["status"] == "rejected"
    assert rejected["result"]["code"] == "reconcile_pending"
    original_child = rejected["child_operation_id"]
    assert context.admin.reconcile_user_id_updates() == {"recovered": 1, "pending": 0, "failed": 0}
    assert context.admin.update_user_id(original_child, "u1", "u3").code == "reconcile_pending"

    resumed = _stack(context.databases, context.players,
                     resolve_id=Mock(side_effect=AssertionError("attempt must retain frozen resolution")))
    with patch.object(resumed.candidates, "snapshot", side_effect=AssertionError("attempt must not scan")):
        result = resumed.application.run("after-external-recovery", "admin")
    assert result["status"] == "completed" and result["batch_id"] == "blocked-by-external"
    assert result["applied"] == 1 and result["unchanged"] == 2 and result["updated_cells"] == 9
    applied = resumed.batches.entries("blocked-by-external")[0]
    assert applied["child_operation_id"] != original_child and applied["attempt"] == 1
    assert applied["source_id"] == "u1" and applied["target_id"] == "u3"
    _assert_migrated(resumed, second="u4", receipt_count=2)


def test_partial_resolution_failure_is_reported_by_compatibility_without_false_success(tmp_path):
    def resolve(source):
        if source == "u2":
            raise OSError("private resolver response must not leak")
        return "u3"

    context = _context(tmp_path, resolve_id=resolve)
    ok, message = _compat(context.application)["migrate_user_id_to_openid"](operation_id="partial")
    assert not ok and "QQID\u8f6c\u6362\u5b8c\u6210" not in message
    assert "private resolver response" not in message
    _assert_migrated(context)
    result = context.application.run("partial", "admin")
    assert result["status"] == "partial" and result["applied"] == 1 and result["resolve_failed"] == 1


def test_scan_resolution_and_update_timings_are_reported_separately(tmp_path):
    context = _context(tmp_path)
    with patch("nonebot_plugin_xiuxian_2.features.admin.qqid_application.perf_counter",
               side_effect=[10.0, 11.0, 20.0, 23.0, 30.0, 35.0]):
        result = context.application.run("timed", "admin")
    assert result["scan_seconds"] == 1.0
    assert result["resolve_seconds"] == 3.0
    assert result["update_seconds"] == 5.0
    ok, message = AdminQqidApplication.format_result(result)
    assert ok and all(value in message for value in ("1.00s", "3.00s", "5.00s"))
    _assert_migrated(context)


@pytest.mark.parametrize("missing", ["database", "batch_schema", "candidate_schema"])
def test_missing_database_or_startup_schema_fails_closed_without_creating_databases_or_tables(tmp_path, missing):
    context = _context(tmp_path, batch_migrated=missing != "batch_schema")
    if missing == "database":
        context.databases["trade_db"].unlink()
    elif missing == "candidate_schema":
        with sqlite3.connect(context.databases["impart_db"]) as connection:
            connection.execute("DROP TABLE user_xiuxian")
    before = _tables(context)
    result = context.application.run("not-ready", "admin")
    assert result["status"] == "failed"
    assert not AdminQqidApplication.format_result(result)[0]
    assert _tables(context) == before
    context.resolver.assert_not_called()
    assert _values(context, "game_db", "SELECT COUNT(*) FROM admin_id_update_operations") == [(0,)]
    if missing == "database":
        assert not context.databases["trade_db"].exists()
    assert (context.players / "u1").is_dir() and not (context.players / "u3").exists()


def test_real_handler_runs_the_complete_owner_in_worker_thread_and_replays_event(tmp_path):
    thread_ids = []

    def resolve(source):
        thread_ids.append(threading.get_ident())
        return {"u1": "u3", "u2": "u2"}[source]

    context = _context(tmp_path, resolve_id=resolve)
    messages, calls = _handler(context.application)
    assert len(messages) == 2 and "QQID\u8f6c\u6362\u5b8c\u6210" in messages[-1]
    assert len(calls) == 1 and calls[0][0] == context.application.run
    assert calls[0][1] == ("admin-qqid-conversion:qqid-event:all", "admin-1")
    assert thread_ids and all(ident != threading.get_ident() for ident in thread_ids)
    _assert_migrated(context)
    with patch.object(context.candidates, "snapshot", side_effect=AssertionError("handler replay scan")), \
            patch.object(context.admin, "update_user_id", side_effect=AssertionError("handler replay write")):
        replayed_messages, _ = _handler(context.application)
    assert "QQID\u8f6c\u6362\u5b8c\u6210" in replayed_messages[-1]
    _assert_migrated(context)


def test_real_handler_empty_gsk_does_not_schedule_or_execute_the_owner(tmp_path):
    context = _context(tmp_path)
    with patch.object(context.application, "run", side_effect=AssertionError("empty gsk run")):
        messages, calls = _handler(context.application, gsk_link="")
    assert len(messages) == 1 and "gsk_link" in messages[0]
    assert calls == [] and context.batches.get_active() is None
    assert _values(context, "game_db", "SELECT user_id FROM user_xiuxian ORDER BY name") == [("u1",), ("u2",)]


def test_real_handler_interrupted_batch_never_reports_complete(tmp_path):
    context = _context(tmp_path, writer_type=id_update_fixtures._FailOnceAfterImpart)
    messages, calls = _handler(context.application)
    assert len(calls) == 1 and len(messages) == 2
    assert "QQID\u8f6c\u6362\u5b8c\u6210" not in messages[-1]
    assert context.batches.get_active() is not None
    assert _values(context, "trade_db", "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [("u1",), ("u2",)]


def test_real_command_registration_retains_superuser_permission():
    tree = ast.parse(ADMIN.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, ast.Assign)
             and any(isinstance(target, ast.Name) and target.id == "migrate_qqid_cmd" for target in node.targets)]
    assert len(nodes) == 1
    permission = object()
    registration = Mock()
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ADMIN), "exec"), {
        "on_command": registration, "SUPERUSER": permission,
    })
    registration.assert_called_once_with("\u8f6c\u6362QQID", permission=permission, priority=5, block=True)


def test_qqid_batch_startup_migration_is_registered_for_game_database_only():
    migrations = build_migrations()
    assert len([migration for migration in migrations if migration.version == "legacy.admin.008"]) == 1
    for key in DATABASE_ORDER:
        selected = [migration for migration in migrations_for_database(migrations, key)
                    if migration.version == "legacy.admin.008"]
        assert len(selected) == (1 if key == "game_db" else 0)
