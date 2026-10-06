from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..id_swap_repository import TARGET_COLUMNS
from ..migrations import apply_admin_qqid_batch
from ..qqid_batch_repository import AdminQqidBatchRepository, QqidBatchSchemaError


@pytest.fixture
def batch(tmp_path):
    path = tmp_path / "game.db"
    with DatabaseUnitOfWork(path) as uow:
        apply_admin_qqid_batch(uow)
        apply_admin_qqid_batch(uow)
    return AdminQqidBatchRepository(path)


def test_plan_and_child_ids_are_durable_and_only_one_batch_can_be_active(batch):
    created = batch.create("first", "operator", ["b", "a", "b"])
    entries = batch.entries("first")
    other = AdminQqidBatchRepository(batch.database)

    def must_not_rescan():
        raise AssertionError("active batch candidates were rescanned")
        yield

    assert other.create("second", "another", must_not_rescan()) == created
    assert other.get_active() == created
    assert other.entries("first") == entries
    assert [entry["source_id"] for entry in entries] == ["a", "b"]
    assert len({entry["child_operation_id"] for entry in entries}) == 2
    with sqlite3.connect(batch.database) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO admin_qqid_batches(batch_id,operator_id,total) VALUES('illegal','x',0)")
        for table in ("admin_qqid_batches", "admin_qqid_batch_entries"):
            columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
            assert not columns.intersection(TARGET_COLUMNS["game_db"])


def test_freezes_unchanged_and_failed_resolution_and_rejects_target_changes(batch):
    batch.create("plan", "operator", ["a", "b"])

    assert batch.freeze_resolution("plan", "a", "a")["status"] == "unchanged"
    assert batch.freeze_resolution("plan", "b", None, "resolver_error")["status"] == "resolve_failed"
    assert batch.freeze_resolution("plan", "a", "a")["target_id"] == "a"
    with pytest.raises(ValueError, match="cannot change"):
        batch.freeze_resolution("plan", "a", "new")
    assert batch.complete("plan")["status"] == "completed"
    assert batch.get_active() is None
    assert batch.create("plan", "operator", ["changed"])["status"] == "completed"
    assert [entry["source_id"] for entry in batch.entries("plan")] == ["a", "b"]


def test_completed_request_alias_does_not_resume_a_new_active_batch(batch):
    batch.create("old", "operator", ["a"])
    batch.bind_request("resume-event", "old")
    batch.freeze_resolution("old", "a", "a")
    completed = batch.complete("old")
    batch.create("new", "operator", ["b"])

    reopened = AdminQqidBatchRepository(batch.database)
    assert reopened.get("resume-event") == completed
    assert reopened.create("resume-event", "operator", ["unexpected"]) == completed
    assert reopened.get("old") == completed
    assert reopened.get_active()["batch_id"] == "new"
    assert reopened.entries("new")[0]["status"] == "pending"


def test_request_alias_is_immutable_and_cannot_shadow_a_batch_identity(batch):
    batch.create("first", "operator", [])
    original = batch.bind_request("resume-event", "first")
    assert batch.bind_request("resume-event", "first") == original
    batch.complete("first")
    batch.create("second", "operator", [])

    for request in ("resume-event", "first"):
        with pytest.raises(ValueError, match="already bound"):
            batch.bind_request(request, "second")
        assert batch.get(request)["batch_id"] == "first"
    assert batch.get_active()["batch_id"] == "second"


def test_duplicate_targets_mark_both_sources_conflicting_before_any_write(batch):
    batch.create("plan", "operator", ["a", "b"])
    assert batch.freeze_resolution("plan", "a", "new")["status"] == "resolved"

    batch.freeze_resolution("plan", "b", "new")

    assert [entry["status"] for entry in batch.entries("plan")] == ["conflict", "conflict"]
    assert batch.complete("plan")["status"] == "completed"


def test_candidate_chains_and_cycles_are_conservatively_conflicting(batch):
    batch.create("plan", "operator", ["a", "b"])
    assert batch.freeze_resolution("plan", "a", "b")["status"] == "conflict"
    assert batch.freeze_resolution("plan", "b", "a")["status"] == "conflict"


def test_failed_progress_rolls_back_and_recovery_retains_same_child(batch):
    batch.create("plan", "operator", ["a"])
    entry = batch.freeze_resolution("plan", "a", "new")
    original = batch._find_entry
    calls = 0

    def fail_after_write(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("progress commit failed")
        return original(*args)

    with patch.object(batch, "_find_entry", side_effect=fail_after_write):
        with pytest.raises(OSError, match="progress commit failed"):
            batch.record_result("plan", "a", "applied", {"status": "applied", "data": {"updated_cells": 3}})

    assert batch.entries("plan") == [entry]
    batch.record_result("plan", "a", "failed", {"status": "failed", "code": "needs_reconcile"})
    assert batch.entries("plan")[0]["child_operation_id"] == entry["child_operation_id"]
    with pytest.raises(ValueError, match="unfinished"):
        batch.complete("plan")
    with pytest.raises(ValueError, match="no-write"):
        batch.retry_rejected("plan", "a")
    batch.record_result("plan", "a", "applied", {"status": "replayed", "data": {"updated_cells": 3}})
    assert batch.complete("plan")["status"] == "completed"


def test_only_no_write_reconcile_pending_rejection_gets_a_new_persisted_attempt(batch):
    batch.create("plan", "operator", ["a"])
    entry = batch.freeze_resolution("plan", "a", "new")
    batch.record_result("plan", "a", "failed", {"status": "rejected", "code": "reconcile_pending"})

    retry = batch.retry_rejected("plan", "a")

    assert retry["status"] == "resolved"
    assert retry["attempt"] == 1
    assert retry["child_operation_id"] != entry["child_operation_id"]
    assert retry["target_id"] == "new"
    assert AdminQqidBatchRepository(batch.database).entries("plan") == [retry]


def test_missing_database_or_schema_never_creates_tables_or_lock(tmp_path):
    path = tmp_path / "missing.db"
    repository = AdminQqidBatchRepository(path)
    with pytest.raises(QqidBatchSchemaError):
        repository.create("plan", "operator", ["a"])
    assert not path.exists()
    with sqlite3.connect(path):
        pass
    with pytest.raises(QqidBatchSchemaError):
        with repository.exclusive_run():
            raise AssertionError("missing schema accepted")
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
    assert not (tmp_path / ".admin-qqid-batch.lock").exists()


def test_distinct_instances_serialize_whole_runs_using_a_separate_lock_path(batch):
    entered, release, second_started, second_entered = Event(), Event(), Event(), Event()
    other = AdminQqidBatchRepository(batch.database)

    def first():
        with batch.exclusive_run():
            entered.set()
            assert release.wait(5)

    def second():
        second_started.set()
        with other.exclusive_run():
            second_entered.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_run = pool.submit(first)
        try:
            assert entered.wait(5)
            second_run = pool.submit(second)
            assert second_started.wait(5)
            assert not second_entered.wait(0.05)
        finally:
            release.set()
        first_run.result(timeout=5)
        second_run.result(timeout=5)
    assert second_entered.is_set()
    assert batch.database.with_name(".admin-qqid-batch.lock").is_file()
    assert not batch.database.with_name(".admin-id-mutation.lock").exists()
