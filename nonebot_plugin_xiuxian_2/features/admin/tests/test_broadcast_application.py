from __future__ import annotations

import asyncio
import importlib.util
import itertools
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def core_module():
    prefix = "_isolated_admin_broadcast"
    created = []
    try:
        for suffix in ("", ".features", ".features.admin"):
            name = prefix + suffix
            module = ModuleType(name)
            module.__path__ = []
            sys.modules[name] = module
            created.append(name)
        modules = {}
        for filename in ("broadcast_repository", "broadcast_application"):
            name = prefix + ".features.admin." + filename
            spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / (filename + ".py"))
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            created.append(name)
            spec.loader.exec_module(module)
            modules[filename] = module
        yield SimpleNamespace(
            repository=modules["broadcast_repository"].AdminBroadcastRepository,
            application=modules["broadcast_application"].AdminBroadcastApplication,
        )
    finally:
        for name in reversed(created):
            sys.modules.pop(name, None)


@pytest.fixture
def make_application(core_module):
    def make(*, targets=(), sender=None, history=None):
        now = [datetime(2026, 10, 7, 12, 0, 0)]
        sequence = itertools.count(1)
        repository = core_module.repository(now=lambda: now[0], id_factory=lambda: f"BC{next(sequence):08d}")
        calls = []
        queries = []

        async def default_history(adapter, bot_id, kind, query_now):
            queries.append((adapter, bot_id, kind, query_now))
            return list(targets)

        async def default_sender(bot, task, scene, target_id, message_id):
            calls.append((task["id"], scene, target_id, message_id))
            return {"status": "sent"}

        application = core_module.application(
            repository, history=history or default_history, sender=sender or default_sender,
        )
        return SimpleNamespace(app=application, repository=repository, now=now, calls=calls, queries=queries)

    return make


async def _start(app, **changes):
    arguments = {
        "adapter": "OneBot V11", "bot_id": "bot-1", "kind": "global", "content": "notice",
    }
    arguments.update(changes)
    return await app.start(object(), **arguments)


async def _patch(app, **changes):
    arguments = {"adapter": "OneBot V11", "bot_id": "bot-1", "scene": "group", "target_id": "g"}
    arguments.update(changes)
    return await app.patch_event(object(), **arguments)


def test_lifecycle_preserves_memory_contract_and_snapshot_isolation(make_application, core_module):
    runtime = make_application(targets=[
        {"scene": "group", "target_id": "g"},
        {"scene": "channel_private", "target_id": "u"},
        {"scene": "unknown", "target_id": "skip"},
    ])
    result = asyncio.run(_start(runtime.app, markdown=True))

    assert result["status"] == "created"
    assert result["success_count"] == 2
    task = result["task"]
    assert task["sent_groups"] == {"group:g"}
    assert task["sent_users"] == {"channel_private:u"}
    assert task["known_groups"] == task["sent_groups"]
    assert task["duration_minutes"] == 1440
    assert task["created_at"] == "2026-10-07 12:00:00"
    assert task["expire_at"] == "2026-10-08 12:00:00"
    assert task["markdown"] is True
    task["sent_groups"].add("group:forged")
    task["content"] = "forged"
    task["errors"].append({"error": "forged"})
    actual = runtime.app.status()[0]
    assert actual["content"] == "notice"
    assert actual["sent_groups"] == {"group:g"}
    assert actual["errors"] == []
    assert core_module.repository().status() == []
    runtime.now[0] += timedelta(days=1)
    assert runtime.app.status() == []


@pytest.mark.parametrize("changes,status", [
    ({"adapter": "unknown"}, "invalid_adapter"),
    ({"adapter": "OneBot V12"}, "invalid_adapter"),
    ({"adapter": "unknown-v11"}, "invalid_adapter"),
    ({"adapter": ""}, "invalid_identity"),
    ({"bot_id": " "}, "invalid_identity"),
    ({"kind": "other"}, "invalid_kind"),
    ({"content": " "}, "invalid_content"),
    ({"duration_minutes": 10**30}, "invalid_duration"),
])
def test_invalid_start_does_not_query_history_or_create_tasks(make_application, changes, status):
    runtime = make_application()

    result = asyncio.run(_start(runtime.app, **changes))

    assert result["status"] == status
    assert runtime.queries == []
    assert runtime.app.status() == []


def test_history_failure_and_bad_rows_do_not_leave_partial_tasks(make_application):
    async def failing_history(*args):
        raise RuntimeError("sensitive platform payload")

    runtime = make_application(history=failing_history)
    result = asyncio.run(_start(runtime.app))
    assert result["status"] == "history_failed"
    assert result["error_type"] == "RuntimeError"
    assert "sensitive" not in repr(result)
    assert runtime.app.status() == []

    async def malformed_history(*args):
        return [None]

    runtime.app.history = malformed_history
    assert asyncio.run(_start(runtime.app))["status"] == "history_failed"
    assert runtime.app.status() == []


def test_duration_starts_after_history_and_invalid_legacy_duration_defaults(make_application):
    runtime = make_application()
    queried = []

    async def slow_history(adapter, bot_id, kind, now):
        queried.append((adapter, bot_id, kind, now))
        runtime.now[0] += timedelta(minutes=20)
        return []

    runtime.app.history = slow_history
    result = asyncio.run(_start(runtime.app, duration_minutes="invalid"))

    assert queried == [("OneBot V11", "bot-1", "global", datetime(2026, 10, 7, 12))]
    assert result["task"]["created_at"] == "2026-10-07 12:20:00"
    assert result["task"]["expire_at"] == "2026-10-08 12:20:00"
    assert result["task"]["duration_minutes"] == 1440


def test_two_patch_events_claim_the_same_target_once(make_application):
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def sender(bot, task, scene, target_id, message_id):
            calls.append(target_id)
            entered.set()
            await release.wait()
            return {"status": "sent"}

        runtime = make_application(sender=sender)
        await _start(runtime.app)
        first = asyncio.create_task(_patch(runtime.app))
        await entered.wait()
        second = await _patch(runtime.app)
        assert second[0]["status"] == "inflight"
        assert runtime.app.status()[0]["inflight_count"] == 1
        release.set()
        assert (await first)[0]["status"] == "sent"
        assert (await _patch(runtime.app))[0]["status"] == "already_sent"
        assert calls == ["g"]
        assert runtime.app.status()[0]["inflight_count"] == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("stop", ["clear", "cancel", "expire"])
def test_stopping_initial_send_prevents_all_later_targets_but_cannot_revoke_inflight(make_application, stop):
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def sender(bot, task, scene, target_id, message_id):
            calls.append(target_id)
            entered.set()
            await release.wait()
            return {"status": "sent"}

        runtime = make_application(sender=sender, targets=[
            {"scene": "group", "target_id": "first"}, {"scene": "group", "target_id": "second"},
        ])
        running = asyncio.create_task(_start(runtime.app, duration_minutes=1))
        await entered.wait()
        task_id = runtime.app.status()[0]["id"]
        if stop == "clear":
            stopped = runtime.app.clear()
            assert stopped["count"] == 1
            assert stopped["inflight_count"] == 1
        elif stop == "cancel":
            stopped = runtime.app.cancel(task_id.lower())
            assert stopped["status"] == "cancelled"
            assert stopped["inflight_count"] == 1
        else:
            runtime.now[0] += timedelta(minutes=1)
            assert runtime.app.status() == []
        release.set()
        result = await running
        assert calls == ["first"]
        assert result["success_count"] == 1
        assert result["status"] == ("cancelled" if stop == "cancel" else "stopped")
        if stop == "cancel":
            assert result["task"]["sent_groups"] == {"group:first"}
        else:
            assert runtime.app.status() == []

    asyncio.run(scenario())


def test_pending_audit_is_not_success_and_blocks_uncertain_retries(make_application):
    calls = []

    async def sender(*args):
        calls.append(args)
        return SimpleNamespace(status="pending_audit", audit_id="audit")

    runtime = make_application(sender=sender, targets=[
        {"scene": "group", "target_id": "g", "message_id": "message"},
        {"scene": "group", "target_id": "g", "message_id": "duplicate"},
    ])
    result = asyncio.run(_start(runtime.app, adapter="QQ"))

    assert result["success_count"] == 0
    assert result["pending_count"] == 1
    assert result["task"]["sent_groups"] == set()
    assert result["task"]["pending_groups"] == {"group:g"}
    assert result["task"]["inflight_count"] == 0
    patch_result = asyncio.run(_patch(runtime.app, adapter="QQ", source_message_id="later"))
    assert patch_result[0]["status"] == "pending_audit"
    assert patch_result[0]["attempted"] is False
    assert len(calls) == 1


def test_failed_send_releases_claim_and_sanitizes_stored_error(make_application):
    attempts = []

    async def sender(*args):
        attempts.append(args)
        if len(attempts) == 1:
            raise RuntimeError("secret token and sensitive content")
        return {"status": "sent"}

    runtime = make_application(sender=sender, targets=[{"scene": "group", "target_id": "g"}])
    result = asyncio.run(_start(runtime.app))

    assert result["failed_count"] == 1
    assert result["task"]["known_groups"] == {"group:g"}
    assert result["task"]["sent_groups"] == set()
    assert result["task"]["inflight_count"] == 0
    assert result["task"]["errors"][0]["error"] == "RuntimeError"
    assert "secret" not in repr(result)
    assert asyncio.run(_patch(runtime.app))[0]["status"] == "sent"
    assert len(attempts) == 2


def test_cancelled_coroutine_releases_claim_and_propagates_cancellation(make_application):
    async def scenario():
        entered = asyncio.Event()

        async def sender(*args):
            entered.set()
            await asyncio.Event().wait()

        runtime = make_application(sender=sender)
        await _start(runtime.app)
        sending = asyncio.create_task(_patch(runtime.app))
        await entered.wait()
        sending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sending
        task = runtime.app.status()[0]
        assert task["inflight_count"] == 0
        assert task["errors"][0]["error"] == "CancelledError"

        async def retry(*args):
            return {"status": "sent"}

        runtime.app.sender = retry
        assert (await _patch(runtime.app))[0]["status"] == "sent"

    asyncio.run(scenario())


@pytest.mark.parametrize("send_result", [None, {}, {"status": "unknown"}, {"status": []}, {"status": False}])
def test_unknown_sender_results_fail_instead_of_marking_sent(make_application, send_result):
    async def sender(*args):
        return send_result

    runtime = make_application(sender=sender, targets=[{"scene": "group", "target_id": "g"}])
    result = asyncio.run(_start(runtime.app))

    assert result["success_count"] == 0
    assert result["failed_count"] == 1
    assert result["task"]["inflight_count"] == 0
    assert result["task"]["sent_groups"] == set()
    assert result["task"]["errors"][0]["error"] == "UnexpectedSendStatus"


def test_exception_reading_send_status_also_releases_claim(make_application):
    class BrokenResult:
        @property
        def status(self):
            raise RuntimeError("sensitive status payload")

    async def sender(*args):
        return BrokenResult()

    runtime = make_application(sender=sender, targets=[{"scene": "group", "target_id": "g"}])
    result = asyncio.run(_start(runtime.app))
    assert result["failed_count"] == 1
    assert result["task"]["inflight_count"] == 0
    assert result["task"]["errors"][0]["error"] == "RuntimeError"


def test_qq_requires_reply_id_and_does_not_cross_adapter_or_bot_identity(make_application):
    runtime = make_application(targets=[{"scene": "group", "target_id": "g"}])
    result = asyncio.run(_start(runtime.app, adapter="QQ"))

    assert result["failed_count"] == 1
    assert runtime.calls == []
    for changes in ({"bot_id": ""}, {"bot_id": "bot-2"}, {"adapter": "OneBot V11"}):
        assert asyncio.run(_patch(runtime.app, **dict({"adapter": "QQ", "source_message_id": "m"}, **changes))) == []
    sent = asyncio.run(_patch(runtime.app, adapter="QQ", source_message_id="m"))
    assert sent[0]["status"] == "sent"
    assert len(runtime.calls) == 1


def test_memory_claim_is_atomic_between_threads_and_old_generation_cannot_complete_new_task(core_module):
    repository = core_module.repository(id_factory=lambda: "BCFIXED")
    parameters = {"adapter": "OneBot V11", "bot_id": "bot", "kind": "group", "content": "notice", "duration_minutes": 1, "markdown": False}
    handle, _ = repository.create(**parameters)

    def claim(_):
        return repository.claim(handle, adapter="OneBot V11", bot_id="bot", scene="group", target_id="g")

    with ThreadPoolExecutor(max_workers=4) as pool:
        claims = list(pool.map(claim, range(16)))
    assert sum(item[0] is not None for item in claims) == 1
    old_claim = next(item[0] for item in claims if item[0] is not None)
    assert repository.clear()["inflight_count"] == 1
    new_handle, _ = repository.create(**parameters)
    new_claim, _, _ = repository.claim(new_handle, adapter="OneBot V11", bot_id="bot", scene="group", target_id="g")
    assert repository.finish(old_claim, status="sent") is False
    assert repository.snapshot(new_handle)["sent_groups"] == set()
    assert repository.snapshot(new_handle)["inflight_count"] == 1
    assert repository.finish(new_claim, status="sent") is True


def test_claim_payload_does_not_copy_growing_task_accounting(make_application):
    runtime = make_application()
    handle, _ = runtime.repository.create(
        adapter="OneBot V11", bot_id="bot", kind="group", content="notice", duration_minutes=1, markdown=False,
    )

    with patch.object(runtime.repository, "_snapshot", side_effect=AssertionError("copied full task during send")):
        for index in range(64):
            claim, payload, status = runtime.repository.claim(
                handle, adapter="OneBot V11", bot_id="bot", scene="group", target_id=str(index),
            )
            assert status == "claimed"
            assert "known_groups" not in payload
            assert payload["content"] == "notice"
            payload["content"] = "changed externally"
            runtime.repository.finish(claim, status="sent")

    snapshot = runtime.repository.snapshot(handle)
    assert snapshot["content"] == "notice"
    assert len(snapshot["sent_groups"]) == 64


def test_sender_mutation_cannot_change_later_payloads_or_owner_status(make_application):
    seen = []

    async def sender(bot, task, *args):
        seen.append(task["content"])
        task["content"] = "corrupted"
        task["bot_id"] = "another-bot"
        return {"status": "sent"}

    runtime = make_application(sender=sender, targets=[
        {"scene": "group", "target_id": "a"}, {"scene": "group", "target_id": "b"},
    ])
    result = asyncio.run(_start(runtime.app))

    assert seen == ["notice", "notice"]
    assert result["task"]["content"] == "notice"
    assert result["task"]["bot_id"] == "bot-1"


def test_error_diagnostics_are_bounded_but_total_is_preserved(make_application):
    async def sender(*args):
        raise RuntimeError("sensitive platform data")

    runtime = make_application(sender=sender)

    async def scenario():
        await _start(runtime.app)
        for _ in range(55):
            await _patch(runtime.app)

    asyncio.run(scenario())
    task = runtime.app.status()[0]
    assert task["error_count"] == 55
    assert len(task["errors"]) == 50
    assert "sensitive" not in repr(task)


def test_cancel_clear_and_kind_selection_keep_existing_result_contracts(make_application):
    runtime = make_application()

    async def scenario():
        group = await _start(runtime.app, kind="group")
        private = await _start(runtime.app, kind="private")
        assert runtime.app.cancel("")["status"] == "missing_id"
        assert runtime.app.cancel("missing")["status"] == "not_found"
        assert runtime.app.clear("wrong")["status"] == "invalid_kind"
        assert runtime.app.cancel(group["id"])["status"] == "cancelled"
        assert runtime.app.clear("group")["count"] == 1
        assert [task["id"] for task in runtime.app.status()] == [private["id"]]
        assert runtime.app.clear("all")["count"] == 1
        assert runtime.app.clear()["status"] == "empty"

    asyncio.run(scenario())
