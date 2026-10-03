from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.dungeon import migrations
from nonebot_plugin_xiuxian_2.features.dungeon.team_application import DungeonTeamApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class Clock:
    def __init__(self, timestamp=200):
        self.timestamp = timestamp

    def now(self):
        return datetime.fromtimestamp(self.timestamp, timezone.utc)


@pytest.fixture
def application(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        migrations.apply_dungeon_team_schema(uow)
        migrations.apply_dungeon_explore_player_schema(uow)
        migrations.apply_dungeon_team_members_index(uow)
        migrations.apply_dungeon_team_invite_expiry(uow)
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
        uow.executemany("INSERT INTO user_xiuxian VALUES(?)", [("leader",), ("member",)])
    app = DungeonTeamApplication(database)
    assert app.create("create", "team", "Trial", "leader", "100", "created", 100).status == "applied"
    return app


def insert_invites(app, count=1):
    with DatabaseUnitOfWork(app.repository.database) as uow:
        uow.executemany(
            "INSERT INTO dungeon_team_invites("
            "invite_id,team_id,inviter_id,invitee_id,group_id,created_at,expires_at,bot_id,source_message_id) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            [(f"invite-{i:03}", "team", "leader", f"target-{i}", "100", 100, 160, "bot", f"message-{i}") for i in range(count)],
        )


def row(app, sql, params=()):
    with DatabaseUnitOfWork(app.repository.database, read_only=True) as uow:
        return uow.query_one(sql, params)


def worker(app, notify, clock=None, **kwargs):
    from nonebot_plugin_xiuxian_2.features.dungeon.invite_expiry import DungeonInviteExpiryWorker

    return DungeonInviteExpiryWorker(app, clock=clock or Clock(), notify=notify, **kwargs)


def test_expiry_migration_is_player_only_and_preserves_old_rows(application):
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    catalog = build_migrations()
    for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db"):
        versions = {item.version for item in migrations_for_database(catalog, key)}
        assert ("dungeon.010" in versions) == (key == "player_db")
    insert_invites(application)
    with DatabaseUnitOfWork(application.repository.database) as uow:
        migrations.apply_dungeon_team_invite_expiry(uow)
        plan = uow.query_all(
            "EXPLAIN QUERY PLAN SELECT invite_id FROM dungeon_team_invites "
            "WHERE status='pending' AND consumed_at IS NULL AND expires_at<=? ORDER BY expires_at,invite_id LIMIT ?", (200, 100)
        )
    assert any("dungeon_team_invites_expiry_idx" in item["detail"] for item in plan)
    assert row(application, "SELECT COUNT(*) AS n FROM dungeon_team_invites")["n"] == 1


def test_invite_freezes_route_and_replays_original_route(application):
    args = ("send", "invite", "team", "leader", "member", "100")
    first = application.invite(*args, 160, 100, bot_id="bot", source_message_id="message", notification_scene="channel_group")
    replay = application.invite(*args, 999, 500, bot_id="other", source_message_id="other", notification_scene="group")
    assert first.status == replay.status == "applied"
    invite = application.invite_by_id("invite")
    assert (invite.bot_id, invite.source_message_id, invite.notification_scene, invite.expires_at) == ("bot", "message", "channel_group", 160)


def test_not_expired_has_no_receipt_and_can_retry_at_deadline(application):
    insert_invites(application)
    early = application.expire("expire", "invite-000", 159.99)
    assert early.status == "not_expired"
    assert row(application, "SELECT 1 FROM dungeon_team_operations WHERE operation_id='expire'") is None
    applied = application.expire("expire", "invite-000", 160)
    replay = application.expire("expire", "invite-000", 100)
    assert (applied.status, replay.status) == ("applied", "duplicate")
    assert row(application, "SELECT resolved_operation_id FROM dungeon_team_invites")["resolved_operation_id"] == "expire"


def test_legacy_not_expired_receipt_is_retryable_without_deleting_row(application):
    insert_invites(application)
    payload = application.repository._json({"action": "expire", "invite_id": "invite-000", "user_id": "", "group_id": ""})
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute(
            "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) "
            "VALUES('expire',?,'not_expired','team','{}','expire')", (payload,)
        )
    before = row(application, "SELECT rowid,created_at FROM dungeon_team_operations WHERE operation_id='expire'")
    assert application.expire("expire", "invite-000", 150).status == "not_expired"
    assert application.expire("expire", "invite-000", 160).status == "applied"
    after = row(application, "SELECT rowid,created_at,result_status FROM dungeon_team_operations WHERE operation_id='expire'")
    assert (after["rowid"], after["created_at"], after["result_status"]) == (before["rowid"], before["created_at"], "applied")


def test_expiry_receipt_failure_rolls_back_state(application):
    insert_invites(application)
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("CREATE TRIGGER fail_expire BEFORE INSERT ON dungeon_team_operations WHEN NEW.action='expire' BEGIN SELECT RAISE(ABORT,'receipt failure'); END")
    with pytest.raises(Exception, match="receipt failure"):
        application.expire("expire", "invite-000", 200)
    assert row(application, "SELECT status,consumed_at FROM dungeon_team_invites") == {"status": "pending", "consumed_at": None}
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("DROP TRIGGER fail_expire")
    assert application.expire("expire", "invite-000", 200).status == "applied"


def test_due_batch_and_restarted_worker_are_bounded(application):
    insert_invites(application, 105)
    notified = []

    async def notify(invite):
        assert row(application, "SELECT status FROM dungeon_team_invites WHERE invite_id=?", (invite.invite_id,))["status"] == "expired"
        notified.append(invite.invite_id)

    first = asyncio.run(worker(application, notify).run())
    restarted = DungeonTeamApplication(application.repository.database)
    second = asyncio.run(worker(restarted, notify).run())
    third = asyncio.run(worker(restarted, notify).run())
    assert (first.scanned, first.applied, second.applied, third.applied) == (100, 100, 5, 0)
    assert len(notified) == len(set(notified)) == 105
    assert application.due_invites(200, limit=1000).invites == ()


def test_scan_limit_and_index_missing_fail_closed(application):
    insert_invites(application, 105)
    assert len(application.due_invites(200, limit=1000).invites) == 100
    assert application.due_invites(200, limit=0).invites == ()
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("DROP INDEX dungeon_team_invites_expiry_idx")
    before = Path(application.repository.database).read_bytes()
    assert application.due_invites(200).status == "schema_missing"
    assert Path(application.repository.database).read_bytes() == before
    assert row(application, "SELECT COUNT(*) AS n FROM dungeon_team_invites WHERE status='pending'")["n"] == 105


def test_missing_database_scan_does_not_create_file(tmp_path):
    app = DungeonTeamApplication(tmp_path / "absent.db")
    assert app.due_invites(200).status == "schema_missing"
    assert not Path(app.repository.database).exists()


def test_worker_uses_current_clock_and_does_not_poison_operation(application):
    insert_invites(application)
    clock = Clock(200)
    original = application.due_invites

    def rollback(*args, **kwargs):
        batch = original(*args, **kwargs)
        clock.timestamp = 150
        return batch

    application.due_invites = rollback

    async def notify(invite):
        pytest.fail("early expiry must not notify")

    assert asyncio.run(worker(application, notify, clock).run()).applied == 0
    application.due_invites = original
    clock.timestamp = 200
    notices = []

    async def notify_later(invite):
        notices.append(invite.invite_id)

    assert asyncio.run(worker(application, notify_later, clock).run()).applied == 1
    assert notices == ["invite-000"]


@pytest.mark.parametrize("action", ["join", "reject"])
def test_competing_consumption_does_not_notify(application, action):
    application.invite("send", "invite", "team", "leader", "member", "100", 160, 100)
    original = application.due_invites

    def consume(*args, **kwargs):
        batch = original(*args, **kwargs)
        if action == "join":
            assert application.join("join", "invite", "team", "leader", "member", "100", 150).status == "applied"
        else:
            assert application.reject("reject", "invite", "member", "100", 150).status == "applied"
        return batch

    application.due_invites = consume

    async def notify(invite):
        pytest.fail("consumed invite must not notify")

    assert asyncio.run(worker(application, notify).run()).applied == 0


def test_worker_is_single_flight_and_notification_failure_is_isolated(application):
    insert_invites(application, 2)

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def notify(invite):
            calls.append(invite.invite_id)
            if len(calls) == 1:
                entered.set()
                await release.wait()
                raise RuntimeError("offline")

        instance = worker(application, notify)
        running = asyncio.create_task(instance.run())
        await entered.wait()
        assert (await instance.run()).status == "busy"
        release.set()
        result = await running
        assert (result.applied, result.notification_failures, calls) == (2, 1, ["invite-000", "invite-001"])

    asyncio.run(scenario())


def test_notification_timeout_and_expiry_failure_do_not_stop_batch(application):
    insert_invites(application, 3)
    original = application.expire

    def expire(operation_id, invite_id, now, **kwargs):
        if invite_id == "invite-000":
            raise RuntimeError("locked")
        return original(operation_id, invite_id, now, **kwargs)

    application.expire = expire

    async def notify(invite):
        if invite.invite_id == "invite-001":
            await asyncio.sleep(10)

    result = asyncio.run(worker(application, notify, notification_timeout=0.01).run())
    assert (result.scanned, result.applied, result.failures, result.notification_failures) == (3, 2, 1, 1)


def test_default_invite_has_no_per_invitation_task_and_scheduler_is_bounded():
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py").read_text()
    assert "create_task(expire_team_invite" not in source
    assert 'id="dungeon_team_invite_expiry"' in source
    assert "max_instances=1" in source


def test_old_migration_rows_without_routing_can_expire(tmp_path):
    app = DungeonTeamApplication(tmp_path / "old.db")
    with DatabaseUnitOfWork(app.repository.database) as uow:
        migrations.apply_dungeon_team_schema(uow)
        migrations.apply_dungeon_explore_player_schema(uow)
        migrations.apply_dungeon_team_members_index(uow)
        uow.execute("INSERT INTO dungeon_team_invites(invite_id,expires_at) VALUES('old',160)")
        migrations.apply_dungeon_team_invite_expiry(uow)
    invite = app.due_invites(200).invites[0]
    assert (invite.bot_id, invite.source_message_id, invite.notification_scene) == ("", "", "")
    assert app.expire("expire", "old", 200).status == "applied"


def test_new_invite_missing_routing_schema_fails_without_ddl(application):
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("DROP INDEX dungeon_team_invites_expiry_idx")
    before = Path(application.repository.database).read_bytes()
    result = application.invite("send", "invite", "team", "leader", "member", "100", 160, 100, bot_id="bot", notification_scene="group")
    assert result.status == "schema_missing"
    assert Path(application.repository.database).read_bytes() == before
    assert row(application, "SELECT 1 FROM dungeon_team_operations WHERE operation_id='send'") is None


def test_notification_budget_expires_all_states_without_sending(application):
    insert_invites(application, 3)

    async def notify(invite):
        pytest.fail("zero notification budget must not send")

    report = asyncio.run(worker(application, notify, notification_budget=0).run())
    assert (report.applied, report.notification_failures) == (3, 3)
    assert row(application, "SELECT COUNT(*) AS n FROM dungeon_team_invites WHERE status='expired'")["n"] == 3


@pytest.mark.parametrize("scene", ["group", "channel_group"])
def test_runtime_notification_uses_exact_origin_bot_and_scene(monkeypatch, scene):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon

    bot = object()
    looked_up = []

    def get_bot(bot_id):
        looked_up.append(bot_id)
        return bot

    delivery = SimpleNamespace(send_to_group=AsyncMock(), send_to_channel=AsyncMock())
    monkeypatch.setattr(dungeon, "get_bot", get_bot)
    monkeypatch.setattr(dungeon, "delivery_service", delivery)
    invite = SimpleNamespace(bot_id="origin", group_id="100", source_message_id="message", notification_scene=scene)
    asyncio.run(dungeon._notify_expired_team_invite(invite))
    assert looked_up == ["origin"]
    chosen = delivery.send_to_channel if scene == "channel_group" else delivery.send_to_group
    other = delivery.send_to_group if scene == "channel_group" else delivery.send_to_channel
    chosen.assert_awaited_once_with(bot, "100", "组队邀请已过期！", source_message_id="message")
    other.assert_not_awaited()


def test_runtime_expiry_job_has_single_instance_and_uses_worker(monkeypatch):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from nonebot_plugin_xiuxian_2.compatibility.legacy_manifest import FEATURE
    from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler

    assert "dungeon_team_invite_expiry" in {job.id for job in FEATURE.jobs}
    job = dungeon.scheduler.get_job("dungeon_team_invite_expiry")
    if job is None:
        declaration = next(job for job in dungeon.scheduler._pending if job.job_id == "dungeon_team_invite_expiry")
        isolated = DeferredScheduler(AsyncIOScheduler())
        isolated.scheduled_job(*declaration.args, **declaration.kwargs)(declaration.function)
        isolated.activate()
        job = isolated.get_job("dungeon_team_invite_expiry")
    assert (job.max_instances, job.coalesce, job.trigger.interval.total_seconds()) == (1, True, 5)
    instance = SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(status="ok", failures=0, notification_failures=0)))
    monkeypatch.setattr(dungeon, "team_invite_expiry_worker", instance)
    asyncio.run(dungeon.expire_pending_team_invites())
    instance.run.assert_awaited_once()


def test_invite_handler_freezes_origin_before_assign_bot(application, monkeypatch):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon

    class Finished(Exception):
        pass

    event = SimpleNamespace(group_id="100", message_id="message", get_user_id=lambda: "leader")
    args = SimpleNamespace(extract_plain_text=lambda: "")
    monkeypatch.setattr(dungeon, "check_user", lambda event: (True, {"user_id": "leader", "user_name": "Leader"}, ""))
    monkeypatch.setattr(dungeon, "assign_bot", AsyncMock(return_value=(SimpleNamespace(self_id="different-app"), "100")))
    monkeypatch.setattr(dungeon, "get_chat_scene", lambda event: "channel_group")
    monkeypatch.setattr(dungeon, "get_at_user_id", lambda args: "member")
    monkeypatch.setattr(dungeon, "dungeon_team_application", application)
    monkeypatch.setattr(dungeon, "runtime_clock", Clock(100))
    monkeypatch.setattr(dungeon, "handle_send", AsyncMock())
    monkeypatch.setattr(dungeon.invite_team_cmd, "finish", AsyncMock(side_effect=Finished))
    with pytest.raises(Finished):
        asyncio.run(dungeon.invite_team_handler(SimpleNamespace(self_id="origin"), event, args))
    invite = application.pending_invite("member", 100)
    stored = application.invite_by_id(invite.invite_id)
    assert (stored.bot_id, stored.source_message_id, stored.notification_scene) == ("origin", "message", "channel_group")


def test_successful_expiry_receipt_replays_without_live_dependencies(application):
    insert_invites(application)
    assert application.expire("expire", "invite-000", 200).status == "applied"
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("DROP TABLE dungeon_team_members")
        uow.execute("DROP TABLE dungeon_team_invites")
    assert application.expire("expire", "invite-000", 100).status == "duplicate"
    assert application.expire("expire", "different-invite", 200).status == "state_changed"


def test_legacy_retry_receipt_update_failure_is_atomic(application):
    insert_invites(application)
    payload = application.repository._json({"action": "expire", "invite_id": "invite-000", "user_id": "", "group_id": ""})
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES('expire',?,'not_expired','team','{}','expire')", (payload,))
        uow.execute("CREATE TRIGGER fail_retry BEFORE UPDATE ON dungeon_team_operations BEGIN SELECT RAISE(ABORT,'receipt failure'); END")
    with pytest.raises(Exception, match="receipt failure"):
        application.expire("expire", "invite-000", 200)
    assert row(application, "SELECT status,consumed_at FROM dungeon_team_invites") == {"status": "pending", "consumed_at": None}
    assert row(application, "SELECT result_status FROM dungeon_team_operations WHERE operation_id='expire'")["result_status"] == "not_expired"


def test_wrong_expiry_index_and_consumed_pending_fail_closed(application):
    insert_invites(application)
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("UPDATE dungeon_team_invites SET consumed_at='already-consumed'")
    assert application.due_invites(200).invites == ()
    assert application.expire("expire", "invite-000", 200).status == "invite_invalid"
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("DROP INDEX dungeon_team_invites_expiry_idx")
        uow.execute("CREATE INDEX dungeon_team_invites_expiry_idx ON dungeon_team_invites(invitee_id,expires_at) WHERE status='pending' AND consumed_at IS NULL")
    assert application.due_invites(200).status == "schema_missing"


def test_notification_cancellation_releases_worker_and_does_not_replay(application):
    insert_invites(application, 2)

    async def scenario():
        entered = asyncio.Event()

        async def notify(invite):
            entered.set()
            await asyncio.Event().wait()

        instance = worker(application, notify)
        task = asyncio.create_task(instance.run())
        await entered.wait()
        assert row(application, "SELECT COUNT(*) AS n FROM dungeon_team_invites WHERE status='expired'")["n"] == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await instance.run()).applied == 0

    asyncio.run(scenario())


def test_runtime_missing_route_skips_bot_lookup(monkeypatch):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon

    monkeypatch.setattr(dungeon, "get_bot", lambda bot_id: pytest.fail("missing route must not resolve bot"))
    asyncio.run(dungeon._notify_expired_team_invite(SimpleNamespace(bot_id="", group_id="100")))


def test_invite_expiry_progress_gates():
    from scripts.check_full_refactor_progress import _slice_status

    dungeon = _slice_status()["dungeon_team"]
    for gate in (
        "team_invite_expiry_startup_migrated",
        "team_invite_expiry_single_bounded_worker",
        "team_invite_expiry_replay_retry_and_route_covered",
        "team_invite_expiry_notifications_bounded_best_effort",
        "team_invite_expiry_lock_wait_and_poison_batch_bounded",
    ):
        assert dungeon[gate], gate


@pytest.mark.parametrize(
    ("action", "status", "result_json", "expected"),
    [
        ("", "not_expired", "{}", "applied"),
        ("reject", "not_expired", "{}", "state_changed"),
        ("expire", "not_expired", '{"status":"applied"}', "state_changed"),
        ("expire", "applied", '{"status":"not_expired"}', "state_changed"),
        ("expire", "not_expired", "[]", "state_changed"),
    ],
)
def test_legacy_expiry_receipt_action_and_result_must_agree(application, action, status, result_json, expected):
    insert_invites(application)
    payload = application.repository._json({"action": "expire", "invite_id": "invite-000", "user_id": "", "group_id": ""})
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute(
            "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES('expire',? ,?,'team',?,?)",
            (payload, status, result_json, action),
        )
    assert application.expire("expire", "invite-000", 200).status == expected
    invite = row(application, "SELECT status FROM dungeon_team_invites")
    assert invite["status"] == ("expired" if expected == "applied" else "pending")
    if expected == "applied":
        assert row(application, "SELECT action FROM dungeon_team_operations WHERE operation_id='expire'")["action"] == "expire"


def test_index_on_another_table_does_not_allow_unindexed_scan(application):
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("DROP INDEX dungeon_team_invites_expiry_idx")
        uow.execute("CREATE TABLE unrelated(expires_at REAL,invite_id TEXT,status TEXT,consumed_at TEXT)")
        uow.execute("CREATE INDEX dungeon_team_invites_expiry_idx ON unrelated(expires_at,invite_id) WHERE status='pending' AND consumed_at IS NULL")
    assert application.due_invites(200).status == "schema_missing"


def test_bad_legacy_invite_does_not_stop_healthy_due_record(application):
    insert_invites(application, 2)
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("UPDATE dungeon_team_invites SET created_at='bad' WHERE invite_id='invite-000'")
    notified = []

    async def notify(invite):
        notified.append(invite.invite_id)

    report = asyncio.run(worker(application, notify).run())
    assert (report.scanned, report.applied, report.failures) == (2, 1, 1)
    assert notified == ["invite-001"]
    assert row(application, "SELECT status FROM dungeon_team_invites WHERE invite_id='invite-000'")["status"] == "pending"


def test_full_poison_batch_does_not_starve_later_healthy_invite(application):
    insert_invites(application, 101)
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("UPDATE dungeon_team_invites SET created_at='bad' WHERE invite_id<'invite-100'")
    notified = []

    async def notify(invite):
        notified.append(invite.invite_id)

    async def scenario():
        instance = worker(application, notify)
        first = await instance.run()
        second = await instance.run()
        third = await instance.run()
        assert (first.scanned, first.failures, second.applied, third.failures) == (100, 100, 1, 100)
        assert notified == ["invite-100"]

    asyncio.run(scenario())


def test_nullable_legacy_action_can_finalize_verified_expiry(application):
    insert_invites(application)
    payload = application.repository._json({"action": "expire", "invite_id": "invite-000", "user_id": "", "group_id": ""})
    with DatabaseUnitOfWork(application.repository.database) as uow:
        uow.execute("ALTER TABLE dungeon_team_operations RENAME TO old_operations")
        uow.execute("CREATE TABLE dungeon_team_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,team_id TEXT,result_json TEXT,action TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
        uow.execute("INSERT INTO dungeon_team_operations SELECT * FROM old_operations")
        uow.execute("DROP TABLE old_operations")
        uow.execute("INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES('expire',?,'not_expired','team','{}',NULL)", (payload,))
    assert application.expire("expire", "invite-000", 200).status == "applied"
    assert row(application, "SELECT action FROM dungeon_team_operations WHERE operation_id='expire'")["action"] == "expire"


@pytest.mark.parametrize("timeout", [0, 0.05, 30])
def test_uow_honors_explicit_lock_timeout(tmp_path, timeout):
    with DatabaseUnitOfWork(tmp_path / "db.sqlite3", timeout=timeout) as uow:
        assert uow.query_one("PRAGMA busy_timeout")["timeout"] == int(timeout * 1000)


def test_locked_database_does_not_block_event_loop_or_accumulate_workers(application):
    insert_invites(application, 100)

    async def scenario():
        notify = AsyncMock()
        instance = worker(application, notify)
        beats = 0
        stop = False

        async def heartbeat():
            nonlocal beats
            while not stop:
                beats += 1
                await asyncio.sleep(0)

        connection = sqlite3.connect(application.repository.database)
        connection.execute("BEGIN IMMEDIATE")
        pulse = asyncio.create_task(heartbeat())
        try:
            before = asyncio.get_running_loop().time()
            report = await instance.run()
            assert asyncio.get_running_loop().time() - before < 1
            assert (report.status, report.applied, report.failures) == ("busy", 0, 1)
            assert beats > 0
            notify.assert_not_awaited()
            task = asyncio.create_task(instance.run())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            connection.rollback()
            connection.close()
            stop = True
            await pulse
        assert (await instance.run()).applied == 100

    asyncio.run(scenario())


def test_scan_lock_conflict_clears_cursor_and_does_not_notify(application):
    error = sqlite3.OperationalError("database is locked")
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY

    def locked(*args, **kwargs):
        raise error

    application.due_invites = locked
    notify = AsyncMock()
    instance = worker(application, notify)
    instance._cursor = (160, "previous")
    assert asyncio.run(instance.run()).status == "busy"
    assert instance._cursor is None
    notify.assert_not_awaited()


def test_invite_worker_does_not_import_database_driver():
    from scripts.check_architecture import check_feature_connections

    assert not any("dungeon/invite_expiry.py" in error for error in check_feature_connections())
