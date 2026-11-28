from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

nonebot.init()


def _execute_job(function):
    async def run():
        scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
        outcome = asyncio.get_running_loop().create_future()
        scheduler.add_listener(outcome.set_result, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        scheduler.add_job(function, "date", id="isolated-failure-probe")
        scheduler.start()
        try:
            return await asyncio.wait_for(outcome, timeout=5)
        finally:
            scheduler.shutdown(wait=False)

    return asyncio.run(run())


def _sect_database(path, branch):
    from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations

    closed = int(branch in {"empty", "no_successor", "inherit"})
    with DatabaseUnitOfWork(path, immediate=True) as uow:
        apply_sect_operations(uow)
        uow.execute(
            "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_scale INTEGER,"
            "sect_owner TEXT,closed INTEGER,join_open INTEGER,elixir_room_level INTEGER,sect_materials INTEGER)"
        )
        uow.execute("INSERT INTO sects VALUES(1,'test-sect',1,?, ?,1,1,500)",
                    (None if closed else "owner", closed))
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,sect_id INTEGER,"
            "sect_position INTEGER,sect_contribution INTEGER,sect_task INTEGER,sect_elixir_get INTEGER)"
        )
        uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)")
        if not closed:
            uow.execute("INSERT INTO user_xiuxian VALUES('owner','test-owner',1,0,1,3,1)")
            uow.execute("INSERT INTO user_cd VALUES('owner','2026-09-01 00:00:00')")
        if branch in {"no_successor", "inherit", "close"}:
            uow.execute("INSERT INTO user_xiuxian VALUES('candidate','test-candidate',1,1,10,3,1)")
            last = "2026-09-01 00:00:00" if branch == "no_successor" else "2026-11-16 00:00:00"
            uow.execute("INSERT INTO user_cd VALUES('candidate',?)", (last,))


def _sect_snapshot(path):
    with DatabaseUnitOfWork(path, read_only=True) as uow:
        return [uow.query_all(f"SELECT * FROM {table} ORDER BY rowid") for table in ("sects", "user_xiuxian", "user_cd")]


@pytest.mark.parametrize("branch", ("empty", "sole_owner", "no_successor", "inherit", "close"))
def test_sect_scheduled_rejection_reaches_scheduler_and_recovers_with_real_sqlite(tmp_path, branch):
    from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
    from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect

    database = tmp_path / "sect.db"
    _sect_database(database, branch)
    table = {"inherit": "sect_owner_inherit_operations", "close": "sect_close_mountain_operations"}.get(
        branch, "sect_inactive_disband_operations"
    )
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f"DROP TABLE {table}")
    before = _sect_snapshot(database)
    with patch.object(sect, "sect_application", SectApplication(database)), patch.object(
        sect, "runtime_clock", SimpleNamespace(now=lambda: datetime(2026, 11, 17, 12, tzinfo=timezone.utc))
    ), patch.object(sect, "logger") as logger:
        failed = _execute_job(sect.auto_handle_inactive_sect_owners)
        assert failed.code == EVENT_JOB_ERROR
        assert "schema_missing" in str(failed.exception)
        assert "sect_id=1" in str(failed.exception)
        assert _sect_snapshot(database) == before
        assert not any("宗门状态检测处理完成" in str(call) for call in logger.info.call_args_list)
        with DatabaseUnitOfWork(database) as uow:
            apply_sect_operations(uow)
        assert _execute_job(sect.auto_handle_inactive_sect_owners).code == EVENT_JOB_EXECUTED
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one(f"SELECT COUNT(*) AS n FROM {table}")["n"] == 1
            state = uow.query_one("SELECT sect_owner,closed FROM sects WHERE sect_id=1")
            if branch in {"empty", "sole_owner", "no_successor"}:
                assert state is None
                assert not uow.query_all("SELECT * FROM user_xiuxian WHERE sect_id=1")
            elif branch == "close":
                assert state == {"sect_owner": None, "closed": 1}
            else:
                assert state == {"sect_owner": "candidate", "closed": 0}
        # Close/inherit may legitimately progress in two scans; the settled state is stable.
        for _ in range(2):
            assert _execute_job(sect.auto_handle_inactive_sect_owners).code == EVENT_JOB_EXECUTED
        stable = _sect_snapshot(database)
        assert _execute_job(sect.auto_handle_inactive_sect_owners).code == EVENT_JOB_EXECUTED
        assert _sect_snapshot(database) == stable
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one(f"SELECT COUNT(*) AS n FROM {table}")["n"] == 1
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]


@pytest.mark.parametrize("failure", ("schema_missing", "operation_conflict"))
def test_sect_daily_reset_rejection_reports_specific_reason_without_writes(tmp_path, failure):
    from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
    from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect

    database = tmp_path / "sect.db"
    _sect_database(database, "sole_owner")
    application = SectApplication(database)
    if failure == "schema_missing":
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("DROP TABLE sect_daily_reset_operations")
    else:
        assert application.reset_daily_maintenance("2026-11-17", {1: 201}).status == "applied"
    before = _sect_snapshot(database)
    with patch.object(sect, "sect_application", application), patch.object(
        sect, "runtime_clock", SimpleNamespace(now=lambda: datetime(2026, 11, 17, tzinfo=timezone.utc))
    ), patch.object(sect, "config", {"宗门丹房参数": {"elixir_room_level": {1: {"level_up_cost": {"建设度": 200}}}}}):
        failed = _execute_job(sect.resetusertask)
        assert failed.code == EVENT_JOB_ERROR
        assert failure in str(failed.exception)
        assert "2026-11-17" in str(failed.exception)
        assert _sect_snapshot(database) == before
        if failure == "schema_missing":
            with DatabaseUnitOfWork(database) as uow:
                apply_sect_operations(uow)
            assert _execute_job(sect.resetusertask).code == EVENT_JOB_EXECUTED
            settled = _sect_snapshot(database)
            assert _execute_job(sect.resetusertask).code == EVENT_JOB_EXECUTED
            assert _sect_snapshot(database) == settled


def test_arena_daily_reward_rejection_is_scheduler_error_and_retries_idempotently(tmp_path):
    from datetime import datetime
    from nonebot_plugin_xiuxian_2.features.arena.migrations import (
        apply_arena_daily_reward_player,
        apply_arena_season_reward,
    )
    from nonebot_plugin_xiuxian_2.features.arena.season_reward_application import ArenaSeasonRewardApplication
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_arena
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_scheduler as scheduler

    game, player = tmp_path / "arena-game.db", tmp_path / "arena-player.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u1')")
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        apply_arena_season_reward(uow)
        uow.execute(
            "CREATE TRIGGER refuse_arena_receipt BEFORE INSERT ON arena_season_reward_operations "
            "BEGIN SELECT RAISE(IGNORE); END"
        )
    with DatabaseUnitOfWork(player, immediate=True) as uow:
        apply_arena_daily_reward_player(uow)
        uow.execute("INSERT INTO arena VALUES('u1',2000,'黄金',10,20,3,1,1,'old','old')")

    class FixedClock:
        def now(self):
            return datetime(2026, 11, 18, 16, 0, tzinfo=timezone.utc)

    application = ArenaSeasonRewardApplication(game, player, clock=FixedClock())
    with patch.object(xiuxian_arena, "arena_season_reward_application", application):
        failed = _execute_job(scheduler.daily_reset_arena)
        assert failed.code == EVENT_JOB_ERROR
        assert "arena_season_reward_receipt_write_rejected: receipt" in str(failed.exception)
        with DatabaseUnitOfWork(player, read_only=True) as uow:
            assert uow.query_one("SELECT honor_points,total_honor_earned,daily_challenges_used,last_reset_date FROM arena WHERE user_id='u1'") == {
                "honor_points": 10, "total_honor_earned": 20,
                "daily_challenges_used": 3, "last_reset_date": "old",
            }
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one("SELECT COUNT(*) AS n FROM back")["n"] == 0
            assert uow.query_one("SELECT COUNT(*) AS n FROM arena_season_reward_operations")["n"] == 0
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("DROP TRIGGER refuse_arena_receipt")
        assert _execute_job(scheduler.daily_reset_arena).code == EVENT_JOB_EXECUTED
        settled = application.reset_daily()
        assert settled[0]["status"] == "duplicate"
        with DatabaseUnitOfWork(player, read_only=True) as uow:
            assert uow.query_one("SELECT honor_points,total_honor_earned,daily_challenges_used,last_reset_date FROM arena WHERE user_id='u1'") == {
                "honor_points": 810, "total_honor_earned": 820,
                "daily_challenges_used": 0, "last_reset_date": "2026-11-18",
            }
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one("SELECT COUNT(*) AS n FROM back")["n"] == 0
            assert uow.query_one("SELECT COUNT(*) AS n FROM arena_season_reward_operations")["n"] == 1
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
        with DatabaseUnitOfWork(player, read_only=True) as uow:
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]


def test_arena_weekly_rank_rejection_is_scheduler_error_and_recovers(tmp_path):
    from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state, apply_arena_weekly_rank_reduction
    from nonebot_plugin_xiuxian_2.features.arena.weekly_rank_application import ArenaWeeklyRankApplication
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_arena
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_scheduler as scheduler

    database = tmp_path / "arena-weekly-player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_arena_state(uow)
        apply_arena_weekly_rank_reduction(uow)
        uow.execute("INSERT INTO arena(user_id,score,rank,win_streak) VALUES('u1',1100,'青铜',2)")
        uow.execute("INSERT INTO arena(user_id,score,rank,win_streak) VALUES('u2',2000,'黄金',3)")
        uow.execute(
            "CREATE TRIGGER ignore_rank_target BEFORE UPDATE OF status ON arena_weekly_rank_reduction_targets "
            "WHEN NEW.user_id='u2' AND NEW.status='applied' BEGIN SELECT RAISE(IGNORE); END"
        )
    application = ArenaWeeklyRankApplication(database)
    with patch.object(xiuxian_arena, "arena_weekly_rank_application", application):
        failed = _execute_job(scheduler.weekly_reduce_arena_rank)
        assert failed.code == EVENT_JOB_ERROR
        assert "arena_weekly_rank_target_write_rejected" in str(failed.exception)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_all("SELECT user_id,score,rank,win_streak FROM arena ORDER BY user_id") == [
                {"user_id": "u1", "score": 1100, "rank": "青铜", "win_streak": 2},
                {"user_id": "u2", "score": 2000, "rank": "黄金", "win_streak": 3},
            ]
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("DROP TRIGGER ignore_rank_target")
        assert _execute_job(scheduler.weekly_reduce_arena_rank).code == EVENT_JOB_EXECUTED
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_all("SELECT score,rank,win_streak FROM arena ORDER BY user_id") == [
                {"score": 1000, "rank": "青铜", "win_streak": 0},
                {"score": 1000, "rank": "青铜", "win_streak": 0},
            ]
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]


def test_sect_batch_preserves_successful_sects_and_reports_failed_subject(tmp_path):
    from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
    from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect

    database = tmp_path / "sect.db"
    _sect_database(database, "inherit")
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DROP TABLE sect_owner_inherit_operations")
        uow.execute("INSERT INTO sects VALUES(2,'empty-sect',1,NULL,1,0,0,0)")
    with patch.object(sect, "sect_application", SectApplication(database)), patch.object(
        sect, "runtime_clock", SimpleNamespace(now=lambda: datetime(2026, 11, 17, 12, tzinfo=timezone.utc))
    ):
        failed = _execute_job(sect.auto_handle_inactive_sect_owners)
        assert failed.code == EVENT_JOB_ERROR
        assert "sect_id=1 status=schema_missing" in str(failed.exception)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one("SELECT sect_owner FROM sects WHERE sect_id=1") == {"sect_owner": None}
            assert uow.query_one("SELECT sect_id FROM sects WHERE sect_id=2") is None
            assert uow.query_one("SELECT COUNT(*) AS n FROM sect_inactive_disband_operations")["n"] == 1
        with DatabaseUnitOfWork(database) as uow:
            apply_sect_operations(uow)
        assert _execute_job(sect.auto_handle_inactive_sect_owners).code == EVENT_JOB_EXECUTED
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one("SELECT sect_owner FROM sects WHERE sect_id=1") == {"sect_owner": "candidate"}
            assert uow.query_one("SELECT COUNT(*) AS n FROM sect_inactive_disband_operations")["n"] == 1
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]


@pytest.mark.parametrize("failure", ("schema_missing", "invalid_harvest_time", "receipt_exception"))
def test_puppet_inner_rejection_or_write_failure_is_not_a_successful_job(tmp_path, failure):
    from nonebot_plugin_xiuxian_2.features.puppet.application import PuppetApplication
    from nonebot_plugin_xiuxian_2.features.puppet.migrations import apply_puppet_operations
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_puppet as puppet
    from tests.test_puppet_harvest_production_path import real_player_schema, seed_player_row

    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,stone INTEGER,blessed_spot_flag INTEGER,puppet_status INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u1','化神境初期',999999999,1,1)")
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
        apply_puppet_operations(uow)
        if failure == "schema_missing":
            uow.execute("DROP TABLE puppet_harvest_operations")
        elif failure == "receipt_exception":
            uow.execute("CREATE TRIGGER reject_harvest BEFORE INSERT ON puppet_harvest_operations BEGIN SELECT RAISE(ABORT, 'isolated harvest receipt failure'); END")
    real_player_schema(player)
    seed_player_row(player, last="bad-time" if failure == "invalid_harvest_time" else "2026-11-15 00:00:00")
    with patch.object(puppet, "puppet_application", PuppetApplication(game, player)), patch.object(
        puppet.xiuxian_impart, "get_user_impart_info_with_id", return_value=None
    ), patch.object(puppet, "UserBuffDate") as buff, patch.object(
        puppet.items, "get_random_id_list_by_rank_and_item_type", return_value=[]
    ), patch.object(puppet, "log_message"), patch.object(
        puppet, "runtime_clock", SimpleNamespace(now=lambda: datetime(2026, 11, 17, 12, tzinfo=timezone.utc))
    ):
        buff.return_value.get_user_main_buff_data.return_value = None
        failed = _execute_job(puppet.auto_harvest_scheduled)
        assert failed.code == EVENT_JOB_ERROR
        assert (failure if failure != "receipt_exception" else "isolated harvest receipt failure") in str(failed.exception)
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one("SELECT stone FROM user_xiuxian")["stone"] == 999999999
            assert uow.query_one("SELECT COUNT(*) AS n FROM back")["n"] == 0
        with DatabaseUnitOfWork(player, read_only=True) as uow:
            last = uow.query_one('SELECT "收取时间" AS last FROM mix_elixir_info')["last"]
            assert last == ("bad-time" if failure == "invalid_harvest_time" else "2026-11-15 00:00:00")
        with DatabaseUnitOfWork(game) as uow:
            if failure == "schema_missing":
                apply_puppet_operations(uow)
            elif failure == "receipt_exception":
                uow.execute("DROP TRIGGER reject_harvest")
        if failure == "invalid_harvest_time":
            with DatabaseUnitOfWork(player) as uow:
                uow.execute('UPDATE mix_elixir_info SET "收取时间"=?', ("2026-11-15 00:00:00",))
        assert _execute_job(puppet.auto_harvest_scheduled).code == EVENT_JOB_EXECUTED
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            stone = uow.query_one("SELECT stone FROM user_xiuxian")["stone"]
            assert stone == 999999999 - puppet.PUPPET_CONFIG[1]["harvest_cost"]
            quantity = uow.query_one("SELECT goods_num FROM back")["goods_num"]
            assert quantity == 5
        assert _execute_job(puppet.auto_harvest_scheduled).code == EVENT_JOB_EXECUTED
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one("SELECT stone FROM user_xiuxian")["stone"] == stone
            assert uow.query_one("SELECT goods_num FROM back")["goods_num"] == quantity
            assert uow.query_one("SELECT COUNT(*) AS n FROM puppet_harvest_operations")["n"] == 1
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
        with DatabaseUnitOfWork(player, read_only=True) as uow:
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]


@pytest.mark.parametrize("failure", ("exception", "request_failed", "remote_rejected", "history_write_failed"))
def test_newapi_partial_batch_reports_inner_failure_and_preserves_actual_history(tmp_path, failure):
    from nonebot_plugin_xiuxian_2.features.entertainment.application import EntertainmentApplication
    from nonebot_plugin_xiuxian_2.features.entertainment.tests.newapi_fixtures import migrate_newapi_state
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import newapi_commands, newapi_scheduler, newapi_store

    database = tmp_path / "game.db"
    migrate_newapi_state(database, tmp_path / "bindings", tmp_path / "history")
    application = EntertainmentApplication(database)
    for index in (1, 2, 3):
        assert application.bind_newapi_account(
            operation_id=f"bind-{index}", user_id="u", mode="token", api_user_id=str(index),
            secret="isolated-secret", base_url="https://newapi.test",
        ).ok
    targets = application.resolve_checkin_targets(user_id="u", selector="").targets
    for target in targets:
        assert application.toggle_auto_checkin(
            operation_id=f"auto-{target.index}", user_id="u", account_id=target.account_id,
            index=target.index, legacy_state_path="isolated-legacy.json",
        ).ok
    original = RuntimeError("isolated checkin worker failure")
    recovering = False
    calls = []

    def checkin(mode, api_user_id, secret, base):
        calls.append(api_user_id)
        if api_user_id == "2" and not recovering:
            if failure == "exception":
                raise original
            if failure == "request_failed":
                return {"_error": "connection timeout"}
            if failure == "remote_rejected":
                return {"success": False, "message": "checkin disabled"}
        return {"success": True, "message": "signed"} if api_user_id != "3" else {
            "success": False, "message": "already signed in"
        }

    append_history = newapi_commands.append_checkin_history

    def history(qq_id, **kwargs):
        if failure == "history_write_failed" and kwargs["account_index"] == 2 and not recovering:
            raise OSError("isolated history unavailable")
        return append_history(qq_id, **kwargs)

    with patch.object(newapi_store, "entertainment_application", application), patch.object(
        newapi_commands, "do_checkin", side_effect=checkin
    ), patch.object(newapi_commands, "append_checkin_history", side_effect=history), patch.object(
        newapi_scheduler, "logger"
    ) as logger:
        failed = _execute_job(newapi_scheduler.newapi_auto_checkin_daily)
        assert failed.code == EVENT_JOB_ERROR
        assert failure in str(failed.exception)
        assert "isolated-secret" not in str(failed.exception)
        assert failed.exception.completed == 2
        assert calls == ["1", "2", "3"]
        if failure == "exception":
            assert failed.exception.__cause__ is original
        if failure == "request_failed":
            assert failed.exception.failures[0][2] == "connection timeout"
        if failure == "remote_rejected":
            assert failed.exception.failures[0][2] == "checkin disabled"
        assert not any("自动签到完成" in str(call) for call in logger.opt.return_value.info.call_args_list)
        records = application.list_checkin_history(user_id="u")
        assert len(records) == (2 if failure in {"exception", "history_write_failed"} else 3)
        assert {row["index"] for row in records} >= {1, 3}
        recovering = True
        calls.clear()
        assert _execute_job(newapi_scheduler.newapi_auto_checkin_daily).code == EVENT_JOB_EXECUTED
        assert calls == ["1", "2", "3"]
        assert len(application.list_checkin_history(user_id="u")) == 3
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]


def test_puppet_harvest_scheduler_rethrows_repository_failure() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_puppet as puppet

    with patch.object(
        puppet.puppet_application,
        "enabled_user_high_watermark",
        side_effect=RuntimeError("puppet db unavailable"),
    ):
        with pytest.raises(RuntimeError, match="puppet db unavailable"):
            asyncio.run(puppet.auto_harvest_scheduled())


def test_dungeon_invite_scheduler_rethrows_worker_failure() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon

    worker = SimpleNamespace(run=AsyncMock(side_effect=RuntimeError("invite db unavailable")))
    with patch.object(dungeon, "team_invite_expiry_worker", worker):
        with pytest.raises(RuntimeError, match="invite db unavailable"):
            asyncio.run(dungeon.expire_pending_team_invites())


def test_dungeon_invite_scheduler_rethrows_explicit_failed_result() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon

    worker = SimpleNamespace(
        run=AsyncMock(
            return_value=SimpleNamespace(
                status="failed",
                failures=("invite-1",),
                notification_failures=(),
            )
        )
    )
    with patch.object(dungeon, "team_invite_expiry_worker", worker):
        with pytest.raises(RuntimeError, match="失败状态"):
            asyncio.run(dungeon.expire_pending_team_invites())


def test_newapi_scheduler_rethrows_checkin_failure() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import newapi_scheduler

    with patch.object(
        newapi_scheduler,
        "run_scheduled_auto_checkins",
        new=AsyncMock(side_effect=RuntimeError("newapi unavailable")),
    ):
        with pytest.raises(RuntimeError, match="newapi unavailable"):
            asyncio.run(newapi_scheduler.newapi_auto_checkin_daily())


def test_stamina_scheduler_rethrows_recovery_failure() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import lay_out

    with patch.object(
        lay_out,
        "recover_player_stamina",
        side_effect=RuntimeError("stamina db unavailable"),
    ):
        with pytest.raises(RuntimeError, match="stamina db unavailable"):
            lay_out.limit_all_stamina_()


def test_sect_maintenance_rethrows_outer_failure() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect

    with patch.object(
        sect.sect_application,
        "list_maintenance_sects",
        side_effect=RuntimeError("sect db unavailable"),
    ):
        with pytest.raises(RuntimeError, match="宗门状态检测失败"):
            asyncio.run(sect.auto_handle_inactive_sect_owners())
