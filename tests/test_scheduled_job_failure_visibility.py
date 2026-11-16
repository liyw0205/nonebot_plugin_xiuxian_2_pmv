from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
import pytest

nonebot.init()


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
        "list_sects_with_member_count",
        side_effect=RuntimeError("sect db unavailable"),
    ):
        with pytest.raises(RuntimeError, match="宗门状态检测失败"):
            asyncio.run(sect.auto_handle_inactive_sect_owners())
