"""NewAPI 每日自动签到。"""
from __future__ import annotations

from nonebot import require
from nonebot.log import logger

from .newapi_commands import run_scheduled_auto_checkins

scheduler = require("nonebot_plugin_apscheduler").scheduler


@scheduler.scheduled_job(
    "cron",
    hour=12,
    minute=30,
    id="newapi_auto_checkin_daily",
    misfire_grace_time=600,
    coalesce=True,
    max_instances=1,
)
async def newapi_auto_checkin_daily():
    try:
        n = await run_scheduled_auto_checkins()
        logger.opt(colors=True).info(
            f"<green>NewAPI 自动签到完成，共处理 {n} 个账号</green>"
        )
    except Exception as e:
        logger.opt(colors=True).error(f"<red>NewAPI 自动签到失败：{e}</red>")