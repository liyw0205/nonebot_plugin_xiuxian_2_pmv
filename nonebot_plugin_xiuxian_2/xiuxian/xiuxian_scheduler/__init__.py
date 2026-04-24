from nonebot import require
from nonebot.log import logger

from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage,
    XIUXIAN_IMPART_BUFF,
    backup_db_files
)
from ..xiuxian_arena import reset_arena_daily_challenges, reduce_arena_rank
from ..xiuxian_base import (
    reset_lottery_participants,
    reset_stone_limits,
    reset_xiangyuan_daily
)
from ..xiuxian_boss import set_boss_limits_reset
from ..xiuxian_buff import two_exp_cd_up
from ..xiuxian_Illusion import reset_illusion_data
from ..xiuxian_impart_pk import impart_re, impart_lv
from ..xiuxian_Interactive import reset_data_by_time
from ..xiuxian_rift import scheduled_rift_generation
from ..xiuxian_sect import resetusertask, auto_handle_inactive_sect_owners
from ..xiuxian_tower import reset_tower_floors
from ..xiuxian_work import resetrefreshnum
from ..xiuxian_compensation import auto_clean_expired_items

sql_message = XiuxianDateManage()
xiuxian_impart = XIUXIAN_IMPART_BUFF()
scheduler = require("nonebot_plugin_apscheduler").scheduler


# =========================
# 通用安全执行包装
# =========================
async def _run_job(job_name: str, func, *args, **kwargs):
    """
    安全执行单个定时任务，避免某个任务异常影响其它任务日志判断
    """
    try:
        logger.opt(colors=True).info(f"<cyan>[定时任务开始]</cyan> <green>{job_name}</green>")
        result = func(*args, **kwargs)
        if hasattr(result, "__await__"):
            await result
        logger.opt(colors=True).success(f"<cyan>[定时任务完成]</cyan> <green>{job_name}</green>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>[定时任务失败] {job_name}: {e}</red>")


# =========================
# 每日 0 点系列：错峰拆分
# =========================

@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=0,
    second=0,
    id="daily_reset_sign",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_sign():
    """每日签到重置"""
    try:
        sql_message.sign_remake()
        logger.opt(colors=True).info("<green>每日修仙签到重置成功！</green>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>每日修仙签到重置失败：{e}</red>")


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=0,
    second=10,
    id="daily_reset_beg",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_beg():
    """每日奇缘重置"""
    try:
        sql_message.beg_remake()
        logger.opt(colors=True).info("<green>仙途奇缘重置成功！</green>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>仙途奇缘重置失败：{e}</red>")


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=0,
    second=20,
    id="daily_reset_day_num",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_day_num():
    """每日丹药使用次数重置"""
    try:
        sql_message.day_num_reset()
        logger.opt(colors=True).info("<green>每日丹药使用次数重置成功！</green>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>每日丹药使用次数重置失败：{e}</red>")


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=0,
    second=30,
    id="daily_reset_mixelixir_num",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_mixelixir_num():
    """每日炼丹次数重置"""
    try:
        sql_message.mixelixir_num_reset()
        logger.opt(colors=True).info("<green>每日炼丹次数重置成功！</green>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>每日炼丹次数重置失败：{e}</red>")


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=0,
    second=40,
    id="daily_reset_impart_num",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_impart_num():
    """每日传承抽卡次数重置"""
    try:
        xiuxian_impart.impart_num_reset()
        logger.opt(colors=True).info("<green>每日传承抽卡次数重置成功！</green>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>每日传承抽卡次数重置失败：{e}</red>")


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    second=0,
    id="daily_reset_arena",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_arena():
    """竞技场每日重置"""
    await _run_job("竞技场每日重置", reset_arena_daily_challenges)


@scheduler.scheduled_job(
    "cron",
    day_of_week="fri",
    hour=20,
    minute=0,
    second=0,
    id="weekly_reduce_arena_rank",
    misfire_grace_time=600,
    coalesce=True,
    max_instances=1
)
async def weekly_reduce_arena_rank():
    """每周五晚8点竞技场降段"""
    await _run_job("竞技场每周降段", reduce_arena_rank, 2)

@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    second=10,
    id="daily_reset_lottery",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_lottery():
    """鸿运参与者重置"""
    await _run_job("鸿运参与者重置", reset_lottery_participants)


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    second=20,
    id="daily_reset_stone_limits",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_stone_limits_job():
    """送灵石额度重置"""
    await _run_job("送灵石额度重置", reset_stone_limits)


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    second=30,
    id="daily_reset_xiangyuan",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_xiangyuan():
    """送仙缘每日重置"""
    await _run_job("送仙缘每日重置", reset_xiangyuan_daily)


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    second=40,
    id="daily_reset_boss_limits",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_boss_limits():
    """世界BOSS额度重置"""
    await _run_job("世界BOSS额度重置", set_boss_limits_reset)


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    second=50,
    id="daily_reset_two_exp",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_two_exp():
    """双修次数重置"""
    await _run_job("双修次数重置", two_exp_cd_up)


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=2,
    second=0,
    id="daily_reset_impart_pk",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_impart_pk():
    """虚神界对决/投影等每日重置"""
    await _run_job("虚神界每日重置", impart_re)


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=2,
    second=10,
    id="daily_clean_expired_items",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_clean_expired_items():
    """清理过期礼包/补偿/兑换码"""
    await _run_job("清理过期礼包/补偿/兑换码", auto_clean_expired_items)


# =========================
# 每周任务：拆开并延后，避免 0:01 拥堵
# =========================

@scheduler.scheduled_job(
    "cron",
    day_of_week="mon",
    hour=0,
    minute=5,
    second=0,
    id="weekly_reduce_impart_lv",
    misfire_grace_time=600,
    coalesce=True,
    max_instances=1
)
async def weekly_reduce_impart_lv():
    """每周一降低虚神界等级"""
    await _run_job("每周虚神界等级下调", impart_lv, 2, 10)


@scheduler.scheduled_job(
    "cron",
    day_of_week="mon",
    hour=0,
    minute=5,
    second=20,
    id="weekly_reset_tower_floors",
    misfire_grace_time=600,
    coalesce=True,
    max_instances=1
)
async def weekly_reset_tower_floors():
    """每周一重置通天塔层数"""
    await _run_job("每周重置通天塔层数", reset_tower_floors)


# =========================
# 每日虚神界深入
# =========================

@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=6,
    second=0,
    id="daily_add_impart_lv",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_add_impart_lv():
    """每日增加虚神界等级"""
    await _run_job("每日虚神界等级提升", impart_lv, 1, 1)


# =========================
# 每日 8 点系列
# =========================

@scheduler.scheduled_job(
    "cron",
    hour=8,
    minute=0,
    second=0,
    id="daily_reset_illusion",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_illusion():
    """幻境寻心重置"""
    await _run_job("幻境寻心重置", reset_illusion_data)


@scheduler.scheduled_job(
    "cron",
    hour=8,
    minute=0,
    second=15,
    id="daily_reset_sect_task",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_sect_task():
    """宗门丹药/宗门任务重置"""
    await _run_job("宗门丹药/宗门任务重置", resetusertask)


@scheduler.scheduled_job(
    "cron",
    hour=8,
    minute=0,
    second=30,
    id="daily_reset_work_refresh_num",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def daily_reset_work_refresh_num():
    """悬赏令次数重置"""
    await _run_job("悬赏令次数重置", resetrefreshnum)


# =========================
# 每日 0/12 点系列
# =========================

@scheduler.scheduled_job(
    "cron",
    hour="0,12",
    minute=5,
    second=0,
    id="scheduled_rift_generation_job",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def scheduled_rift_generation_job():
    """重置秘境"""
    await _run_job("秘境重置", scheduled_rift_generation)


@scheduler.scheduled_job(
    "cron",
    hour="0,12",
    minute=5,
    second=20,
    id="auto_handle_inactive_sect_owners_job",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def auto_handle_inactive_sect_owners_job():
    """处理宗门状态"""
    await _run_job("处理宗门状态", auto_handle_inactive_sect_owners)


@scheduler.scheduled_job(
    "cron",
    hour="0,12",
    minute=5,
    second=40,
    id="reset_data_by_time_job",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1
)
async def reset_data_by_time_job():
    """处理早/晚数据"""
    await _run_job("处理早晚数据", reset_data_by_time)


# =========================
# 每 4 小时数据库备份
# =========================

@scheduler.scheduled_job(
    "cron",
    hour="*/4",
    minute=10,
    second=0,
    id="backup_database_files",
    misfire_grace_time=1800,
    coalesce=True,
    max_instances=1
)
async def backup_database_files():
    """定时备份数据库"""
    try:
        logger.opt(colors=True).info("<cyan>[定时任务开始]</cyan> <green>数据库备份</green>")
        success, message = backup_db_files()
        if success:
            logger.opt(colors=True).info(f"<green>{message}</green>")
            logger.opt(colors=True).success("<cyan>[定时任务完成]</cyan> <green>数据库备份</green>")
        else:
            logger.opt(colors=True).error(f"<red>{message}</red>")
    except Exception as e:
        logger.opt(colors=True).error(f"<red>[定时任务失败] 数据库备份: {e}</red>")