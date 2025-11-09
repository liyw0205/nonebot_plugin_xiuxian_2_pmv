from nonebot import require
from nonebot.log import logger
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, backup_db_files
from ..xiuxian_back import auto_merge_fangshi_to_xianshi
from ..xiuxian_base import reset_lottery_participants, reset_stone_limits, reset_xiangyuan_daily
from ..xiuxian_boss import set_boss_limits_reset
from ..xiuxian_buff import two_exp_cd_up
from ..xiuxian_Illusion import reset_illusion_data
from ..xiuxian_impart_pk import impart_re, impart_lv
from ..xiuxian_Interactive import reset_data_by_time
from ..xiuxian_rift import scheduled_rift_generation
from ..xiuxian_sect import resetusertask, auto_handle_inactive_sect_owners
from ..xiuxian_tower import reset_tower_floors
from ..xiuxian_work import resetrefreshnum

sql_message = XiuxianDateManage()
scheduler = require("nonebot_plugin_apscheduler").scheduler


@scheduler.scheduled_job("cron", hour=0, minute=0)
async def _():# 每天0点
# 重置每日签到
    sql_message.sign_remake()
    logger.opt(colors=True).info(f"<green>每日修仙签到重置成功！</green>")
    
# 重置奇缘
    sql_message.beg_remake()
    logger.opt(colors=True).info(f"<green>仙途奇缘重置成功！</green>")

# 重置丹药每日使用次数
    sql_message.day_num_reset()
    logger.opt(colors=True).info(f"<green>每日丹药使用次数重置成功！</green>")    
    await reset_lottery_participants()  # 借运/鸿运
    await reset_stone_limits()  # 送灵石额度
    await reset_xiangyuan_daily()  # 送仙缘
    await set_boss_limits_reset()  # 世界BOSS额度
    await two_exp_cd_up()  # 双修次数
    await impart_re()  # 虚神界对决


@scheduler.scheduled_job("cron", hour=8, minute=0)
async def _():  # 每天8点
    await reset_illusion_data()  # 幻境寻心
    await resetusertask()  # 宗门丹药/宗门任务
    await resetrefreshnum()  # 悬赏令次数
    
@scheduler.scheduled_job("cron", day_of_week=0, hour=0, minute=0)
async def _():  # 每周一0点
    await impart_lv()  # 深入虚神界
    await reset_tower_floors()  # 重置通天塔层数

@scheduler.scheduled_job("cron", hour='0,12', minute=5)
async def _():  # 每天0/12点5分
    await scheduled_rift_generation()  # 重置秘境
    await auto_handle_inactive_sect_owners()  # 处理宗门状态
    await reset_data_by_time()  # 处理早/晚数据

@scheduler.scheduled_job("cron", hour='*/4')
async def _():  # 每4小时执行一次
    """定时备份数据库"""
    success, message = backup_db_files()
    if success:
        logger.opt(colors=True).info(f"<green>{message}</green>")
    else:
        logger.opt(colors=True).error(f"<red>{message}</red>")