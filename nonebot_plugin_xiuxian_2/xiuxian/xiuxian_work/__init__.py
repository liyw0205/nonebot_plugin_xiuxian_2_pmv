import os
import random
import asyncio
import json
from typing import Tuple, Any, Dict
from nonebot import on_regex, require, on_command
from nonebot.params import RegexGroup
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, OtherSet
from .work_handle import workhandle
from datetime import datetime, timedelta
from ..xiuxian_utils.xiuxian_opertion import do_is_work
from ..xiuxian_utils.utils import check_user, check_user_type, get_msg_pic, handle_send, number_to, log_message
from nonebot.log import logger
from .reward_data_source import PLAYERSDATA, readf, savef, delete_work_file, has_unaccepted_work
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank, XiuConfig
from pathlib import Path

# 定时任务
resetrefreshnum = require("nonebot_plugin_apscheduler").scheduler
sql_message = XiuxianDateManage()  # sql类
items = Items()
count = 5  # 每日刷新次数
WORK_EXPIRE_MINUTES = 30  # 悬赏令过期时间(分钟)

# 用户提醒状态和任务字典
user_reminder_status: Dict[str, Dict] = {}  # 格式: {user_id: {"pending": bool, "reminded": bool, "refresh_time": datetime}}
user_reminder_tasks: Dict[str, asyncio.Task] = {}  # 跟踪每个用户的刷新提醒任务
user_settle_tasks: Dict[str, asyncio.Task] = {}  # 跟踪每个用户的结算提醒任务

do_work = on_regex(
    r"^悬赏令(查看|刷新|终止|结算|接取|重置|帮助|确认刷新)?\s*(\d+)?",
    priority=10,
    block=True
)

do_work_cz = on_command("悬赏力量", permission=SUPERUSER, priority=6, block=True)

def calculate_remaining_time(create_time: str, work_name: str = None, user_id: str = None) -> Tuple[int, int, int]:
    """
    计算悬赏令剩余时间
    :param create_time: 创建时间字符串
    :param work_name: 悬赏名称（可选，用于获取总耗时）
    :param user_id: 用户ID（可选，用于获取总耗时）
    :return: (remaining_minutes, elapsed_minutes, total_minutes) 
             剩余分钟数、已过分钟数和总分钟数（如果是进行中悬赏）
    """
    try:
        # 统一处理时间格式（兼容带和不带毫秒）
        try:
            work_time = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            work_time = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S.%f")
        
        # 计算时间差
        time_diff = datetime.now() - work_time
        elapsed_minutes = int(time_diff.total_seconds() // 60)
        
        total_minutes = None
        if work_name and user_id:
            # 如果是进行中悬赏，获取总耗时
            total_minutes = workhandle().do_work(key=1, name=work_name, user_id=user_id)
            remaining_minutes = max(total_minutes - elapsed_minutes, 0)
        else:
            # 计算悬赏令过期剩余时间
            remaining_minutes = max(WORK_EXPIRE_MINUTES - elapsed_minutes, 0)
        
        return remaining_minutes, elapsed_minutes, total_minutes
    except Exception as e:
        logger.error(f"计算悬赏令剩余时间失败: {e}, 时间: {create_time}")
        return 0, 0, None  # 如果解析失败，默认返回0

def get_user_work_status(user_id: str) -> Tuple[int, Any]:
    """
    获取用户悬赏令状态(包含自动更新过期状态)
    
    参数:
        user_id: 用户ID
    
    返回:
        (状态码, 悬赏数据)
        状态码说明:
        0 - 无悬赏
        1 - 进行中的悬赏
        2 - 可结算的悬赏
        3 - 未过期的悬赏令
        4 - 已过期的悬赏令
    """
    # 先检查是否有进行中的悬赏
    user_cd_message = sql_message.get_user_cd(user_id)
    if user_cd_message and user_cd_message['type'] == 2:
        try:
            remaining_minutes, _, _ = calculate_remaining_time(
                user_cd_message['create_time'],
                user_cd_message['scheduled_time'],
                user_id
            )
            
            if remaining_minutes > 0:
                return 1, user_cd_message  # 进行中的悬赏
            else:
                return 2, user_cd_message  # 可结算的悬赏
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"解析悬赏令时间失败: {e}, 数据: {user_cd_message}")
            # 如果时间解析失败，视为可结算状态
            return 2, user_cd_message

    # 使用新的 has_unaccepted_work 函数检查未接取悬赏令
    has_work, work_info = has_unaccepted_work(user_id)
    if has_work:
        return 3, work_info  # 未过期的悬赏令
    elif work_info:  # 有数据但已过期或已接取
        return 4, work_info  # 已过期的悬赏令

    return 0, None  # 无悬赏

def check_work_expired(create_time: str) -> bool:
    """检查悬赏令是否过期"""
    try:
        # 尝试解析带毫秒的格式
        try:
            work_time = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # 尝试解析不带毫秒的格式
            work_time = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")
            
        # 计算时间差（分钟）
        time_diff = datetime.now() - work_time
        return time_diff > timedelta(minutes=WORK_EXPIRE_MINUTES)
    except Exception as e:
        logger.error(f"解析悬赏令时间失败: {e}, 时间: {create_time}")
        return True  # 如果解析失败，默认视为过期

async def get_work_status_message(user_id: str, work_data: dict) -> str:
    """获取悬赏令状态消息"""
    status, work_data = get_user_work_status(user_id)
    
    if status == 1:  # 进行中的悬赏
        remaining_minutes, _, total_minutes = calculate_remaining_time(
            work_data['create_time'],
            work_data['scheduled_time'],
            user_id
        )
        
        return (
            f"进行中的悬赏令【{work_data['scheduled_time']}】\n"
            f"剩余时间：{remaining_minutes}分钟（总耗时：{total_minutes}分钟）\n"
            f"请继续努力完成悬赏！"
        )
    elif status == 2:  # 可结算的悬赏
        return (
            f"悬赏令【{work_data['scheduled_time']}】已完成！\n"
            f"请输入【悬赏令结算】领取奖励！"
        )
    elif status == 3:  # 未过期的悬赏令
        remaining_minutes, _, _ = calculate_remaining_time(work_data["refresh_time"])
        
        work_list = []
        work_msg_f = f"\n══  道友的悬赏令   ═══\n剩余时间：{remaining_minutes}分钟\n════════════\n"
        tasks = list(work_data["tasks"].items())
        for n, (task_name, task_data) in enumerate(tasks, 1):
            item_msg = "无"
            if task_data["item_id"] != 0:
                item_info = items.get_data_by_item_id(task_data["item_id"])
                item_msg = f"{item_info['level']}:{item_info['name']}"
            work_list.append([task_name, task_data["time"]])
            work_msg_f += (
                f"悬赏编号：{n}\n"
                f"悬赏名称：{task_name}\n"
                f"完成概率：{task_data['rate']}%\n"
                f"基础报酬：{number_to(task_data['award'])}修为\n"
                f"预计耗时：{task_data['time']}分钟\n"
                f"额外奖励：{item_msg}\n"
                "════════════\n"
            )
        work_msg_f += "请输入【悬赏令接取+编号】接取悬赏"
        return work_msg_f
    elif status == 4:  # 已过期的悬赏令
        return "悬赏令已过期，请重新刷新获取新悬赏！"
    else:  # 无悬赏
        return "没有查到您的悬赏令信息，请输入【悬赏令刷新】获取新悬赏！"

async def settle_work(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, user_id: str, work_data: dict):
    """结算悬赏令"""    
    user_info = sql_message.get_user_info_with_id(user_id)    
    msg, give_exp, s_o_f, item_id, big_suc = workhandle().do_work(
        2,
        work_list=work_data['scheduled_time'],
        level=user_info['level'],
        exp=user_info['exp'],
        user_id=user_id
    )
    
    # 结算后删除JSON文件
    delete_work_file(user_id)
    
    item_flag = False
    item_info = None
    item_msg = None
    if item_id != 0:
        item_flag = True
        item_info = items.get_data_by_item_id(item_id)
        item_msg = f"{item_info['level']}:{item_info['name']}"
    
    current_exp = user_info['exp']
    max_exp = int(OtherSet().set_closing_type(user_info['level'])) * XiuConfig().closing_exp_upper_limit
    
    if big_suc:  # 大成功
        exp_rate = random.uniform(1.5, 2.5)
        gain_exp = int(give_exp * exp_rate)
        success_msg = "悬赏大成功！"
    else:
        gain_exp = give_exp
        success_msg = "悬赏完成！"
    
    if current_exp + gain_exp >= max_exp:
        remaining_exp = max_exp - current_exp
        gain_exp = remaining_exp
    gain_exp = max(gain_exp, 0)
    
    if big_suc or s_o_f:  # 大成功 or 普通成功
        sql_message.update_exp(user_id, gain_exp)
        sql_message.do_work(user_id, 0)
        msg = (
            f"{success_msg}\n"
            f"悬赏名称：{work_data['scheduled_time']}\n"
            f"获得修为：{number_to(gain_exp)}"
        )
        if item_flag:
            sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], 1)
            msg += f"\n额外奖励：{item_msg}！"
    else:  # 失败
        gain_exp = give_exp // 2
        if current_exp + gain_exp >= max_exp:
            remaining_exp = max_exp - current_exp
            gain_exp = remaining_exp
        gain_exp = max(gain_exp, 0)
        sql_message.update_exp(user_id, gain_exp)
        sql_message.do_work(user_id, 0)
        msg = (
            f"悬赏勉强完成\n"
            f"悬赏名称：{work_data['scheduled_time']}\n"
            f"获得修为：{number_to(gain_exp)}"
        )
    log_message(user_id, msg)
    await handle_send(bot, event, msg)
    return msg

def generate_work_message(work_list: list, freenum: int) -> str:
    """生成悬赏令消息"""
    remaining_minutes, _, _ = calculate_remaining_time(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    work_msg_f = (
        f"\n══  道友的悬赏令   ═══\n"
        f"剩余刷新次数：{freenum}次\n"
        f"悬赏令剩余时间：{remaining_minutes}分钟\n"
        f"════════════\n"
    )
    
    for n, i in enumerate(work_list, 1):
        work_msg_f += f"悬赏编号：{n}\n{get_work_msg(i)}"
    
    work_msg_f += (
        f"请输入【悬赏令接取+编号】接取悬赏"
    )
    return work_msg_f

def get_work_msg(work_):
    if work_[4] == 0:
        item_msg = "无"
    else:
        item_info = items.get_data_by_item_id(work_[4])
        item_msg = f"{item_info['level']}:{item_info['name']}"
    return (
        f"悬赏名称：{work_[0]}\n"
        f"完成概率：{work_[1]}%\n"
        f"基础报酬：{number_to(work_[2])}修为\n"
        f"预计耗时：{work_[3]}分钟\n"
        f"额外奖励：{item_msg}\n"
        "════════════\n"
    )

# 重置悬赏令刷新次数
@resetrefreshnum.scheduled_job("cron", hour=0, minute=0)
async def resetrefreshnum_():
    sql_message.reset_work_num(count)
    logger.opt(colors=True).info(f"用户悬赏令刷新次数重置成功")

__work_help__ = f"""
═══  悬赏令帮助   ═════

【悬赏令操作】
悬赏令查看 - 浏览当前可接取的悬赏任务
悬赏令刷新 - 刷新悬赏列表（每日剩余次数：{count}次）
悬赏令接取+编号 - 接取指定悬赏任务
悬赏令结算 - 领取已完成悬赏的奖励
悬赏令终止 - 放弃当前进行中的悬赏
悬赏令重置 - 放弃已刷新/接取的悬赏

【悬赏奖励】
完成悬赏可获得丰厚奖励
境界越高额外奖励越珍贵
悬赏大成功可触发额外奖励

【规则说明】
悬赏令有效时间：{WORK_EXPIRE_MINUTES}分钟
每日0点重置刷新次数
高境界可获得更多悬赏奖励

【温馨提示】
1. 接取前请仔细查看悬赏要求
2. 终止悬赏可能导致灵石惩罚
3. 过期悬赏令将自动失效
""".strip()

@do_work.handle(parameterless=[Cooldown(stamina_cost=1, at_sender=False)])        
async def do_work_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Tuple[Any, ...] = RegexGroup()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)    
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await do_work.finish()
    
    user_level = user_info['level']
    user_id = user_info['user_id']
    user_rank = convert_rank(user_info['level'])[0]
    sql_message.update_last_check_info_time(user_id)  # 更新查看修仙信息时间
    
    if user_rank == 0:
        msg = "道友实力通天彻地，悬赏令已经不能满足道友的需求了！"
        await handle_send(bot, event, msg)
        await do_work.finish()
        
    mode = args[0]  # 刷新、终止、结算、接取等操作

    if mode == "查看":            
        status, work_data = get_user_work_status(user_id)
        msg = await get_work_status_message(user_id, work_data)
        await handle_send(bot, event, msg)
        await do_work.finish()

    elif mode == "刷新":
        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        status, work_data = get_user_work_status(user_id)
        
        if status == 1 or status == 2:  # 进行中或可结算的悬赏
            msg = await get_work_status_message(user_id, work_data)
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        usernums = sql_message.get_work_num(user_id)
        if usernums <= 0:
            msg = (
                f"道友今日的悬赏令刷新次数已用尽\n"
                f"每日0点重置刷新次数\n"
                f"请明日再来！"
            )
            await handle_send(bot, event, msg)
            await do_work.finish()
        
        # 检查是否已有未接取的悬赏令
        has_work, work_data = has_unaccepted_work(user_id)
        if has_work:
            # 取消任何现有的延迟提醒任务
            if user_id in user_reminder_tasks:
                user_reminder_tasks[user_id].cancel()
                try:
                    await user_reminder_tasks[user_id]
                except asyncio.CancelledError:
                    pass
                del user_reminder_tasks[user_id]
                
            # 设置提醒状态
            user_reminder_status[user_id] = {
                "pending": True,
                "reminded": False,
                "refresh_time": datetime.now()
            }
            
            # 启动新的延迟提醒任务
            async def delayed_reminder():
                await asyncio.sleep(180)  # 3分钟
                if user_id in user_reminder_status and user_reminder_status[user_id]["pending"]:
                    has_work, work_data = has_unaccepted_work(user_id)
                    if has_work:
                        remaining_minutes = (datetime.now() - user_reminder_status[user_id]["refresh_time"]).total_seconds() / 60
                        remaining_minutes = max(WORK_EXPIRE_MINUTES - remaining_minutes, 0)
                        reminder_msg = (
                            "您已有未接取的悬赏令\n"
                            f"剩余时间：{int(remaining_minutes)}分钟\n"
                            "请输入【悬赏令查看】查看当前悬赏"
                        )
                        await handle_send(bot, event, reminder_msg)
                    user_reminder_status[user_id]["pending"] = False
                    user_reminder_status[user_id]["reminded"] = True
            
            task = asyncio.create_task(delayed_reminder())
            user_reminder_tasks[user_id] = task
            
            msg = (
                f"您已有未接取的悬赏令\n"
                f"请输入【悬赏令查看】查看当前悬赏\n"
                f"如需强制刷新，请输入【悬赏令确认刷新】"
            )
            await handle_send(bot, event, msg)
            await do_work.finish()
        elif status == 4 or status == 0:  # 已过期的悬赏令/无悬赏令
            # 生成新悬赏令
            work_msg = workhandle().do_work(0, level=user_level, exp=user_info['exp'], user_id=user_id)
            msg = generate_work_message(work_msg, usernums - 1)
            sql_message.update_work_num(user_id, usernums - 1)
            
            # 取消任何现有的延迟提醒任务
            if user_id in user_reminder_tasks:
                user_reminder_tasks[user_id].cancel()
                try:
                    await user_reminder_tasks[user_id]
                except asyncio.CancelledError:
                    pass
                del user_reminder_tasks[user_id]
                
            # 设置新悬赏令的提醒状态
            user_reminder_status[user_id] = {
                "pending": True,
                "reminded": False,
                "refresh_time": datetime.now()
            }
            
            # 启动新的延迟提醒任务
            async def delayed_reminder():
                await asyncio.sleep(180)  # 3分钟
                if user_id in user_reminder_status and user_reminder_status[user_id]["pending"]:
                    has_work, work_data = has_unaccepted_work(user_id)
                    if has_work:
                        remaining_minutes = (datetime.now() - user_reminder_status[user_id]["refresh_time"]).total_seconds() / 60
                        remaining_minutes = max(WORK_EXPIRE_MINUTES - remaining_minutes, 0)
                        reminder_msg = (
                            "您已有未接取的悬赏令\n"
                            f"剩余时间：{int(remaining_minutes)}分钟\n"
                            "请输入【悬赏令查看】查看当前悬赏"
                        )
                        await handle_send(bot, event, reminder_msg)
                    user_reminder_status[user_id]["pending"] = False
                    user_reminder_status[user_id]["reminded"] = True
            
            task = asyncio.create_task(delayed_reminder())
            user_reminder_tasks[user_id] = task
            
            await handle_send(bot, event, msg)
            await do_work.finish()

    elif mode == "确认刷新":
        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        usernums = sql_message.get_work_num(user_id)
        if usernums <= 0:
            msg = "道友今日的悬赏令刷新次数已用尽！"
            await handle_send(bot, event, msg)
            await do_work.finish()
        
        # 取消任何现有的延迟提醒任务
        if user_id in user_reminder_tasks:
            user_reminder_tasks[user_id].cancel()
            try:
                await user_reminder_tasks[user_id]
            except asyncio.CancelledError:
                pass
            del user_reminder_tasks[user_id]
        
        # 确认刷新，删除旧悬赏令        
        delete_work_file(user_id)
        work_msg = workhandle().do_work(0, level=user_level, exp=user_info['exp'], user_id=user_id)
        msg = generate_work_message(work_msg, usernums - 1)
        sql_message.update_work_num(user_id, usernums - 1)
        
        # 设置新悬赏令的提醒状态
        user_reminder_status[user_id] = {
            "pending": True,
            "reminded": False,
            "refresh_time": datetime.now()
        }
        
        # 启动新的延迟提醒任务
        async def delayed_reminder():
            await asyncio.sleep(180)  # 3分钟
            if user_id in user_reminder_status and user_reminder_status[user_id]["pending"]:
                has_work, work_data = has_unaccepted_work(user_id)
                if has_work:
                    remaining_minutes = (datetime.now() - user_reminder_status[user_id]["refresh_time"]).total_seconds() / 60
                    remaining_minutes = max(WORK_EXPIRE_MINUTES - remaining_minutes, 0)
                    reminder_msg = (
                        "您已有未接取的悬赏令\n"
                        f"剩余时间：{int(remaining_minutes)}分钟\n"
                        "请输入【悬赏令查看】查看当前悬赏"
                    )
                    await handle_send(bot, event, reminder_msg)
                user_reminder_status[user_id]["pending"] = False
                user_reminder_status[user_id]["reminded"] = True
        
        task = asyncio.create_task(delayed_reminder())
        user_reminder_tasks[user_id] = task
        
        await handle_send(bot, event, msg)
        await do_work.finish()

    elif mode == "结算":
        is_type, msg = check_user_type(user_id, 2)
        if not is_type:
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        status, work_data = get_user_work_status(user_id)
        
        if status == 1:  # 进行中的悬赏
            msg = await get_work_status_message(user_id, work_data)
            await handle_send(bot, event, msg)
            await do_work.finish()
        elif status != 2:  # 没有可结算的悬赏
            msg = "没有查到您的可结算悬赏令信息！"
            await handle_send(bot, event, msg)
            await do_work.finish()
    
        await settle_work(bot, event, user_id, work_data)
        await do_work.finish()

    elif mode == "终止":            
        status, work_data = get_user_work_status(user_id)
    
        if status == 2:  # 可结算的悬赏，自动结算
            await settle_work(bot, event, user_id, work_data)
            await do_work.finish()
        elif status == 1:  # 进行中的悬赏，终止并惩罚
            stone = 4000000
            sql_message.update_ls(user_id, stone, 2)
            sql_message.do_work(user_id, 0)
            msg = (
                f"道友终止了悬赏令【{work_data['scheduled_time']}】\n"
                f"灵石减少：{number_to(stone)}\n"
                f"悬赏已终止！"
            )
        elif status == 3 or status == 4:  # 有未接取的悬赏
            msg = "未接取的悬赏令已终止！"
        else:
            msg = "没有查到您的悬赏令信息！"
        delete_work_file(user_id)
        await handle_send(bot, event, msg)
        await do_work.finish()

    elif mode == "接取":
        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        status, work_data = get_user_work_status(user_id)
        
        # 如果已有进行中或可结算的悬赏，显示当前悬赏状态
        if status == 1 or status == 2:
            msg = await get_work_status_message(user_id, work_data)
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        if status != 3:  # 未过期的悬赏令
            msg = "没有查到您的悬赏令信息，请输入【悬赏令刷新】获取新悬赏！"
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        num = args[1]
        if num is None or str(num) not in ['1', '2', '3']:
            msg = '请输入正确的悬赏编号（1、2或3）'
            await handle_send(bot, event, msg)
            await do_work.finish()
        
        work_num = int(num)
        tasks = list(work_data["tasks"].items())
        if work_num < 1 or work_num > len(tasks):
            msg = "没有这样的悬赏编号！"
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        task_name, task_data = tasks[work_num - 1]
        sql_message.do_work(user_id, 2, task_name)
        
        # 更新悬赏状态为已接取
        work_data["status"] = 2
        savef(user_id, work_data)
                
        msg = (
            f"成功接取悬赏令！\n"
            f"悬赏名称：{task_name}\n"
            f"请努力完成悬赏！"
        )
        await handle_send(bot, event, msg)
        await do_work.finish()

    elif mode == "重置":
        delete_work_file(user_id)
        user_cd_message = sql_message.get_user_cd(user_id)
        if user_cd_message['type'] == 2:
            sql_message.do_work(user_id, 0)
        msg = "已重置悬赏令"
        await handle_send(bot, event, msg)

    elif mode == "帮助":
        msg = f"\n{__work_help__}"
        await handle_send(bot, event, msg)
