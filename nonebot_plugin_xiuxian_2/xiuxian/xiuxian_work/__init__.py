import os
import random
import time
import asyncio
import json
from typing import Tuple, Any, Dict
from nonebot import require
from ..on_compat import on_regex, on_command
from nonebot.params import RegexGroup
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..adapter_compat import (
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
from urllib.parse import quote
from ..xiuxian_utils.xiuxian_opertion import do_is_work
from ..xiuxian_utils.utils import check_user, check_user_type, get_msg_pic, handle_send, number_to, log_message, update_statistics_value, send_help_message
from ..xiuxian_tasks.task_data import record_task_progress
from nonebot.log import logger
from .reward_data_source import PLAYERSDATA, readf, savef, delete_work_file, has_unaccepted_work
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank, XiuConfig
from pathlib import Path
from ...paths import get_paths
from .settlement_service import WorkSettlementService
from .claim_service import WorkClaimService
from .item_use_service import WorkItemUseService
from .refresh_settlement_service import WorkRefreshSettlementService
from .abort_cleanup_service import WorkAbortCleanupService
from .daily_refresh_reset_service import WorkDailyRefreshResetService

work_settlement_service = WorkSettlementService(get_paths().game_db)
work_claim_service = WorkClaimService(get_paths().game_db)
work_item_use_service = WorkItemUseService(get_paths().game_db)
work_refresh_service = WorkRefreshSettlementService(get_paths().game_db)
work_abort_cleanup_service = WorkAbortCleanupService(get_paths().game_db)
work_daily_refresh_reset_service = WorkDailyRefreshResetService(get_paths().game_db)
sql_message = XiuxianDateManage()  # sql类
items = Items()
count = 5  # 每日刷新次数
WORK_EXPIRE_MINUTES = 30  # 悬赏令过期时间(分钟)

def format_reward_item(item_id: int) -> str:
    """格式化额外奖励物品，支持点击查看效果。"""
    if item_id == 0:
        return "无"
    item_info = items.get_data_by_item_id(item_id)
    if not item_info:
        return "未知物品"
    item_name = item_info["name"]
    item_level = item_info.get("level", item_info.get("type", "未知品阶"))
    view_cmd = quote(f"查看效果 {item_name}", safe="")
    return f"{item_level}:[{item_name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"


def strip_inline_links(msg: str) -> str:
    """原生 Markdown 不可用时降级为纯文本物品名。"""
    import re
    return re.sub(r"\[([^\]]+)\]\(mqqapi://aio/inlinecmd\?[^)]+\)", r"\1", msg)


def append_native_buttons(msg: str, **kwargs) -> str:
    buttons = []
    for i in range(1, 5):
        show = kwargs.get(f"k{i}")
        command = kwargs.get(f"v{i}")
        if show and command:
            command = quote(str(command), safe="")
            buttons.append(f"[{show}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false)")
    if not buttons:
        return msg
    return f"{msg}\n\n---\n" + " | ".join(buttons)


async def send_work_message(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, msg: str, **kwargs):
    """发送悬赏消息，保留额外奖励物品点击查看效果。"""
    if "mqqapi://aio/inlinecmd" in msg:
        await handle_send(
            bot,
            event,
            append_native_buttons(msg, **kwargs),
            native_markdown=True,
            fallback_msg=strip_inline_links(msg),
        )
        return
    await handle_send(bot, event, msg, **kwargs)

# 用户提醒状态和任务字典
user_reminder_status: Dict[str, Dict] = {}  # 格式: {user_id: {"pending": bool, "reminded": bool, "refresh_time": datetime}}
user_reminder_tasks: Dict[str, asyncio.Task] = {}  # 跟踪每个用户的刷新提醒任务
user_settle_tasks: Dict[str, asyncio.Task] = {}  # 跟踪每个用户的结算提醒任务

do_work = on_regex(
    r"^悬赏令(查看|刷新|终止|结算|接取|重置|帮助|确认刷新)?\s*(\d+)?",
    priority=10,
    block=True
)

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
        work_msg_f = f"【道友的悬赏令】\n剩余时间：{remaining_minutes}分钟\n"
        tasks = list(work_data["tasks"].items())
        for n, (task_name, task_data) in enumerate(tasks, 1):
            item_msg = format_reward_item(task_data["item_id"])
            work_list.append([task_name, task_data["time"]])
            work_msg_f += (
                f"悬赏编号：{n}\n"
                f"悬赏名称：{task_name}\n"
                f"完成概率：{task_data['rate']}%\n"
                f"基础报酬：{number_to(task_data['award'])}修为\n"
                f"预计耗时：{task_data['time']}分钟\n"
                f"额外奖励：{item_msg}\n\n"
            )
        work_msg_f += "请输入【悬赏令接取+编号】接取悬赏"
        return work_msg_f
    elif status == 4:  # 已过期的悬赏令
        return "悬赏令已过期，请重新刷新获取新悬赏！"
    else:
        return "没有查到您的悬赏令信息，请输入【悬赏令刷新】获取新悬赏！"
async def settle_work(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, user_id: str, work_data: dict):
    """结算悬赏令。随机结果先固定，再由事务服务一次提交。"""
    event_message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"work-settlement:{user_id}:{event_message_id or time.time_ns()}"
    # 先回放：成功后 type=0 / 奖励已发，前置状态检查会挡住同事件重放；随机结果不可重掷。
    prior = work_settlement_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        success_msg = {
            "big": "悬赏大成功！",
            "ok": "悬赏完成！",
            "half": "悬赏勉强完成",
        }.get(prior.success_kind, "悬赏完成！")
        msg = (
            f"{success_msg}\n"
            f"悬赏名称：{prior.scheduled_time or work_data.get('scheduled_time', '')}\n"
            f"获得修为：{number_to(prior.exp)}"
        )
        if prior.item_awarded and prior.item_msg:
            msg += f"\n额外奖励：{prior.item_msg}！"
        msg += "\n该结算请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg, md_type="悬赏令", k1="刷新", v1="悬赏令刷新", k2="数据", v2="统计数据", k3="帮助", v3="悬赏令帮助")
        return msg

    user_info = sql_message.get_user_info_with_id(user_id)
    _, give_exp, s_o_f, item_id, big_suc = workhandle().do_work(
        2,
        work_list=work_data["scheduled_time"],
        level=user_info["level"],
        exp=user_info["exp"],
        user_id=user_id,
    )
    max_exp = int(OtherSet().set_closing_type(user_info["level"])) * XiuConfig().closing_exp_upper_limit
    item_info = items.get_data_by_item_id(item_id) if item_id else None
    item_flag = bool(item_info) and (big_suc or s_o_f)
    item_msg = f"{item_info['level']}:{item_info['name']}" if item_flag else None

    if big_suc:
        gain_exp = int(give_exp * random.uniform(1.5, 2.5))
        success_msg = "悬赏大成功！"
        success_kind = "big"
    elif s_o_f:
        gain_exp = give_exp
        success_msg = "悬赏完成！"
        success_kind = "ok"
    else:
        gain_exp = give_exp // 2
        success_msg = "悬赏勉强完成"
        success_kind = "half"

    result = work_settlement_service.settle(
        operation_id,
        user_id,
        {"create_time": work_data["create_time"], "scheduled_time": work_data["scheduled_time"]},
        gain_exp,
        {"id": item_id, "name": item_info["name"], "type": item_info["type"]} if item_flag else None,
        max_exp,
        XiuConfig().max_goods_num,
        success_kind=success_kind,
        item_msg=item_msg or "",
    )
    if result.status == "duplicate":
        success_msg = {
            "big": "悬赏大成功！",
            "ok": "悬赏完成！",
            "half": "悬赏勉强完成",
        }.get(result.success_kind, success_msg)
        msg = (
            f"{success_msg}\n"
            f"悬赏名称：{result.scheduled_time or work_data['scheduled_time']}\n"
            f"获得修为：{number_to(result.exp)}"
        )
        if result.item_awarded:
            msg += f"\n额外奖励：{result.item_msg or item_msg}！"
        msg += "\n该结算请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg, md_type="悬赏令", k1="刷新", v1="悬赏令刷新", k2="数据", v2="统计数据", k3="帮助", v3="悬赏令帮助")
        return msg
    if result.status == "inventory_full":
        msg = "背包物品已达上限，悬赏奖励尚未结算。"
        await handle_send(bot, event, msg)
        return msg
    if result.status in {"state_changed", "user_missing"}:
        msg = "悬赏令状态已变化，请重新查看后再试。"
        await handle_send(bot, event, msg)
        return msg

    delete_work_file(user_id)
    msg = (
        f"{success_msg}\n"
        f"悬赏名称：{work_data['scheduled_time']}\n"
        f"获得修为：{number_to(result.exp)}"
    )
    if result.item_awarded:
        msg += f"\n额外奖励：{item_msg}！"
    if result.status == "applied":
        log_message(user_id, msg)
        update_statistics_value(user_id, "悬赏令结算次数")
        record_task_progress(
            user_id, "work", operation_id=f"task-progress:{operation_id}"
        )
    await handle_send(bot, event, msg, md_type="悬赏令", k1="刷新", v1="悬赏令刷新", k2="数据", v2="统计数据", k3="帮助", v3="悬赏令帮助")
    return msg

def generate_work_message(work_list: list, freenum: int) -> str:
    """生成悬赏令消息"""
    remaining_minutes, _, _ = calculate_remaining_time(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    work_msg_f = (
        f"【道友的悬赏令】\n"
        f"剩余刷新次数：{freenum}次\n"
        f"悬赏令剩余时间：{remaining_minutes}分钟\n"
    )
    
    for n, i in enumerate(work_list, 1):
        work_msg_f += f"悬赏编号：{n}\n{get_work_msg(i)}"
    
    work_msg_f += (
        f"请输入【悬赏令接取+编号】接取悬赏"
    )
    return work_msg_f


def _work_operation_id(event, action: str, user_id: str) -> str:
    message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    return f"work-{action}:{user_id}:{message_id or time.time_ns()}"


def _work_cd_snapshot(user_id: str) -> dict:
    cd = sql_message.get_user_cd(user_id) or {}
    return {
        "type": int(cd.get("type", 0)),
        "create_time": cd.get("create_time"),
        "scheduled_time": cd.get("scheduled_time"),
    }


def _prepare_work_offer(operation_id: str, user_id: str, user_level: str, exp: int):
    """Generate a stable offer for an operation without leaking RNG state."""
    random_state = random.getstate()
    try:
        random.seed(operation_id)
        return workhandle().do_work(
            0, level=user_level, exp=exp, user_id=user_id, persist=False
        )
    finally:
        random.setstate(random_state)

def get_work_msg(work_):
    item_msg = format_reward_item(work_[4])
    return (
        f"悬赏名称：{work_[0]}\n"
        f"完成概率：{work_[1]}%\n"
        f"基础报酬：{number_to(work_[2])}修为\n"
        f"预计耗时：{work_[3]}分钟\n"
        f"额外奖励：{item_msg}\n\n"
    )

# 重置悬赏令刷新次数
async def resetrefreshnum():
    business_date = datetime.now().date().isoformat()
    while True:
        result = work_daily_refresh_reset_service.reset(business_date, count)
        if result.status == "operation_conflict":
            raise RuntimeError(f"悬赏令刷新次数重置配置冲突：{business_date}")
        if result.task_status != "running":
            logger.opt(colors=True).info(
                f"用户悬赏令刷新次数重置完成：{result.completed}/{result.total}，"
                f"实际变更{result.changed}人"
            )
            return result
        await asyncio.sleep(0)

async def delayed_reminder(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, user_id: str):
    try:
        await asyncio.sleep(180)
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
                await handle_send(bot, event, reminder_msg, md_type="悬赏令", k1="接取", v1="悬赏令接取", k2="刷新", v2="悬赏令确认刷新", k3="查看", v3="悬赏令查看")
            user_reminder_status[user_id]["pending"] = False
            user_reminder_status[user_id]["reminded"] = True
    except Exception as e:
        logger.error(f"延迟提醒任务发生异常: {e}", exc_info=True)

__work_help__ = f"""
═══  悬赏令帮助   ════

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
每日8点重置刷新次数
高境界可获得更多悬赏奖励

【温馨提示】
1. 接取前请仔细查看悬赏要求
2. 终止悬赏可能导致灵石惩罚
3. 过期悬赏令将自动失效
""".strip()

@do_work.handle(parameterless=[Cooldown(stamina_cost=1)])        
async def do_work_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Tuple[Any, ...] = RegexGroup()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)    
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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

    def _offer_task_list(offer: dict | None) -> list:
        task_list = []
        for name, data in ((offer or {}).get("tasks") or {}).items():
            task_list.append([
                name,
                data.get("rate", data.get("成功率", 0)),
                data.get("award", data.get("修为", 0)),
                data.get("time", 0),
                data.get("item_id", 0),
            ])
        return task_list

    def _refresh_replay_msg(offer: dict | None, remaining_count: int, fallback_list=None) -> str:
        task_list = _offer_task_list(offer) or list(fallback_list or [])
        if task_list:
            return generate_work_message(task_list, remaining_count) + "\n该刷新请求已经处理，无需重复提交。"
        return f"【道友的悬赏令】\n剩余刷新次数：{remaining_count}次\n该刷新请求已经处理，无需重复提交。"

    if mode == "查看":            
        status, work_data = get_user_work_status(user_id)
        msg = await get_work_status_message(user_id, work_data)
        await send_work_message(bot, event, msg, md_type="悬赏令", k1="接取", v1="悬赏令接取", k2="刷新", v2="悬赏令确认刷新", k3="终止", v3="悬赏令终止")
        await do_work.finish()

    elif mode == "刷新":
        # 先回放：成功后已有未接取悬赏/次数变化，前置拦截会挡住同事件重放。
        operation_id = _work_operation_id(event, "refresh", user_id)
        prior = work_refresh_service.get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = _refresh_replay_msg(prior.offer, prior.remaining_count)
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
            await do_work.finish()

        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="悬赏令帮助", v3="悬赏令帮助")
            await do_work.finish()
            
        status, work_data = get_user_work_status(user_id)
        
        if status == 1 or status == 2:  # 进行中或可结算的悬赏
            msg = await get_work_status_message(user_id, work_data)
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="查看", v1="悬赏令查看 ", k2="结算", v2="悬赏令确认结算", k3="终止", v3="悬赏令终止")
            await do_work.finish()
            
        usernums = sql_message.get_work_num(user_id)
        if usernums <= 0:
            msg = (
                f"道友今日的悬赏令刷新次数已用尽\n"
                f"每日8点重置刷新次数\n"
                f"请明日再来！"
            )
            await handle_send(bot, event, msg)
            await do_work.finish()
        
        # 检查是否已有未接取的悬赏令
        has_work, work_data = has_unaccepted_work(user_id)
        if has_work:
            # 取消任何现有的延迟提醒任务
            if user_id in user_reminder_tasks:
                user_reminder_tasks[user_id].cancel()  # 取消任务
                del user_reminder_tasks[user_id]
                
            # 设置提醒状态
            user_reminder_status[user_id] = {
                "pending": True,
                "reminded": False,
                "refresh_time": datetime.now()
            }
            
            task = asyncio.create_task(delayed_reminder(bot, event, user_id))
            user_reminder_tasks[user_id] = task
            
            msg = (
                f"您已有未接取的悬赏令\n"
                f"请输入【悬赏令查看】查看当前悬赏\n"
                f"如需强制刷新，请输入【悬赏令确认刷新】"
            )
            await handle_send(bot, event, msg, md_type="悬赏令", k1="接取", v1="悬赏令接取", k2="刷新", v2="悬赏令确认刷新", k3="查看", v3="悬赏令查看")
            await do_work.finish()
        elif status == 4 or status == 0:  # 已过期的悬赏令/无悬赏令
            work_msg, new_offer = _prepare_work_offer(
                operation_id, user_id, user_level, user_info['exp']
            )
            result = work_refresh_service.refresh(
                operation_id,
                user_id,
                usernums,
                _work_cd_snapshot(user_id),
                work_data,
                new_offer,
            )
            if result.status == "duplicate":
                msg = _refresh_replay_msg(result.offer, result.remaining_count, work_msg)
                await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
                await do_work.finish()
            if result.status in {"state_changed", "user_missing", "offer_exists"}:
                await handle_send(bot, event, "悬赏状态或刷新次数已变化，请重新查看后再试。")
                await do_work.finish()
            if result.status == "operation_conflict":
                await handle_send(bot, event, "该次刷新请求参数与首次处理不一致，请重新发起。")
                await do_work.finish()
            savef(user_id, result.offer, sync_snapshot=False)
            msg = generate_work_message(work_msg, result.remaining_count)
            
            # 取消任何现有的延迟提醒任务
            if user_id in user_reminder_tasks:
                user_reminder_tasks[user_id].cancel()  # 取消任务
                del user_reminder_tasks[user_id]
                
            # 设置新悬赏令的提醒状态
            user_reminder_status[user_id] = {
                "pending": True,
                "reminded": False,
                "refresh_time": datetime.now()
            }
            
            task = asyncio.create_task(delayed_reminder(bot, event, user_id))
            user_reminder_tasks[user_id] = task
            
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
            await do_work.finish()

    elif mode == "确认刷新":
        operation_id = _work_operation_id(event, "force-refresh", user_id)
        prior = work_refresh_service.get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = _refresh_replay_msg(prior.offer, prior.remaining_count)
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
            await do_work.finish()

        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="悬赏令帮助", v3="悬赏令帮助")
            await do_work.finish()
            
        usernums = sql_message.get_work_num(user_id)
        if usernums <= 0:
            msg = "道友今日的悬赏令刷新次数已用尽！"
            await handle_send(bot, event, msg)
            await do_work.finish()
        
        # 取消任何现有的延迟提醒任务
        if user_id in user_reminder_tasks:
            user_reminder_tasks[user_id].cancel()  # 取消任务
            del user_reminder_tasks[user_id]
        
        expected_offer = readf(user_id)
        work_msg, new_offer = _prepare_work_offer(
            operation_id, user_id, user_level, user_info['exp']
        )
        result = work_refresh_service.refresh(
            operation_id,
            user_id,
            usernums,
            _work_cd_snapshot(user_id),
            expected_offer,
            new_offer,
            force=True,
        )
        if result.status == "duplicate":
            msg = _refresh_replay_msg(result.offer, result.remaining_count, work_msg)
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
            await do_work.finish()
        if result.status in {"state_changed", "user_missing"}:
            await handle_send(bot, event, "悬赏状态或刷新次数已变化，请重新查看后再试。")
            await do_work.finish()
        if result.status == "operation_conflict":
            await handle_send(bot, event, "该次刷新请求参数与首次处理不一致，请重新发起。")
            await do_work.finish()
        savef(user_id, result.offer, sync_snapshot=False)
        msg = generate_work_message(work_msg, result.remaining_count)
        
        # 设置新悬赏令的提醒状态
        user_reminder_status[user_id] = {
            "pending": True,
            "reminded": False,
            "refresh_time": datetime.now()
        }
        
        task = asyncio.create_task(delayed_reminder(bot, event, user_id))
        user_reminder_tasks[user_id] = task
        
        await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
        await do_work.finish()

    elif mode == "结算":
        # 先回放：成功后 type=0，check_user_type(2) 会误拒同事件。
        event_message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        settle_operation_id = f"work-settlement:{user_id}:{event_message_id or time.time_ns()}"
        prior_settle = work_settlement_service.get_result(settle_operation_id)
        if prior_settle is not None and prior_settle.succeeded:
            await settle_work(bot, event, user_id, {"scheduled_time": prior_settle.scheduled_time})
            await do_work.finish()

        is_type, msg = check_user_type(user_id, 2)
        if not is_type:
            await handle_send(bot, event, msg, md_type="2", k2="修仙帮助", v2="修仙帮助", k3="悬赏令帮助", v3="悬赏令帮助")
            await do_work.finish()
            
        status, work_data = get_user_work_status(user_id)
        
        if status == 1:  # 进行中的悬赏
            msg = await get_work_status_message(user_id, work_data)
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="结算", v1="悬赏令结算", k2="终止", v2="悬赏令终止", k3="帮助", v3="悬赏令帮助")
            await do_work.finish()
        elif status != 2:  # 没有可结算的悬赏
            msg = "没有查到您的可结算悬赏令信息！"
            await handle_send(bot, event, msg, md_type="悬赏令", k1="查看", v1="悬赏令查看", k2="刷新", v2="悬赏令确认刷新", k3="帮助", v3="悬赏令帮助")
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
            result = work_abort_cleanup_service.cleanup(
                _work_operation_id(event, "abort", user_id),
                user_id,
                "active_abort",
                _work_cd_snapshot(user_id),
                readf(user_id),
                int(user_info["stone"]),
                stone,
            )
            if not result.succeeded:
                await handle_send(bot, event, "悬赏状态或灵石余额已变化，请刷新后重试。")
                await do_work.finish()
            msg = (
                f"道友终止了悬赏令【{work_data['scheduled_time']}】\n"
                f"灵石减少：{number_to(result.penalty)}\n"
                f"悬赏已终止！"
            )
        elif status == 3 or status == 4:  # 有未接取的悬赏
            reason = "offer_abort" if status == 3 else "expired"
            result = work_abort_cleanup_service.cleanup(
                _work_operation_id(event, reason, user_id),
                user_id,
                reason,
                _work_cd_snapshot(user_id),
                work_data,
            )
            if not result.succeeded:
                await handle_send(bot, event, "悬赏状态已变化，请重新查看后再试。")
                await do_work.finish()
            msg = "未接取的悬赏令已终止！"
        else:
            msg = "没有查到您的悬赏令信息！"
            await handle_send(bot, event, msg)
            await do_work.finish()
        delete_work_file(user_id, delete_snapshot=False)
        await handle_send(bot, event, msg, md_type="悬赏令", k1="查看", v1="悬赏令查看", k2="刷新", v2="悬赏令确认刷新", k3="帮助", v3="悬赏令帮助")
        await do_work.finish()

    elif mode == "接取":
        # 先解析编号并回放：成功后 type=2，check_user_type(0) 会误拒同事件。
        num = args[1]
        if num is None or str(num) not in ['1', '2', '3']:
            msg = '请输入正确的悬赏编号（1、2或3）'
            await handle_send(bot, event, msg, md_type="悬赏令", k1="接取", v1="悬赏令接取", k2="刷新", v2="悬赏令确认刷新", k3="查看", v3="悬赏令查看")
            await do_work.finish()
        work_num = int(num)
        event_message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"work-claim:{user_id}:{event_message_id or time.time_ns()}"
        prior = work_claim_service.get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = (
                f"成功接取悬赏令！\n"
                f"悬赏名称：{prior.task_name}\n"
                f"请努力完成悬赏！\n"
                "该接取请求已经处理，无需重复提交。"
            )
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="结算", v1="悬赏令结算", k2="终止", v2="悬赏令终止", k3="帮助", v3="悬赏令帮助")
            await do_work.finish()

        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="悬赏令帮助", v3="悬赏令帮助")
            await do_work.finish()
            
        status, work_data = get_user_work_status(user_id)
        
        # 如果已有进行中或可结算的悬赏，显示当前悬赏状态
        if status == 1 or status == 2:
            msg = await get_work_status_message(user_id, work_data)
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="结算", v1="悬赏令结算", k2="终止", v2="悬赏令终止", k3="帮助", v3="悬赏令帮助")
            await do_work.finish()
            
        if status != 3:  # 未过期的悬赏令
            msg = "没有查到您的悬赏令信息，请输入【悬赏令刷新】获取新悬赏！"
            await handle_send(bot, event, msg, md_type="悬赏令", k1="查看", v1="悬赏令查看", k2="刷新", v2="悬赏令确认刷新", k3="帮助", v3="悬赏令帮助")
            await do_work.finish()
        
        tasks = list(work_data["tasks"].items())
        if work_num < 1 or work_num > len(tasks):
            msg = "没有这样的悬赏编号！"
            await handle_send(bot, event, msg)
            await do_work.finish()
            
        task_name, task_data = tasks[work_num - 1]
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = work_claim_service.claim(
            operation_id,
            user_id,
            sql_message.get_work_num(user_id),
            work_data,
            work_num,
            started_at,
        )
        if result.status == "duplicate":
            msg = (
                f"成功接取悬赏令！\n"
                f"悬赏名称：{result.task_name or task_name}\n"
                f"请努力完成悬赏！\n"
                "该接取请求已经处理，无需重复提交。"
            )
            await send_work_message(bot, event, msg, md_type="悬赏令", k1="结算", v1="悬赏令结算", k2="终止", v2="悬赏令终止", k3="帮助", v3="悬赏令帮助")
            await do_work.finish()
        if result.status in {"state_changed", "user_missing"}:
            await handle_send(bot, event, "悬赏状态或可用次数已变化，请重新查看后再试。")
            await do_work.finish()
        if result.status == "operation_conflict":
            await handle_send(bot, event, "该次接取请求参数与首次处理不一致，请重新发起。")
            await do_work.finish()

        # JSON 文件仅保留为旧读取路径的投影，权威状态已由事务服务落库。
        work_data["status"] = 2
        savef(user_id, work_data)
                
        msg = (
            f"成功接取悬赏令！\n"
            f"悬赏名称：{result.task_name or task_name}\n"
            f"请努力完成悬赏！"
        )
        await send_work_message(bot, event, msg, md_type="悬赏令", k1="结算", v1="悬赏令结算", k2="终止", v2="悬赏令终止", k3="帮助", v3="悬赏令帮助")
        await do_work.finish()

    elif mode == "重置":
        result = work_abort_cleanup_service.cleanup(
            _work_operation_id(event, "reset", user_id),
            user_id,
            "reset",
            _work_cd_snapshot(user_id),
            readf(user_id),
        )
        if not result.succeeded:
            await handle_send(bot, event, "悬赏状态已变化，请重新查看后再试。")
            await do_work.finish()
        delete_work_file(user_id, delete_snapshot=False)
        msg = "已重置悬赏令"
        await handle_send(bot, event, msg, md_type="悬赏令", k1="查看", v1="悬赏令查看", k2="刷新", v2="悬赏令确认刷新", k3="帮助", v3="悬赏令帮助")

    elif mode == "帮助":
        msg = f"\n{__work_help__}"
        await send_help_message(bot, event, msg, k1="查看", v1="悬赏令查看", k2="刷新", v2="悬赏令确认刷新", k3="帮助", v3="悬赏令帮助")

async def use_work_order(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用悬赏令立即结算当前悬赏"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    status, work_data = get_user_work_status(user_id)
    if status in (1, 2):
        event_message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"work-item-accelerate:{user_id}:{event_message_id or time.time_ns()}"
        item_count = sql_message.goods_num(user_id, item_id)
        result = work_item_use_service.accelerate(
            operation_id,
            user_id,
            item_id,
            item_count,
            {
                "type": work_data["type"],
                "create_time": work_data["create_time"],
                "scheduled_time": work_data["scheduled_time"],
            },
            "1970-01-01 00:00:00",
        )
        if result.status == "item_missing":
            await handle_send(bot, event, "背包中的悬赏令数量不足。")
            return
        if result.status in {"state_changed", "user_missing", "operation_conflict"}:
            await handle_send(bot, event, "悬赏或道具状态已变化，请重新查看后再试。")
            return
        await handle_send(bot, event, "悬赏令燃起灵光，当前悬赏立即进入结算。")
        _, current_work = get_user_work_status(user_id)
        await settle_work(bot, event, user_id, current_work)
        return

    if status == 3:
        msg = "当前悬赏尚未接取，请先发送【悬赏令接取+编号】，再使用悬赏令立即结算。"
    elif status == 4:
        msg = "当前悬赏令已过期，无法立即结算。请重新刷新并接取悬赏。"
    else:
        msg = "当前没有已接取的悬赏，无法使用悬赏令立即结算。"

    await handle_send(bot, event, msg, md_type="悬赏令", k1="查看", v1="悬赏令查看", k2="接取", v2="悬赏令接取", k3="帮助", v3="悬赏令帮助")
    return

async def use_work_capture_order(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用追捕令刷新悬赏"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 检查当前状态
    is_type, msg = check_user_type(user_id, 0)
    if not is_type:
        await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="悬赏令帮助", v3="悬赏令帮助")
        return
    
    # 随机结果先固定，事务成功后再同步旧 JSON 读取投影。
    work_msg, work_data = workhandle().do_work(
        0, level=user_info['level'], exp=user_info['exp'], user_id=user_id, persist=False
    )
    if not work_data:
        msg = "悬赏令数据异常，请重新尝试！"
        await handle_send(bot, event, msg, md_type="悬赏令", k1="接取", v1="悬赏令接取", k2="刷新", v2="悬赏令确认刷新", k3="帮助", v3="悬赏令帮助")
        return
    
    # 修改奖励倍率(2-5倍)并更新到数据中
    reward_multiplier = random.randint(2, 5)
    for task_name, task_data in work_data["tasks"].items():
        task_data["award"] = int(task_data["award"] * reward_multiplier)
    
    event_message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"work-item-capture:{user_id}:{event_message_id or time.time_ns()}"
    item_count = sql_message.goods_num(user_id, item_id)
    user_cd = sql_message.get_user_cd(user_id)
    result = work_item_use_service.capture(
        operation_id,
        user_id,
        item_id,
        item_count,
        int(user_cd["type"] or 0),
        work_data,
    )
    if result.status == "item_missing":
        await handle_send(bot, event, "背包中的追捕令数量不足。")
        return
    if result.status in {"state_changed", "user_missing", "operation_conflict"}:
        await handle_send(bot, event, "悬赏或道具状态已变化，请重新查看后再试。")
        return
    work_data = dict(result.result_snapshot["offer"])
    savef(user_id, work_data)
    
    # 更新work_msg显示数据
    updated_work_msg = []
    for task_name, task_data in work_data["tasks"].items():
        updated_work_msg.append([
            task_name,
            task_data["rate"],
            task_data["award"],
            task_data["time"],
            task_data["item_id"],
            task_data["success_msg"],
            task_data["fail_msg"]
        ])
    
    # 生成显示消息
    msg = generate_work_message(updated_work_msg, sql_message.get_work_num(user_id))
    msg2 = f"※使用追捕令效果：所有悬赏修为奖励提升{reward_multiplier}倍！"
    
    await handle_send(bot, event, msg2)
    await send_work_message(bot, event, msg, md_type="悬赏令", k1="悬赏壹", v1="悬赏令接取 1", k2="悬赏贰", v2="悬赏令接取 2", k3="悬赏叁", v3="悬赏令接取 3", k4="刷新", v4="悬赏令确认刷新")
    return
