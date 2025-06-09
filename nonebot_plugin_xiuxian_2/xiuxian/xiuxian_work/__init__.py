import os
import random
from typing import Any, Tuple, Dict
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
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, OtherSet
from .work_handle import workhandle
from datetime import datetime
from ..xiuxian_utils.xiuxian_opertion import do_is_work
from ..xiuxian_utils.utils import check_user, check_user_type, get_msg_pic, handle_send
from nonebot.log import logger
from .reward_data_source import PLAYERSDATA
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank, XiuConfig

# 定时任务
resetrefreshnum = require("nonebot_plugin_apscheduler").scheduler
work = {}  # 悬赏令信息记录
refreshnum: Dict[str, int] = {}  # 用户悬赏令刷新次数记录
sql_message = XiuxianDateManage()  # sql类
items = Items()
count = 5  # 刷新次数


# 重置悬赏令刷新次数
@resetrefreshnum.scheduled_job("cron", hour=8, minute=0)
async def resetrefreshnum_():
    sql_message.reset_work_num()
    logger.opt(colors=True).info(f"<green>用户悬赏令刷新次数重置成功</green>")


do_work = on_regex(
    r"^悬赏令(查看|刷新|终止|结算|接取|帮助)?(\d+)?",
    priority=10,
    block=True
)
__work_help__ = f"""
悬赏令帮助信息:
指令：
1、悬赏令查看:获取当前悬赏令
2、悬赏令刷新:刷新当前悬赏令,每日{count}次
3、悬赏令终止:终止当前悬赏令任务
4、悬赏令结算:结算悬赏奖励
5、悬赏令接取+编号：接取对应的悬赏令
""".strip()


@do_work.handle(parameterless=[Cooldown(stamina_cost = 1, at_sender=False)])
async def do_work_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Tuple[Any, ...] = RegexGroup()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)    
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await do_work.finish()
    user_level = user_info['level']
    user_level_sx = user_info['level']
    user_id = user_info['user_id']
    user_rank = convert_rank(user_info['level'])[0]
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    user_cd_message = sql_message.get_user_cd(user_id)    
    if not os.path.exists(PLAYERSDATA / str(user_id) / "workinfo.json") and user_cd_message['type'] == 2:
        sql_message.do_work(user_id, 0)
        msg = "悬赏令已更新，已重置道友的状态！"
        await handle_send(bot, event, msg)
        await do_work.finish()
    mode = args[0]  # 刷新、终止、结算、接取    

    if mode == "查看":  # 刷新逻辑
        if (user_cd_message['scheduled_time'] is None) or (user_cd_message['type'] == 0):
            try:
                msg = work[user_id].msg
            except KeyError:
                msg = "没有查到你的悬赏令信息呢，请刷新！"
        elif user_cd_message['type'] == 2:
            work_time = datetime.strptime(
                user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
            )
            exp_time = (datetime.now() - work_time).seconds // 60  # 时长计算
            time2 = workhandle().do_work(key=1, name=user_cd_message['scheduled_time'], user_id=user_info['user_id'])
            if exp_time < time2:
                msg = f"进行中的悬赏令【{user_cd_message['scheduled_time']}】，预计{time2 - exp_time}分钟后可结束"
            else:
                msg = f"进行中的悬赏令【{user_cd_message['scheduled_time']}】，已结束，请输入【悬赏令结算】结算任务信息！"
        else:
            msg = "状态未知错误！"
        await handle_send(bot, event, msg)
        await do_work.finish()

    if mode == "刷新":  # 刷新逻辑
        if user_cd_message['type'] == 2:
            work_time = datetime.strptime(
                user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
            )
            exp_time = (datetime.now() - work_time).seconds // 60
            time2 = workhandle().do_work(key=1, name=user_cd_message['scheduled_time'], user_id=user_info['user_id'])
            if exp_time < time2:
                msg = f"进行中的悬赏令【{user_cd_message['scheduled_time']}】，预计{time2 - exp_time}分钟后可结束"
            else:
                msg = f"进行中的悬赏令【{user_cd_message['scheduled_time']}】，已结束，请输入【悬赏令结算】结算任务信息！"
            await handle_send(bot, event, msg)
            await do_work.finish()
        usernums = sql_message.get_work_num(user_id)

        isUser, user_info, msg = check_user(event)
        if not isUser:
            await handle_send(bot, event, msg)
            await do_work.finish()
        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            await handle_send(bot, event, msg)
            await do_work.finish()
        freenum = count - usernums - 1
        if freenum < 0:
            freenum = 0
            msg = "道友今日的悬赏令刷新次数已用尽"
            await handle_send(bot, event, msg)
            await do_work.finish()

        work_msg = workhandle().do_work(0, level=user_level, exp=user_info['exp'], user_id=user_id)
        n = 1
        work_list = []
        work_msg_f = f"☆------道友的个人悬赏令------☆\n"
        for i in work_msg:
            work_list.append([i[0], i[3]])
            work_msg_f += f"{n}、{get_work_msg(i)}"
            n += 1
        work_msg_f += f"(悬赏令每日刷新次数：{count}，今日可刷新次数：{freenum}次)"
        work[user_id] = do_is_work(user_id)
        work[user_id].msg = work_msg_f
        work[user_id].world = work_list
        sql_message.update_work_num(user_id, usernums + 1)
        msg = work[user_id].msg
        await handle_send(bot, event, msg)
        await do_work.finish()

    elif mode == "终止":
        is_type, msg = check_user_type(user_id, 2)  # 需要在悬赏令中的用户
        if is_type:
            stone = 4000000
            sql_message.update_ls(user_id, stone, 2)
            sql_message.do_work(user_id, 0)
            msg = f"道友不讲诚信，被打了一顿灵石减少{stone},悬赏令已终止！"
            await handle_send(bot, event, msg)
            await do_work.finish()
        else:
            msg = "没有查到你的悬赏令信息呢，请刷新！"
            await handle_send(bot, event, msg)
            await do_work.finish()

    elif mode == "结算":
        is_type, msg = check_user_type(user_id, 2)  # 需要在悬赏令中的用户
        if is_type:
            user_cd_message = sql_message.get_user_cd(user_id)
            work_time = datetime.strptime(
                user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
            )
            exp_time = (datetime.now() - work_time).seconds // 60  # 时长计算
            time2 = workhandle().do_work(
                key=1, name=user_cd_message['scheduled_time'], level=user_level, exp=user_info['exp'],
                user_id=user_info['user_id']
            )
            if exp_time <= time2 and (time2 - exp_time) != 0:
                msg = f"进行中的悬赏令【{user_cd_message['scheduled_time']}】，预计{time2 - exp_time}分钟后可结束"
                await handle_send(bot, event, msg)
                await do_work.finish()
            else:
                msg, give_exp, s_o_f, item_id, big_suc = workhandle().do_work(2,
                                                                              work_list=user_cd_message['scheduled_time'],
                                                                              level=user_level,
                                                                              exp=user_info['exp'],
                                                                              user_id=user_info['user_id'])
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
                    exp_rate = random.uniform(1.1, 1.5)
                    gain_exp = int(give_exp * exp_rate)
                else:
                    gain_exp = give_exp
                if current_exp + gain_exp >= max_exp:
                    remaining_exp = max_exp - current_exp
                    gain_exp = remaining_exp
                gain_exp = max(gain_exp, 0)
                if big_suc or s_o_f:  # 大成功 or 普通成功
                    sql_message.update_exp(user_id, gain_exp)
                    sql_message.do_work(user_id, 0)
                    msg = f"悬赏令结算，{msg}增加修为{gain_exp}"
                    if item_flag:
                        sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], 1)
                        msg += f"，额外获得奖励：{item_msg}!"
                    else:
                        msg += "!"
                    await handle_send(bot, event, msg)
                    await do_work.finish()

                else:  # 失败
                    gain_exp = give_exp // 2

                    if current_exp + gain_exp >= max_exp:
                        remaining_exp = max_exp - current_exp
                        gain_exp = remaining_exp
                    gain_exp = max(gain_exp, 0)
                    sql_message.update_exp(user_id, gain_exp)
                    sql_message.do_work(user_id, 0)
                    msg = f"悬赏令结算，{msg}增加修为{gain_exp}!"
                    await handle_send(bot, event, msg)
                    await do_work.finish()
        else:
            msg = "没有查到你的悬赏令信息呢，请刷新！"
            await handle_send(bot, event, msg)
            await do_work.finish()

    elif mode == "接取":
        num = args[1]
        is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
        if is_type:  # 接取逻辑
            if num is None or str(num) not in ['1', '2', '3']:
                msg = '请输入正确的任务序号'
                await handle_send(bot, event, msg)
                await do_work.finish()
            work_num = 1
            try:
                if work[user_id]:
                    work_num = int(num)  # 任务序号
                try:
                    get_work = work[user_id].world[work_num - 1]
                    sql_message.do_work(user_id, 2, get_work[0])
                    del work[user_id]
                    msg = f"接取任务【{get_work[0]}】成功"
                    await handle_send(bot, event, msg)
                    await do_work.finish()

                except IndexError:
                    msg = "没有这样的任务"
                    await handle_send(bot, event, msg)
                    await do_work.finish()

            except KeyError:
                msg = "没有查到你的悬赏令信息呢，请刷新！"
                await handle_send(bot, event, msg)
                await do_work.finish()
        else:
            await handle_send(bot, event, msg)
            await do_work.finish()

    elif mode == "帮助":
        msg = __work_help__
        await handle_send(bot, event, msg)
        await do_work.finish()


def get_work_msg(work_):
    msg = f"{work_[0]},完成机率{work_[1]},基础报酬{work_[2]}修为,预计需{work_[3]}分钟{work_[4]}\n"
    return msg
