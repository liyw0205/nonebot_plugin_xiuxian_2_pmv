try:
    import ujson as json
except ImportError:
    import json
import os
from pathlib import Path
from typing import Any, Tuple
from nonebot import on_regex
from nonebot.log import logger
from nonebot.params import RegexGroup
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    GROUP,
    MessageSegment,
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from datetime import datetime
from .bankconfig import get_config
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send
from ..xiuxian_config import XiuConfig

config = get_config()
BANKLEVEL = config["BANKLEVEL"]
sql_message = XiuxianDateManage()  # sql类
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

bank = on_regex(
    r'^灵庄(存灵石|取灵石|升级会员|信息|结算)?(.*)?',
    priority=9,    
    block=True
)

__bank_help__ = f"""
【修仙灵庄系统】🏦

💰 存取服务：
  灵庄存灵石 [金额] - 存入灵石获取利息
  灵庄取灵石 [金额] - 取出灵石(自动结算利息)
  
🌟 会员服务：
  灵庄升级会员 - 提升会员等级增加利息倍率
  
📊 查询服务：
  灵庄信息 - 查看账户余额和会员信息
  灵庄结算 - 手动结算当前利息

📈 利息规则：
  • 按小时计算利息
  • 会员等级越高收益越高
  • 存取操作自动结算
""".strip()


@bank.handle(parameterless=[Cooldown(at_sender=False)])
async def bank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Tuple[Any, ...] = RegexGroup()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await bank.finish()
    mode = args[0]  # 存灵石、取灵石、升级会员、信息查看
    num = args[1]  # 数值
    if mode is None:
        msg = __bank_help__
        await handle_send(bot, event, msg)
        await bank.finish()

    if mode == '存灵石' or mode == '取灵石':
        try:
            num = int(num)
            if num <= 0:
                msg = f"请输入正确的金额！"
                await handle_send(bot, event, msg)
                await bank.finish()
        except ValueError:
            msg = f"请输入正确的金额！"
            await handle_send(bot, event, msg)
            await bank.finish()
    user_id = user_info['user_id']
    try:
        bankinfo = readf(user_id)
    except:
        bankinfo = {
            'savestone': 0,
            'savetime': str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            'banklevel': '1',
        }

    if mode == '存灵石':  # 存灵石逻辑
        if int(user_info['stone']) < num:
            msg = f"道友所拥有的灵石为{user_info['stone']}枚，金额不足，请重新输入！"
            await handle_send(bot, event, msg)
            await bank.finish()

        max = BANKLEVEL[bankinfo['banklevel']]['savemax']
        nowmax = max - bankinfo['savestone']

        if num > nowmax:
            msg = f"道友当前灵庄会员等级为{BANKLEVEL[bankinfo['banklevel']]['level']}，可存储的最大灵石为{max}枚,当前已存{bankinfo['savestone']}枚灵石，可以继续存{nowmax}枚灵石！"
            await handle_send(bot, event, msg)
            await bank.finish()

        bankinfo, give_stone, timedeff = get_give_stone(bankinfo)
        userinfonowstone = int(user_info['stone']) - num
        bankinfo['savestone'] += num
        sql_message.update_ls(user_id, num, 2)
        sql_message.update_ls(user_id, give_stone, 1)
        bankinfo['savetime'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        savef(user_id, bankinfo)
        msg = f"道友本次结息时间为：{timedeff}小时，获得灵石：{give_stone}枚!\n道友存入灵石{num}枚，当前所拥有灵石{userinfonowstone + give_stone}枚，灵庄存有灵石{bankinfo['savestone']}枚"
        await handle_send(bot, event, msg)
        await bank.finish()

    elif mode == '取灵石':  # 取灵石逻辑
        if int(bankinfo['savestone']) < num:
            msg = f"道友当前灵庄所存有的灵石为{bankinfo['savestone']}枚，金额不足，请重新输入！"
            await handle_send(bot, event, msg)
            await bank.finish()

        # 先结算利息
        bankinfo, give_stone, timedeff = get_give_stone(bankinfo)

        userinfonowstone = int(user_info['stone']) + num + give_stone
        bankinfo['savestone'] -= num
        sql_message.update_ls(user_id, num + give_stone, 1)
        savef(user_id, bankinfo)
        msg = f"道友本次结息时间为：{timedeff}小时，获得灵石：{give_stone}枚!\n取出灵石{num}枚，当前所拥有灵石{userinfonowstone}枚，灵庄存有灵石{bankinfo['savestone']}枚!"
        await handle_send(bot, event, msg)
        await bank.finish()

    elif mode == '升级会员':  # 升级会员逻辑
        userlevel = bankinfo["banklevel"]
        if userlevel == str(len(BANKLEVEL)):
            msg = f"道友已经是本灵庄最大的会员啦！"
            await handle_send(bot, event, msg)
            await bank.finish()

        stonecost = BANKLEVEL[f"{int(userlevel)}"]['levelup']
        if int(user_info['stone']) < stonecost:
            msg = f"道友所拥有的灵石为{user_info['stone']}枚，当前升级会员等级需求灵石{stonecost}枚金额不足，请重新输入！"
            await handle_send(bot, event, msg)
            await bank.finish()

        sql_message.update_ls(user_id, stonecost, 2)
        bankinfo['banklevel'] = f"{int(userlevel) + 1}"
        savef(user_id, bankinfo)
        msg = f"道友成功升级灵庄会员等级，消耗灵石{stonecost}枚，当前为：{BANKLEVEL[str(int(userlevel) + 1)]['level']}，灵庄可存有灵石上限{BANKLEVEL[str(int(userlevel) + 1)]['savemax']}枚"

        await handle_send(bot, event, msg)
        await bank.finish()

    elif mode == '信息':  # 查询灵庄信息
        msg = f'''道友的灵庄信息：
已存：{bankinfo['savestone']}灵石
存入时间：{bankinfo['savetime']}
灵庄会员等级：{BANKLEVEL[bankinfo['banklevel']]['level']}
当前拥有灵石：{user_info['stone']}
当前等级存储灵石上限：{BANKLEVEL[bankinfo['banklevel']]['savemax']}枚
'''
        await handle_send(bot, event, msg)
        await bank.finish()

    elif mode == '结算':

        bankinfo, give_stone, timedeff = get_give_stone(bankinfo)
        sql_message.update_ls(user_id, give_stone, 1)
        savef(user_id, bankinfo)
        msg = f"道友本次结息时间为：{timedeff}小时，获得灵石：{give_stone}枚！"
        await handle_send(bot, event, msg)
        await bank.finish()


def get_give_stone(bankinfo):
    """获取利息：利息=give_stone,结算时间=timedeff"""
    savetime = bankinfo['savetime']  # str
    nowtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # str
    timedeff = round((datetime.strptime(nowtime, '%Y-%m-%d %H:%M:%S') -
                      datetime.strptime(savetime, '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600, 2)
    give_stone = int(bankinfo['savestone'] * timedeff * BANKLEVEL[bankinfo['banklevel']]['interest'])
    bankinfo['savetime'] = nowtime

    return bankinfo, give_stone, timedeff


def readf(user_id):
    user_id = str(user_id)
    FILEPATH = PLAYERSDATA / user_id / "bankinfo.json"
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def savef(user_id, data):
    user_id = str(user_id)
    if not os.path.exists(PLAYERSDATA / user_id):
        logger.opt(colors=True).info(f"<green>用户目录不存在，创建目录</green>")
        os.makedirs(PLAYERSDATA / user_id)
    FILEPATH = PLAYERSDATA / user_id / "bankinfo.json"
    data = json.dumps(data, ensure_ascii=False, indent=3)
    savemode = "w" if os.path.exists(FILEPATH) else "x"
    with open(FILEPATH, mode=savemode, encoding="UTF-8") as f:
        f.write(data)
        f.close()
    return True
