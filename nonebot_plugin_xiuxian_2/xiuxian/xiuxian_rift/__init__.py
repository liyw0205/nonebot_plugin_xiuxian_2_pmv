import random
from datetime import datetime
from nonebot import get_bots, get_bot, on_command, require, on_fullmatch
from nonebot.params import CommandArg
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    GROUP_ADMIN,
    GROUP_OWNER,
    MessageSegment
)
from .old_rift_info import old_rift_info
from .. import DRIVER
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.utils import (
    check_user, check_user_type,
    send_msg_handler, get_msg_pic, CommandObjectID, handle_send
)
from .riftconfig import get_rift_config, savef_rift
from .jsondata import save_rift_data, read_rift_data
from ..xiuxian_config import XiuConfig, convert_rank
from .riftmake import (
    Rift, get_rift_type, get_story_type, NONEMSG, get_battle_type,
    get_dxsj_info, get_boss_battle_info, get_treasure_info
)


config = get_rift_config()
sql_message = XiuxianDateManage()  # sql类
cache_help = {}
group_rift = {}  # dict
groups = config['open']  # list
# 定时任务
set_rift = require("nonebot_plugin_apscheduler").scheduler

set_group_rift = on_command("秘境", priority=4, permission=SUPERUSER, block=True)
explore_rift = on_fullmatch("探索秘境", priority=5, block=True)
rift_help = on_fullmatch("秘境帮助", priority=6, block=True)
create_rift = on_fullmatch("生成秘境", priority=5, permission=SUPERUSER, block=True)
complete_rift = on_command("秘境结算", aliases={"结算秘境"}, priority=7, block=True)
break_rift = on_command("秘境探索终止", aliases={"终止探索秘境"}, priority=7, block=True)
use_rift_key = on_command("使用秘境钥匙", priority=5, block=True)  # 新增命令

__rift_help__ = f"""
秘境帮助信息:
指令：
1、秘境开启、关闭:秘境生成的通知，管理员权限
2、生成秘境:生成一个随机秘境，管理员权限
3、探索秘境:探索秘境获取随机奖励
4、秘境结算、结算秘境:结算秘境奖励
5、秘境探索终止、终止探索秘境:终止秘境事件
6、使用秘境钥匙:使用秘境钥匙立即结算当前秘境
7、秘境帮助:获取秘境帮助信息
非指令：
1、每天0点和12点生成一个随机等级的秘境
""".strip()


@DRIVER.on_startup
async def read_rift_():
    global group_rift
    group_rift.update(old_rift_info.read_rift_info())
    logger.opt(colors=True).info(f"<green>历史rift数据读取成功</green>")

@DRIVER.on_shutdown
async def save_rift_():
    global group_rift
    old_rift_info.save_rift(group_rift)
    logger.opt(colors=True).info(f"<green>rift数据已保存</green>")

# 定时任务生成秘境
@set_rift.scheduled_job("cron", hour='0,12', minute=0)
async def scheduled_rift_generation():
    """
    定时任务：每天0,12点触发秘境生成
    """
    global group_rift
    if not groups:
        logger.warning("秘境未开启，定时任务终止")
        return
    
    await generate_rift_for_group()   
    
    logger.info("秘境定时生成完成")

      
async def generate_rift_for_group():
    group_id = "000000"
    rift = Rift()
    rift.name = get_rift_type()
    rift.rank = config['rift'][rift.name]['rank']
    rift.time = config['rift'][rift.name]['time']
    group_rift[group_id] = rift
    msg = f"野生的{rift.name}出现了！请诸位道友发送 探索秘境 来加入吧！"
    logger.info(msg)
    old_rift_info.save_rift(group_rift)
    for notify_group_id in groups:
        if notify_group_id == "000000":
            continue
        bot = get_bot()
        await bot.send_group_msg(group_id=int(notify_group_id), message=msg)



@rift_help.handle(parameterless=[Cooldown(at_sender=False)])
async def rift_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """秘境帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        await bot.send_group_msg(group_id=int(send_group_id), message=MessageSegment.image(cache_help[session_id]))
        await rift_help.finish()
    else:
        msg = __rift_help__
        await handle_send(bot, event, msg)
        await rift_help.finish()


@create_rift.handle(parameterless=[Cooldown(at_sender=False)])
async def create_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """生成秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    if group_id not in groups:
        msg = '尚未开启秘境，请联系管理员开启秘境'
        await handle_send(bot, event, msg)
        await create_rift.finish()

    try:
        msg = f"当前已存在{group_rift[group_id].name}，请诸位道友发送 探索秘境 来加入吧！"
        await handle_send(bot, event, msg)
        await create_rift.finish()
    except KeyError:
        rift = Rift()
        rift.name = get_rift_type()
        rift.rank = config['rift'][rift.name]['rank']
        rift.time = config['rift'][rift.name]['time']
        group_rift[group_id] = rift
        msg = f"野生的{rift.name}出现了！请诸位道友发送 探索秘境 来加入吧！"
        old_rift_info.save_rift(group_rift)
        await handle_send(bot, event, msg)
        await create_rift.finish()


@explore_rift.handle(parameterless=[Cooldown(stamina_cost=6, at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """探索秘境"""
    group_rift.update(old_rift_info.read_rift_info())
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await explore_rift.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg)
        await explore_rift.finish()
    else:
        group_id = "000000"        
        try:
            group_rift[group_id]
        except:
            msg = '野外秘境尚未生成，请道友耐心等待!'
            await handle_send(bot, event, msg)
            await explore_rift.finish()
        if user_id in group_rift[group_id].l_user_id:
            msg = '道友已经参加过本次秘境啦，请把机会留给更多的道友！'
            await handle_send(bot, event, msg)
            await explore_rift.finish()
        
        user_rank = convert_rank(user_info["level"])[0]
         # 搬血中期 - 秘境rank
        required_rank = convert_rank("感气境中期")[0] - group_rift[group_id].rank
         
        if user_rank > required_rank:
            rank_name_list = convert_rank(user_info["level"])[1]
            required_rank_name = rank_name_list[len(rank_name_list) - required_rank - 1]
            msg = f"秘境凶险万分，道友的境界不足，无法进入秘境：{group_rift[group_id].name}，请道友提升到{required_rank_name}以上再来！"
            await handle_send(bot, event, msg)
            await explore_rift.finish()

        group_rift[group_id].l_user_id.append(user_id)
        msg = f"道友进入秘境：{group_rift[group_id].name}，探索需要花费时间：{group_rift[group_id].time}分钟！"
        rift_data = {
            "name": group_rift[group_id].name,
            "time": group_rift[group_id].time,
            "rank": group_rift[group_id].rank
        }

        save_rift_data(user_id, rift_data)
        sql_message.do_work(user_id, 3, rift_data["time"])
        old_rift_info.save_rift(group_rift)
        await handle_send(bot, event, msg)
        await explore_rift.finish()


@complete_rift.handle(parameterless=[Cooldown(at_sender=False)])
async def complete_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """秘境结算"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await complete_rift.finish()

    user_id = user_info['user_id']

    group_id = "000000"   

    is_type, msg = check_user_type(user_id, 3)  # 需要在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg)
        await complete_rift.finish()
    else:
        rift_info = None
        try:
            rift_info = read_rift_data(user_id)
        except:
            msg = '发生未知错误！'
            sql_message.do_work(user_id, 0)
            await handle_send(bot, event, msg)
            await complete_rift.finish()

        user_cd_message = sql_message.get_user_cd(user_id)
        work_time = datetime.strptime(
            user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
        )
        exp_time = (datetime.now() - work_time).seconds // 60  # 时长计算
        time2 = rift_info["time"]
        if exp_time < time2:
            msg = f"进行中的：{rift_info['name']}探索，预计{time2 - exp_time}分钟后可结束"
            await handle_send(bot, event, msg)
            await complete_rift.finish()
        else:  # 秘境结算逻辑
            sql_message.do_work(user_id, 0)
            rift_rank = rift_info["rank"]  # 秘境等级
            rift_type = get_story_type()  # 无事、宝物、战斗
            if rift_type == "无事":
                msg = random.choice(NONEMSG)
                await handle_send(bot, event, msg)
                await complete_rift.finish()
            elif rift_type == "战斗":
                rift_type = get_battle_type()
                if rift_type == "掉血事件":
                    msg = get_dxsj_info("掉血事件", user_info)
                    await handle_send(bot, event, msg)
                    await complete_rift.finish()
                elif rift_type == "Boss战斗":
                    result, msg = await get_boss_battle_info(user_info, rift_rank, bot.self_id)
                    await send_msg_handler(bot, event, result)
                    await handle_send(bot, event, msg)
                    await complete_rift.finish()
            elif rift_type == "宝物":
                msg = get_treasure_info(user_info, rift_rank)
                await handle_send(bot, event, msg)
                await complete_rift.finish()


@break_rift.handle(parameterless=[Cooldown(at_sender=False)])
async def break_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """终止探索秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await break_rift.finish()
    user_id = user_info['user_id']
    group_id = "000000"        

    is_type, msg = check_user_type(user_id, 3)  # 需要在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg)
        await break_rift.finish()
    else:
        user_id = user_info['user_id']
        rift_info = None
        try:
            rift_info = read_rift_data(user_id)
        except:
            msg = '发生未知错误！'
            sql_message.do_work(user_id, 0)
            await handle_send(bot, event, msg)
            await break_rift.finish()

        sql_message.do_work(user_id, 0)
        msg = f"已终止{rift_info['name']}秘境的探索！"
        await handle_send(bot, event, msg)
        await break_rift.finish()

        
@set_group_rift.handle(parameterless=[Cooldown(at_sender=False)])
async def set_group_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """秘境开启、关闭"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    mode = args.extract_plain_text().strip()
    group_id = str(send_group_id)  # 使用实际群号
    isInGroup = group_id in config['open']  # 检查群号是否在通知列表中

    if mode == '开启':
        if isInGroup:
            msg = f"本群已开启秘境通知，请勿重复开启!"
            await handle_send(bot, event, msg)
            await set_group_rift.finish()

        else:
            config['open'].append(group_id)
            savef_rift(config)
            msg = f"已开启本群秘境通知!"
            await handle_send(bot, event, msg)
            await set_group_rift.finish()

    elif mode == '关闭':
        if isInGroup:
            config['open'].remove(group_id)
            savef_rift(config)
            msg = f"已关闭本群秘境通知!"
            await handle_send(bot, event, msg)
            await set_group_rift.finish()
        else:
            msg = f"未开启本群秘境通知!"
            await handle_send(bot, event, msg)
            await set_group_rift.finish()
    
    elif mode == '帮助':
        msg = __rift_help__
        await handle_send(bot, event, msg)
        await set_group_rift.finish()


@use_rift_key.handle(parameterless=[Cooldown(at_sender=False)])
async def use_rift_key_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """使用秘境钥匙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await use_rift_key.finish()

    user_id = user_info['user_id']
    group_id = "000000"    

    # 检查是否在秘境中
    is_type, _ = check_user_type(user_id, 3)  # 类型 3 表示在秘境中
    if not is_type:
        msg = "道友当前不在秘境中，无法使用秘境钥匙！"
        await handle_send(bot, event, msg)
        await use_rift_key.finish()

    # 检查背包中的秘境钥匙
    back_msg = sql_message.get_back_msg(user_id)
    rift_key_id = 20001  # 秘境钥匙的 goods_id
    rift_key_num = 0
    for item in back_msg:
        if item['goods_id'] == rift_key_id:
            rift_key_num = item['goods_num']
            break

    if rift_key_num < 1:
        msg = "道友背包中没有秘境钥匙，无法使用！"
        await handle_send(bot, event, msg)
        await use_rift_key.finish()

    # 读取秘境信息并立即结算
    try:
        rift_info = read_rift_data(user_id)
    except:
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        await use_rift_key.finish()

    sql_message.do_work(user_id, 0)  # 清除秘境状态
    rift_rank = rift_info["rank"]
    rift_type = get_story_type()  # 无事、宝物、战斗
    result_msg = ""

    if rift_type == "无事":
        result_msg = random.choice(NONEMSG)
    elif rift_type == "战斗":
        battle_type = get_battle_type()
        if battle_type == "掉血事件":
            result_msg = get_dxsj_info("掉血事件", user_info)
        elif battle_type == "Boss战斗":
            result, result_msg = await get_boss_battle_info(user_info, rift_rank, bot.self_id)
            await send_msg_handler(bot, event, result)
    elif rift_type == "宝物":
        result_msg = get_treasure_info(user_info, rift_rank)

    # 消耗秘境钥匙
    sql_message.update_back_j(user_id, rift_key_id)
    msg = f"道友使用 1 个秘境钥匙，秘境 {rift_info['name']} 已立即结算！\n结算结果：{result_msg}"

    await handle_send(bot, event, msg)
    await use_rift_key.finish()


def is_in_groups(event: GroupMessageEvent | PrivateMessageEvent):
    return "000000" in groups