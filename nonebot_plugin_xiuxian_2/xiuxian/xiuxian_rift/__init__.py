import random
from datetime import datetime, timedelta
from nonebot import get_bots, get_bot, on_command, on_fullmatch
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
from nonebot.log import logger
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.utils import (
    check_user, check_user_type,
    send_msg_handler, get_msg_pic, CommandObjectID, log_message, handle_send, update_statistics_value
)
from .riftconfig import get_rift_config, savef_rift
from .jsondata import save_rift_data, read_rift_data
from ..xiuxian_config import XiuConfig, convert_rank
from .riftmake import (
    Rift, get_rift_type, get_story_type, NONEMSG, get_battle_type,
    get_dxsj_info, get_boss_battle_info, get_treasure_info
)

sql_message = XiuxianDateManage()  # sql类
cache_help = {}
group_rift = {}  # dict
config = get_rift_config() # 获取秘境配置
groups = config['open']  # list

my_rift_count = on_command("秘境次数", aliases={"秘境进度"}, priority=7, block=True)
explore_rift = on_fullmatch("探索秘境", priority=5, block=True)
rift_help = on_fullmatch("秘境帮助", priority=6, block=True)
complete_rift = on_command("秘境结算", aliases={"结算秘境"}, priority=7, block=True)
break_rift = on_command("秘境终止", aliases={"终止秘境"}, priority=7, block=True)

__rift_help__ = f"""
【秘境探索系统】🗝️

🔍 探索指令：
  • 探索秘境 - 进入秘境获取随机奖励
  • 秘境结算 - 领取秘境奖励
  • 秘境终止 - 放弃当前秘境
  • 秘境次数 - 获取秘境保底奖励次数

⏰ 秘境刷新：
  • 每日自动生成时间：0点 & 12点
  • 秘境等级随机生成

💡 小贴士：
  1. 秘境奖励随探索时间增加
  2. 使用道具可提升收益
  3. 终止探索会损失奖励
""".strip()



@DRIVER.on_startup
async def read_rift_():
    """读取历史秘境数据"""
    global group_rift
    group_rift.update(old_rift_info.read_rift_info())
    logger.opt(colors=True).info(f"<green>历史rift数据读取成功</green>")

@DRIVER.on_shutdown
async def save_rift_():
    """保存秘境数据"""
    global group_rift
    old_rift_info.save_rift(group_rift)
    logger.opt(colors=True).info(f"<green>rift数据已保存</green>")

# 定时任务生成秘境
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
    """为群组生成新的秘境"""
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

def update_rift_explore_count(user_id: int, do_give: bool = True) -> str:
    """
    更新秘境完成次数，并判断是否达到10次赠送随机秘境道具

    参数:
        user_id: 用户ID
        do_give: 是否执行赠送道具和清零操作（默认True）
                 - True：用于秘境结算时（会赠送并清零）
                 - False：用于查询次数时（只返回当前进度，不修改数据）

    返回:
        str: 给用户显示的消息
    """
    player_manager = PlayerDataManager()
    user_id_str = str(user_id)
    
    # 定义可获得的奖励道具
    reward_items = {
        "秘藏令": 20007,
        "秘境钥匙": 20001,
        "神秘经书·残": 20008,
        "神秘经书": 20009,
        "灵签宝箓": 20010,
        "秘境加速券": 20012,
        "秘境加速券": 20012,
        "秘境大加速券": 20013,
        "斩妖令": 20018,
        "解绑符": 20019
    }
    
    # 获取当前次数，没有则初始化为0
    count = player_manager.get_field_data(user_id_str, "rift", "explore_count")
    if count is None:
        count = 0
        player_manager.update_or_write_data(user_id_str, "rift", "explore_count", 0)

    if not do_give:
        # 只查询，不赠送
        need = 10 - count
        if need <= 0:
            return f"道友当前秘境完成次数：{count}/10\n已可领取秘境奖励，请进行一次秘境结算来领取！"
        else:
            return f"道友当前秘境完成次数：{count}/10\n再完成 {need} 次即可获得秘境奖励"

    # +1
    new_count = count + 1

    msg = ""
    if new_count >= 10:
        # 随机选择一个奖励道具
        reward_name, reward_id = random.choice(list(reward_items.items()))
        
        sql_message.send_back(
            user_id,
            reward_id,
            reward_name,
            "特殊道具",
            1,
            1
        )
        # 清零
        new_count = 0
        msg = f"\n【秘境累计完成10次！】\n赠送道友 {reward_name} x1！"
    else:
        need = 10 - new_count
        if need > 0:
            msg = f"\n当前秘境完成次数：{new_count}/10（再完成 {need} 次可获秘境奖励）"

    player_manager.update_or_write_data(
        user_id_str, "rift", "explore_count", new_count
    )

    return msg

@my_rift_count.handle(parameterless=[Cooldown(cd_time=3)])
async def show_rift_progress(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """显示秘境探索进度"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_rift_count.finish()

    user_id = user_info['user_id']

    # 只查询，不赠送
    progress_msg = update_rift_explore_count(user_id, do_give=False)

    await handle_send(bot, event, progress_msg)
    await my_rift_count.finish()

@rift_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def rift_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """秘境帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        await bot.send_group_msg(group_id=int(send_group_id), message=MessageSegment.image(cache_help[session_id]))
        await rift_help.finish()
    else:
        msg = __rift_help__
        await handle_send(bot, event, msg, md_type="秘境", k1="探索", v1="探索秘境", k2="结算", v2="秘境结算", k3="帮助", v3="秘境帮助")
        await rift_help.finish()

async def create_rift(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """生成秘境（手动触发，通常由管理员使用）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    if group_id not in groups:
        msg = '尚未开启秘境，请联系管理员开启秘境'
        await handle_send(bot, event, msg)
        return

    rift = Rift()
    rift.name = get_rift_type()
    rift.rank = config['rift'][rift.name]['rank']
    rift.time = config['rift'][rift.name]['time']
    group_rift[group_id] = rift
    msg = f"野生的{rift.name}出现了！请诸位道友发送 探索秘境 来加入吧！"
    old_rift_info.save_rift(group_rift)
    await handle_send(bot, event, msg, md_type="秘境", k1="探索", v1="探索秘境", k2="结算", v2="秘境结算", k3="帮助", v3="秘境帮助")
    return


@explore_rift.handle(parameterless=[Cooldown(stamina_cost=6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """探索秘境"""
    group_rift.update(old_rift_info.read_rift_info()) # 确保秘境数据最新
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await explore_rift.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
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
        required_rank_for_check = convert_rank("感气境中期")[0] - group_rift[group_id].rank
         
        if user_rank > required_rank_for_check:
            rank_name_list = convert_rank(user_info["level"])[1] # 获取用户境界的文字描述列表
            
            msg = f"秘境凶险万分，道友的境界不足，无法进入秘境：{group_rift[group_id].name}，请道友提升境界后再来！"
            await handle_send(bot, event, msg)
            await explore_rift.finish()

        group_rift[group_id].l_user_id.append(user_id)
        msg = f"进入秘境：{group_rift[group_id].name}，探索需要花费时间：{group_rift[group_id].time}分钟！"
        rift_data = {
            "name": group_rift[group_id].name,
            "time": group_rift[group_id].time,
            "rank": group_rift[group_id].rank
        }

        save_rift_data(user_id, rift_data)
        sql_message.do_work(user_id, 3, rift_data["time"])
        update_statistics_value(user_id, "秘境次数")
        old_rift_info.save_rift(group_rift)
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        await explore_rift.finish()

async def use_rift_explore(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘藏令"""
    async def _check_and_enter_rift(user_id, user_info, bot, event):
        group_id = "000000"        
        try:
            current_rift = group_rift[group_id]
        except KeyError:
            return False, '野外秘境尚未生成，请道友耐心等待!'
                
        user_rank = convert_rank(user_info["level"])[0]
        required_rank_for_check = convert_rank("感气境中期")[0] - current_rift.rank
        
        if user_rank > required_rank_for_check:
            return False, f"秘境凶险万分，道友的境界不足，无法进入秘境：{current_rift.name}，请道友提升境界后再来！"
        
        return True, current_rift

    group_rift.update(old_rift_info.read_rift_info()) # 确保秘境数据最新
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return
    else:
        can_enter, check_msg_or_rift = await _check_and_enter_rift(user_id, user_info, bot, event)
        if not can_enter:
            await handle_send(bot, event, check_msg_or_rift)
            return

        current_rift = check_msg_or_rift # 此时 check_msg_or_rift 是 Rift 对象

        group_rift["000000"].l_user_id.append(user_id) # 添加用户到秘境参与者列表
        msg = f"进入秘境：{current_rift.name}，探索需要花费时间：{current_rift.time}分钟！"
        rift_data = {
            "name": current_rift.name,
            "time": current_rift.time,
            "rank": current_rift.rank
        }

        save_rift_data(user_id, rift_data)
        sql_message.do_work(user_id, 3, rift_data["time"])
        sql_message.update_back_j(user_id, item_id) # 消耗道具
        update_statistics_value(user_id, "秘境次数")
        old_rift_info.save_rift(group_rift)
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        return

# 秘境结算
async def _perform_rift_settlement(user_id, user_info, rift_info, bot, event):
    """
    执行秘境结算的核心逻辑，根据秘境事件类型发放奖励。
    返回一个消息字符串。
    """
    rift_rank = rift_info["rank"]  # 秘境等级
    rift_type = get_story_type()  # 无事、宝物、战斗
    result_msg = ""
    result_name = None
    log_content = ""

    count_msg = update_rift_explore_count(user_id, do_give=True) # 更新秘境探索次数

    if rift_type == "无事":
        result_msg = random.choice(NONEMSG)
        log_content = result_msg
    elif rift_type == "战斗":
        battle_type = get_battle_type()
        if battle_type == "掉血事件":
            result_msg = get_dxsj_info("掉血事件", user_info)
            log_content = result_msg
        elif battle_type == "Boss战斗":
            # Boss战斗可能需要发送图片消息，所以要特殊处理
            result, boss_msg = await get_boss_battle_info(user_info, rift_rank, bot.self_id)
            await send_msg_handler(bot, event, result, title=boss_msg)
            update_statistics_value(user_id, "秘境打怪")
            result_msg = boss_msg # 将 boss_msg 作为主要结果消息
            log_content = boss_msg
    elif rift_type == "宝物":
        result_name, treasure_msg = get_treasure_info(user_info, rift_rank)
        result_msg = treasure_msg
        log_content = treasure_msg
        if result_name:
            # 宝物获得特殊处理，可以带上查看物品的快捷键
            final_msg = f"{result_msg}{count_msg}"
            await handle_send(bot, event, final_msg, md_type="秘境", k1="物品", v1=f"查看效果 {result_name}", k2="闭关", v2="闭关", k3="帮助", v3="秘境帮助")
            log_message(user_id, final_msg)
            return

    final_msg = f"{result_msg}{count_msg}"
    await handle_send(bot, event, final_msg)
    log_message(user_id, final_msg)
    return

@complete_rift.handle(parameterless=[Cooldown(cd_time=1.4)])
async def complete_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """秘境结算"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await complete_rift.finish()

    user_id = user_info['user_id']
    group_id = "000000"   

    is_type, msg = check_user_type(user_id, 3)  # 需要在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        await complete_rift.finish()
    else:
        rift_info = None
        try:
            rift_info = read_rift_data(user_id)
        except Exception as e:
            logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
            msg = '发生未知错误，秘境数据读取失败！'
            sql_message.do_work(user_id, 0) # 清除工作状态，避免卡死
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
            await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
            await complete_rift.finish()
        else:  # 秘境结算逻辑
            sql_message.do_work(user_id, 0) # 清除秘境状态
            await _perform_rift_settlement(user_id, user_info, rift_info, bot, event)
            await complete_rift.finish()


@break_rift.handle(parameterless=[Cooldown(cd_time=1.4)])
async def break_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """终止探索秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await break_rift.finish()
    user_id = user_info['user_id']
    group_id = "000000"        

    is_type, msg = check_user_type(user_id, 3)  # 需要在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        await break_rift.finish()
    else:
        user_id = user_info['user_id']
        rift_info = None
        try:
            rift_info = read_rift_data(user_id)
        except Exception as e:
            logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
            msg = '发生未知错误，秘境数据读取失败！'
            await handle_send(bot, event, msg)
            await break_rift.finish()

        sql_message.do_work(user_id, 0)
        msg = f"已终止{rift_info['name']}秘境的探索！"
        await handle_send(bot, event, msg)
        await break_rift.finish()

async def use_rift_key(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘境钥匙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    group_id = "000000"    

    # 检查是否在秘境中
    is_type, msg = check_user_type(user_id, 3)  # 类型 3 表示在秘境中
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return

    # 读取秘境信息
    rift_info = None
    try:
        rift_info = read_rift_data(user_id)
    except Exception as e:
        logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        return

    sql_message.do_work(user_id, 0)  # 清除秘境状态

    # 消耗秘境钥匙
    sql_message.update_back_j(user_id, item_id)
    
    await _perform_rift_settlement(user_id, user_info, rift_info, bot, event)
    return

async def use_rift_boss(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用斩妖令"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    group_id = "000000"    

    # 检查是否在秘境中
    is_type, msg = check_user_type(user_id, 3)  # 类型 3 表示在秘境中
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return

    # 读取秘境信息并立即结算
    try:
        rift_info = read_rift_data(user_id)
    except Exception as e:
        logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        return

    sql_message.do_work(user_id, 0)  # 清除秘境状态
    rift_rank = rift_info["rank"]
    
    # 直接触发Boss战斗结算，并消耗道具
    result, result_msg = await get_boss_battle_info(user_info, rift_rank, bot.self_id)
    update_statistics_value(user_id, "秘境打怪")
    await send_msg_handler(bot, event, result, title=result_msg)

    # 消耗斩妖令
    sql_message.update_back_j(user_id, item_id)
    
    # 更新秘境探索次数
    count_msg = update_rift_explore_count(user_id, do_give=True)
    
    final_msg = f"秘境 {rift_info['name']} 已使用斩妖令结算！\n战斗结果：{result_msg}{count_msg}"
    log_message(user_id, final_msg) # 记录完整消息
    await handle_send(bot, event, final_msg) # 发送最终结算消息
    return

async def use_rift_speedup(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘境加速券"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 检查是否在秘境中
    is_type, msg = check_user_type(user_id, 3)  # 需要正在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return
    
    # 读取秘境信息
    rift_info = None
    try:
        rift_info = read_rift_data(user_id)
    except Exception as e:
        logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        return

    original_time = rift_info["time"]
    
    # 如果时间已经是10分钟，则不需要使用
    if original_time <= 10:
        msg = "秘境探索时间已经小于等于10分钟，无需使用加速券！"
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        return
    
    # 计算加速后的时间（最少保留1分钟）
    new_time = max(1, int(original_time * 0.5))
    rift_info["time"] = new_time
    save_rift_data(user_id, rift_info)
    
    # 检查是否可以结算
    user_cd_message = sql_message.get_user_cd(user_id)
    work_time = datetime.strptime(
        user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
    )
    exp_time = (datetime.now() - work_time).seconds // 60
    time2 = rift_info["time"]
    
    if exp_time >= time2:
        rift_status = "可结算"
    else:
        rift_status = f"探索{rift_info['name']} {time2 - exp_time}分后"
    
    # 消耗道具
    sql_message.update_back_j(user_id, item_id)
    
    msg = f"秘境探索时间减少50%了！\n当前状态：{rift_status}"
    await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
    return

async def use_rift_big_speedup(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘境大加速券"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 检查是否在秘境中
    is_type, msg = check_user_type(user_id, 3)  # 需要正在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return
    
    # 读取秘境信息
    rift_info = None
    try:
        rift_info = read_rift_data(user_id)
    except Exception as e:
        logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        return

    original_time = rift_info["time"]
    
    # 如果时间已经小于等于10分钟，则不需要使用
    if original_time <= 10:
        msg = "秘境探索时间已经小于等于10分钟，无需使用大加速券！"
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        return
    
    # 计算大加速后的时间（最少保留1分钟）
    new_time = max(1, int(original_time * 0.1))
    rift_info["time"] = new_time
    save_rift_data(user_id, rift_info)
    
    # 检查是否可以结算
    user_cd_message = sql_message.get_user_cd(user_id)
    work_time = datetime.strptime(
        user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
    )
    exp_time = (datetime.now() - work_time).seconds // 60
    time2 = rift_info["time"]
    
    if exp_time >= time2:
        rift_status = "可结算"
    else:
        rift_status = f"探索{rift_info['name']} {time2 - exp_time}分后"
    
    # 消耗道具
    sql_message.update_back_j(user_id, item_id)
    
    msg = f"秘境探索时间减少90%了！\n当前状态：{rift_status}"
    await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
    return