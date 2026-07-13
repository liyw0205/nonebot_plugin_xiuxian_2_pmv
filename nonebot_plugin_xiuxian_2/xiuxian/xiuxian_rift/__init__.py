import random
import time
from datetime import datetime, timedelta
from nonebot import get_bots, get_bot
from ...paths import get_paths
from ..on_compat import on_command
from nonebot.params import CommandArg
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..messaging import delivery_service
from .old_rift_info import GLOBAL_RIFT_KEY, old_rift_info
from .. import DRIVER
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown
from nonebot.log import logger
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.utils import (
    check_user, check_user_type,
    send_msg_handler, get_msg_pic, log_message, handle_send,
    build_md_command_link
)
from .riftconfig import get_rift_config
from .jsondata import save_rift_data, read_rift_data
from .entry_service import RiftEntryService
from .termination_service import RiftTerminationService
from .key_event_settlement_service import RiftKeyEventSettlementService
from .demon_token_battle_settlement_service import RiftDemonTokenBattleSettlementService
from .settlement_service import RiftSettlementService
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_map import (
    get_player_current_position,
    get_random_trial_node,
    get_random_trial_nodes_by_realm,
)
from .riftmake import (
    Rift, get_rift_type, get_story_type, NONEMSG, get_battle_type,
    get_dxsj_info, get_boss_battle_info, get_treasure_info
)

sql_message = XiuxianDateManage()  # sql类
rift_entry_service = RiftEntryService(get_paths().game_db)
rift_termination_service = RiftTerminationService(get_paths().game_db)
rift_key_event_settlement_service = RiftKeyEventSettlementService(get_paths().game_db, get_paths().player_db)
rift_demon_token_battle_settlement_service = RiftDemonTokenBattleSettlementService(
    get_paths().game_db, get_paths().player_db
)
rift_settlement_service = RiftSettlementService(get_paths().game_db)
cache_help = {}
group_rift = {}  # dict
config = get_rift_config() # 获取秘境配置
groups = config['open']  # list

my_rift_count = on_command("秘境次数", aliases={"秘境进度"}, priority=7, block=True)
explore_rift = on_command("探索秘境", priority=5, block=True)
rift_help = on_command("秘境帮助", priority=6, block=True)
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

__rift_help_md__ = f"""
【秘境探索系统】🗝️

🔍 探索指令：
  • {build_md_command_link("探索秘境")} - 进入秘境获取随机奖励
  • {build_md_command_link("秘境结算")} - 领取秘境奖励
  • {build_md_command_link("秘境终止")} - 放弃当前秘境
  • {build_md_command_link("秘境次数")} - 获取秘境保底奖励次数

⏰ 秘境刷新：
  • 每日自动生成时间：0点 & 12点
  • 秘境等级随机生成

💡 小贴士：
  1. 秘境奖励随探索时间增加
  2. 使用道具可提升收益
  3. 终止探索会损失奖励

---
{build_md_command_link("探索", "探索秘境")} | {build_md_command_link("结算", "秘境结算")} | {build_md_command_link("存档", "我的修仙信息")}
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
    await generate_rift_for_group()   
    
    logger.info("秘境定时生成完成")

      
async def generate_rift_for_group():
    """为群组生成新的秘境"""
    rift = Rift()
    rift.name = get_rift_type()
    rift.rank = config['rift'][rift.name]['rank']
    rift.time = config['rift'][rift.name]['time']
    assign_rift_trial_node(rift)
    group_rift[GLOBAL_RIFT_KEY] = rift
    msg = build_rift_appear_msg(rift)
    logger.info(msg)
    old_rift_info.save_rift(group_rift)
    for notify_group_id in groups:
        bot = get_bot()
        await delivery_service.send_to_group(bot, notify_group_id, msg)


def _normalise_rift_target_node(node_info: dict) -> dict:
    return {
        "realm": node_info.get("realm", ""),
        "heaven": node_info.get("heaven", ""),
        "node_id": node_info.get("node_id", ""),
        "node_name": node_info.get("node_name", ""),
        "node_type": node_info.get("node_type", ""),
    }


def get_rift_target_nodes(rift: Rift) -> list[dict]:
    target_nodes = [
        _normalise_rift_target_node(node)
        for node in getattr(rift, "target_nodes", [])
        if isinstance(node, dict) and node.get("node_id")
    ]
    if target_nodes:
        return target_nodes

    if getattr(rift, "target_node_id", ""):
        return [{
            "realm": getattr(rift, "target_realm", ""),
            "heaven": getattr(rift, "target_heaven", ""),
            "node_id": getattr(rift, "target_node_id", ""),
            "node_name": getattr(rift, "target_node_name", ""),
            "node_type": "试炼",
        }]

    return []


def format_rift_target_nodes(rift: Rift) -> str:
    target_nodes = get_rift_target_nodes(rift)
    return "\n".join(
        f"{index}. {node['realm']}·{node['heaven']}·{node['node_name']}"
        for index, node in enumerate(target_nodes, 1)
    )


def assign_rift_trial_node(rift: Rift):
    """给秘境在每一界各绑定一个地图试炼节点。"""
    node_infos = get_random_trial_nodes_by_realm()
    if not node_infos:
        node_info = get_random_trial_node()
        node_infos = [node_info] if node_info else []
    if not node_infos:
        return rift
    rift.target_nodes = [_normalise_rift_target_node(node_info) for node_info in node_infos]
    first_node = rift.target_nodes[0]
    rift.target_realm = first_node["realm"]
    rift.target_heaven = first_node["heaven"]
    rift.target_node_id = first_node["node_id"]
    rift.target_node_name = first_node["node_name"]
    return rift


def build_rift_appear_msg(rift: Rift) -> str:
    target_msg = format_rift_target_nodes(rift)
    if target_msg:
        return (
            f"野生的{rift.name}出现在以下地点：\n"
            f"{target_msg}\n"
            f"请诸位道友前往任一目标节点后发送 探索秘境 来加入吧！"
        )
    return f"野生的{rift.name}出现了！请诸位道友发送 探索秘境 来加入吧！"


def build_rift_data(rift: Rift) -> dict:
    target_nodes = get_rift_target_nodes(rift)
    first_node = target_nodes[0] if target_nodes else {}
    return {
        "name": rift.name,
        "time": rift.time,
        "rank": rift.rank,
        "target_nodes": target_nodes,
        "target_realm": first_node.get("realm", getattr(rift, "target_realm", "")),
        "target_heaven": first_node.get("heaven", getattr(rift, "target_heaven", "")),
        "target_node_id": first_node.get("node_id", getattr(rift, "target_node_id", "")),
        "target_node_name": first_node.get("node_name", getattr(rift, "target_node_name", "")),
    }


def check_rift_target_position(user_id: int, rift: Rift) -> tuple[bool, str]:
    """普通探索需要玩家位于秘境绑定的任一试炼节点。"""
    target_nodes = get_rift_target_nodes(rift)
    if not target_nodes:
        assign_rift_trial_node(rift)
        target_nodes = get_rift_target_nodes(rift)
        if not target_nodes:
            return True, ""

    current = get_player_current_position(str(user_id))
    target_positions = {
        (node["realm"], node["heaven"], node["node_id"])
        for node in target_nodes
    }
    if current and (
        current.get("realm"),
        current.get("heaven"),
        current.get("node_id"),
    ) in target_positions:
        return True, ""

    target_msg = format_rift_target_nodes(rift)
    if current:
        current_msg = f"{current['realm']}·{current['heaven']}·{current['node_name']}"
        return False, f"本次秘境可在以下地点探索：\n{target_msg}\n道友当前在【{current_msg}】，请先前往任一目标节点再探索。"
    return False, f"本次秘境可在以下地点探索：\n{target_msg}\n请先前往任一目标节点再探索。"

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

@rift_help.handle(parameterless=[Cooldown(cd_time=0)])
async def rift_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """秘境帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await handle_send(
        bot,
        event,
        __rift_help_md__,
        native_markdown=True,
        fallback_msg=__rift_help__,
    )
    await rift_help.finish()

async def create_rift(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """生成秘境（手动触发，通常由管理员使用）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    rift = Rift()
    rift.name = get_rift_type()
    rift.rank = config['rift'][rift.name]['rank']
    rift.time = config['rift'][rift.name]['time']
    assign_rift_trial_node(rift)
    group_rift[GLOBAL_RIFT_KEY] = rift
    msg = build_rift_appear_msg(rift)
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
        try:
            current_rift = group_rift[GLOBAL_RIFT_KEY]
        except Exception:
            msg = '野外秘境尚未生成，请道友耐心等待!'
            await handle_send(bot, event, msg)
            await explore_rift.finish()
        if user_id in current_rift.l_user_id:
            msg = '道友已经参加过本次秘境啦，请把机会留给更多的道友！'
            await handle_send(bot, event, msg)
            await explore_rift.finish()
        
        user_rank = convert_rank(user_info["level"])[0]
        required_rank_for_check = convert_rank("感气境中期")[0] - current_rift.rank
         
        if user_rank > required_rank_for_check:
            rank_name_list = convert_rank(user_info["level"])[1] # 获取用户境界的文字描述列表
            
            msg = f"秘境凶险万分，道友的境界不足，无法进入秘境：{current_rift.name}，请道友提升境界后再来！"
            await handle_send(bot, event, msg)
            await explore_rift.finish()

        can_reach_rift, position_msg = check_rift_target_position(user_id, current_rift)
        if not can_reach_rift:
            old_rift_info.save_rift(group_rift)
            await handle_send(bot, event, position_msg)
            await explore_rift.finish()

        rift_data = build_rift_data(current_rift)
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        entry = rift_entry_service.enter(f"rift-entry:{event_id or time.time_ns()}:{user_id}", user_id, GLOBAL_RIFT_KEY, rift_data, rift_data["time"])
        if not entry.succeeded:
            await handle_send(bot, event, "秘境进入状态已变化，请稍后重试。")
            await explore_rift.finish()
        current_rift.l_user_id.append(user_id)
        target_msg = ""
        target_nodes_msg = format_rift_target_nodes(current_rift)
        if target_nodes_msg:
            target_msg = f"\n秘境可探索地点：\n{target_nodes_msg}"
        msg = f"进入秘境：{current_rift.name}，探索需要花费时间：{current_rift.time}分钟！{target_msg}"
        save_rift_data(user_id, rift_data)
        old_rift_info.save_rift(group_rift)
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        await explore_rift.finish()

async def use_rift_explore(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘藏令"""
    async def _check_and_enter_rift(user_id, user_info, bot, event):
        try:
            current_rift = group_rift[GLOBAL_RIFT_KEY]
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

        rift_data = build_rift_data(current_rift)
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        entry = rift_entry_service.enter(f"rift-ticket-entry:{event_id or time.time_ns()}:{user_id}", user_id, GLOBAL_RIFT_KEY, rift_data, rift_data["time"], item_id)
        if not entry.succeeded:
            await handle_send(bot, event, "秘藏令或秘境状态已变化，请稍后重试。")
            return
        current_rift.l_user_id.append(user_id)
        target_msg = ""
        target_nodes_msg = format_rift_target_nodes(current_rift)
        if target_nodes_msg:
            target_msg = f"\n秘藏令已绕过位置要求。\n秘境可探索地点：\n{target_nodes_msg}"
        msg = f"进入秘境：{current_rift.name}，探索需要花费时间：{current_rift.time}分钟！{target_msg}"
        save_rift_data(user_id, rift_data)
        old_rift_info.save_rift(group_rift)
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        return

def _rift_progress_snapshot(user_id):
    return int(PlayerDataManager().get_field_data(str(user_id), "rift", "explore_count") or 0)


def _roll_rift_progress(count):
    if int(count) + 1 < 10:
        new_count = int(count) + 1
        return None, f"\n当前秘境完成次数：{new_count}/10（再完成 {10 - new_count} 次可获秘境奖励）"
    rewards = {
        "秘藏令": 20007, "秘境钥匙": 20001, "神秘经书·残": 20008, "神秘经书": 20009,
        "灵签宝箓": 20010, "秘境加速券": 20012, "秘境大加速券": 20013,
        "斩妖令": 20018, "解绑符": 20019,
    }
    name, item_id = random.choice(list(rewards.items()))
    reward = {"id": item_id, "name": name, "type": "特殊道具", "amount": 1}
    return reward, f"\n【秘境累计完成10次！】\n赠送道友 {name} x1！"


async def _roll_rift_event(user_info, rift_info, bot_id):
    """Fix the random event, battle result and all persistence deltas."""
    rift_rank = rift_info["rank"]  # 秘境等级
    rift_type = get_story_type()  # 无事、宝物、战斗
    battle_result = None
    result_msg = ""
    result_name = None
    outcome = {"delta": {}, "items": [], "statistics": {}}

    if rift_type == "无事":
        result_msg = random.choice(NONEMSG)
    elif rift_type == "战斗":
        battle_type = get_battle_type()
        if battle_type == "掉血事件":
            result_msg, outcome = get_dxsj_info("掉血事件", user_info)
        elif battle_type == "Boss战斗":
            battle_result, result_msg, outcome = await get_boss_battle_info(
                user_info, rift_rank, bot_id, persist=False
            )
    elif rift_type == "宝物":
        result_name, result_msg, outcome = get_treasure_info(user_info, rift_rank)
    outcome["message"] = result_msg
    return battle_result, result_name, outcome

@complete_rift.handle(parameterless=[Cooldown(cd_time=0)])
async def complete_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """秘境结算"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await complete_rift.finish()

    user_id = user_info['user_id']

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
        else:
            event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
            result = rift_settlement_service.settle(
                f"rift-settlement:{event_id or time.time_ns()}:{user_id}", user_id, rift_info,
                {key: int(user_info.get(key, 0)) for key in ("stone", "exp", "hp", "mp")},
                {"stone": 0, "exp": 0, "hp": 0, "mp": 0},
            )
            if not result.succeeded:
                await handle_send(bot, event, "秘境结算状态已变化，请重新查询后再试。")
                await complete_rift.finish()
            final_msg = f"{random.choice(NONEMSG)}\n当前秘境完成次数：{result.explore_count}"
            await handle_send(bot, event, final_msg)
            log_message(user_id, final_msg)
            await complete_rift.finish()


@break_rift.handle(parameterless=[Cooldown(cd_time=0)])
async def break_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """终止探索秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await break_rift.finish()
    user_id = user_info['user_id']

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

        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        result = rift_termination_service.terminate(
            f"rift-termination:{event_id or time.time_ns()}:{user_id}", user_id, rift_info
        )
        if not result.succeeded:
            await handle_send(bot, event, "秘境状态已变化，请稍后重试。")
            await break_rift.finish()
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

    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"rift-key-event:{event_id or time.time_ns()}:{user_id}"
    replay = rift_key_event_settlement_service.replay(operation_id)
    if replay is not None:
        await handle_send(bot, event, replay.message)
        return
    battle_result, result_name, outcome = await _roll_rift_event(user_info, rift_info, bot.self_id)
    explore_count = _rift_progress_snapshot(user_id)
    progress_reward, progress_msg = _roll_rift_progress(explore_count)
    outcome["progress_reward"] = progress_reward
    outcome["message"] = f"{outcome['message']}{progress_msg}"
    result = rift_key_event_settlement_service.settle(
        operation_id, user_id, item_id, rift_info,
        {key: int(user_info.get(key, 0)) for key in ("stone", "exp", "hp", "mp")},
        explore_count, outcome, XiuConfig().max_goods_num,
    )
    if not result.succeeded:
        await handle_send(bot, event, "秘境钥匙或秘境状态已变化，请稍后重试。")
        return
    if battle_result is not None:
        await send_msg_handler(bot, event, battle_result, title=outcome["message"].split("\n", 1)[0])
    if result_name:
        await handle_send(bot, event, result.message, md_type="秘境", k1="物品", v1=f"查看效果 {result_name}", k2="闭关", v2="闭关", k3="帮助", v3="秘境帮助")
    else:
        await handle_send(bot, event, result.message)
    log_message(user_id, result.message)
    return

async def use_rift_boss(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用斩妖令"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']

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

    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"rift-demon-token-battle:{event_id or time.time_ns()}:{user_id}"
    replay = rift_demon_token_battle_settlement_service.replay(operation_id)
    if replay is not None:
        await handle_send(bot, event, replay.message)
        return
    battle_result, result_msg, outcome = await get_boss_battle_info(
        user_info, rift_info["rank"], bot.self_id, persist=False
    )
    explore_count = _rift_progress_snapshot(user_id)
    progress_reward, progress_msg = _roll_rift_progress(explore_count)
    outcome["progress_reward"] = progress_reward
    outcome["message"] = f"秘境 {rift_info['name']} 已使用斩妖令结算！\n战斗结果：{result_msg}{progress_msg}"
    settlement = rift_demon_token_battle_settlement_service.settle(
        operation_id, user_id, item_id, rift_info,
        {key: int(user_info.get(key, 0)) for key in ("stone", "exp", "hp", "mp")},
        explore_count, outcome, XiuConfig().max_goods_num,
    )
    if not settlement.succeeded:
        await handle_send(bot, event, "斩妖令或秘境状态已变化，请稍后重试。")
        return
    await send_msg_handler(bot, event, battle_result, title=result_msg)
    log_message(user_id, settlement.message)
    await handle_send(bot, event, settlement.message)
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
