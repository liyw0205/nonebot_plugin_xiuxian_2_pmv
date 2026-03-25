import asyncio
import json
from datetime import datetime, timedelta
from nonebot import on_command
from nonebot.log import logger
from nonebot.params import CommandArg
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, XIUXIAN_IMPART_BUFF, PlayerDataManager, UserBuffDate
from ..xiuxian_utils.data_source import jsondata
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic,
    handle_send
)
from ..xiuxian_impart.impart_uitls import (
    impart_check,
    update_user_impart_data
)

xiuxian_impart = XIUXIAN_IMPART_BUFF()
player_data_manager = PlayerDataManager()
items = Items()
confirm_lunhui_cache = {}

__warring_help__ = f"""
【轮回重修系统】♾️

⚠️ 警告：此操作不可逆！
散尽毕生修为，轮回重修，凝聚万世道果为极致天赋，开启永恒不灭之路，执掌轮回命运果位

🔥 所有修为、功法、神通、灵石、修炼等级、虚神界修炼时间将被清空！

🔄 进入轮回
   • 千世轮回获得【轮回灵根】
   • 最低境界要求：{XiuConfig().lunhui_min_level}
   
   • 万世轮回获得【真·轮回灵根】 
   • 最低境界要求：{XiuConfig().twolun_min_level}

   • 永恒轮回获得【永恒灵根】
   • 最低境界要求：{XiuConfig().threelun_min_level}
   
♾️ 进入无限轮回 - 获得【命运灵根】
   • 最低境界要求：{XiuConfig().Infinite_reincarnation_min_level}

💀 自废修为 - 仅感气境可用
  • 完全重置修为（慎用！）

📜 关于【轮回印记】：
   • 每次轮回时，系统会自动记录你当前的：
     主功法 / 辅修功法 / 神通 / 身法 / 瞳术
   • 这些技能会被「永久保存」在轮回印记中
   • 后期可通过指令【回忆前世 + 类型】取回
     示例：
       回忆前世 主功法
       回忆前世 神通
   • **每种类型只能取回一次**（取回后该印记作废）
   • 取回有境界要求：需达到轮回前境界-9个小境界才能解锁

📌 注意事项：
• 轮回后将更新灵根资质
• 所有装备、物品不会丢失

""".strip()

cache_help_fk = {}
sql_message = XiuxianDateManage()  # sql类

warring_help = on_command("轮回重修帮助", aliases={"轮回帮助"}, priority=12, block=True)
lunhui = on_command('进入轮回', aliases={"开始轮回"}, priority=15,  block=True)
Infinite_reincarnation = on_command('进入无限轮回', priority=15,  block=True)
retrieve_memory = on_command("回忆前世", aliases={"回忆前世", "取回记忆"}, priority=15, block=True)
view_memory = on_command("轮回印记", priority=15, block=True)
resetting = on_command('自废修为', priority=15,  block=True)
confirm_lunhui = on_command('确认轮回', priority=15,  block=True)

@warring_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def warring_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """轮回重修帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __warring_help__
    await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="印记", v3="轮回印记")
    await warring_help.finish()
        
@resetting.handle(parameterless=[Cooldown(cd_time=1.4)])
async def resetting_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    
                    
    if user_msg['level'] in ['感气境初期', '感气境中期', '感气境圆满']:
        exp = user_msg['exp']
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, exp) #重置用户修为
        sql_message.update_exp(user_id, 100)  
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"{user_name}现在是一介凡人了！！"
        await handle_send(bot, event, msg)
        await resetting.finish()
    else:
        msg = f"道友境界未达要求，自废修为的最低境界为感气境！"
        await handle_send(bot, event, msg)
        await resetting.finish()
        
@lunhui.handle(parameterless=[Cooldown(cd_time=1.4)])
async def lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await lunhui.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    
    if str(user_id) in confirm_lunhui_cache:
        msg = "请发送【确认轮回】！"
        await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await lunhui.finish()
    if user_root == '轮回道果':
        root_level = 7
        lunhui_level = XiuConfig().twolun_min_level
        lunhui_level2 = "万世轮回"
        msg = f"万世道果集一身，脱出凡道入仙道，恭喜大能{user_name}万世轮回成功！"
    elif user_root == '真·轮回道果':
        root_level = 8
        lunhui_level = XiuConfig().threelun_min_level
        lunhui_level2 = "永恒轮回"
        msg = f"穿越千劫万难，证得不朽之身，恭喜大能{user_name}步入永恒之道，成就无上永恒！"
    elif user_root == '永恒道果':
        root_level = 9
        lunhui_level = XiuConfig().Infinite_reincarnation_min_level
        lunhui_level2 = "无限轮回"
        msg = f"超越永恒，超脱命运，执掌因果轮回！恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
    elif user_root == '命运道果':
        await Infinite_reincarnation_(bot, event)
        await lunhui.finish()
    else:
        root_level = 6
        lunhui_level = XiuConfig().lunhui_min_level
        lunhui_level2 = "千世轮回"
        msg = f"千世轮回磨不灭，重回绝颠谁能敌，恭喜大能{user_name}轮回成功！"
        
    if list_level_all.index(level) >= list_level_all.index(lunhui_level):
        await confirm_lunhui_invite(bot, event, user_id, root_level, lunhui_level2, msg)
    else:
        msg = f"道友境界未达要求\n当前进入：{lunhui_level2}\n最低境界为：{lunhui_level}"
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
    await lunhui.finish()

@Infinite_reincarnation.handle(parameterless=[Cooldown(cd_time=1.4)])
async def Infinite_reincarnation_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await Infinite_reincarnation.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    
    if str(user_id) in confirm_lunhui_cache:
        msg = "请发送【确认轮回】！"
        await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await Infinite_reincarnation.finish()

    if user_root != '命运道果' :
        msg = "道友还未完成轮回，请先进入轮回！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await Infinite_reincarnation.finish()
    if (list_level_all.index(level) >= list_level_all.index(XiuConfig().Infinite_reincarnation_min_level)) and user_root == '命运道果':
        msg = f"超越永恒，超脱命运，执掌因果轮回！\n恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
        await confirm_lunhui_invite(bot, event, user_id, 0, "无限轮回", msg)
    else:
        msg = f"道友境界未达要求，无限轮回的最低境界为{XiuConfig().Infinite_reincarnation_min_level}！"
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
    await Infinite_reincarnation.finish()

@retrieve_memory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    type_map = {
        "主功法": "main_buff",
        "辅修": "sub_buff",
        "神通": "sec_buff",
        "身法": "effect1_buff",
        "瞳术": "effect2_buff"
    }
    
    if not arg or arg not in type_map:
        msg = "可取回类型：主功法 / 辅修 / 神通 / 身法 / 瞳术\n示例：回忆前世 主功法"
        await handle_send(bot, event, msg)
        await handle_send(bot, event, msg, md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")
        return
    
    skill_type = type_map[arg]
    success, reason = retrieve_reincarnation_skill(user_id, skill_type)
    
    await handle_send(bot, event, reason, md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")

@view_memory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
    
    user_id = user_info['user_id']
    memory = get_reincarnation_memory(user_id)
    
    if not memory:
        await handle_send(bot, event, "你没有任何轮回印记。")
        return
    
    lines = ["你的轮回印记："]
    
    type_names = {
        "main_buff": "主功法",
        "sub_buff": "辅修功法",
        "sec_buff": "神通",
        "effect1_buff": "身法",
        "effect2_buff": "瞳术"
    }
    
    for key, name in type_names.items():
        skill_id = memory.get(key, 0)
        retrieved = memory["retrieved"].get(key.replace("_buff", ""), False)
        
        if skill_id == 0:
            lines.append(f"  {name}：无（前世未修炼）")
        elif retrieved:
            lines.append(f"  {name}：{items.get_data_by_item_id(skill_id)['name']}（已取回）")
        else:
            lines.append(f"  {name}：{items.get_data_by_item_id(skill_id)['name']}")
    
    await handle_send(bot, event, "\n".join(lines), md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")

@confirm_lunhui.handle(parameterless=[Cooldown(cd_time=1.4)])
async def confirm_lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理确认轮回"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await confirm_lunhui.finish()

    user_id = user_info['user_id']
    if str(user_id) not in confirm_lunhui_cache:
        msg = "没有待处理的轮回！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await confirm_lunhui.finish()

    confirm_data = confirm_lunhui_cache[str(user_id)]
    root_level = confirm_data['root_level']
    original_msg = confirm_data['msg']
    impart_data_draw = await impart_check(user_id) 
    impaer_exp_time = impart_data_draw["exp_day"] if impart_data_draw is not None else 0 

    # 执行轮回操作
    user_msg = sql_message.get_user_info_with_id(user_id)
    user_name = user_msg['user_name']
    exp = user_msg['exp']
    stone = user_info['stone']
    now_stone = int(stone - 1_0000_0000)
    if now_stone >= 0:
        sql_message.update_ls(user_id, now_stone, 2)
        # 重置用户灵石（保留1亿）
    save_reincarnation_memory(user_id)
    # 记录轮回印记
    sql_message.updata_level(user_id, '江湖好手')  
    # 重置用户境界
    sql_message.update_levelrate(user_id, 0)  
    # 重置突破成功率
    sql_message.update_j_exp(user_id, exp)
    sql_message.update_exp(user_id, 100)
    # 重置用户修为
    sql_message.update_user_hp(user_id)  
    # 重置用户HP，mp，atk状态
    sql_message.updata_user_main_buff(user_id, 0)  
    # 重置用户主功法
    sql_message.updata_user_sub_buff(user_id, 0)  
    # 重置用户辅修功法
    sql_message.updata_user_sec_buff(user_id, 0)  
    # 重置用户神通
    sql_message.updata_user_effect1_buff(user_id, 0)  
    # 重置用户身法
    sql_message.updata_user_effect2_buff(user_id, 0)  
    # 重置用户瞳术
    sql_message.reset_user_drug_resistance(user_id)  
    # 重置用户耐药性
    xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
    # 重置用户虚神界修炼时间
    xiuxian_impart.convert_stone_to_wishing_stone(user_id)
    # 转换思恋结晶
    if root_level != 0:
        sql_message.update_user_atkpractice(user_id, 0) #重置用户攻修等级
        sql_message.update_user_hppractice(user_id, 0) #重置用户元血等级
        sql_message.update_user_mppractice(user_id, 0) #重置用户灵海等级
        sql_message.update_root(user_id, root_level)  # 更换灵根
    if root_level == 0 or root_level == 9:
        sql_message.updata_root_level(user_id, 1)  # 更新轮回等级
    msg = f"{original_msg}！"
    await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="印记", v3="轮回印记")

    # 删除确认缓存
    del confirm_lunhui_cache[str(user_id)]
    await confirm_lunhui.finish()

async def confirm_lunhui_invite(bot, event, user_id, root_level, lunhui_level2, msg):
    """发送确认轮回"""
    invite_id = f"{user_id}_lunhui_{datetime.now().timestamp()}"
    confirm_lunhui_cache[str(user_id)] = {
        'root_level': root_level,
        'msg': msg,
        'invite_id': invite_id
    }

    # 设置60秒过期
    asyncio.create_task(expire_confirm_lunhui_invite(user_id, invite_id, bot, event))

    msg = f"您即将进入【{lunhui_level2}】，请在60秒内确认！\n发送【确认轮回】以继续，或等待60秒后自动取消。"
    await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")

async def expire_confirm_lunhui_invite(user_id, invite_id, bot, event):
    """确认轮回过期处理"""
    await asyncio.sleep(60)
    if str(user_id) in confirm_lunhui_cache and confirm_lunhui_cache[str(user_id)]['invite_id'] == invite_id:
        del confirm_lunhui_cache[str(user_id)]
        msg = "确认轮回已过期！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")

def save_reincarnation_memory(user_id):
    """轮回时保存当前功法/技能记忆"""
    buff = UserBuffDate(user_id).BuffInfo
    
    memory = {
        "main_buff":    buff.get('main_buff', 0),
        "sub_buff":     buff.get('sub_buff', 0),
        "sec_buff":     buff.get('sec_buff', 0),
        "effect1_buff": buff.get('effect1_buff', 0),
        "effect2_buff": buff.get('effect2_buff', 0),
        # 记录时的境界，用于后续判断可取回的最低境界
        "memory_level": sql_message.get_user_info_with_id(user_id)['level'],
        # 已取回的标记（每种技能只能取一次）
        "retrieved_main": 0,
        "retrieved_sub": 0,
        "retrieved_sec": 0,
        "retrieved_effect1": 0,
        "retrieved_effect2": 0
    }
    
    # 字段化存储，每个属性独立存储
    for field, value in memory.items():
        player_data_manager.update_or_write_data(
            str(user_id),
            "reincarnation_memory",
            field,
            value,
            data_type="TEXT" if field == "memory_level" else "INTEGER"
        )
    
    logger.info(f"[轮回印记] 用户 {user_id} 轮回记忆已保存")
    return memory


def get_reincarnation_memory(user_id):
    """读取轮回印记"""
    data = player_data_manager.get_fields(str(user_id), "reincarnation_memory")
    if not data:
        return None
    
    memory = {
        "main_buff": data.get("main_buff", 0),
        "sub_buff": data.get("sub_buff", 0),
        "sec_buff": data.get("sec_buff", 0),
        "effect1_buff": data.get("effect1_buff", 0),
        "effect2_buff": data.get("effect2_buff", 0),
        "memory_level": data.get("memory_level", ""),
        "retrieved": {
            "main": bool(data.get("retrieved_main", 0)),
            "sub": bool(data.get("retrieved_sub", 0)),
            "sec": bool(data.get("retrieved_sec", 0)),
            "effect1": bool(data.get("retrieved_effect1", 0)),
            "effect2": bool(data.get("retrieved_effect2", 0))
        }
    }
    
    return memory


def can_retrieve_skill(user_id, skill_type):
    """
    判断某类技能是否可以取回
    返回 (can_retrieve: bool, reason: str, required_level_name: str or None)
    """
    memory = get_reincarnation_memory(user_id)
    if not memory:
        return False, "你没有任何轮回印记", None
    
    # 检查是否已取回过
    retrieved_key = {
        "main_buff": "main",
        "sub_buff": "sub",
        "sec_buff": "sec",
        "effect1_buff": "effect1",
        "effect2_buff": "effect2"
    }.get(skill_type)
    
    if not retrieved_key:
        return False, "无效的技能类型", None
        
    if memory["retrieved"].get(retrieved_key, False):
        return False, "该类技能的轮回印记已被取回过", None
    
    # 获取轮回前的境界
    old_level = memory.get("memory_level")
    if not old_level:
        return False, "轮回记忆数据异常", None
    
    # 计算可取回的最低境界（前 9 阶）
    rank_list = convert_rank("江湖好手")[1]  # 所有境界列表
    try:
        old_rank_idx = rank_list.index(old_level)
        min_rank_idx = max(0, old_rank_idx - 9)
        min_level_name = rank_list[min_rank_idx]
    except ValueError:
        return False, "轮回前境界数据异常", None
    
    # 当前境界
    current_level = sql_message.get_user_info_with_id(user_id)['level']
    current_rank_idx = rank_list.index(current_level) if current_level in rank_list else -1
    
    if current_rank_idx < min_rank_idx:
        return False, f"境界不足，需要达到{min_level_name}才能取回该记忆", min_level_name
    
    # 记忆中是否有该技能
    skill_id = memory.get(skill_type, 0)
    if skill_id == 0:
        return False, f"轮回记忆中没有记录{skill_type}相关技能", None
    
    return True, "可取回", None


def retrieve_reincarnation_skill(user_id, skill_type):
    """
    执行取回某类轮回技能
    返回 (success: bool, msg: str)
    """
    can, reason, _ = can_retrieve_skill(user_id, skill_type)
    if not can:
        return False, reason
    
    memory = get_reincarnation_memory(user_id)
    skill_id = memory.get(skill_type, 0)
    if skill_id == 0:
        return False, "记忆中没有该技能"
    
    # 标记已取回
    retrieved_field_map = {
        "main_buff": "retrieved_main",
        "sub_buff": "retrieved_sub",
        "sec_buff": "retrieved_sec",
        "effect1_buff": "retrieved_effect1",
        "effect2_buff": "retrieved_effect2"
    }
    
    retrieved_field = retrieved_field_map.get(skill_type)
    if retrieved_field:
        player_data_manager.update_or_write_data(
            str(user_id),
            "reincarnation_memory",
            retrieved_field,
            1,
            data_type="INTEGER"
        )
    
    # 写入当前 Buff
    update_func_map = {
        "main_buff": sql_message.updata_user_main_buff,
        "sub_buff": sql_message.updata_user_sub_buff,
        "sec_buff": sql_message.updata_user_sec_buff,
        "effect1_buff": sql_message.updata_user_effect1_buff,
        "effect2_buff": sql_message.updata_user_effect2_buff
    }
    
    update_func = update_func_map.get(skill_type)
    if update_func:
        update_func(user_id, skill_id)
    
    skill_name = items.get_data_by_item_id(skill_id).get('name', '未知技能')
    return True, f"成功回忆前世中的技能：{skill_name}"