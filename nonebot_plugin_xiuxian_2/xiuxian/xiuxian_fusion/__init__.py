from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from nonebot.params import CommandArg
from nonebot import on_command
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank
from ..xiuxian_back.back_util import get_item_msg
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, number_to, handle_send
)
import random

items = Items()
sql_message = XiuxianDateManage()

# 合成必定成功ID列表
FIXED_SUCCESS_IDS = [7084, 1002, 1003]

fusion_help_text = f"""
合成帮助:
1.合成 物品名:合成指定的物品。
2.查看可合成物品 [物品名可选] 可以查看当前可合成的所有物品以及相关信息。
""".strip()

# 合成函数
async def general_fusion(user_id, equipment_id, equipment):
    """
    合成函数
    :param user_id: 用户ID
    :param equipment_id: 装备ID
    :param equipment: 装备信息
    :return: (成功与否, 消息)
    """
    user_info = sql_message.get_user_info_with_id(user_id)
    back_msg = sql_message.get_back_msg(user_id)
    
    fusion_info = equipment.get('fusion', None)
    if not fusion_info:
        return False, f"{equipment['name']} 不是一件可以合成的物品！"
    
    # 检查限制
    limit = fusion_info.get('limit', None)
    if limit is not None:
        current_amount = 0
        for back in back_msg:
            if back['goods_id'] == int(equipment_id):
                current_amount = back['goods_num']
                break
        if current_amount >= limit:
            return False, f"道友的背包中已有足够数量的 {equipment['name']}，无法再次合成！"
    
    # 检查境界
    required_rank = fusion_info.get('need_rank', '江湖好手')
    if convert_rank(user_info['level'])[0] > convert_rank(required_rank)[0]:
        return False, f"道友的境界不足，合成 {equipment['name']} 需要达到 {required_rank}！"
    
    # 检查修为
    if user_info['exp'] < int(fusion_info.get('need_exp', 0)):
        return False, f"道友的修为不足，合成 {equipment['name']} 需要修为 {int(fusion_info.get('need_exp', 0))}！"
    
    # 检查灵石
    if user_info['stone'] < int(fusion_info.get('need_stone', 0)):
        return False, f"道友的灵石不足，合成 {equipment['name']} 需要 {number_to(int(fusion_info.get('need_stone', 0)))} 枚灵石呢！"
    
    # 检查材料
    needed_items = fusion_info.get('need_item', {})
    missing_items = []
    for item_id, amount_needed in needed_items.items():
        total_amount = 0
        for back in back_msg:
            if back['goods_id'] == int(item_id):
                total_amount += back['goods_num']
        if total_amount < amount_needed:
            missing_items.append((item_id, amount_needed - total_amount))
    
    if missing_items:
        missing_names = [f"{amount_needed} 个 {items.get_data_by_item_id(int(item_id))['name']}" for item_id, amount_needed in missing_items]
        return False, "道友还缺少：\n" + "\n".join(missing_names)
    
    # 检查是否必定成功
    if int(equipment_id) in FIXED_SUCCESS_IDS:
        # 必定成功，直接扣除材料并添加物品
        sql_message.update_ls(user_id, int(fusion_info.get('need_stone', 0)), 2)  # 扣灵石
        for item_id, amount_needed in needed_items.items():
            sql_message.update_back_j(user_id, int(item_id), amount_needed)  # 扣道具
        
        sql_message.send_back(user_id, int(equipment_id), equipment['name'], equipment['type'], 1, 1)
        
        item_type = equipment.get('type', '物品')
        return True, f"道友成功合成了{item_type}: {equipment['name']}！！"
    
    # 概率合成（30%成功率）
    roll = random.randint(1, 100)
    
    if roll <= 30:
        # 成功，扣除材料并添加物品
        sql_message.update_ls(user_id, int(fusion_info.get('need_stone', 0)), 2)  # 扣灵石
        for item_id, amount_needed in needed_items.items():
            sql_message.update_back_j(user_id, int(item_id), amount_needed)  # 扣道具
        
        sql_message.send_back(user_id, int(equipment_id), equipment['name'], equipment['type'], 1, 1)
        
        item_type = equipment.get('type', '物品')
        return True, f"道友成功合成了{item_type}: {equipment['name']}！！"
    else:
        # 失败，检查是否有福缘石
        has_protection = False
        for back in back_msg:
            if back['goods_id'] == 20006 and back['goods_num'] > 0:
                has_protection = True
                # 使用一个福缘石
                sql_message.update_back_j(user_id, 20006, 1)
                break
        
        if has_protection:
            return False, f"合成失败！幸好使用了福缘石，材料没有损失。"
        else:
            # 没有福缘石，扣除材料
            sql_message.update_ls(user_id, int(fusion_info.get('need_stone', 0)), 2)  # 扣灵石
            for item_id, amount_needed in needed_items.items():
                sql_message.update_back_j(user_id, int(item_id), amount_needed)  # 扣道具
            
            return False, f"合成失败！材料已消耗。"

# 命令处理器
fusion = on_command('合成', priority=15, block=True)
force_fusion = on_command('强行合成', priority=15, block=True)
fusion_help = on_command("合成帮助", priority=15, block=True)
available_fusion = on_command('查看可合成物品', priority=15, block=True)

@fusion_help.handle(parameterless=[Cooldown(at_sender=False)])
async def fusion_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = fusion_help_text
    await handle_send(bot, event, msg)
    await fusion_help.finish()

@fusion.handle(parameterless=[Cooldown(at_sender=False)])
async def fusion_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await fusion.finish()

    user_id = user_info['user_id']
    args_str = args.extract_plain_text().strip()
    
    if not args_str:
        msg = fusion_help_text
        await handle_send(bot, event, msg)
        await fusion.finish()

    equipment_id, equipment = items.get_data_by_item_name(args_str)
    if equipment is None:
        msg = f"未找到可合成的物品：{args_str}"
        await handle_send(bot, event, msg)
        await fusion.finish()
    
    # 检查是否是必定成功ID，如果是则跳过福缘石检测
    if int(equipment_id) not in FIXED_SUCCESS_IDS:
        # 检查是否有福缘石
        back_msg = sql_message.get_back_msg(user_id)
        has_protection = False
        for back in back_msg:
            if back['goods_id'] == 20006 and back['goods_num'] > 0:
                has_protection = True
                break
        
        if not has_protection:
            msg = "道友没有福缘石，合成失败可能会损失材料！\n使用【强行合成】命令确认操作。"
            await handle_send(bot, event, msg)
            await fusion.finish()
    
    success, msg = await general_fusion(user_id, equipment_id, equipment)
    await handle_send(bot, event, msg)
    await fusion.finish()

@force_fusion.handle(parameterless=[Cooldown(at_sender=False)])
async def force_fusion_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await force_fusion.finish()

    user_id = user_info['user_id']
    args_str = args.extract_plain_text().strip()
    
    if not args_str:
        msg = fusion_help_text
        await handle_send(bot, event, msg)
        await fusion.finish()

    equipment_id, equipment = items.get_data_by_item_name(args_str)
    if equipment is None:
        msg = f"未找到可合成的物品：{args_str}"
        await handle_send(bot, event, msg)
        await force_fusion.finish()
    
    success, msg = await general_fusion(user_id, equipment_id, equipment)
    await handle_send(bot, event, msg)
    await force_fusion.finish()

@available_fusion.handle(parameterless=[Cooldown(at_sender=False)])
async def available_fusion_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args_str = args.extract_plain_text().strip()

    if args_str:
        equipment_id, equipment = items.get_data_by_item_name(args_str)
        if equipment and 'fusion' in equipment:
            msg = get_item_msg(int(equipment_id))
        else:
            msg = f"未找到可合成的物品：{args_str}"
    else:
        fusion_items = items.get_fusion_items()
        if not fusion_items:
            msg = "目前没有可合成的物品。"
        else:
            msg = "可合成的物品如下：\n" + "\n".join(fusion_items)

    await handle_send(bot, event, msg)
    await available_fusion.finish()
