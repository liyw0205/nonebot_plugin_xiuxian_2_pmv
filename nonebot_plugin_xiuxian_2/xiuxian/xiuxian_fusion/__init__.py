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
FIXED_SUCCESS_IDS = [7084]

fusion = on_command('合成', priority=15, block=True)
force_fusion = on_command('强行合成', priority=15, block=True)
fusion_help = on_command("合成帮助", priority=15, block=True)
available_fusion = on_command('查看可合成物品', aliases={"查看合成"}, priority=15, block=True)

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
    
    # 获取所有可合成物品
    all_fusion_items = []
    for item_id, item_info in items.items.items():
        if 'fusion' in item_info:
            all_fusion_items.append({
                'id': item_id,
                'name': item_info['name'],
                'type': item_info.get('type', '未知'),
                'item_type': item_info.get('item_type', '未知'),
                'info': item_info
            })
    
    if not all_fusion_items:
        msg = "目前没有可合成的物品。"
        await handle_send(bot, event, msg)
        await available_fusion.finish()
    
    # 检查参数是否为数字（分页请求）
    is_page_request = args_str.isdigit()
    page_num = int(args_str) if is_page_request else 1
    
    # 无参数或参数为数字：显示分页列表
    if not args_str or is_page_request:
        # 按类型分组，优先使用item_type，不存在时使用type
        items_by_type = {}
        for item in all_fusion_items:
            # 优先使用item_type，不存在时使用type
            category = item['item_type'] if item['item_type'] != '未知' else item['type']
            if category not in items_by_type:
                items_by_type[category] = []
            items_by_type[category].append(item)
        
        # 按类型名称排序
        sorted_categories = sorted(items_by_type.keys())
        total_pages = (len(sorted_categories) + 1) // 2  # 每页2个类型
        
        # 检查请求的页数是否有效
        if page_num < 1 or page_num > total_pages:
            msg = f"页码无效，请输入1-{total_pages}之间的数字"
            await handle_send(bot, event, msg)
            await available_fusion.finish()
        
        # 获取当前页的类型
        start_idx = (page_num - 1) * 2
        end_idx = start_idx + 2
        current_categories = sorted_categories[start_idx:end_idx]
        
        # 构建消息
        msg_parts = [f"☆------(第{page_num}/{total_pages}页)------☆"]
        
        for category in current_categories:
            msg_parts.append(f"\n【{category}】")
            for item in items_by_type[category]:
                msg_parts.append(f"• {item['name']}")
        
        msg_parts.append("\n【查看可合成物品 页数】")
        msg_parts.append("【查看可合成物品 物品名/类型】")
        
        await handle_send(bot, event, "\n".join(msg_parts))
        await available_fusion.finish()
    
    # 有参数且不是数字：匹配物品名、类型和物品类型
    matched_items = []
    for item in all_fusion_items:
        # 检查是否匹配物品名、类型或物品类型
        if (args_str.lower() in item['name'].lower() or 
            args_str.lower() == item['type'].lower() or 
            args_str.lower() == item['item_type'].lower()):
            matched_items.append(item)
    
    if not matched_items:
        msg = f"未找到匹配【{args_str}】的可合成物品"
        await handle_send(bot, event, msg)
        await available_fusion.finish()
    
    # 如果匹配的是类型，显示该类型下的所有物品
    if any(args_str.lower() == item['type'].lower() or 
           args_str.lower() == item['item_type'].lower() 
           for item in matched_items):
        # 构建类型筛选结果消息
        type_name = args_str
        msg_parts = [f"☆------【{type_name}】------☆"]
        
        for item in matched_items:
            fusion_info = item['info']['fusion']            
            msg_parts.append(f"\n• {item['name']}")
        
        msg_parts.append("\n【查看可合成物品 物品名】")
        
        await handle_send(bot, event, "\n".join(msg_parts))
        await available_fusion.finish()
    
    # 如果匹配多个物品名，显示列表
    if len(matched_items) > 1:
        msg_parts = [f"☆------找到多个匹配【{args_str}】的物品------☆"]
        for item in matched_items:
            msg_parts.append(f"• {item['name']}")
        msg_parts.append("\n请使用更精确的名称查看详细信息")
        
        await handle_send(bot, event, "\n".join(msg_parts))
        await available_fusion.finish()
    
    # 显示单个物品的详细信息
    item = matched_items[0]
    item_id = item['id']
    fusion_info = item['info']['fusion']
    
    # 构建详细信息
    msg_parts = [f"☆------{item['name']} 合成信息------☆"]
    msg_parts.append(f"物品ID: {item_id}")
    msg_parts.append(f"类型: {item['type']}")
    if item.get('item_type') and item['item_type'] != '未知':
        msg_parts.append(f"物品类型: {item['item_type']}")
    
    # 合成要求
    msg_parts.append("\n【合成要求】")
    need_rank = fusion_info.get('need_rank', '无要求')
    need_exp = number_to(int(fusion_info.get('need_exp', 0)))
    need_stone = number_to(int(fusion_info.get('need_stone', 0)))
    
    msg_parts.append(f"境界: {need_rank}")
    if int(fusion_info.get('need_exp', 0)) > 0:
        msg_parts.append(f"修为: {need_exp}")
    if int(fusion_info.get('need_stone', 0)) > 0:
        msg_parts.append(f"灵石: {need_stone}")
    
    # 材料要求
    need_items = fusion_info.get('need_item', {})
    if need_items:
        msg_parts.append("\n【所需材料】")
        for material_id, amount in need_items.items():
            material_info = items.get_data_by_item_id(int(material_id))
            if material_info:
                msg_parts.append(f"• {material_info['name']} x{amount}")
    
    # 数量限制
    limit = fusion_info.get('limit')
    if limit:
        msg_parts.append(f"\n【数量限制】")
        msg_parts.append(f"最多可合成: {limit}个")
    
    # 成功率信息
    if int(item_id) in FIXED_SUCCESS_IDS:
        msg_parts.append("\n【成功率】")
        msg_parts.append("必定成功")
    else:
        msg_parts.append("\n【成功率】")
        msg_parts.append("30%成功率")
        msg_parts.append("(失败会消耗材料，使用福缘石可避免损失)")
    
    await handle_send(bot, event, "\n".join(msg_parts))
    await available_fusion.finish()

