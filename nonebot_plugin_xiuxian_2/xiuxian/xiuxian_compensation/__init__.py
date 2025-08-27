import random
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from nonebot import on_command, require
from nonebot.adapters.onebot.v11 import (
    Bot,
    Message,
    MessageEvent,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.log import logger

from ..xiuxian_utils.lay_out import assign_bot, Cooldown, CooldownIsolateLevel
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.utils import (
    check_user,
    Txt2Img,
    get_msg_pic,
    CommandObjectID,
    handle_send,
    number_to
)

items = Items()
sql_message = XiuxianDateManage()  # sql类

# 补偿系统文件路径
COMPENSATION_DATA_PATH = Path(__file__).parent / "compensation_data"
COMPENSATION_RECORDS_PATH = COMPENSATION_DATA_PATH / "compensation_records.json"
COMPENSATION_CLAIMED_PATH = COMPENSATION_DATA_PATH / "claimed_records.json"

# 确保目录存在
COMPENSATION_DATA_PATH.mkdir(exist_ok=True)

# 初始化补偿记录文件
if not COMPENSATION_RECORDS_PATH.exists():
    with open(COMPENSATION_RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

# 初始化领取记录文件
if not COMPENSATION_CLAIMED_PATH.exists():
    with open(COMPENSATION_CLAIMED_PATH, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

# 礼包系统文件路径
GIFT_PACKAGE_DATA_PATH = Path(__file__).parent / "gift_package_data"
GIFT_PACKAGE_RECORDS_PATH = GIFT_PACKAGE_DATA_PATH / "gift_package_records.json"
GIFT_PACKAGE_CLAIMED_PATH = GIFT_PACKAGE_DATA_PATH / "claimed_gift_packages.json"

# 确保目录存在
GIFT_PACKAGE_DATA_PATH.mkdir(exist_ok=True)

# 初始化礼包记录文件
if not GIFT_PACKAGE_RECORDS_PATH.exists():
    with open(GIFT_PACKAGE_RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

# 初始化领取记录文件
if not GIFT_PACKAGE_CLAIMED_PATH.exists():
    with open(GIFT_PACKAGE_CLAIMED_PATH, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

def load_compensation_data() -> Dict[str, dict]:
    """加载补偿数据"""
    with open(COMPENSATION_RECORDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_compensation_data(data: Dict[str, dict]):
    """保存补偿数据"""
    with open(COMPENSATION_RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_claimed_data() -> Dict[str, List[str]]:
    """加载领取记录"""
    with open(COMPENSATION_CLAIMED_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_claimed_data(data: Dict[str, List[str]]):
    """保存领取记录"""
    with open(COMPENSATION_CLAIMED_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_gift_package_data() -> Dict[str, dict]:
    """加载礼包数据"""
    with open(GIFT_PACKAGE_RECORDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_gift_package_data(data: Dict[str, dict]):
    """保存礼包数据"""
    with open(GIFT_PACKAGE_RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_claimed_gift_packages() -> Dict[str, List[str]]:
    """加载礼包领取记录"""
    with open(GIFT_PACKAGE_CLAIMED_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_claimed_gift_packages(data: Dict[str, List[str]]):
    """保存礼包领取记录"""
    with open(GIFT_PACKAGE_CLAIMED_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def parse_duration(duration_str: str) -> timedelta:
    """解析时间持续时间字符串 (支持多种格式)
    支持的格式:
    - xx天/xx小时 (原有格式)
    - yymmdd (6位数字，如257011表示2025年7月11日23:59:59)
    """
    try:
        # 尝试解析为6位数字日期 (yymmdd)
        if duration_str.isdigit() and len(duration_str) == 6:
            year = int("20" + duration_str[:2])  # 25 -> 2025
            month = int(duration_str[2:4])
            day = int(duration_str[4:6])
            expire_time = datetime(year, month, day).replace(hour=23, minute=59, second=59)
            return expire_time - datetime.now()
        elif "天" in duration_str:  # xx天
            days = int(duration_str.split("天")[0])
            return timedelta(days=days)
        elif "小时" in duration_str:  # xx小时
            hours = int(duration_str.split("小时")[0])
            return timedelta(hours=hours)
        else:
            raise ValueError("无效的时间格式")
    except Exception as e:
        raise ValueError(f"时间格式错误: {str(e)}")

def add_compensation(compensation_id: str, duration_str: str, items_str: str, reason: str):
    """新增补偿
    :param compensation_id: 补偿ID
    :param duration_str: 持续时间字符串
    :param items_str: 物品字符串
    :param reason: 发放原因
    """
    data = load_compensation_data()
    if compensation_id in data:
        raise ValueError(f"补偿ID {compensation_id} 已存在")
    
    try:
        duration = parse_duration(duration_str)
        expire_time = (datetime.now() + duration).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        raise ValueError(f"时间格式错误: {str(e)}")
    
    # 解析物品字符串
    items_list = []
    for item_part in items_str.split(','):
        item_part = item_part.strip()
        if 'x' in item_part:
            item_id_or_name, quantity = item_part.split('x', 1)
            quantity = int(quantity)
        else:
            item_id_or_name = item_part
            quantity = 1
        
        # 处理灵石特殊物品
        if item_id_or_name == "灵石":
            items_list.append({
                "type": "stone",
                "id": "stone",
                "name": "灵石",
                "quantity": quantity if quantity > 0 else 1000000,  # 默认100万
                "desc": f"获得 {number_to(quantity if quantity > 0 else 1000000)} 灵石"
            })
            continue
        
        # 尝试转换为物品ID
        goods_id = None
        if item_id_or_name.isdigit():  # 如果是数字，直接作为ID
            goods_id = int(item_id_or_name)
            item_info = items.get_data_by_item_id(goods_id)
            if not item_info:
                raise ValueError(f"物品ID {goods_id} 不存在")
        else:  # 否则作为物品名称查找
            for k, v in items.items.items():
                if item_id_or_name == v['name']:
                    goods_id = k
                    break
            if not goods_id:
                raise ValueError(f"物品 {item_id_or_name} 不存在")
        
        item_info = items.get_data_by_item_id(goods_id)
        items_list.append({
            "type": item_info['type'],
            "id": goods_id,
            "name": item_info['name'],
            "quantity": quantity,
            "desc": item_info['desc']
        })
    
    if not items_list:
        raise ValueError("未指定有效的补偿物品")
    
    data[compensation_id] = {
        "items": items_list,
        "reason": reason,
        "expire_time": expire_time,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    save_compensation_data(data)
    return True

def get_compensation_info(compensation_id: str) -> Optional[dict]:
    """获取补偿信息"""
    data = load_compensation_data()
    return data.get(compensation_id)

def is_compensation_expired(compensation_info: dict) -> bool:
    """检查补偿是否过期"""
    expire_time = datetime.strptime(compensation_info["expire_time"], "%Y-%m-%d %H:%M:%S")
    return datetime.now() > expire_time

def has_claimed_compensation(user_id: str, compensation_id: str) -> bool:
    """检查是否已领取补偿"""
    claimed_data = load_claimed_data()
    return compensation_id in claimed_data.get(user_id, [])

async def claim_compensation(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, user_id: str, compensation_id: str) -> bool:
    """领取补偿"""
    # 检查补偿是否存在
    compensation_info = get_compensation_info(compensation_id)
    if not compensation_info:
        return False
    
    # 检查是否已过期
    if is_compensation_expired(compensation_info):
        return False
    
    # 检查是否已领取
    if has_claimed_compensation(user_id, compensation_id):
        return False
    
    msg_parts = [f"成功领取补偿 {compensation_id}:"]
    
    # 发放所有补偿物品
    for item in compensation_info["items"]:
        if item["type"] == "stone":  # 灵石特殊处理
            sql_message.update_ls(user_id, item["quantity"], 1)
            msg_parts.append(f"获得灵石 {number_to(item['quantity'])} 枚")
        else:
            # 使用补偿信息中的物品ID
            goods_id = item["id"]
            goods_name = item["name"]
            goods_type = item["type"]
            quantity = item["quantity"]
            
            # 处理物品类型
            if goods_type in ["辅修功法", "神通", "功法", "身法", "瞳术"]:
                goods_type_item = "技能"
            elif goods_type in ["法器", "防具"]:
                goods_type_item = "装备"
            else:
                goods_type_item = goods_type
            
            # 发放物品
            sql_message.send_back(
                user_id,
                goods_id,
                goods_name,
                goods_type_item,
                quantity,
                1  # 非绑定
            )
            msg_parts.append(f"获得 {goods_name} x{quantity}")
    
    msg = "\n".join(msg_parts)
    await handle_send(bot, event, msg)
    
    # 记录领取状态
    claimed_data = load_claimed_data()
    if user_id not in claimed_data:
        claimed_data[user_id] = []
    claimed_data[user_id].append(compensation_id)
    save_claimed_data(claimed_data)
    return True

# 命令处理器
add_compensation_cmd = on_command("新增补偿", permission=SUPERUSER, priority=5, block=True)
delete_compensation_cmd = on_command("删除补偿", permission=SUPERUSER, priority=5, block=True)
list_compensation_cmd = on_command("补偿列表", priority=5, block=True)
claim_compensation_cmd = on_command("领取补偿", priority=5, block=True)
compensation_help_cmd = on_command("补偿", priority=7, block=True)
compensation_admin_help_cmd = on_command("补偿管理", permission=SUPERUSER, priority=5, block=True)

__compensation_help__ = f"""
⚖️ 补偿帮助 ⚖️
═════════════
1. 补偿列表 - 查看所有可领取补偿
2. 领取补偿 [ID] - 领取指定补偿

【注意事项】
- 每个补偿每人限领一次
- 过期补偿将无法领取
═════════════
当前时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

__compensation_admin_help__ = f"""
⚖️ 补偿管理 ⚖️ 
═════════════
1. 新增补偿 [ID] [时间] [物品] [原因]
   - 示例: 新增补偿 comp_001 3天 1001x1,灵石x500000 维护补偿

2. 删除补偿 [ID] - 删除指定补偿

3. 补偿列表 - 查看所有补偿(含过期)

【参数说明】
- 时间: 如"3天"或"48小时"
- 物品: 物品ID或名称,可带数量
   - 示例1: 1001,1002
   - 示例2: 灵石x500000
   - 示例3: 渡厄丹x1,两仪心经x1

【注意事项】
- 补偿ID必须唯一
- 删除操作不可逆
═════════════
当前服务器时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

@compensation_help_cmd.handle(parameterless=[Cooldown(at_sender=False)])
async def handle_compensation_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """补偿帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if XiuConfig().img:
        pic = await get_msg_pic(__compensation_help__)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, __compensation_help__)
    await compensation_help_cmd.finish()

@compensation_admin_help_cmd.handle(parameterless=[Cooldown(at_sender=False)])
async def handle_compensation_admin_help(bot: Bot, event: MessageEvent):
    """补偿管理"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if XiuConfig().img:
        pic = await get_msg_pic(__compensation_admin_help__)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, __compensation_admin_help__)
    await compensation_admin_help_cmd.finish()

@list_compensation_cmd.handle()
async def handle_list_compensation(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """列出所有补偿（合并版）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    data = load_compensation_data()
    if not data:
        msg = "当前没有可用的补偿"
        await handle_send(bot, event, msg)
        return
    
    # 构建消息内容
    msg_lines = [
        "📋 补偿列表 📋",
        "====================",
        "【有效补偿】"
    ]
    
    # 先显示有效补偿
    valid_comps = []
    expired_comps = []
    
    for comp_id, info in data.items():
        expired = is_compensation_expired(info)
        if expired:
            expired_comps.append((comp_id, info))
        else:
            valid_comps.append((comp_id, info))
    
    if not valid_comps and not expired_comps:
        msg_lines.append("暂无任何补偿")
    else:
        # 有效补偿
        if valid_comps:
            for comp_id, info in valid_comps:
                items_msg = []
                for item in info["items"]:
                    if item["type"] == "stone":
                        items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                    else:
                        items_msg.append(f"{item['name']}(ID:{item['id']}) x{item['quantity']}")
                
                msg_lines.extend([
                    f"🆔 补偿ID: {comp_id}",
                    f"📝 原因: {info['reason']}",
                    f"📦 补偿内容: {', '.join(items_msg)}",
                    f"⏰ 有效期至: {info['expire_time']}",
                    f"🕒 创建时间: {info['create_time']}",
                    "------------------"
                ])
        else:
            msg_lines.append("暂无有效补偿")
        
        # 过期补偿
        msg_lines.append("\n【过期补偿】")
        if expired_comps:
            for comp_id, info in expired_comps:
                items_msg = []
                for item in info["items"]:
                    if item["type"] == "stone":
                        items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                    else:
                        items_msg.append(f"{item['name']}(ID:{item['id']}) x{item['quantity']}")
                
                msg_lines.extend([
                    f"🆔 补偿ID: {comp_id}",
                    f"📝 原因: {info['reason']}",
                    f"📦 补偿内容: {', '.join(items_msg)}",
                    f"⏰ 过期时间: {info['expire_time']}",
                    f"🕒 创建时间: {info['create_time']}",
                    "------------------"
                ])
        else:
            msg_lines.append("暂无过期补偿")
    
    # 添加服务器时间信息
    msg_lines.append(f"\n⏱️ 当前服务器时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 发送合并后的消息
    msg = "\n".join(msg_lines)
    if XiuConfig().img:
        pic = await get_msg_pic(msg)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, msg)

@add_compensation_cmd.handle()
async def handle_add_compensation(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    """新增补偿命令处理"""
    try:
        # 新格式: 补偿ID 时间 物品 补偿原因
        arg_str = args.extract_plain_text().strip()
        parts = arg_str.split(maxsplit=3)
        if len(parts) < 4:
            raise ValueError("参数不足，格式应为: 补偿ID 时间 物品 补偿原因")
        
        comp_id, duration, items_str, reason = parts
        
        if add_compensation(comp_id, duration, items_str, reason):
            # 获取补偿详情用于显示
            comp_info = get_compensation_info(comp_id)
            items_msg = []
            for item in comp_info["items"]:
                if item["type"] == "stone":
                    items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                else:
                    items_msg.append(f"{item['name']}(ID:{item['id']}) x{item['quantity']}")
            
            msg = f"成功新增补偿 {comp_id}\n"
            msg += f"物品: {', '.join(items_msg)}\n"
            msg += f"原因: {reason}\n"
            msg += f"有效期: {duration}"
            await handle_send(bot, event, msg)
        else:
            await handle_send(bot, event, "新增补偿失败")
    except Exception as e:
        await handle_send(bot, event, f"新增补偿出错: {str(e)}")

@claim_compensation_cmd.handle()
async def handle_claim_compensation(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """领取补偿命令处理"""
    user_id = event.get_user_id()
    comp_id = args.extract_plain_text().strip()
    
    if not comp_id:
        await handle_send(bot, event, "请指定要领取的补偿ID")
        return
    
    comp_info = get_compensation_info(comp_id)
    if not comp_info:
        await handle_send(bot, event, f"补偿ID {comp_id} 不存在")
        return
    
    if is_compensation_expired(comp_info):
        await handle_send(bot, event, f"补偿 {comp_id} 已过期，无法领取")
        return
    
    if has_claimed_compensation(user_id, comp_id):
        await handle_send(bot, event, f"您已经领取过补偿 {comp_id} 了")
        return
    
    if await claim_compensation(bot, event, user_id, comp_id):
        pass  # 消息已在claim_compensation中发送
    else:
        await handle_send(bot, event, "领取补偿失败")

@delete_compensation_cmd.handle()
async def handle_delete_compensation(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    """删除补偿命令处理"""
    comp_id = args.extract_plain_text().strip()
    
    if not comp_id:
        await handle_send(bot, event, "请指定要删除的补偿ID")
        return
    
    data = load_compensation_data()
    if comp_id not in data:
        await handle_send(bot, event, f"补偿ID {comp_id} 不存在")
        return
    
    # 从补偿记录中删除
    del data[comp_id]
    save_compensation_data(data)
    
    # 从所有用户的领取记录中删除该补偿ID
    claimed_data = load_claimed_data()
    for user_id in list(claimed_data.keys()):
        if comp_id in claimed_data[user_id]:
            claimed_data[user_id].remove(comp_id)
            # 如果用户没有其他补偿记录，删除该用户条目
            if not claimed_data[user_id]:
                del claimed_data[user_id]
    save_claimed_data(claimed_data)
    
    await handle_send(bot, event, f"成功删除补偿 {comp_id} 及其所有领取记录")

def add_gift_package(gift_id: str, duration_str: str, items_str: str, reason: str):
    """新增礼包
    :param gift_id: 礼包ID
    :param duration_str: 持续时间字符串
    :param items_str: 物品字符串
    :param reason: 发放原因
    """
    data = load_gift_package_data()
    if gift_id in data:
        raise ValueError(f"礼包ID {gift_id} 已存在")
    
    try:
        duration = parse_duration(duration_str)
        expire_time = (datetime.now() + duration).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        raise ValueError(f"时间格式错误: {str(e)}")
    
    # 解析物品字符串（与补偿系统相同）
    items_list = []
    for item_part in items_str.split(','):
        item_part = item_part.strip()
        if 'x' in item_part:
            item_id_or_name, quantity = item_part.split('x', 1)
            quantity = int(quantity)
        else:
            item_id_or_name = item_part
            quantity = 1
        
        if item_id_or_name == "灵石":
            items_list.append({
                "type": "stone",
                "id": "stone",
                "name": "灵石",
                "quantity": quantity if quantity > 0 else 1000000,
                "desc": f"获得 {number_to(quantity if quantity > 0 else 1000000)} 灵石"
            })
            continue
        
        goods_id = None
        if item_id_or_name.isdigit():
            goods_id = int(item_id_or_name)
            item_info = items.get_data_by_item_id(goods_id)
            if not item_info:
                raise ValueError(f"物品ID {goods_id} 不存在")
        else:
            for k, v in items.items.items():
                if item_id_or_name == v['name']:
                    goods_id = k
                    break
            if not goods_id:
                raise ValueError(f"物品 {item_id_or_name} 不存在")
        
        item_info = items.get_data_by_item_id(goods_id)
        items_list.append({
            "type": item_info['type'],
            "id": goods_id,
            "name": item_info['name'],
            "quantity": quantity,
            "desc": item_info['desc']
        })
    
    if not items_list:
        raise ValueError("未指定有效的礼包物品")
    
    data[gift_id] = {
        "items": items_list,
        "reason": reason,
        "expire_time": expire_time,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type": "gift"  # 标记为礼包类型
    }
    save_gift_package_data(data)
    return True

def get_gift_package_info(gift_id: str) -> Optional[dict]:
    """获取礼包信息"""
    data = load_gift_package_data()
    return data.get(gift_id)

def is_gift_package_expired(gift_info: dict) -> bool:
    """检查礼包是否过期"""
    expire_time = datetime.strptime(gift_info["expire_time"], "%Y-%m-%d %H:%M:%S")
    return datetime.now() > expire_time

def has_claimed_gift_package(user_id: str, gift_id: str) -> bool:
    """检查是否已领取礼包"""
    claimed_data = load_claimed_gift_packages()
    return gift_id in claimed_data.get(user_id, [])

async def claim_gift_package(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, user_id: str, gift_id: str) -> bool:
    """领取礼包"""
    gift_info = get_gift_package_info(gift_id)
    if not gift_info:
        return False
    
    if is_gift_package_expired(gift_info):
        return False
    
    if has_claimed_gift_package(user_id, gift_id):
        return False
    
    msg_parts = [f"成功领取礼包 {gift_id}:"]
    
    for item in gift_info["items"]:
        if item["type"] == "stone":
            sql_message.update_ls(user_id, item["quantity"], 1)
            msg_parts.append(f"获得灵石 {number_to(item['quantity'])} 枚")
        else:
            goods_id = item["id"]
            goods_name = item["name"]
            goods_type = item["type"]
            quantity = item["quantity"]
            
            if goods_type in ["辅修功法", "神通", "功法", "身法", "瞳术"]:
                goods_type_item = "技能"
            elif goods_type in ["法器", "防具"]:
                goods_type_item = "装备"
            else:
                goods_type_item = goods_type
            
            sql_message.send_back(
                user_id,
                goods_id,
                goods_name,
                goods_type_item,
                quantity,
                1
            )
            msg_parts.append(f"获得 {goods_name} x{quantity}")
    
    msg = "\n".join(msg_parts)
    await handle_send(bot, event, msg)
    
    claimed_data = load_claimed_gift_packages()
    if user_id not in claimed_data:
        claimed_data[user_id] = []
    claimed_data[user_id].append(gift_id)
    save_claimed_gift_packages(claimed_data)
    return True

# 礼包命令处理器
add_gift_package_cmd = on_command("新增礼包", permission=SUPERUSER, priority=5, block=True)
delete_gift_package_cmd = on_command("删除礼包", permission=SUPERUSER, priority=5, block=True)
list_gift_packages_cmd = on_command("礼包列表", priority=5, block=True)
claim_gift_package_cmd = on_command("领取礼包", priority=5, block=True)
gift_package_help_cmd = on_command("礼包帮助", priority=7, block=True)
gift_package_admin_help_cmd = on_command("礼包管理", permission=SUPERUSER, priority=5, block=True)

__gift_package_help__ = f"""
🎁 礼包帮助 🎁
═════════════
1. 礼包列表 - 查看所有可领取礼包
2. 领取礼包 [ID] - 领取指定礼包

【注意事项】
- 每个礼包每人限领一次
- 过期礼包将无法领取
═════════════
当前时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

__gift_package_admin_help__ = f"""
🎁 礼包管理 🎁 
═════════════
1. 新增礼包 [ID] [时间] [物品] [原因]
   - 示例: 新增礼包 gift_001 7天 1001x1,1002x2 节日福利

2. 删除礼包 [ID] - 删除指定礼包

3. 礼包列表 - 查看所有礼包(含过期)

【参数说明】
- 时间: 如"7天"或"48小时"
- 物品: 物品ID或名称,可带数量
   - 示例1: 1001,1002
   - 示例2: 灵石x1000000
   - 示例3: 渡厄丹x1,两仪心经x1

【注意事项】
- 礼包ID必须唯一
- 删除操作不可逆
═════════════
当前服务器时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

@gift_package_help_cmd.handle(parameterless=[Cooldown(at_sender=False)])
async def handle_gift_package_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """礼包帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if XiuConfig().img:
        pic = await get_msg_pic(__gift_package_help__)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, __gift_package_help__)
    await gift_package_help_cmd.finish()

@gift_package_admin_help_cmd.handle(parameterless=[Cooldown(at_sender=False)])
async def handle_gift_package_admin_help(bot: Bot, event: MessageEvent):
    """礼包管理"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if XiuConfig().img:
        pic = await get_msg_pic(__gift_package_admin_help__)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, __gift_package_admin_help__)
    await gift_package_admin_help_cmd.finish()

@list_gift_packages_cmd.handle()
async def handle_list_gift_packages(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """列出所有礼包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    data = load_gift_package_data()
    if not data:
        msg = "当前没有可用的礼包"
        await handle_send(bot, event, msg)
        return
    
    msg_lines = [
        "🎁 礼包列表 🎁",
        "====================",
        "【有效礼包】"
    ]
    
    valid_gifts = []
    expired_gifts = []
    
    for gift_id, info in data.items():
        expired = is_gift_package_expired(info)
        if expired:
            expired_gifts.append((gift_id, info))
        else:
            valid_gifts.append((gift_id, info))
    
    if not valid_gifts and not expired_gifts:
        msg_lines.append("暂无任何礼包")
    else:
        if valid_gifts:
            for gift_id, info in valid_gifts:
                items_msg = []
                for item in info["items"]:
                    if item["type"] == "stone":
                        items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                    else:
                        items_msg.append(f"{item['name']}(ID:{item['id']}) x{item['quantity']}")
                
                msg_lines.extend([
                    f"🎁 ID: {gift_id}",
                    f"📝 原因: {info['reason']}",
                    f"🎁 内容: {', '.join(items_msg)}",
                    f"⏰ 有效期至: {info['expire_time']}",
                    "------------------"
                ])
        else:
            msg_lines.append("暂无有效礼包")
        
        msg_lines.append("\n【过期礼包】")
        if expired_gifts:
            for gift_id, info in expired_gifts:
                items_msg = []
                for item in info["items"]:
                    if item["type"] == "stone":
                        items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                    else:
                        items_msg.append(f"{item['name']}(ID:{item['id']}) x{item['quantity']}")
                
                msg_lines.extend([
                    f"🎁 ID: {gift_id}",
                    f"📝 原因: {info['reason']}",
                    f"🎁 内容: {', '.join(items_msg)}",
                    f"⏰ 过期时间: {info['expire_time']}",
                    "------------------"
                ])
        else:
            msg_lines.append("暂无过期礼包")
    
    msg_lines.append(f"\n⏱ 当前服务器时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    msg = "\n".join(msg_lines)
    
    if XiuConfig().img:
        pic = await get_msg_pic(msg)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, msg)

@add_gift_package_cmd.handle()
async def handle_add_gift_package(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    """新增礼包命令处理"""
    try:
        arg_str = args.extract_plain_text().strip()
        parts = arg_str.split(maxsplit=3)
        if len(parts) < 4:
            raise ValueError("参数不足，格式应为: 礼包ID 时间 物品 发放原因")
        
        gift_id, duration, items_str, reason = parts
        
        if add_gift_package(gift_id, duration, items_str, reason):
            gift_info = get_gift_package_info(gift_id)
            items_msg = []
            for item in gift_info["items"]:
                if item["type"] == "stone":
                    items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                else:
                    items_msg.append(f"{item['name']}(ID:{item['id']}) x{item['quantity']}")
            
            msg = f"成功新增礼包 {gift_id}\n"
            msg += f"🎁 内容: {', '.join(items_msg)}\n"
            msg += f"📝 原因: {reason}\n"
            msg += f"⏰ 有效期: {duration}"
            await handle_send(bot, event, msg)
        else:
            await handle_send(bot, event, "新增礼包失败")
    except Exception as e:
        await handle_send(bot, event, f"新增礼包出错: {str(e)}")

@claim_gift_package_cmd.handle()
async def handle_claim_gift_package(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """领取礼包命令处理"""
    user_id = event.get_user_id()
    gift_id = args.extract_plain_text().strip()
    
    if not gift_id:
        await handle_send(bot, event, "请指定要领取的礼包ID")
        return
    
    gift_info = get_gift_package_info(gift_id)
    if not gift_info:
        await handle_send(bot, event, f"礼包ID {gift_id} 不存在")
        return
    
    if is_gift_package_expired(gift_info):
        await handle_send(bot, event, f"礼包 {gift_id} 已过期，无法领取")
        return
    
    if has_claimed_gift_package(user_id, gift_id):
        await handle_send(bot, event, f"您已经领取过礼包 {gift_id} 了")
        return
    
    if await claim_gift_package(bot, event, user_id, gift_id):
        pass  # 消息已在claim_gift_package中发送
    else:
        await handle_send(bot, event, "领取礼包失败")

@delete_gift_package_cmd.handle()
async def handle_delete_gift_package(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    """删除礼包命令处理"""
    gift_id = args.extract_plain_text().strip()
    
    if not gift_id:
        await handle_send(bot, event, "请指定要删除的礼包ID")
        return
    
    data = load_gift_package_data()
    if gift_id not in data:
        await handle_send(bot, event, f"礼包ID {gift_id} 不存在")
        return
    
    del data[gift_id]
    save_gift_package_data(data)
    
    claimed_data = load_claimed_gift_packages()
    for user_id in list(claimed_data.keys()):
        if gift_id in claimed_data[user_id]:
            claimed_data[user_id].remove(gift_id)
            if not claimed_data[user_id]:
                del claimed_data[user_id]
    save_claimed_gift_packages(claimed_data)
    
    await handle_send(bot, event, f"成功删除礼包 {gift_id} 及其所有领取记录")
