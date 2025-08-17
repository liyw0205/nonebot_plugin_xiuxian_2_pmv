import random
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

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
    handle_send
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

def parse_duration(duration_str: str) -> timedelta:
    """解析时间持续时间字符串 (xx天/xx小时)"""
    if "天" in duration_str:
        days = int(duration_str.split("天")[0])
        return timedelta(days=days)
    elif "小时" in duration_str:
        hours = int(duration_str.split("小时")[0])
        return timedelta(hours=hours)
    else:
        raise ValueError("无效的时间格式，请使用'xx天'或'xx小时'")

def add_compensation(compensation_id: str, package_id: str, duration_str: str, reason: str):
    """新增补偿"""
    data = load_compensation_data()
    if compensation_id in data:
        raise ValueError(f"补偿ID {compensation_id} 已存在")
    
    try:
        duration = parse_duration(duration_str)
        expire_time = (datetime.now() + duration).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        raise ValueError(f"时间格式错误: {str(e)}")
    
    # 获取礼包描述信息
    goods_info = items.get_data_by_item_id(package_id)
    if not goods_info:
        raise ValueError(f"礼包ID {package_id} 不存在")
    
    data[compensation_id] = {
        "package_id": package_id,
        "reason": reason,
        "expire_time": expire_time,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "description": goods_info.get('desc', '无描述信息')  # 添加描述字段
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
    
    # 使用补偿信息中的package_id作为物品ID
    goods_id = compensation_info["package_id"]
    goods_info = items.get_data_by_item_id(goods_id)
    
    if not goods_info:
        return False
    
    num = 1
    package_name = goods_info['name']
    msg_parts = []
    i = 1
    
    while True:
        buff_key = f'buff_{i}'
        name_key = f'name_{i}'
        type_key = f'type_{i}'
        amount_key = f'amount_{i}'

        if name_key not in goods_info:
            break

        item_name = goods_info[name_key]
        item_amount = goods_info.get(amount_key, 1) * num
        item_type = goods_info.get(type_key)
        buff_id = goods_info.get(buff_key)

        if item_name == "灵石":
            key = 1 if item_amount > 0 else 2  # 正数增加，负数减少
            sql_message.update_ls(user_id, abs(item_amount), key)
            msg_parts.append(f"获得灵石 {item_amount} 枚\n")
        else:
            if item_type in ["辅修功法", "神通", "功法", "身法", "瞳术"]:
                goods_type_item = "技能"
            elif item_type in ["法器", "防具"]:
                goods_type_item = "装备"
            else:
                goods_type_item = item_type
            if buff_id is not None:
                sql_message.send_back(user_id, buff_id, item_name, goods_type_item, item_amount, 1)
                msg_parts.append(f"获得 {item_name} x{item_amount}\n")
        
        i += 1            

    if buff_id is not None:
        sql_message.send_back(user_id, buff_id, item_name, goods_type_item, item_amount, 1)
    
    msg = f"成功领取补偿 {compensation_id}:\n" + "".join(msg_parts)
    await handle_send(bot, event, msg)
    
    # 记录领取状态
    claimed_data = load_claimed_data()
    if user_id not in claimed_data:
        claimed_data[user_id] = []
    claimed_data[user_id].append(compensation_id)
    save_claimed_data(claimed_data)
    return True


add_compensation_cmd = on_command("新增补偿", permission=SUPERUSER, priority=5, block=True)
delete_compensation_cmd = on_command("删除补偿", permission=SUPERUSER, priority=5, block=True)
list_compensation_cmd = on_command("补偿列表", priority=5, block=True)
claim_compensation_cmd = on_command("领取补偿", priority=5, block=True)
compensation_help_cmd = on_command("补偿", aliases={"补偿帮助"}, priority=7, block=True)

__compensation_help__ = f"""
补偿系统帮助文档

【基本介绍】
本系统用于管理游戏内的补偿发放，包含以下功能：
1. 新增补偿 - 管理员创建新的补偿项
2. 补偿列表 - 查看所有可用补偿
3. 领取补偿 - 玩家领取指定补偿
4. 删除补偿 - 管理员删除补偿项

【功能详情】
═════════════
1. 新增补偿
- 仅管理员可用
- 格式：新增补偿 [补偿ID] [时间] [礼包ID] [补偿原因]
- 补偿ID: 补偿的唯一标识
- 时间: 有效期，如"3天"或"48小时"
- 礼包ID: 对应要发放的物品ID
- 补偿原因: 补偿说明(可包含空格)

示例: 新增补偿 comp_001 3天 15052 "登录问题补偿"

2. 补偿列表
- 查看所有补偿信息
- 显示内容：补偿ID、原因、创建时间、过期时间、状态

3. 领取补偿
- 玩家领取未过期的补偿
- 格式：领取补偿 [ID]
- 每个补偿每人限领一次

4. 删除补偿
- 仅管理员可用
- 格式：删除补偿 [补偿ID]
- 会同时删除补偿记录和所有用户的该补偿领取记录
- 删除后不可恢复

示例: 删除补偿 comp_001

【注意事项】
- 补偿ID必须对应一个有效的新手礼包ID
- 过期补偿将自动失效
- 补偿发放物品与对应ID的新手礼包一致
- 删除补偿操作不可逆，请谨慎使用
═════════════
当前服务器时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

@compensation_help_cmd.handle(parameterless=[Cooldown(at_sender=False)])
async def handle_compensation_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """补偿帮助命令处理"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if XiuConfig().img:
        pic = await get_msg_pic(__compensation_help__)
        await handle_send(bot, event, MessageSegment.image(pic))
    else:
        await handle_send(bot, event, __compensation_help__)
    await compensation_help_cmd.finish()

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
        "📋📋 补偿列表 📋📋",
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
                msg_lines.extend([
                    f"🆔🆔🆔 补偿ID: {comp_id}",
                    f"📝📝 原因: {info['reason']}",
                    f"📦📦 礼包内容: {info.get('description', '无描述信息')}",
                    f"⏰⏰⏰ 有效期至: {info['expire_time']}",
                    f"🕒🕒🕒 创建时间: {info['create_time']}",
                    "------------------"
                ])
        else:
            msg_lines.append("暂无有效补偿")
        
        # 过期补偿
        msg_lines.append("\n【过期补偿】")
        if expired_comps:
            for comp_id, info in expired_comps:
                msg_lines.extend([
                    f"🆔🆔🆔 补偿ID: {comp_id}",
                    f"📝📝 原因: {info['reason']}",
                    f"📦📦 礼包内容: {info.get('description', '无描述信息')}",
                    f"⏰⏰⏰ 过期时间: {info['expire_time']}",
                    f"🕒🕒🕒 创建时间: {info['create_time']}",
                    "------------------"
                ])
        else:
            msg_lines.append("暂无过期补偿")
    
    # 添加服务器时间信息
    msg_lines.append(f"\n⏱⏱⏱️ 当前服务器时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
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
        # 新格式: 补偿ID 时间 礼包ID 补偿原因
        arg_str = args.extract_plain_text().strip()
        parts = arg_str.split(maxsplit=3)
        if len(parts) < 4:
            raise ValueError("参数不足，格式应为: 补偿ID 时间 礼包ID 补偿原因")
        
        comp_id, duration, package_id, reason = parts
        
        if add_compensation(comp_id, package_id, duration, reason):
            await handle_send(bot, event, f"成功新增补偿 {comp_id}\n礼包ID: {package_id}\n原因: {reason}\n有效期: {duration}")
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
