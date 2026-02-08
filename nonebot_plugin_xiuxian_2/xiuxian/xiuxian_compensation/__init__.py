import random
import json
import os
import string
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Literal

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
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.utils import (
    check_user,
    Txt2Img,
    get_msg_pic,
    CommandObjectID,
    handle_send,
    send_msg_handler,
    number_to
)

items = Items()
sql_message = XiuxianDateManage()  # sql类

# ════════════
# 通用类型定义
# ════════════
ItemType = Literal["补偿", "礼包", "兑换码"]
DATA_PATH = Path(__file__).parent / "compensation_data"
# ════════════
# 文件路径配置
# ════════════
DATA_CONFIG = {
    "补偿": {
        "data_path": DATA_PATH / "compensation" / "compensation_records.json",
        "claimed_path": DATA_PATH / "compensation" / "claimed_records.json",
        "records_folder": DATA_PATH / "compensation",
        "type_key": "补偿",
        "type_field": "type"  # 补偿没有特定的 type 字段
    },
    "礼包": {
        "data_path": DATA_PATH / "gift_package" / "gift_package_records.json",
        "claimed_path": DATA_PATH / "gift_package" / "claimed_gift_packages.json",
        "records_folder": DATA_PATH / "gift_package",
        "type_key": "礼包",
        "type_field": "type"  # 礼包有 "type": "gift"
    },
    "兑换码": {
        "data_path": DATA_PATH / "redeem_code" / "redeem_codes.json",
        "claimed_path": DATA_PATH / "redeem_code" / "claimed_redeem_codes.json",
        "records_folder": DATA_PATH / "redeem_code",
        "type_key": "兑换码",
        "type_field": "type"  # 兑换码有 "type": "redeem_code"
    }
}

# ════════════
# 初始化数据文件夹和文件
# ════════════
if not DATA_PATH.exists():
    os.makedirs(DATA_PATH, exist_ok=True)
for config in DATA_CONFIG.values():
    config["records_folder"].mkdir(exist_ok=True)
    if not config["data_path"].exists():
        with open(config["data_path"], "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=4)
    if not config["claimed_path"].exists():
        with open(config["claimed_path"], "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=4)

# ════════════
# 通用函数定义
# ════════════

def load_data(config: Dict[str, Any]) -> Dict[str, dict]:
    """加载指定类型的数据"""
    with open(config["data_path"], "r", encoding="utf-8") as f:
        return json.load(f)

def save_data(config: Dict[str, Any], data: Dict[str, dict]):
    """保存指定类型的数据"""
    with open(config["data_path"], "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_claimed_data(config: Dict[str, Any]) -> Dict[str, List[str]]:
    """加载指定类型的领取记录"""
    with open(config["claimed_path"], "r", encoding="utf-8") as f:
        return json.load(f)

def save_claimed_data(config: Dict[str, Any], data: Dict[str, List[str]]):
    """保存指定类型的领取记录"""
    with open(config["claimed_path"], "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def generate_unique_id(existing_ids: List[str]) -> str:
    """生成4-6位随机不重复ID（大写字母+数字）"""
    while True:
        length = random.randint(4, 6)
        characters = string.ascii_uppercase + string.digits
        new_id = ''.join(random.choice(characters) for _ in range(length))
        if not any(c.isalpha() for c in new_id) or not any(c.isdigit() for c in new_id):
            continue
        if new_id not in existing_ids:
            return new_id

def parse_duration(duration_str: str, is_start_time: bool = False) -> Union[datetime, timedelta, str]:
    """解析时间持续时间字符串"""
    try:
        if duration_str.lower() in ["无限", "0"]:
            return "无限" if not is_start_time else datetime.now()
        if is_start_time and duration_str.isdigit() and len(duration_str) == 6:
            year = int("20" + duration_str[:2])
            month = int(duration_str[2:4])
            day = int(duration_str[4:6])
            return datetime(year, month, day).replace(hour=0, minute=0, second=0)
        if is_start_time:
            if "小时" in duration_str:
                hours = int(duration_str.split("小时")[0])
                return datetime.now() + timedelta(hours=hours)
            elif "天" in duration_str:
                days = int(duration_str.split("天")[0])
                return (datetime.now() + timedelta(days=days)).replace(hour=0, minute=0, second=0)
            else:
                raise ValueError(f"无效的生效期格式: {duration_str}")
        else:
            if "天" in duration_str:
                days = int(duration_str.split("天")[0])
                return timedelta(days=days)
            elif "小时" in duration_str:
                hours = int(duration_str.split("小时")[0])
                return timedelta(hours=hours)
            elif duration_str.isdigit() and len(duration_str) == 6:
                year = int("20" + duration_str[:2])
                month = int(duration_str[2:4])
                day = int(duration_str[4:6])
                return datetime(year, month, day).replace(hour=23, minute=59, second=59)
            else:
                raise ValueError(f"无效的有效期格式: {duration_str}")
    except Exception as e:
        raise ValueError(f"时间格式错误: {str(e)}")

def get_item_list(items_str: str, items: Any) -> List[Dict[str, Any]]:
    """解析物品字符串，返回物品列表"""
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
            "desc": item_info.get('desc', "")
        })
    if not items_list:
        raise ValueError("未指定有效的物品")
    return items_list

def create_item_message(items: List[Dict[str, Any]]) -> List[str]:
    """创建物品描述消息"""
    items_msg = []
    for item in items:
        if item["type"] == "stone":
            items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
        else:
            items_msg.append(f"{item['name']} x{item['quantity']}")
    return items_msg

async def send_success_message(bot: Bot, event: MessageEvent, config: Dict[str, Any], comp_id: str, items: List[Dict[str, Any]], reason: str, expire_time: Union[datetime, timedelta, str], start_time: Union[datetime, str, None], usage_limit: Union[int, str, None] = None) -> None:
    """发送成功添加的消息"""
    items_msg = create_item_message(items)
    expire_msg = "无限" if expire_time == "无限" else (expire_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(expire_time, datetime) else expire_time)
    start_msg = start_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(start_time, datetime) else (start_time if start_time else "立即生效")
    usage_msg = "无限次" if usage_limit == 0 else f"{usage_limit}次" if usage_limit is not None else "未指定"
    msg = f"\n成功新增{config['type_key']} {comp_id}\n"
    msg += f"物品: {', '.join(items_msg)}\n"

    if config['type_key'] == "兑换码":
        msg += f"🔄 使用上限: {usage_msg}\n"
    else:
        msg += f"原因: {reason}\n"
    msg += f"⏰ 有效期至: {expire_msg}\n"
    msg += f"🕒 生效时间: {start_msg}\n"
    await handle_send(bot, event, msg, md_type="compensation", k1="领取", v1=f"领取{config['type_key']} {comp_id}", k2="列表", v2=f"{config['type_key']}列表", k3="帮助", v3=f"{config['type_key']}帮助")

def is_expired(item_info: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """检查是否过期"""
    expire_time = item_info.get("expire_time")
    if expire_time == "无限":
        return False
    if isinstance(expire_time, str):
        expire_time_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
    elif isinstance(expire_time, datetime):
        expire_time_dt = expire_time
    else:
        return False  # 无法识别的格式，默认不过期
    return datetime.now() > expire_time_dt

def has_claimed(user_id: str, item_id: str, config: Dict[str, Any]) -> bool:
    """检查用户是否已经领取或使用过"""
    claimed_data = load_claimed_data(config)
    return item_id in claimed_data.get(user_id, [])

async def claim_item(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, user_id: str, item_id: str, config: Dict[str, Any]) -> bool:
    """通用领取逻辑"""
    item_info = load_data(config).get(item_id)
    if not item_info:
        return False
    if is_expired(item_info, config):
        return False
    if has_claimed(user_id, item_id, config):
        return False
    # 检查是否已生效
    start_time = item_info.get("start_time")
    if start_time:
        if isinstance(start_time, str):
            start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        elif isinstance(start_time, datetime):
            start_time_dt = start_time
        else:
            return False  # 无法识别的格式，默认不生效
        if datetime.now() < start_time_dt:
            return False
    msg_parts = [f"成功领取{config['type_key']} {item_id}:"]
    for item in item_info["items"]:
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
    claimed_data = load_claimed_data(config)
    if user_id not in claimed_data:
        claimed_data[user_id] = []
    claimed_data[user_id].append(item_id)
    save_claimed_data(config, claimed_data)
    # 更新使用次数
    if config["type_key"] == "兑换码":
        redeem_data = load_data(config)
        redeem_data[item_id]["used_count"] += 1
        save_data(config, redeem_data)
    return True

async def list_items(config: Dict[str, Any], bot: Bot, event: MessageEvent) -> None:
    """通用列表展示逻辑"""
    if config['type_key'] == "兑换码":
        await handle_list_redeem_codes(bot, event)
        return    
    data = load_data(config)
    if not data:
        msg = f"当前没有可用的{config['type_key']}"
        await handle_send(bot, event, msg, md_type="compensation", k1="领取", v1=f"领取{config['type_key']}", k2="列表", v2=f"{config['type_key']}列表", k3="帮助", v3=f"{config['type_key']}帮助")
        return
    current_time = datetime.now()
    title = f"📋 {config['type_key']}列表 📋"
    msg_lines = ["====================", "【有效】"]
    valid_items = []
    expired_items = []
    not_yet_started_items = []
    for item_id, info in data.items():
        expire_time = info.get("expire_time")
        start_time = info.get("start_time")
        if expire_time == "无限":
            expire_time_dt = None
        else:
            if isinstance(expire_time, str):
                expire_time_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
            elif isinstance(expire_time, datetime):
                expire_time_dt = expire_time
            else:
                expire_time_dt = None  # 无法识别的格式，默认有效
        if start_time:
            if isinstance(start_time, str):
                start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            elif isinstance(start_time, datetime):
                start_time_dt = start_time
            else:
                start_time_dt = None  # 无法识别的格式，默认立即生效
        else:
            start_time_dt = None
        if start_time_dt and current_time < start_time_dt:
            not_yet_started_items.append((item_id, info))
        elif expire_time_dt and current_time > expire_time_dt:
            expired_items.append((item_id, info))
        else:
            valid_items.append((item_id, info))
    if valid_items:
        for item_id, info in valid_items:
            items_msg = create_item_message(info["items"])
            expire_msg = "无限" if info.get("expire_time") == "无限" else (info['expire_time'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(info.get("expire_time"), datetime) else info.get("expire_time"))
            start_msg = info.get("start_time", "立即生效")
            if isinstance(start_msg, datetime):
                start_msg = start_msg.strftime("%Y-%m-%d %H:%M:%S")
            msg_lines.extend([
                f"🆔 {config['type_key']}ID: {item_id}",
                f"📝 原因: {info['reason']}",
                f"📦 内容: {', '.join(items_msg)}",
                f"⏰ 有效期至: {expire_msg}",
                f"🕒 生效时间: {start_msg}",
                "------------------"
            ])
    else:
        msg_lines.append("暂无有效")
    if not_yet_started_items:
        msg_lines.append("\n【尚未生效】")
        for item_id, info in not_yet_started_items:
            items_msg = create_item_message(info["items"])
            start_time = info.get("start_time")
            if isinstance(start_time, datetime):
                start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
            expire_msg = "无限" if info.get("expire_time") == "无限" else (info['expire_time'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(info.get("expire_time"), datetime) else info.get("expire_time"))
            msg_lines.extend([
                f"🆔 {config['type_key']}ID: {item_id}",
                f"📝 原因: {info['reason']}",
                f"📦 内容: {', '.join(items_msg)}",
                f"⏰ 有效期至: {expire_msg}",
                f"🕒 生效时间: {start_time}",
                "------------------"
            ])
    if expired_items:
        msg_lines.append("\n【过期】")
        for item_id, info in expired_items:
            items_msg = create_item_message(info["items"])
            expire_msg = "无限" if info.get("expire_time") == "无限" else (info['expire_time'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(info.get("expire_time"), datetime) else info.get("expire_time"))
            start_msg = info.get("start_time", "立即生效")
            if isinstance(start_msg, datetime):
                start_msg = start_msg.strftime("%Y-%m-%d %H:%M:%S")
            msg_lines.extend([
                f"🆔 {config['type_key']}ID: {item_id}",
                f"📝 原因: {info['reason']}",
                f"📦 内容: {', '.join(items_msg)}",
                f"⏰ 过期时间: {expire_msg}",
                f"🕒 生效时间: {start_msg}",
                "------------------"
            ])

    page = ["领取", f"领取{config['type_key']}", "列表", f"{config['type_key']}列表", "帮助", f"{config['type_key']}帮助", f"时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}"]
    await send_msg_handler(bot, event, f"{config['type_key']}列表", bot.self_id, msg_lines, title=title, page=page)

def delete_item(item_id: str, config: Dict[str, Any]) -> None:
    """通用删除逻辑"""
    data = load_data(config)
    if item_id not in data:
        return
    del data[item_id]
    save_data(config, data)
    claimed_data = load_claimed_data(config)
    if item_id in claimed_data:
        del claimed_data[item_id]
    else:
        for user_id in list(claimed_data.keys()):
            if item_id in claimed_data[user_id]:
                claimed_data[user_id].remove(item_id)
                if not claimed_data[user_id]:
                    del claimed_data[user_id]
    save_claimed_data(config, claimed_data)

def clear_all_items(config: Dict[str, Any]) -> None:
    """清空指定类型的所有记录及其领取记录"""
    data_path = config["data_path"]
    claimed_path = config["claimed_path"]
    
    # 清空主数据文件
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)
    
    # 清空领取记录文件
    with open(claimed_path, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)
    
    logger.info(f"已清空所有{config['type_key']}记录及其领取记录。")

# ════════════
# 通用命令处理
# ════════════

def register_common_commands(item_type: ItemType, config: Dict[str, Any]):
    """注册通用命令"""
    add_cmd = on_command(f"新增{item_type}", permission=SUPERUSER, priority=5, block=True)
    delete_cmd = on_command(f"删除{item_type}", permission=SUPERUSER, priority=5, block=True)
    clear_cmd = on_command(f"清空{item_type}", permission=SUPERUSER, priority=5, block=True)  # 新增清空命令
    list_cmd = on_command(f"{item_type}列表", priority=5, block=True)
    claim_cmd = on_command(f"领取{item_type}", priority=5, block=True)
    help_cmd = on_command(f"{item_type}帮助", priority=7, block=True)
    admin_help_cmd = on_command(f"{item_type}管理", permission=SUPERUSER, priority=5, block=True)

    @clear_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])  # 清空命令的处理函数
    async def handle_clear(bot: Bot, event: MessageEvent):
        try:
            clear_all_items(config)
            await handle_send(bot, event, f"成功清空所有{item_type}记录及其领取记录。")
        except Exception as e:
            await handle_send(bot, event, f"清空{item_type}出错: {str(e)}")

    @add_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
    async def handle_add(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
        try:
            arg_str = args.extract_plain_text().strip()
            parts = arg_str.split(maxsplit=5)
            if len(parts) < 3:
                raise ValueError(f"参数不足，格式应为: {item_type}ID 物品数据 原因 有效期 生效期")
            item_id = parts[0] if parts[0] not in ["随机", "0"] else None
            items_str = parts[1]
            reason = parts[2]
            expire_time_str = parts[3] if len(parts) > 3 else None
            start_time_str = parts[4] if len(parts) > 4 else None
            data = load_data(config)
            if item_id is None or item_id in ["随机", "0"]:
                existing_ids = list(data.keys())
                item_id = generate_unique_id(existing_ids)
            else:
                if item_id in data:
                    logger.info(f"{item_type}ID {item_id} 已存在，将覆盖旧的记录。")
            start_time = None
            expire_delta = None
            if start_time_str:
                start_time = parse_duration(start_time_str, is_start_time=True)
                if not isinstance(start_time, datetime):
                    raise ValueError(f"无效的生效期格式: {start_time_str}")
            else:
                start_time = parse_duration("0", is_start_time=True)
            if expire_time_str:
                expire_delta = parse_duration(expire_time_str, is_start_time=False)
                if expire_delta == "无限":
                    expire_time = "无限"
                elif isinstance(expire_delta, timedelta):
                    expire_time = datetime.now() + expire_delta
                    expire_time = expire_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    raise ValueError(f"无效的有效期格式: {expire_time_str}")
            else:
                expire_time = "无限"
            if start_time and expire_time != "无限":
                expire_datetime = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S") if isinstance(expire_time, str) else expire_time
                if start_time > expire_datetime:
                    raise ValueError("生效期不能超过有效期")
            items_list = get_item_list(items_str, items)
            if not items_list:
                raise ValueError("未指定有效的物品")
            data[item_id] = {
                "items": items_list,
                "reason": reason,
                "expire_time": expire_time,
                "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else None,
            }
            usage_limit = None
            if config["type_key"] == "兑换码":
                usage_limit_str = parts[2]
                try:
                    usage_limit = int(usage_limit_str)
                except ValueError:
                    raise ValueError("兑换次数必须是数字")
                data[item_id]["usage_limit"] = usage_limit
                data[item_id]["used_count"] = 0
            save_data(config, data)
            await send_success_message(bot, event, config, item_id, items_list, reason, expire_time, start_time, usage_limit)
        except Exception as e:
            await handle_send(bot, event, f"新增{item_type}出错: {str(e)}")

    @delete_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
    async def handle_delete(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
        item_id = args.extract_plain_text().strip()
        if not item_id:
            await handle_send(bot, event, f"请指定要删除的{item_type}ID")
            return
        delete_item(item_id, config)
        await handle_send(bot, event, f"成功删除{item_type} {item_id} 及其所有领取记录")

    @list_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
    async def handle_list(bot: Bot, event: MessageEvent):
        await list_items(config, bot, event)

    @claim_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
    async def handle_claim(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
        user_id = event.get_user_id()
        item_id = args.extract_plain_text().strip()
        if not item_id:
            await handle_send(bot, event, f"请指定要领取的{item_type}ID")
            return
        await claim_item(bot, event, user_id, item_id, config)

    @help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
    async def handle_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
        bot, send_group_id = await assign_bot(bot=bot, event=event)
        if item_type == "补偿":
            await handle_send(bot, event, __compensation_help__)
        elif item_type == "礼包":
            await handle_send(bot, event, __gift_package_help__)
        elif item_type == "兑换码":
            await handle_send(bot, event, __redeem_code_help__)
        await handle_help_cmd.finish()

    @admin_help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
    async def handle_admin_help(bot: Bot, event: MessageEvent):
        bot, send_group_id = await assign_bot(bot=bot, event=event)
        if item_type == "补偿":
            await handle_send(bot, event, __compensation_admin_help__)
        elif item_type == "礼包":
            await handle_send(bot, event, __gift_package_admin_help__)
        elif item_type == "兑换码":
            await handle_send(bot, event, __redeem_code_admin_help__)
        await handle_admin_help_cmd.finish()

# ════════════
# 特殊处理：兑换码的使用次数和领取逻辑
# ════════════
claim_redeem_code_cmd = on_command("兑换", priority=10, block=True)
@claim_redeem_code_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def claim_redeem_code_cmd(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = event.get_user_id()
    redeem_code = args.extract_plain_text().strip()
    if not redeem_code:
        await handle_send(bot, event, "请指定要兑换的兑换码")
        return
    config = DATA_CONFIG["兑换码"]
    data = load_data(config)
    redeem_info = data.get(redeem_code)
    if not redeem_info:
        await handle_send(bot, event, "兑换码不存在")
        return
    if config["type_key"] == "兑换码":
        usage_limit = redeem_info.get("usage_limit", 0)
        if usage_limit != 0 and redeem_info.get("used_count", 0) >= usage_limit:
            await handle_send(bot, event, "该兑换码已被使用完")
            return
    if has_claimed(user_id, redeem_code, config):
        await handle_send(bot, event, f"您已经使用过兑换码 {redeem_code} 了")
        return
    if config["type_key"] == "兑换码":
        start_time = redeem_info.get("start_time")
        if start_time:
            if isinstance(start_time, str):
                start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            elif isinstance(start_time, datetime):
                start_time_dt = start_time
            else:
                await handle_send(bot, event, "兑换码生效时间格式错误")
                return
            if datetime.now() < start_time_dt:
                await handle_send(bot, event, f"兑换码 {redeem_code} 尚未生效，生效时间为 {start_time}，请稍后再试。")
                return
    msg_parts = [f"成功兑换 {redeem_code}:"]
    for item in redeem_info["items"]:
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
    if config["type_key"] == "兑换码":
        redeem_data = load_data(config)
        redeem_data[redeem_code]["used_count"] += 1
        save_data(config, redeem_data)
    claimed_data = load_claimed_data(config)
    if user_id not in claimed_data:
        claimed_data[user_id] = []
    claimed_data[user_id].append(redeem_code)
    save_claimed_data(config, claimed_data)

async def handle_list_redeem_codes(bot: Bot, event: MessageEvent):
    """列出所有兑换码(仅管理员可见)"""
    config = DATA_CONFIG["兑换码"]
    data = load_data(config)
    if not data:
        return
    current_time = datetime.now()
    title = "🎟 兑换码列表 🎟"
    msg_lines = ["===================="]
    valid_codes = []
    expired_codes = []
    not_yet_started_codes = []
    for code, info in data.items():
        expire_time = info.get("expire_time")
        start_time = info.get("start_time")
        if expire_time == "无限":
            expire_time_dt = None
        else:
            if isinstance(expire_time, str):
                expire_time_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
            elif isinstance(expire_time, datetime):
                expire_time_dt = expire_time
            else:
                expire_time_dt = None
        if start_time:
            if isinstance(start_time, str):
                start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            elif isinstance(start_time, datetime):
                start_time_dt = start_time
            else:
                start_time_dt = None
        else:
            start_time_dt = None
        if start_time_dt and current_time < start_time_dt:
            not_yet_started_codes.append((code, info))
        elif expire_time_dt and current_time > expire_time_dt:
            expired_codes.append((code, info))
        else:
            valid_codes.append((code, info))
    if not_yet_started_codes:
        msg_lines.append("\n【尚未生效的兑换码】")
        for code, info in not_yet_started_codes:
            items_msg = create_item_message(info["items"])
            usage_limit = "无限次" if info.get("usage_limit", 0) == 0 else f"{info.get('usage_limit', 0)}次"
            used_count = info.get("used_count", 0)
            start_time_str = info.get("start_time", "未知")
            expire_time_str = info.get("expire_time", "未知")
            create_time_str = info.get("create_time", "未知")
            msg_lines.extend([
                f"🎟 兑换码: {code}",
                f"🎁 内容: {', '.join(items_msg)}",
                f"🔄 使用情况: {used_count}/{usage_limit}",
                f"⏰ 有效期至: {expire_time_str}",
                f"🕒 生效时间: {start_time_str}",
                f"🕒 创建时间: {create_time_str}",
                "------------------"
            ])
    if valid_codes:
        msg_lines.append("\n【有效兑换码】")
        for code, info in valid_codes:
            items_msg = create_item_message(info["items"])
            usage_limit = "无限次" if info.get("usage_limit", 0) == 0 else f"{info.get('usage_limit', 0)}次"
            used_count = info.get("used_count", 0)
            expire_time_str = info.get("expire_time", "未知")
            start_time_str = info.get("start_time", "未知")
            create_time_str = info.get("create_time", "未知")
            msg_lines.extend([
                f"🎟 兑换码: {code}",
                f"🎁 内容: {', '.join(items_msg)}",
                f"🔄 使用情况: {used_count}/{usage_limit}",
                f"⏰ 有效期至: {expire_time_str}",
                f"🕒 生效时间: {start_time_str}",
                f"🕒 创建时间: {create_time_str}",
                "------------------"
            ])
    if expired_codes:
        msg_lines.append("\n【过期兑换码】")
        for code, info in expired_codes:
            items_msg = create_item_message(info["items"])
            usage_limit = "无限次" if info.get("usage_limit", 0) == 0 else f"{info.get('usage_limit', 0)}次"
            used_count = info.get("used_count", 0)
            expire_time_str = info.get("expire_time", "未知")
            start_time_str = info.get("start_time", "未知")
            create_time_str = info.get("create_time", "未知")
            msg_lines.extend([
                f"🎟 兑换码: {code}",
                f"🎁 内容: {', '.join(items_msg)}",
                f"🔄 使用情况: {used_count}/{usage_limit}",
                f"⏰ 过期时间: {expire_time_str}",
                f"🕒 生效时间: {start_time_str}",
                f"🕒 创建时间: {create_time_str}",
                "------------------"
            ])

    page = ["兑换", "兑换", "列表", f"{config['type_key']}列表", "帮助", f"{config['type_key']}帮助", f"时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}"]
    await send_msg_handler(bot, event, f"{config['type_key']}列表", bot.self_id, msg_lines, title=title, page=page)

def clean_expired_items():
    """自动清理所有过期（补偿、礼包、兑换码）"""
    for item_type, config in DATA_CONFIG.items():
        if item_type == "补偿":
            clean_expired_compensations()
        elif item_type == "礼包":
            clean_expired_gift_packages()
        elif item_type == "兑换码":
            clean_expired_redeem_codes()

def clean_expired_compensations():
    """自动清理过期的补偿项，并清除对应的领取记录"""
    config = DATA_CONFIG["补偿"]
    data = load_data(config)
    claimed_data = load_claimed_data(config)
    to_delete = []
    for comp_id, comp_info in data.items():
        expire_time = comp_info.get("expire_time")
        if expire_time == "无限":
            continue
        if isinstance(expire_time, str):
            expire_time_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
        elif isinstance(expire_time, datetime):
            expire_time_dt = expire_time
        else:
            continue  # 无法识别的格式，默认不过期
        if datetime.now() > expire_time_dt:
            to_delete.append(comp_id)
    for comp_id in to_delete:
        del data[comp_id]
        if comp_id in claimed_data:
            del claimed_data[comp_id]
        else:
            for user_id in list(claimed_data.keys()):
                if comp_id in claimed_data[user_id]:
                    claimed_data[user_id].remove(comp_id)
                    if not claimed_data[user_id]:
                        del claimed_data[user_id]
    if to_delete:
        save_data(config, data)
        save_claimed_data(config, claimed_data)
        logger.info(f"已自动清理 {len(to_delete)} 个过期补偿: {to_delete}")
    else:
        logger.info("没有发现过期补偿，无需清理")

def clean_expired_gift_packages():
    """自动清理过期的礼包项，并清除对应的领取记录"""
    config = DATA_CONFIG["礼包"]
    data = load_data(config)
    claimed_data = load_claimed_data(config)
    to_delete = []
    for gift_id, gift_info in data.items():
        expire_time = gift_info.get("expire_time")
        if expire_time == "无限":
            continue
        if isinstance(expire_time, str):
            expire_time_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
        elif isinstance(expire_time, datetime):
            expire_time_dt = expire_time
        else:
            continue  # 无法识别的格式，默认不过期
        if datetime.now() > expire_time_dt:
            to_delete.append(gift_id)
    for gift_id in to_delete:
        del data[gift_id]
        if gift_id in claimed_data:
            del claimed_data[gift_id]
        else:
            for user_id in list(claimed_data.keys()):
                if gift_id in claimed_data[user_id]:
                    claimed_data[user_id].remove(gift_id)
                    if not claimed_data[user_id]:
                        del claimed_data[user_id]
    if to_delete:
        save_data(config, data)
        save_claimed_data(config, claimed_data)
        logger.info(f"已自动清理 {len(to_delete)} 个过期礼包: {to_delete}")
    else:
        logger.info("没有发现过期礼包，无需清理")

def clean_expired_redeem_codes():
    """自动清理过期的兑换码项，并清除对应的领取记录"""
    config = DATA_CONFIG["兑换码"]
    data = load_data(config)
    claimed_data = load_claimed_data(config)
    to_delete = []
    for code, code_info in data.items():
        expire_time = code_info.get("expire_time")
        if expire_time == "无限":
            continue
        if isinstance(expire_time, str):
            expire_time_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
        elif isinstance(expire_time, datetime):
            expire_time_dt = expire_time
        else:
            continue  # 无法识别的格式，默认不过期
        if datetime.now() > expire_time_dt:
            to_delete.append(code)
    for code in to_delete:
        del data[code]
        if code in claimed_data:
            del claimed_data[code]
        else:
            for user_id in list(claimed_data.keys()):
                if code in claimed_data[user_id]:
                    claimed_data[user_id].remove(code)
                    if not claimed_data[user_id]:
                        del claimed_data[user_id]
    if to_delete:
        save_data(config, data)
        save_claimed_data(config, claimed_data)
        logger.info(f"已自动清理 {len(to_delete)} 个过期兑换码: {to_delete}")
    else:
        logger.info("没有发现过期兑换码，无需清理")

# ════════════
# 注册通用命令
# ════════════

for item_type, config in DATA_CONFIG.items():
    register_common_commands(item_type, config)

# ════════════
# 邀请功能
# ════════════

INVITATION_DATA_PATH = DATA_PATH / "invitation_data"
INVITATION_REWARDS_FILE = INVITATION_DATA_PATH / "invitation_rewards.json"
INVITATION_RECORDS_FILE = INVITATION_DATA_PATH / "invitation_records.json"
INVITATION_CLAIMED_FILE = INVITATION_DATA_PATH / "invitation_claimed.json"

# 确保目录存在
INVITATION_DATA_PATH.mkdir(exist_ok=True)

# 初始化邀请奖励文件
if not INVITATION_REWARDS_FILE.exists():
    with open(INVITATION_REWARDS_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

# 初始化邀请记录文件
if not INVITATION_RECORDS_FILE.exists():
    with open(INVITATION_RECORDS_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

# 初始化领取记录文件
if not INVITATION_CLAIMED_FILE.exists():
    with open(INVITATION_CLAIMED_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)

def load_invitation_rewards():
    """加载邀请奖励配置"""
    with open(INVITATION_REWARDS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_invitation_rewards(data):
    """保存邀请奖励配置"""
    with open(INVITATION_REWARDS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_invitation_records():
    """加载邀请记录"""
    with open(INVITATION_RECORDS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_invitation_records(data):
    """保存邀请记录"""
    with open(INVITATION_RECORDS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_claimed_records():
    """加载领取记录"""
    with open(INVITATION_CLAIMED_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_claimed_records(data):
    """保存领取记录"""
    with open(INVITATION_CLAIMED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_user_invitation_count(inviter_id):
    """获取用户的邀请数量"""
    records = load_invitation_records()
    return len(records.get(str(inviter_id), []))

def add_invitation_record(inviter_id, invited_id):
    """添加邀请记录"""
    records = load_invitation_records()
    if str(inviter_id) not in records:
        records[str(inviter_id)] = []
    if str(invited_id) not in records[str(inviter_id)]:
        records[str(inviter_id)].append(str(invited_id))
        save_invitation_records(records)
        return True
    return False

def has_invitation_code(user_id):
    """检查用户是否已经填写过邀请码"""
    records = load_invitation_records()
    for inviter_id, invited_list in records.items():
        if str(user_id) in invited_list:
            return True
    return False

def get_inviter_id(user_id):
    """获取用户的邀请人ID"""
    records = load_invitation_records()
    for inviter_id, invited_list in records.items():
        if str(user_id) in invited_list:
            return inviter_id
    return None

def has_claimed_reward(user_id, threshold):
    """检查用户是否已经领取过某个门槛的奖励"""
    claimed = load_claimed_records()
    if str(user_id) not in claimed:
        return False
    return str(threshold) in claimed[str(user_id)]

def mark_reward_claimed(user_id, threshold):
    """标记奖励已领取"""
    claimed = load_claimed_records()
    if str(user_id) not in claimed:
        claimed[str(user_id)] = []
    claimed[str(user_id)].append(str(threshold))
    save_claimed_records(claimed)

handle_invitation_use = on_command("邀请码", priority=5, block=True)
@handle_invitation_use.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_use(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """使用邀请码"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info['user_id']
    inviter_id = args.extract_plain_text().strip()
    if not inviter_id:
        msg = "请输入邀请人的ID！格式：邀请码 [邀请人ID]"
        await handle_send(bot, event, msg)
        return
    if str(user_id) == inviter_id:
        msg = "不能邀请自己！"
        await handle_send(bot, event, msg)
        return
    inviter_info = sql_message.get_user_info_with_id(inviter_id)
    if not inviter_info:
        msg = "邀请人不存在！"
        await handle_send(bot, event, msg)
        return
    success = add_invitation_record(inviter_id, user_id)
    if not success:
        msg = "邀请记录添加失败，可能已经邀请过该用户！"
        await handle_send(bot, event, msg)
        return
    msg = f"成功绑定邀请人！您的邀请人是：{inviter_info['user_name']}(ID:{inviter_id})"
    await handle_send(bot, event, msg)

handle_invitation_check = on_command("邀请人", priority=5, block=True)
@handle_invitation_check.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_check(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看邀请人信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info['user_id']
    inviter_id = get_inviter_id(user_id)
    if not inviter_id:
        msg = "您还没有填写邀请码！"
        await handle_send(bot, event, msg)
        return
    inviter_info = sql_message.get_user_info_with_id(inviter_id)
    if not inviter_info:
        msg = "邀请人信息不存在！"
        await handle_send(bot, event, msg)
        return
    msg = f"您的邀请人是：{inviter_info['user_name']}(ID:{inviter_id})"
    await handle_send(bot, event, msg)

handle_invitation_info = on_command("我的邀请", priority=5, block=True)
@handle_invitation_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看我的邀请信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info['user_id']
    count = get_user_invitation_count(user_id)
    rewards = load_invitation_rewards()
    claimed = load_claimed_records().get(str(user_id), [])
    available_rewards = []
    for threshold_str in sorted(rewards.keys(), key=lambda x: int(x)):
        threshold = int(threshold_str)
        if count >= threshold and threshold_str not in claimed:
            available_rewards.append(threshold)
    msg = [
        f"☆------我的邀请信息------☆",
        f"邀请人数：{count}人",
        f"可领取奖励：{', '.join(map(str, available_rewards)) if available_rewards else '无'}"
    ]
    await handle_send(bot, event, "\n".join(msg))

handle_invitation_claim = on_command("邀请奖励领取", priority=5, block=True)
@handle_invitation_claim.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_claim(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """领取邀请奖励"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    count = get_user_invitation_count(user_id)
    rewards = load_invitation_rewards()
    if not rewards:
        msg = "目前没有设置任何邀请奖励！"
        await handle_send(bot, event, msg)
        return
    if not arg:
        claimed_any = False
        reward_msgs = []
        for threshold_str in sorted(rewards.keys(), key=lambda x: int(x)):
            threshold = int(threshold_str)
            if count >= threshold and not has_claimed_reward(user_id, threshold):
                reward_items = rewards[threshold_str]
                for item in reward_items:
                    if item["type"] == "stone":
                        sql_message.update_ls(user_id, item["quantity"], 1)
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
                mark_reward_claimed(user_id, threshold)
                claimed_any = True
                items_msg = []
                for item in reward_items:
                    if item["type"] == "stone":
                        items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
                    else:
                        items_msg.append(f"{item['name']} x{item['quantity']}")
                reward_msgs.append(f"邀请{threshold}人奖励：{', '.join(items_msg)}")
        if claimed_any:
            msg = f"成功领取以下奖励：\n" + "\n".join(reward_msgs)
        else:
            msg = "没有可领取的奖励！"
        await handle_send(bot, event, msg)
        return
    try:
        threshold = int(arg)
        if threshold <= 0:
            raise ValueError
    except ValueError:
        msg = "门槛人数必须是正整数！"
        await handle_send(bot, event, msg)
        return
    if str(threshold) not in rewards:
        msg = f"没有设置邀请{threshold}人的奖励！"
        await handle_send(bot, event, msg)
        return
    if count < threshold:
        msg = f"您的邀请人数不足{threshold}人，当前只有{count}人！"
        await handle_send(bot, event, msg)
        return
    if has_claimed_reward(user_id, threshold):
        msg = f"您已经领取过邀请{threshold}人的奖励！"
        await handle_send(bot, event, msg)
        return
    reward_items = rewards[str(threshold)]
    for item in reward_items:
        if item["type"] == "stone":
            sql_message.update_ls(user_id, item["quantity"], 1)
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
    mark_reward_claimed(user_id, threshold)
    items_msg = []
    for item in reward_items:
        if item["type"] == "stone":
            items_msg.append(f"{item['name']} x{number_to(item['quantity'])}")
        else:
            items_msg.append(f"{item['name']} x{item['quantity']}")
    msg = f"成功领取邀请{threshold}人奖励：\n{', '.join(items_msg)}"
    await handle_send(bot, event, msg)

handle_invitation_set_reward = on_command("邀请奖励设置", permission=SUPERUSER, priority=5, block=True)
@handle_invitation_set_reward.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_set_reward(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    """设置邀请奖励"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg_str = args.extract_plain_text().strip()
    parts = arg_str.split(maxsplit=1)
    if len(parts) < 2:
        msg = "格式错误！正确格式：邀请奖励设置 [门槛人数] [奖励物品]\n示例：邀请奖励设置 5 渡厄丹x5,灵石x10000000"
        await handle_send(bot, event, msg)
        return
    try:
        threshold = int(parts[0])
        if threshold <= 0:
            raise ValueError
    except ValueError:
        msg = "门槛人数必须是正整数！"
        await handle_send(bot, event, msg)
        return
    items_str = parts[1]
    items_list = get_item_list(items_str, items)
    if not items_list:
        msg = "未指定有效的奖励物品！"
        await handle_send(bot, event, msg)
        return
    rewards = load_invitation_rewards()
    rewards[str(threshold)] = items_list
    save_invitation_rewards(rewards)
    items_msg = create_item_message(items_list)
    msg = f"成功设置邀请{threshold}人的奖励：\n{', '.join(items_msg)}"
    await handle_send(bot, event, msg)

handle_invitation_reward_list = on_command("邀请奖励列表", priority=5, block=True)
@handle_invitation_reward_list.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_reward_list(bot: Bot, event: MessageEvent):
    """查看邀请奖励列表"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    rewards = load_invitation_rewards()
    if not rewards:
        msg = "当前没有设置任何邀请奖励"
        await handle_send(bot, event, msg)
        return
    msg_lines = [
        "🎁 邀请奖励列表 🎁",
        "====================",
    ]
    sorted_thresholds = sorted([int(k) for k in rewards.keys()])
    for threshold in sorted_thresholds:
        threshold_str = str(threshold)
        reward_items = rewards[threshold_str]
        items_msg = create_item_message(reward_items)
        msg_lines.extend([
            f"🎯 门槛: 邀请{threshold}人",
            f"🎁 奖励内容: {', '.join(items_msg)}",
            "------------------"
        ])
    msg = "\n".join(msg_lines)
    await handle_send(bot, event, msg)

# ════════════
# 邀请帮助
# ════════════

invitation_help_cmd = on_command("邀请帮助", priority=7, block=True)
invitation_admin_help_cmd = on_command("邀请管理", permission=SUPERUSER, priority=5, block=True)

@invitation_help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """邀请帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, __invitation_help__)
    await invitation_help_cmd.finish()

@invitation_admin_help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_invitation_admin_help(bot: Bot, event: MessageEvent):
    """邀请管理帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, __invitation_admin_help__)
    await invitation_admin_help_cmd.finish()

__invitation_help__ = f"""
🤝 邀请系统帮助 🤝
═════════════
1. 邀请码 [ID] - 填写邀请人的ID
2. 邀请人 - 查看自己的邀请人信息
3. 我的邀请 - 查看自己的邀请信息
4. 邀请奖励列表 - 查看所有邀请奖励设置
5. 邀请奖励领取 [门槛] - 领取邀请奖励
   - 不填门槛：领取所有可领取的奖励
   - 填写门槛：领取指定   门槛的奖励
6. 邀请奖励设置 [门槛人数] [奖励物品] - 设置邀请奖励（仅管理员）
   - 示例：邀请奖励设置 5 渡厄丹x5,灵石x10000000
"""

__invitation_admin_help__ = f"""
👑 邀请系统管理帮助 👑
═════════════════
1. 邀请奖励设置 [门槛人数] [奖励物品] - 设置邀请奖励
   - 示例：邀请奖励设置 5 渡厄丹x5,灵石x10000000
2. 邀请奖励列表 - 查看所有邀请奖励设置
"""

# ════════════
# 自动清理任务
# ════════════

async def auto_clean_expired_items():
    """自动清理过期"""
    clean_expired_items()
    logger.info("自动清理过期任务执行完成")
