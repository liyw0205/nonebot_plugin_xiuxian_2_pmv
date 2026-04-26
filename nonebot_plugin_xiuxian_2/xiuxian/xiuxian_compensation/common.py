import json
import os
import random
import string
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union

from nonebot.log import logger

from ..adapter_compat import Bot, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user,
    handle_send,
    send_msg_handler,
    number_to,
)

items = Items()
sql_message = XiuxianDateManage()

DATA_PATH = Path(__file__).parent / "compensation_data"


DATA_CONFIG = {
    "补偿": {
        "data_path": DATA_PATH / "compensation" / "compensation_records.json",
        "claimed_path": DATA_PATH / "compensation" / "claimed_records.json",
        "records_folder": DATA_PATH / "compensation",
        "type_key": "补偿",
    },
    "礼包": {
        "data_path": DATA_PATH / "gift_package" / "gift_package_records.json",
        "claimed_path": DATA_PATH / "gift_package" / "claimed_gift_packages.json",
        "records_folder": DATA_PATH / "gift_package",
        "type_key": "礼包",
    },
    "兑换码": {
        "data_path": DATA_PATH / "redeem_code" / "redeem_codes.json",
        "claimed_path": DATA_PATH / "redeem_code" / "claimed_redeem_codes.json",
        "records_folder": DATA_PATH / "redeem_code",
        "type_key": "兑换码",
    },
}


def init_data_files():
    """初始化数据目录和文件"""
    DATA_PATH.mkdir(exist_ok=True)

    for config in DATA_CONFIG.values():
        config["records_folder"].mkdir(parents=True, exist_ok=True)

        if not config["data_path"].exists():
            with open(config["data_path"], "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=4)

        if not config["claimed_path"].exists():
            with open(config["claimed_path"], "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=4)


init_data_files()


def load_data(config: Dict[str, Any]) -> Dict[str, dict]:
    with open(config["data_path"], "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(config: Dict[str, Any], data: Dict[str, dict]):
    with open(config["data_path"], "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_claimed_data(config: Dict[str, Any]) -> Dict[str, List[str]]:
    with open(config["claimed_path"], "r", encoding="utf-8") as f:
        return json.load(f)


def save_claimed_data(config: Dict[str, Any], data: Dict[str, List[str]]):
    with open(config["claimed_path"], "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def generate_unique_id(existing_ids: List[str]) -> str:
    """生成 4-6 位随机 ID，必须包含字母和数字"""
    while True:
        length = random.randint(4, 6)
        chars = string.ascii_uppercase + string.digits
        new_id = "".join(random.choice(chars) for _ in range(length))

        if not any(c.isalpha() for c in new_id):
            continue

        if not any(c.isdigit() for c in new_id):
            continue

        if new_id not in existing_ids:
            return new_id


def parse_duration(duration_str: str, is_start_time: bool = False) -> Union[datetime, timedelta, str]:
    """
    解析时间格式。

    有效期：
    - 无限 / 0
    - 30天
    - 12小时
    - 240101，表示 2024-01-01 23:59:59

    生效期：
    - 0，立即生效
    - 10小时
    - 5天
    - 240101，表示 2024-01-01 00:00:00
    """
    try:
        duration_str = duration_str.strip()

        if duration_str.lower() in ["无限", "0"]:
            return datetime.now() if is_start_time else "无限"

        if duration_str.isdigit() and len(duration_str) == 6:
            year = int("20" + duration_str[:2])
            month = int(duration_str[2:4])
            day = int(duration_str[4:6])

            if is_start_time:
                return datetime(year, month, day, 0, 0, 0)
            else:
                return datetime(year, month, day, 23, 59, 59)

        if "小时" in duration_str:
            hours = int(duration_str.split("小时")[0])
            if is_start_time:
                return datetime.now() + timedelta(hours=hours)
            return timedelta(hours=hours)

        if "天" in duration_str:
            days = int(duration_str.split("天")[0])
            if is_start_time:
                return (datetime.now() + timedelta(days=days)).replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            return timedelta(days=days)

        raise ValueError(f"无效时间格式：{duration_str}")

    except Exception as e:
        raise ValueError(f"时间格式错误：{e}")


def get_item_list(items_str: str) -> List[Dict[str, Any]]:
    """
    解析物品字符串。

    示例：
    灵石x100000,渡厄丹x5

    注意：
    这里禁止直接发放饰品。
    """
    result = []

    for item_part in items_str.split(","):
        item_part = item_part.strip()

        if not item_part:
            continue

        if "x" in item_part:
            item_id_or_name, quantity_str = item_part.split("x", 1)
            quantity = int(quantity_str)
        else:
            item_id_or_name = item_part
            quantity = 1

        item_id_or_name = item_id_or_name.strip()

        if quantity <= 0:
            raise ValueError("物品数量必须大于 0")

        if item_id_or_name == "灵石":
            result.append({
                "type": "stone",
                "id": "stone",
                "name": "灵石",
                "quantity": quantity,
                "desc": f"获得 {number_to(quantity)} 灵石",
            })
            continue

        goods_id = None

        if item_id_or_name.isdigit():
            goods_id = int(item_id_or_name)
            item_info = items.get_data_by_item_id(goods_id)
            if not item_info:
                raise ValueError(f"物品 ID {goods_id} 不存在")
        else:
            for k, v in items.items.items():
                if item_id_or_name == v["name"]:
                    goods_id = k
                    break

            if not goods_id:
                raise ValueError(f"物品 {item_id_or_name} 不存在")

            item_info = items.get_data_by_item_id(goods_id)

        if item_info.get("item_type") == "饰品":
            raise ValueError(
                f"物品 {item_info.get('name', item_id_or_name)} 为饰品，"
                f"请不要通过补偿/礼包/兑换码直接发放饰品。"
            )

        result.append({
            "type": item_info["type"],
            "id": goods_id,
            "name": item_info["name"],
            "quantity": quantity,
            "desc": item_info.get("desc", ""),
        })

    if not result:
        raise ValueError("未指定有效物品")

    return result


def create_item_message(item_list: List[Dict[str, Any]]) -> List[str]:
    msg = []

    for item in item_list:
        if item["type"] == "stone":
            msg.append(f"{item['name']} x{number_to(item['quantity'])}")
        else:
            msg.append(f"{item['name']} x{item['quantity']}")

    return msg


def is_expired(item_info: Dict[str, Any]) -> bool:
    expire_time = item_info.get("expire_time")

    if expire_time == "无限":
        return False

    if not expire_time:
        return False

    if isinstance(expire_time, str):
        expire_time = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")

    return datetime.now() > expire_time


def is_not_started(item_info: Dict[str, Any]) -> bool:
    start_time = item_info.get("start_time")

    if not start_time:
        return False

    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    return datetime.now() < start_time


def has_claimed(user_id: str, item_id: str, config: Dict[str, Any]) -> bool:
    claimed_data = load_claimed_data(config)
    return item_id in claimed_data.get(str(user_id), [])


def mark_claimed(user_id: str, item_id: str, config: Dict[str, Any]):
    claimed_data = load_claimed_data(config)

    user_id = str(user_id)

    if user_id not in claimed_data:
        claimed_data[user_id] = []

    if item_id not in claimed_data[user_id]:
        claimed_data[user_id].append(item_id)

    save_claimed_data(config, claimed_data)


def send_reward_to_user(user_id: str, reward_items: List[Dict[str, Any]]) -> List[str]:
    """
    发放物品给用户，返回发放结果文本。
    """
    msg_parts = []

    for item in reward_items:
        if item["type"] == "stone":
            sql_message.update_ls(user_id, item["quantity"], 1)
            msg_parts.append(f"获得灵石 {number_to(item['quantity'])} 枚")
            continue

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
            1,
        )

        msg_parts.append(f"获得 {goods_name} x{quantity}")

    return msg_parts


async def create_reward_record(
    bot: Bot,
    event: MessageEvent,
    config: Dict[str, Any],
    arg_str: str,
    is_redeem_code: bool = False,
):
    """
    创建补偿 / 礼包 / 兑换码通用函数。

    补偿/礼包格式：
    新增补偿 ID 物品 原因 有效期 生效期

    兑换码格式：
    新增兑换码 CODE 物品 使用次数 有效期 生效期
    """
    parts = arg_str.split(maxsplit=4)

    if is_redeem_code:
        if len(parts) < 3:
            raise ValueError("格式：新增兑换码 兑换码 物品 使用上限 有效期 生效期")
    else:
        if len(parts) < 3:
            raise ValueError(f"格式：新增{config['type_key']} ID 物品 原因 有效期 生效期")

    record_id = parts[0]
    items_str = parts[1]
    third_arg = parts[2]

    expire_time_str = parts[3] if len(parts) >= 4 else "无限"
    start_time_str = parts[4] if len(parts) >= 5 else "0"

    data = load_data(config)

    if record_id in ["随机", "0"]:
        record_id = generate_unique_id(list(data.keys()))

    reward_items = get_item_list(items_str)

    start_time = parse_duration(start_time_str, is_start_time=True)

    expire_parsed = parse_duration(expire_time_str, is_start_time=False)

    if expire_parsed == "无限":
        expire_time = "无限"
    elif isinstance(expire_parsed, timedelta):
        expire_time = datetime.now() + expire_parsed
    elif isinstance(expire_parsed, datetime):
        expire_time = expire_parsed
    else:
        expire_time = "无限"

    if isinstance(expire_time, datetime) and isinstance(start_time, datetime):
        if start_time > expire_time:
            raise ValueError("生效时间不能晚于过期时间")

    record = {
        "items": reward_items,
        "expire_time": expire_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(expire_time, datetime) else expire_time,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(start_time, datetime) else None,
    }

    if is_redeem_code:
        try:
            usage_limit = int(third_arg)
        except ValueError:
            raise ValueError("兑换码使用上限必须是数字，0 表示无限")

        record["usage_limit"] = usage_limit
        record["used_count"] = 0
    else:
        record["reason"] = third_arg

    data[record_id] = record
    save_data(config, data)

    items_msg = create_item_message(reward_items)

    msg = f"成功新增{config['type_key']}：{record_id}\n"
    msg += f"内容：{', '.join(items_msg)}\n"

    if is_redeem_code:
        usage_text = "无限次" if record["usage_limit"] == 0 else f"{record['usage_limit']}次"
        msg += f"使用上限：{usage_text}\n"
    else:
        msg += f"原因：{record['reason']}\n"

    msg += f"有效期至：{record['expire_time']}\n"
    msg += f"生效时间：{record['start_time']}"

    await handle_send(bot, event, msg)


async def claim_normal_reward(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    config: Dict[str, Any],
    record_id: str,
):
    """
    领取补偿 / 礼包。

    注意：兑换码不要使用这个函数。
    """
    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])

    data = load_data(config)
    record = data.get(record_id)

    if not record:
        await handle_send(bot, event, f"{config['type_key']}不存在")
        return

    if is_expired(record):
        await handle_send(bot, event, f"{config['type_key']}已过期")
        return

    if is_not_started(record):
        await handle_send(bot, event, f"{config['type_key']}尚未生效，生效时间：{record.get('start_time')}")
        return

    if has_claimed(user_id, record_id, config):
        await handle_send(bot, event, f"你已经领取过该{config['type_key']}了")
        return

    reward_msg = send_reward_to_user(user_id, record["items"])
    mark_claimed(user_id, record_id, config)

    await handle_send(
        bot,
        event,
        f"成功领取{config['type_key']} {record_id}：\n" + "\n".join(reward_msg),
    )


def delete_record(record_id: str, config: Dict[str, Any]):
    data = load_data(config)

    if record_id in data:
        del data[record_id]
        save_data(config, data)

    claimed_data = load_claimed_data(config)

    for user_id in list(claimed_data.keys()):
        if record_id in claimed_data[user_id]:
            claimed_data[user_id].remove(record_id)

        if not claimed_data[user_id]:
            del claimed_data[user_id]

    save_claimed_data(config, claimed_data)


def clear_records(config: Dict[str, Any]):
    save_data(config, {})
    save_claimed_data(config, {})
    logger.info(f"已清空所有{config['type_key']}数据")


async def list_normal_rewards(
    bot: Bot,
    event: MessageEvent,
    config: Dict[str, Any],
):
    """
    展示补偿 / 礼包列表。
    兑换码列表不要用这个，因为兑换码列表需要管理员权限和使用次数展示。
    """
    data = load_data(config)

    if not data:
        await handle_send(bot, event, f"当前没有可用的{config['type_key']}")
        return

    current_time = datetime.now()

    valid = []
    not_started = []
    expired = []

    for record_id, info in data.items():
        if is_not_started(info):
            not_started.append((record_id, info))
        elif is_expired(info):
            expired.append((record_id, info))
        else:
            valid.append((record_id, info))

    lines = [
        f"📋 {config['type_key']}列表",
        "====================",
    ]

    def append_records(title: str, records: list):
        lines.append(f"\n【{title}】")

        if not records:
            lines.append("暂无")
            return

        for record_id, info in records:
            item_msg = create_item_message(info["items"])

            lines.extend([
                f"ID：{record_id}",
                f"原因：{info.get('reason', '无')}",
                f"内容：{', '.join(item_msg)}",
                f"有效期至：{info.get('expire_time')}",
                f"生效时间：{info.get('start_time')}",
                "------------------",
            ])

    append_records("有效", valid)
    append_records("尚未生效", not_started)
    append_records("过期", expired)

    lines.append(f"\n时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    await send_msg_handler(
        bot,
        event,
        f"{config['type_key']}列表",
        bot.self_id,
        lines,
        title=f"{config['type_key']}列表",
    )


def clean_expired_by_config(config: Dict[str, Any]):
    data = load_data(config)
    claimed_data = load_claimed_data(config)

    to_delete = []

    for record_id, info in data.items():
        if is_expired(info):
            to_delete.append(record_id)

    for record_id in to_delete:
        data.pop(record_id, None)

        for user_id in list(claimed_data.keys()):
            if record_id in claimed_data[user_id]:
                claimed_data[user_id].remove(record_id)

            if not claimed_data[user_id]:
                del claimed_data[user_id]

    if to_delete:
        save_data(config, data)
        save_claimed_data(config, claimed_data)
        logger.info(f"已自动清理过期{config['type_key']}：{to_delete}")


def clean_all_expired():
    for config in DATA_CONFIG.values():
        clean_expired_by_config(config)