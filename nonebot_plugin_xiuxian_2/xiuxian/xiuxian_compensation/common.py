import string
from types import SimpleNamespace
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union

from nonebot.log import logger
from ...paths import get_paths
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from ...infrastructure.ids import UUIDGenerator
from ...features.compensation.application import CompensationApplication
from ...features.base.economy_application import PlayerEconomyApplication
from ...features.base.inventory_application import PlayerInventoryApplication

from ..adapter_compat import Bot, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig
from .transaction_service import CompensationDefinitionService
from ..xiuxian_utils.utils import (
    check_user,
    handle_send,
    send_msg_handler,
    number_to,
)

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

_compensation_definition_service_instance = None
_compensation_application_instance = None
_item_catalog_instance = None
_sql_message_instance = None
_economy_application_instance = None
_inventory_application_instance = None


def _sql_message():
    global _sql_message_instance
    if _sql_message_instance is None:
        from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage

        _sql_message_instance = XiuxianDateManage()
    return _sql_message_instance


def _economy_application(database=None):
    global _economy_application_instance
    database = database or get_paths().game_db
    if (
        _economy_application_instance is None
        or str(_economy_application_instance.repository.database) != str(database)
    ):
        _economy_application_instance = PlayerEconomyApplication(database)
    return _economy_application_instance


def _inventory_application(database=None):
    global _inventory_application_instance
    database = database or get_paths().game_db
    if (
        _inventory_application_instance is None
        or str(_inventory_application_instance.repository.database) != str(database)
    ):
        _inventory_application_instance = PlayerInventoryApplication(database)
    return _inventory_application_instance


def _compensation_application(database=None):
    global _compensation_application_instance
    database = database or get_paths().game_db
    if _compensation_application_instance is None or str(_compensation_application_instance.database) != str(database):
        _compensation_application_instance = CompensationApplication(database)
    return _compensation_application_instance


def _compensation_definition_service():
    global _compensation_definition_service_instance
    if _compensation_definition_service_instance is None:
        _compensation_definition_service_instance = CompensationDefinitionService(
            get_paths().game_db,
            DATA_CONFIG["补偿"]["data_path"],
            DATA_CONFIG["补偿"]["claimed_path"],
        )
    return _compensation_definition_service_instance
runtime_clock = SystemClock()
runtime_random = SystemRandom()
runtime_ids = UUIDGenerator()


def _item_catalog():
    global _item_catalog_instance
    if _item_catalog_instance is None:
        _item_catalog_instance = Items()
    return _item_catalog_instance


def _run_compensation_action(
    action: str,
    operation_id: str,
    user_id: str,
    call,
    database=None,
    ledger_payload=None,
    **payload,
):
    """Route one historical compensation mutation through the new boundary."""
    # Resolve the database from the active legacy service.  Tests and runtime
    # migrations may replace that service with one backed by a temporary or
    # alternate catalog; the idempotency ledger must follow the same store.
    database = database or getattr(_compensation_definition_service(), "_database", None)
    compensation_application = CompensationApplication(database or get_paths().game_db)

    def invoke():
        result = call()
        # Historical JSON writers mutate in place and return None on success.
        return {"status": "applied"} if result is None else result

    outcome = compensation_application.execute_legacy_call(
        operation_id=str(operation_id),
        user_id=str(user_id),
        action=action,
        payload=payload,
        ledger_payload=ledger_payload,
        call=invoke,
    )
    data = dict(outcome.data or {})
    data.setdefault("status", outcome.status)
    data["succeeded"] = outcome.ok
    return SimpleNamespace(**data)


def load_data(config: Dict[str, Any]) -> Dict[str, dict]:
    if config["type_key"] == "补偿":
        return _compensation_definition_service().list()
    return _compensation_application().reward_definitions(config["type_key"])


def get_reward_definition(config: Dict[str, Any], record_id: str):
    if config["type_key"] == "补偿":
        return _compensation_definition_service().list().get(record_id)
    return _compensation_application().reward_definition(
        config["type_key"], record_id
    )


def save_data(config: Dict[str, Any], data: Dict[str, dict]):
    if config["type_key"] == "补偿":
        _compensation_definition_service().sync(data)
        return
    raise RuntimeError("gift and redeem definitions must be changed through CompensationApplication")


def upsert_reward_definition(
    config: Dict[str, Any],
    operation_id: str,
    request_identity: str,
    record_id: str,
    record: Dict[str, Any],
):
    return _compensation_application().upsert_reward_definition(
        operation_id,
        config["type_key"],
        record_id,
        request_identity,
        record,
        expected_version=record.get("_definition_version"),
    )


def load_claimed_data(config: Dict[str, Any]) -> Dict[str, List[str]]:
    if config["type_key"] == "补偿":
        return _compensation_definition_service().claimed_data()
    return _compensation_application().list_claims(config["type_key"])


def get_claim_count(config: Dict[str, Any], record_id: str) -> int:
    return _compensation_application().get_claim_count(
        config["type_key"], record_id
    )


def get_reward_used_count(config: Dict[str, Any], record_id: str, legacy=0) -> int:
    return _compensation_application().get_used_count(
        config["type_key"], record_id, legacy
    )


def save_claimed_data(config: Dict[str, Any], data: Dict[str, List[str]]):
    raise RuntimeError("reward claims must be changed through CompensationApplication")


def generate_unique_id(existing_ids: List[str]) -> str:
    """生成 4-6 位随机 ID，必须包含字母和数字"""
    while True:
        length = runtime_random.randint(4, 6)
        chars = string.ascii_uppercase + string.digits
        new_id = "".join(runtime_random.choice(chars) for _ in range(length))

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
            return runtime_clock.now() if is_start_time else "无限"

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
                return runtime_clock.now() + timedelta(hours=hours)
            return timedelta(hours=hours)

        if "天" in duration_str:
            days = int(duration_str.split("天")[0])
            if is_start_time:
                return (runtime_clock.now() + timedelta(days=days)).replace(
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
    item_catalog = None

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
        if item_catalog is None:
            item_catalog = _item_catalog()

        if item_id_or_name.isdigit():
            goods_id = int(item_id_or_name)
            item_info = item_catalog.get_data_by_item_id(goods_id)
            if not item_info:
                raise ValueError(f"物品 ID {goods_id} 不存在")
        else:
            for k, v in item_catalog.items.items():
                if item_id_or_name == v["name"]:
                    goods_id = k
                    break

            if not goods_id:
                raise ValueError(f"物品 {item_id_or_name} 不存在")

            item_info = item_catalog.get_data_by_item_id(goods_id)

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

    return runtime_clock.now() > expire_time


def is_not_started(item_info: Dict[str, Any]) -> bool:
    start_time = item_info.get("start_time")

    if not start_time:
        return False

    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    return runtime_clock.now() < start_time


def has_claimed(user_id: str, item_id: str, config: Dict[str, Any]) -> bool:
    return _compensation_application().has_claimed(
        config["type_key"], item_id, str(user_id)
    )


# Historical ``_sql_message().send_back(`` writes were replaced by the inventory
# application below; keep the note outside the writer body for migration audits.
def send_reward_to_user(user_id: str, reward_items: List[Dict[str, Any]]) -> List[str]:
    """
    发放物品给用户，返回发放结果文本。
    """
    msg_parts = []

    for item in reward_items:
        if item["type"] == "stone":
            result = _economy_application().grant_stone(user_id, item["quantity"])
            if not result.succeeded:
                continue
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

        result = _inventory_application().grant_item(
            user_id,
            goods_id,
            goods_name,
            goods_type_item,
            quantity,
            bind_flag=1,
            max_goods_num=int(XiuConfig().max_goods_num),
        )
        if result.succeeded and result.applied > 0:
            msg_parts.append(f"获得 {goods_name} x{result.applied}")

    return msg_parts


def format_reward_delivery(reward_items: List[Dict[str, Any]]) -> List[str]:
    """Format the existing delivery messages without mutating player assets."""
    return [
        f"获得灵石 {number_to(item['quantity'])} 枚"
        if item["type"] == "stone"
        else f"获得 {item['name']} x{item['quantity']}"
        for item in reward_items
    ]


def _compensation_upsert_operation_id(event: MessageEvent) -> str:
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    admin_id = str(event.get_user_id()).strip()
    return (
        f"compensation-upsert:{event_id}:{admin_id}"
        if event_id
        else f"compensation-upsert:{runtime_ids.new_id()}:{admin_id}"
    )


def _compensation_operation_id(event: MessageEvent, action: str, target: str = "") -> str:
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    user_id = str(event.get_user_id()).strip()
    suffix = f":{target}" if target else ""
    return f"compensation:{action}:{event_id or runtime_ids.new_id()}:{user_id}{suffix}"


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

    request_identity = arg_str.strip()
    operation_id = _compensation_operation_id(event, "create", parts[0])
    replay = None
    if config["type_key"] == "补偿":
        operation_id = _compensation_upsert_operation_id(event)
        replay = _compensation_definition_service().replay_upsert(
            operation_id, request_identity
        )
        if replay is not None and replay.status == "operation_conflict":
            raise ValueError("同一消息事件不能使用不同的补偿参数")
    else:
        replay = _compensation_application().replay_reward_definition_upsert(
            operation_id, config["type_key"], request_identity
        )
        if replay is not None and replay.status == "operation_conflict":
            raise ValueError("同一消息事件不能使用不同的奖励参数")
        if replay is not None and not replay.succeeded:
            raise ValueError("奖励定义操作仍在处理中或未成功")

    record_id = parts[0]
    items_str = parts[1]
    third_arg = parts[2]

    expire_time_str = parts[3] if len(parts) >= 4 else "无限"
    start_time_str = parts[4] if len(parts) >= 5 else "0"

    if replay is not None:
        record_id = replay.record_id
        record = dict(replay.record or {})
        reward_items = list(record.get("items") or [])
    else:
        data = load_data(config)

        if record_id in ["随机", "0"]:
            record_id = generate_unique_id(list(data.keys()))

        reward_items = get_item_list(items_str)

        start_time = parse_duration(start_time_str, is_start_time=True)

        expire_parsed = parse_duration(expire_time_str, is_start_time=False)

        if expire_parsed == "无限":
            expire_time = "无限"
        elif isinstance(expire_parsed, timedelta):
            expire_time = runtime_clock.now() + expire_parsed
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
            "create_time": runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(start_time, datetime) else None,
        }

        if is_redeem_code:
            try:
                usage_limit = int(third_arg)
            except ValueError:
                raise ValueError("兑换码使用上限必须是数字，0 表示无限")

            record["usage_limit"] = usage_limit
        else:
            record["reason"] = third_arg

        if config["type_key"] == "补偿":
            expected_version = (
                data[record_id].get("_definition_version")
                if record_id in data
                else None
            )
            result = _run_compensation_action(
                "definition_upsert",
                operation_id,
                str(event.get_user_id()),
                lambda: _compensation_definition_service().upsert(
                    operation_id,
                    request_identity,
                    record_id,
                    record,
                    expected_version,
                ),
                database=getattr(_compensation_definition_service(), "_database", None),
                record_id=record_id,
                request_identity=request_identity,
                expected_version=expected_version,
            )
            if result.status == "operation_conflict":
                raise ValueError("同一消息事件不能使用不同的补偿参数")
            if not result.succeeded:
                raise ValueError("补偿定义已变化，请重新执行新增")
            record_id = result.record_id
            record = dict(result.record or {})
            reward_items = list(record.get("items") or [])
        else:
            expected_version = (
                data[record_id].get("_definition_version")
                if record_id in data
                else None
            )
            result = _compensation_application().upsert_reward_definition(
                operation_id,
                config["type_key"],
                record_id,
                request_identity=request_identity,
                record=record,
                expected_version=expected_version,
            )
            if not result.succeeded:
                raise ValueError(f"{config['type_key']}定义写入失败：{result.status}")
            record_id = result.record_id
            record = dict(result.record or {})
            reward_items = list(record.get("items") or [])

    items_msg = create_item_message(reward_items)

    msg = f"{config['type_key']}已创建\n"
    msg += f"ID：{record_id}\n"
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

    if config["type_key"] == "补偿":
        record = load_data(config).get(record_id)
    else:
        record = _compensation_application().reward_definition(
            config["type_key"], record_id
        )

    if not record:
        # 已领后定义被删时仍允许服务层 duplicate 回放（若 claim 表有记录）
        if _compensation_application().has_claimed(config["type_key"], record_id, user_id):
            await handle_send(
                bot,
                event,
                f"你已经领取过该{config['type_key']}了\n该领取请求已经处理，无需重复提交。",
            )
            return
        await handle_send(bot, event, f"{config['type_key']}不存在")
        return

    if is_expired(record):
        if _compensation_application().has_claimed(config["type_key"], record_id, user_id):
            await handle_send(
                bot,
                event,
                f"你已经领取过该{config['type_key']}了\n该领取请求已经处理，无需重复提交。",
            )
            return
        await handle_send(bot, event, f"{config['type_key']}已过期")
        return

    if is_not_started(record):
        await handle_send(bot, event, f"{config['type_key']}尚未生效，生效时间：{record.get('start_time')}")
        return

    # 先 claim：成功后 has_claimed 会挡住同事件重放。
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"compensation:claim:{event_id or runtime_ids.new_id()}:{user_id}:{config['type_key']}:{record_id}"
    database = (
        getattr(_compensation_definition_service(), "_database", None)
        if config["type_key"] == "补偿"
        else None
    )
    result = _compensation_application(database).claim_reward(
        operation_id=operation_id,
        reward_type=config["type_key"],
        record_id=record_id,
        user_id=user_id,
        reward_items=record["items"],
        max_goods_num=XiuConfig().max_goods_num,
        expected_definition_version=record.get("_definition_version"),
    )
    if result.status == "duplicate":
        reward_msg = format_reward_delivery(record["items"])
        await handle_send(
            bot,
            event,
            f"{config['type_key']}领取成功\n"
            f"ID：{record_id}\n"
            f"奖励：\n" + "\n".join(f"- {line}" for line in reward_msg)
            + "\n该领取请求已经处理，无需重复提交。",
        )
        return
    if result.status == "user_missing":
        await handle_send(bot, event, "修仙界没有你的足迹，输入 我要修仙 加入修仙世界吧！")
        return
    if result.status in {"record_missing", "definition_changed"}:
        await handle_send(bot, event, "补偿定义已变更，请重新查询后领取")
        return
    if result.status == "schema_missing":
        await handle_send(bot, event, "补偿数据尚未就绪，请稍后重试")
        return
    if result.status != "claimed":
        await handle_send(bot, event, f"你已经领取过该{config['type_key']}了")
        return
    reward_msg = format_reward_delivery(record["items"])

    await handle_send(
        bot,
        event,
        f"{config['type_key']}领取成功\n"
        f"ID：{record_id}\n"
        f"奖励：\n" + "\n".join(f"- {line}" for line in reward_msg),
    )


def delete_record(
    record_id: str,
    config: Dict[str, Any],
    operation_id: str | None = None,
):
    if config["type_key"] == "补偿":
        definition = _compensation_definition_service().get(record_id)
        version = None if definition is None else definition.version
        operation_id = operation_id or f"compensation-delete:{record_id}:v{version or 'missing'}"
        return _run_compensation_action(
            "definition_delete",
            operation_id,
            "system",
            lambda: _compensation_definition_service().delete(operation_id, record_id, version),
            database=getattr(_compensation_definition_service(), "_database", None),
            ledger_payload={"record_id": record_id},
            record_id=record_id,
            expected_version=version,
        )

    operation_id = operation_id or f"compensation-delete:{runtime_ids.new_id()}"
    return _compensation_application().delete_reward_definition(
        operation_id, config["type_key"], record_id
    )


def clear_records(config: Dict[str, Any], operation_id: str | None = None):
    if config["type_key"] == "补偿":
        catalog_version = _compensation_definition_service().catalog_version()
        operation_id = operation_id or f"compensation-clear:{catalog_version}"
        result = _run_compensation_action(
            "definition_clear",
            operation_id,
            "system",
            lambda: _compensation_definition_service().clear(operation_id, catalog_version),
            database=getattr(_compensation_definition_service(), "_database", None),
            ledger_payload={"scope": "all"},
            expected_catalog_version=catalog_version,
        )
        logger.info(
            f"已清空所有补偿数据：定义{result.removed_definitions}条，"
            f"领取记录{result.removed_claims}条"
        )
        return result

    operation_id = operation_id or f"compensation-clear:{runtime_ids.new_id()}"
    result = _compensation_application().clear_reward_definitions(
        operation_id, config["type_key"]
    )
    if not result.succeeded:
        return result
    logger.info(f"已清空所有{config['type_key']}数据")
    return result


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

    current_time = runtime_clock.now()

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
        f"{config['type_key']}列表",
        f"更新时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}",
    ]

    def append_records(title: str, records: list):
        lines.append("")
        lines.append(f"【{title}】")

        if not records:
            lines.append("暂无")
            return

        for record_id, info in records:
            item_msg = create_item_message(info["items"])

            lines.extend([
                f"- ID：{record_id}",
                f"原因：{info.get('reason', '无')}",
                f"内容：{', '.join(item_msg)}",
                f"有效期至：{info.get('expire_time')}",
                f"生效时间：{info.get('start_time')}",
            ])

    append_records("有效", valid)
    append_records("尚未生效", not_started)
    append_records("过期", expired)

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

    if config["type_key"] == "补偿":
        deleted = []
        for record_id, info in data.items():
            if not is_expired(info):
                continue
            version = int(info["_definition_version"])
            result = _compensation_definition_service().delete(
                f"compensation-expire:{record_id}:v{version}",
                record_id,
                version,
            )
            if result.succeeded:
                deleted.append(record_id)
        if deleted:
            logger.info(f"已自动清理过期补偿：{deleted}")
        return

    deleted = []
    for record_id, info in data.items():
        if not is_expired(info):
            continue
        version = int(info["_definition_version"])
        result = _compensation_application().delete_reward_definition(
            f"compensation-expire:{config['type_key']}:{record_id}:v{version}",
            config["type_key"],
            record_id,
        )
        if result.succeeded:
            deleted.append(record_id)
    if deleted:
        logger.info(f"已自动清理过期{config['type_key']}：{deleted}")


def clean_all_expired():
    for config in DATA_CONFIG.values():
        clean_expired_by_config(config)
