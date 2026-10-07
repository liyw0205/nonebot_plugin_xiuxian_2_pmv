import random
import asyncio
import json
import time
from types import SimpleNamespace
from pathlib import Path
from datetime import datetime
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
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send, number_to, log_message, send_help_message
from ..xiuxian_config import XiuConfig
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from ...paths import get_paths
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from ...infrastructure.ids import UUIDGenerator
from ...features.dufang.application import DufangApplication

_sql_message_instance = None
runtime_clock = SystemClock()
runtime_random = SystemRandom()
runtime_ids = UUIDGenerator()
_player_data_manager_instance = None
dufang_application = DufangApplication(get_paths().game_db, get_paths().player_db)


def _sql_message():
    global _sql_message_instance
    if _sql_message_instance is None:
        _sql_message_instance = XiuxianDateManage()
    return _sql_message_instance


def _resolve_player_data_manager():
    global _player_data_manager_instance
    if _player_data_manager_instance is None:
        _player_data_manager_instance = PlayerDataManager()
    return _player_data_manager_instance


class _LazyPlayerDataManager:
    def __getattr__(self, name):
        return getattr(_resolve_player_data_manager(), name)


player_data_manager = _LazyPlayerDataManager()


def _player_data_manager():
    return player_data_manager


def _run_dufang_action(action, operation_id, user_id, call, **payload):
    outcome = dufang_application.execute_legacy_call(
        operation_id=operation_id,
        user_id=str(user_id),
        action=action,
        payload=payload,
        call=call,
    )
    data = dict(outcome.data or {})
    data.setdefault("status", outcome.status)
    data["succeeded"] = outcome.ok
    return SimpleNamespace(**data)


def _share_settlement_from_outcome(outcome):
    data = dict(outcome.data or {})
    data["recipients"] = [
        SimpleNamespace(**recipient) if isinstance(recipient, dict) else recipient
        for recipient in data.get("recipients", ())
    ]
    settlement = SimpleNamespace(**data)
    settlement.succeeded = outcome.ok
    settlement.status = "duplicate" if outcome.replayed else outcome.status
    return settlement


def _record_shared_settlement(user_id, user_name, settlement):
    affected_users = []
    for recipient in settlement.recipients:
        if recipient.amount <= 0:
            continue
        sign = "+" if settlement.event_type == "profit" else "-"
        affected_users.append(f"{recipient.user_name}({sign}{number_to(recipient.amount)})")
        if recipient.status != "applied":
            continue
        if settlement.event_type == "profit":
            log_message(
                recipient.user_id,
                f"受到道友{user_name}的鉴石福泽共享，获得灵石：{number_to(recipient.amount)}枚",
            )
            log_message(
                user_id,
                f"鉴石福泽共享给道友{recipient.user_name}，共享灵石：{number_to(recipient.amount)}枚",
            )
        else:
            log_message(
                recipient.user_id,
                f"受到道友{user_name}的鉴石影响，损失灵石：{number_to(recipient.amount)}枚",
            )
            log_message(
                user_id,
                f"鉴石影响波及道友{recipient.user_name}，造成损失：{number_to(recipient.amount)}枚",
            )
    return affected_users


def _shared_settlement_text(settlement, affected_users):
    if not affected_users:
        return None
    return "\n".join(
        (
            f"【{settlement.event_title}】",
            settlement.event_description,
            f"共享倍率：+{settlement.bonus_percent}%",
            f"受影响道友：{', '.join(affected_users)}",
        )
    )
PLAYERSDATA = get_paths().players
SHARING_DATA_PATH = Path(__file__).parent / "unseal_sharing.json"
BANNED_UNSEAL_IDS = XiuConfig().banned_unseal_ids  # 禁止鉴石的群

# 加载共享用户数据
def load_sharing_users():
    users = dufang_application.sharing_user_ids()
    return [] if users is None else list(users)

def save_sharing_users(users):
    return dufang_application.import_sharing_users(
        tuple(str(user_id) for user_id in users),
        runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

# 添加共享用户
def add_sharing_user(user_id):
    return dufang_application.set_sharing_enabled(
        str(user_id), True, runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")
    )

# 移除共享用户
def remove_sharing_user(user_id):
    return dufang_application.set_sharing_enabled(
        str(user_id), False, runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")
    )

# 检查是否在共享列表中
def is_sharing_user(user_id):
    return dufang_application.sharing_enabled(str(user_id)) is True

# 获取随机共享用户(排除自己)
def get_random_sharing_users(user_id, count=3):
    users = load_sharing_users()
    users = [uid for uid in users if uid != str(user_id)]
    if not users:
        return []
    count = min(count, len(users))
    return runtime_random.sample(users, count)

# 鉴石数据管理
def get_unseal_data(user_id):
    user_id = str(user_id)

    default_data = {
        "unseal_info": {
            "count": 0,
            "total_cost": 0,
            "profit": 0,
            "loss": 0
        },
        "sharing_info": {
            "shared_profit": 0,
            "shared_loss": 0,
            "received_profit": 0,
            "received_loss": 0
        },
        "last_update": runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    row = _player_data_manager().get_fields(user_id, "unseal_data")
    if not row:
        save_unseal_data(user_id, default_data)
        return default_data

    def to_int(v, d=0):
        try:
            return int(v)
        except Exception:
            return d

    data = {
        "unseal_info": {
            "count": to_int(row.get("count", 0)),
            "total_cost": to_int(row.get("total_cost", 0)),
            "profit": to_int(row.get("profit", 0)),
            "loss": to_int(row.get("loss", 0)),
        },
        "sharing_info": {
            "shared_profit": to_int(row.get("shared_profit", 0)),
            "shared_loss": to_int(row.get("shared_loss", 0)),
            "received_profit": to_int(row.get("received_profit", 0)),
            "received_loss": to_int(row.get("received_loss", 0)),
        },
        "last_update": row.get("last_update") or runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return data


def save_unseal_data(user_id, data):
    return dufang_application.import_legacy_player_stats(
        str(user_id),
        data,
        runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

# 鉴石命令
unseal = on_command("鉴石", priority=9, block=True)
unseal_share_on = on_command("鉴石共享开启", priority=10, block=True)
unseal_share_off = on_command("鉴石共享关闭", priority=10, block=True)
unseal_help = on_command("鉴石帮助", priority=10, block=True)
unseal_message = on_command("鉴石信息", priority=10, block=True)

# 鉴石帮助
@unseal_help.handle(parameterless=[Cooldown(cd_time=0)])
async def unseal_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    help_msg = """
**鉴石帮助**
---
**指令**
- 鉴石
> 消耗灵石鉴定物品
- 鉴石共享开启
> 开启鉴石结果共享
- 鉴石共享关闭
> 关闭鉴石结果共享
- 鉴石信息
> 查看鉴石统计

> 基础消耗100万灵石；可追加灵石，最多不超过当前灵石的10%。
> 共享开启后，你的结果可能影响其他已开共享的道友。
"""
    await send_help_message(bot, event, help_msg, k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")

# 共享开启
@unseal_share_on.handle(parameterless=[Cooldown(cd_time=0)])
async def unseal_share_on_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    changed = add_sharing_user(user_id)
    if changed is None:
        await handle_send(bot, event, "鉴石共享配置尚未就绪，请先完成数据迁移。", md_type="鉴石")
        return
    if not changed:
        msg = "你已经开启了鉴石结果共享！"
    else:
        msg = "成功开启鉴石结果共享！你的鉴石过程可能会对其他道友产生影响。"
        log_message(user_id, "开启了鉴石结果共享功能")
    
    await handle_send(bot, event, msg, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="关闭", v3="鉴石共享关闭")

# 共享关闭
@unseal_share_off.handle(parameterless=[Cooldown(cd_time=0)])
async def unseal_share_off_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    changed = remove_sharing_user(user_id)
    if changed is None:
        await handle_send(bot, event, "鉴石共享配置尚未就绪，请先完成数据迁移。", md_type="鉴石")
        return
    if not changed:
        msg = "你尚未开启鉴石结果共享！"
    else:
        msg = "成功关闭鉴石结果共享！你的鉴石过程将不再影响其他道友。"
        log_message(user_id, "关闭了鉴石结果共享功能")
    
    await handle_send(bot, event, msg, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="开启", v3="鉴石共享开启")

# 处理共享事件
async def handle_shared_event(
    user_id: str,
    current_cost: int,
    total_cost: int,
    result_type: str,
    operation_id: str,
):
    """
    处理共享事件
    :param current_cost: 本次鉴石消耗的灵石
    :param total_cost: 累计鉴石总消耗
    """
    sharing_users = get_random_sharing_users(user_id, runtime_random.randint(1, 3))
    if not sharing_users:
        return None, None
    
    # 获取当前用户信息
    user_info = _sql_message().get_user_info_with_id(user_id)
    if not user_info:
        return None, None
    
    user_name = user_info['user_name']
    
    # 计算共享基数 (本次鉴石消耗×0.1)
    base_amount = int(current_cost * 0.1)
    
    # 计算消耗倍率 (每10亿增加1%，最多50%)
    cost_bonus = min(total_cost // 1000000000, 50) / 100
    
    # 最终影响值 = 基数 × (1 + 倍率)
    effect_amount = int(base_amount * (1 + cost_bonus))
    
    # 根据鉴石结果类型选择共享事件类型
    if result_type in ["great_success", "success"]:
        event_type = "profit"  # 正面事件
        eligible_events = [e for e in SHARING_EVENTS if "福泽" in e["title"]]
    else:
        event_type = "loss"    # 负面事件
        eligible_events = [e for e in SHARING_EVENTS if "福泽" not in e["title"]]
    
    if not eligible_events:
        return None, None
    
    event_data = runtime_random.choice(eligible_events)
    
    recipients = []
    for target_id in sharing_users:
        target_info = _sql_message().get_user_info_with_id(target_id)
        if not target_info:
            continue
        recipients.append((target_id, target_info.get('user_name', '未知道友')))
    if not recipients:
        return None, None

    settled_at = runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")
    settlement_outcome = dufang_application.share_settle(
        operation_id=operation_id,
        user_id=user_id,
        event_type=event_type,
        title=event_data["title"],
        desc=event_data["desc"],
        effect_amount=effect_amount,
        cost_bonus_percent=int(cost_bonus * 100),
        recipients=recipients,
        settled_at=settled_at,
    )
    settlement = _share_settlement_from_outcome(settlement_outcome)
    if not settlement.succeeded:
        return None, None
    affected_users = _record_shared_settlement(user_id, user_name, settlement)
    message = _shared_settlement_text(settlement, affected_users)
    if not message:
        return None, None
    return message, settlement.total_amount

# 鉴石信息
@unseal_message.handle(parameterless=[Cooldown(cd_time=0)])
async def unseal_message_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    snapshot = dufang_application.player_stats_snapshot(user_id)
    if snapshot.status == "schema_missing":
        await handle_send(bot, event, "鉴石统计数据尚未就绪，请先完成数据迁移。", md_type="鉴石")
        return
    data = snapshot.legacy_data()
    
    msg = (
        "【鉴石统计】\n"
        f"鉴石次数：{data['unseal_info']['count']}次\n"
        f"总消耗：{number_to(data['unseal_info'].get('total_cost', 0))}灵石\n"
        f"总收益：{number_to(data['unseal_info']['profit'])}灵石\n"
        f"总损失：{number_to(data['unseal_info']['loss'])}灵石\n\n"
        "【共享统计】\n"
        f"共享福泽：{number_to(data['sharing_info']['shared_profit'])}灵石\n"
        f"共享损失：{number_to(data['sharing_info']['shared_loss'])}灵石\n"
        f"获得福泽：{number_to(data['sharing_info']['received_profit'])}灵石\n"
        f"承受损失：{number_to(data['sharing_info']['received_loss'])}灵石\n\n"
        f"最后更新：{data['last_update']}"
    )
    
    await handle_send(bot, event, msg, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")

def _draw_unseal_resolution(user_id, cost, previous_total_cost):
    entity = runtime_random.choice(SEALED_ENTITIES)
    process = runtime_random.choice(UNSEAL_PROCESS)
    result_type = runtime_random.choices(
        ["great_success", "success", "failure", "critical_failure"],
        weights=[15, 50, 30, 5],
    )[0]
    eligible_events = [
        item for item in UNSEAL_EVENTS[result_type]
        if "all" in item["type"] or entity["type"] in item["type"]
    ]
    event = runtime_random.choice(eligible_events or UNSEAL_EVENTS[result_type])
    ratio = event["effect"]()
    if result_type in {"great_success", "success"}:
        payout_outcome, gain, requested_loss = "win", int(cost * ratio), 0
    else:
        payout_outcome, gain, requested_loss = "loss", 0, int(cost * ratio)

    sharing = None
    if is_sharing_user(user_id):
        should_share = (
            result_type in {"failure", "critical_failure"} and runtime_random.random() < 0.2
        ) or result_type == "critical_failure"
        if not should_share:
            should_share = (result_type == "success" and runtime_random.random() < 0.1) or result_type == "great_success"
        if should_share:
            sharing_users = get_random_sharing_users(user_id, runtime_random.randint(1, 3))
            source_info = _sql_message().get_user_info_with_id(user_id)
            if sharing_users and source_info:
                event_type = "profit" if result_type in {"great_success", "success"} else "loss"
                eligible = [item for item in SHARING_EVENTS if ("福泽" in item["title"]) == (event_type == "profit")]
                share_event = runtime_random.choice(eligible)
                recipients = []
                for target_id in sharing_users:
                    target_info = _sql_message().get_user_info_with_id(target_id)
                    if target_info:
                        recipients.append([target_id, target_info.get("user_name", "未知道友")])
                if recipients:
                    total_cost = int(previous_total_cost) + int(cost)
                    bonus_percent = min(total_cost // 1000000000, 50)
                    sharing = {
                        "event_type": event_type,
                        "title": share_event["title"],
                        "desc": share_event["desc"],
                        "effect_amount": int(int(cost * 0.1) * (1 + bonus_percent / 100)),
                        "bonus_percent": int(bonus_percent),
                        "recipients": recipients,
                    }

    return {
        "entity": {"name": entity["name"], "desc": entity["desc"]},
        "process": process,
        "result_type": result_type,
        "event": {"title": event["title"], "desc": event["desc"], "outcome": event["outcome"]},
        "payout_outcome": payout_outcome,
        "gain": gain,
        "requested_loss": requested_loss,
        "sharing": sharing,
    }


async def _settle_frozen_share(bot, event, user_id, bet_id, sharing, settled_at):
    if not isinstance(sharing, dict) or not sharing.get("recipients"):
        return None
    share_operation_id = f"dufang-share:{bet_id}"
    if dufang_application.share_exists(share_operation_id):
        outcome = dufang_application.resume_share(
            operation_id=share_operation_id,
            user_id=user_id,
            settled_at=settled_at,
        )
    else:
        outcome = dufang_application.share_settle(
            operation_id=share_operation_id,
            user_id=user_id,
            event_type=sharing["event_type"],
            title=sharing["title"],
            desc=sharing["desc"],
            effect_amount=sharing["effect_amount"],
            cost_bonus_percent=sharing["bonus_percent"],
            recipients=sharing["recipients"],
            settled_at=settled_at,
        )
    if not outcome.ok:
        return None
    settlement = _share_settlement_from_outcome(outcome)
    source_info = _sql_message().get_user_info_with_id(user_id) or {}
    if outcome.replayed:
        sign = "+" if settlement.event_type == "profit" else "-"
        affected = [
            f"{recipient.user_name}({sign}{number_to(recipient.amount)})"
            for recipient in settlement.recipients
            if recipient.amount > 0
        ]
    else:
        affected = _record_shared_settlement(
            user_id,
            source_info.get("user_name", user_id),
            settlement,
        )
    return _shared_settlement_text(settlement, affected)


# 鉴石主逻辑
@unseal.handle(parameterless=[Cooldown(stamina_cost=20)])
async def unseal_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = str(user_info['user_id'])
    if str(send_group_id) in BANNED_UNSEAL_IDS:
        await handle_send(bot, event, "本群不可鉴石！", md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")
        return

    dufang_application.reconcile_pending(
        limit=5,
        settled_at=runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = str(user_info['user_id'])
    current_stone = int(user_info['stone'])

    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"dufang-bet:{event_id}:{user_id}" if event_id else f"dufang-bet:{user_id}:{runtime_ids.new_id()}"
    payout_operation_id = f"dufang-payout:{operation_id}"
    frozen = dufang_application.resolution(operation_id)
    if frozen.status == "schema_missing":
        await handle_send(bot, event, "鉴石暂不可用：下注数据迁移尚未就绪。", md_type="鉴石")
        return
    if frozen.status == "resolution_missing":
        await handle_send(bot, event, "本次鉴石记录缺少已保存结果，已停止自动结算以避免重抽。", md_type="鉴石")
        return

    if frozen.status == "not_found":
        if current_stone < 100000000:
            needed = 100000000 - current_stone
            msg = f"金银阁暂不接待灵石不足的道友，还需{number_to(needed)}灵石"
            await handle_send(bot, event, msg, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")
            return

        arg = args.extract_plain_text().strip()
        if arg.isdigit():
            input_stone = int(arg)
            max_stone = min(current_stone // 10, 1000000000)
            cost = min(input_stone, max_stone) if max_stone > 0 else 0
            if cost <= 0:
                cost = 1000000
        else:
            cost = 1000000
        if current_stone < cost:
            msg = f"解封需要{cost}枚灵石作为法力消耗，当前仅有{current_stone}枚！"
            await handle_send(bot, event, msg, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")
            return
        planned = dufang_application.plan_for_bet(
            operation_id,
            draw=lambda: _draw_unseal_resolution(
                user_id,
                cost,
                dufang_application.player_total_cost(user_id),
            ),
        )
        if planned.status == "schema_missing":
            await handle_send(bot, event, "鉴石暂不可用：下注数据迁移尚未就绪。", md_type="鉴石")
            return
        if planned.status == "resolution_missing":
            await handle_send(bot, event, "本次鉴石记录缺少已保存结果，已停止自动结算以避免重抽。", md_type="鉴石")
            return
        if planned.status == "not_found":
            resolution = planned.resolution
        else:
            cost = planned.cost
            resolution = planned.resolution
    else:
        cost = frozen.cost
        resolution = frozen.resolution

    if not isinstance(resolution, dict) or not isinstance(resolution.get("event"), dict):
        await handle_send(bot, event, "本次鉴石结果数据无效，已停止自动结算。", md_type="鉴石")
        return

    placed_at = runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")
    bet_outcome = dufang_application.bet(
        operation_id=operation_id,
        user_id=user_id,
        cost=cost,
        placed_at=placed_at,
        resolution=resolution,
    )
    bet_data = dict(bet_outcome.data or {})
    bet_data.setdefault("status", bet_outcome.status)
    bet_data["succeeded"] = bet_outcome.ok
    bet = SimpleNamespace(**bet_data)
    if bet.status == "schema_missing":
        await handle_send(bot, event, "鉴石暂不可用：下注数据迁移尚未就绪。", md_type="鉴石")
        return
    if bet.status == "stone_insufficient":
        await handle_send(bot, event, "灵石余额已变化，本次鉴石未下注。", md_type="鉴石")
        return
    if bet.status == "user_missing":
        await handle_send(bot, event, "未找到修仙数据，本次鉴石未下注。", md_type="我要修仙")
        return
    if not bet.succeeded:
        await handle_send(bot, event, "鉴石未结算：下注当前状态已更新。", md_type="鉴石")
        return
    cost = int(bet.cost)
    resolution = bet.resolution or resolution
    payout_outcome = dufang_application.payout(
        operation_id=payout_operation_id,
        user_id=user_id,
        bet_id=operation_id,
        settled_at=runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    payout_data = dict(payout_outcome.data or {})
    payout_data.setdefault("status", payout_outcome.status)
    payout_data["succeeded"] = payout_outcome.ok
    payout = SimpleNamespace(**payout_data)
    if payout.status == "schema_missing":
        await handle_send(bot, event, "鉴石暂不可用：派彩数据迁移尚未就绪。", md_type="鉴石")
        return
    if not payout.succeeded:
        await handle_send(bot, event, "鉴石派彩未入账或已处理，请稍后查看灵石余额。", md_type="鉴石")
        return

    payout_outcome_type = str(resolution["payout_outcome"])
    if payout_outcome_type == "win":
        effect_text = f"获得 {number_to(payout.gain)} 灵石"
        if not payout_outcome.replayed and payout.status != "duplicate":
            log_message(user_id, f"进行鉴石，消耗灵石：{number_to(cost)}枚\n鉴石成功！获得灵石：{number_to(payout.gain)}枚")
    else:
        effect_text = f"损失 {number_to(payout.loss)} 灵石"
        if not payout_outcome.replayed and payout.status != "duplicate":
            log_message(user_id, f"进行鉴石，消耗灵石：{number_to(cost)}枚\n鉴石失败！损失灵石：{number_to(payout.loss)}枚")

    entity = resolution["entity"]
    result_event = resolution["event"]
    base_msg = [
        "【发现尘封之物】",
        f"名称：{entity['name']}",
        str(entity["desc"]),
        "你开始谨慎地解封这个尘封已久的...",
        str(resolution["process"]),
    ]
    final_msg = "\n".join([
        "\n".join(base_msg),
        f"\n【{result_event['title']}】",
        str(result_event["desc"]),
        f"{result_event['outcome']}，{effect_text}",
        f"\n消耗：{number_to(cost)}灵石",
        f"当前灵石：{payout.wallet_stone}({number_to(payout.wallet_stone)})",
    ])
    await handle_send(bot, event, final_msg, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")

    share_text = await _settle_frozen_share(
        bot,
        event,
        user_id,
        operation_id,
        resolution.get("sharing"),
        runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    if share_text:
        await handle_send(bot, event, share_text, md_type="鉴石", k1="鉴石", v1="鉴石", k2="信息", v2="鉴石信息", k3="灵石", v3="灵石")


# 尘封之物类型（共20种）
SEALED_ENTITIES = [
    {"name": "上古玉简", "desc": "一块泛着微光的古老玉简，表面符文流转", "type": "传承"},
    {"name": "灵兽卵", "desc": "一枚布满奇异纹路的卵，生命气息时强时弱", "type": "灵宠"},
    {"name": "古剑残片", "desc": "一截断裂的剑尖，仍散发着凌厉剑气", "type": "法宝"},
    {"name": "封印石匣", "desc": "表面刻有九重禁制的青灰色石匣", "type": "容器"},
    {"name": "妖族大能", "desc": "一位被玄冰封印的妖族修士，面容模糊", "type": "存在"},
    {"name": "丹鼎碎片", "desc": "破损的炼丹炉残片，隐约有药香残留", "type": "器具"},
    {"name": "灵植种子", "desc": "几粒干瘪的种子，却蕴含着惊人生命力", "type": "灵材"},
    {"name": "洞府令牌", "desc": "一块青铜令牌，刻有'玄天'二字", "type": "钥匙"},
    {"name": "破损阵盘", "desc": "残缺的阵法核心，复杂纹路依稀可辨", "type": "阵法"},
    {"name": "古修遗蜕", "desc": "一具盘坐的干尸，身着古老道袍", "type": "遗物"},
    {"name": "灵泉结晶", "desc": "拳头大小的透明晶体，内含水状灵气", "type": "灵材"},
    {"name": "魔修法器", "desc": "一件血色铃铛，轻轻摇动却无声响", "type": "法宝"},
    {"name": "天外陨铁", "desc": "漆黑如墨的金属，表面有星辰纹路", "type": "材料"},
    {"name": "古佛舍利", "desc": "一颗金色骨珠，散发着祥和佛光", "type": "佛宝"},
    {"name": "龙族逆鳞", "desc": "一片巴掌大的七彩鳞片，坚硬无比", "type": "材料"},
    {"name": "鬼修魂灯", "desc": "一盏青铜古灯，灯焰呈幽绿色", "type": "鬼器"},
    {"name": "仙酿玉壶", "desc": "玲珑剔透的玉壶，壶口有灵雾缭绕", "type": "container"},
    {"name": "巫族图腾", "desc": "刻有狰狞兽首的黑色木牌", "type": "巫器"},
    {"name": "剑仙剑意", "desc": "一缕被封存的凌厉剑气", "type": "意境"},
    {"name": "时空碎片", "desc": "不规则的透明薄片，周围空间微微扭曲", "type": "奇物"}
]

# 解封结果事件（共40种不同事件）
UNSEAL_EVENTS = {
    "great_success": [
        {
            "title": "上古传承现世",
            "desc": "玉简突然大放光明，海量信息直接灌入你的识海！",
            "outcome": "你将这份完整传承复刻后高价拍卖",
            "effect": lambda: runtime_random.uniform(2.0, 2.5),  # 200%-250%收益
            "type": ["传承"]
        },
        {
            "title": "灵兽认主",
            "desc": "卵壳破裂，一只稀有灵兽破壳而出，立即与你缔结契约！",
            "outcome": "各大宗门争相出价购买这只潜力无限的灵兽",
            "effect": lambda: runtime_random.uniform(1.8, 2.3),  # 180%-230%收益
            "type": ["灵宠"]
        },
        {
            "title": "法宝认主",
            "desc": "残剑突然发出龙吟之声，化作流光融入你的丹田！",
            "outcome": "这件古宝主动认你为主，引起轰动",
            "effect": lambda: runtime_random.uniform(1.7, 2.2),  # 170%-220%收益
            "type": ["法宝"]
        },
        {
            "title": "秘境开启",
            "desc": "石匣中飞出一把钥匙，在空中划出一道空间裂隙！",
            "outcome": "你将秘境入口信息出售给修真联盟",
            "effect": lambda: runtime_random.uniform(2.2, 2.7),  # 220%-270%收益
            "type": ["容器", "钥匙"]
        },
        {
            "title": "前辈指点",
            "desc": "妖族大能苏醒后，为感谢你解封之恩传授秘法！",
            "outcome": "你将部分功法心得出售",
            "effect": lambda: runtime_random.uniform(1.6, 2.1),  # 160%-210%收益
            "type": ["存在"]
        }
    ],
    "success": [
        {
            "title": "残缺功法",
            "desc": "玉简中记载着一部残缺的上古功法",
            "outcome": "将残篇出售给收藏家",
            "effect": lambda: runtime_random.uniform(1.5, 1.8),  # 150%-180%收益
            "type": ["传承"]
        },
        {
            "title": "灵材现世",
            "desc": "解封出一批珍贵的炼器材料",
            "outcome": "炼器师们高价收购",
            "effect": lambda: runtime_random.uniform(1.5, 1.9),  # 150%-190%收益
            "type": ["灵材", "材料"]
        },
        {
            "title": "古丹方",
            "desc": "发现几张古老的丹药配方",
            "outcome": "炼丹师们争相购买",
            "effect": lambda: runtime_random.uniform(1.5, 1.8),  # 150%-180%收益
            "type": ["器具", "传承"]
        },
        {
            "title": "灵宠幼体",
            "desc": "孵化出一只普通灵兽",
            "outcome": "灵兽店老板出价收购",
            "effect": lambda: runtime_random.uniform(1.5, 1.7),  # 150%-170%收益
            "type": ["灵宠"]
        },
        {
            "title": "法器残件",
            "desc": "解封出几件尚可使用的法器",
            "outcome": "低阶修士抢购这些古物",
            "effect": lambda: runtime_random.uniform(1.5, 1.7),  # 150%-170%收益
            "type": ["法宝", "器具"]
        }
    ],
    "failure": [
        {
            "title": "禁制反噬",
            "desc": "解封时触发防御禁制，狂暴灵气将你击伤！",
            "outcome": "不得不花费灵石购买疗伤丹药",
            "effect": lambda: 0.5,  # 50%损失
            "type": ["all"]
        },
        {
            "title": "灵性尽失",
            "desc": "解封手法不当，物品灵性尽失化为凡物",
            "outcome": "白白浪费了法力",
            "effect": lambda: 0,  # 0%损失
            "type": ["all"]
        },
        {
            "title": "劫修偷袭",
            "desc": "就在你专注解封时，一伙劫修突然袭击！",
            "outcome": "被抢走部分灵石",
            "effect": lambda: 0.2,  # 20%损失
            "type": ["all"]
        },
        {
            "title": "邪气侵蚀",
            "desc": "解封过程中冒出诡异黑雾，污染了你的灵石",
            "outcome": "不得不丢弃被污染的灵石",
            "effect": lambda: 0.3,  # 30%损失
            "type": ["all"]
        },
        {
            "title": "幻境困阵",
            "desc": "陷入物品自带的幻阵，耗费大量法力才脱困",
            "outcome": "修为损耗严重",
            "effect": lambda: 0.1,  # 10%损失
            "type": ["all"]
        }
    ],
    "critical_failure": [
        {
            "title": "魔头出世",
            "desc": "不慎释放出被封印的千年魔头！天地为之变色！",
            "outcome": "魔头抢走你全部灵石后扬长而去",
            "effect": lambda: 1.0,  # 100%损失
            "type": ["存在", "鬼器", "魔器"]
        },
        {
            "title": "古老诅咒",
            "desc": "触发物品上的恶毒诅咒，厄运缠身！",
            "outcome": "花费巨资请高人解咒",
            "effect": lambda: 0.8,  # 80%损失
            "type": ["遗物", "巫器"]
        },
        {
            "title": "灵气暴走",
            "desc": "引发恐怖的灵气风暴，摧毁了周围一切！",
            "outcome": "赔偿损失耗尽积蓄",
            "effect": lambda: 0.9,  # 90%损失
            "type": ["奇物", "意境"]
        },
        {
            "title": "时空乱流",
            "desc": "被卷入狂暴的时空裂隙，九死一生才逃脱！",
            "outcome": "疗伤花费巨大",
            "effect": lambda: 0.7,  # 70%损失
            "type": ["时空碎片"]
        },
        {
            "title": "宗门追责",
            "desc": "解封的物品竟是某大宗门失窃的至宝！",
            "outcome": "被迫交出全部身家作为赔偿",
            "effect": lambda: 1.0,  # 100%损失
            "type": ["佛宝", "传承"]
        }
    ]
}

# 解封过程描述（20种）
UNSEAL_PROCESS = [
    "周围灵气突然剧烈波动，古老符文在空中若隐若现...",
    "一股强大的威压让你呼吸困难，解封过程异常艰难...",
    "物品表面开始浮现出复杂的道纹，闪烁着奇异光芒...",
    "耳边响起神秘的低语，仿佛来自远古的呼唤...",
    "解封法诀打出后，物品突然悬浮到半空中...",
    "周围的温度急剧下降，呼出的气息都凝结成了白霜...",
    "地面微微震颤，仿佛有什么可怕的存在正在苏醒...",
    "你的法力如潮水般被物品吸收，几乎要被抽干...",
    "物品突然发出刺目的强光，让你不得不闭上眼睛...",
    "时间仿佛在这一刻变得异常缓慢，每一个动作都无比费力...",
    "解封过程中，你恍惚看到了远古战场的幻象...",
    "物品周围的空间开始扭曲变形，产生细小的裂痕...",
    "一股沁人心脾的异香突然弥漫开来，让人精神一振...",
    "你的神识被拉入一个奇异空间，面对着一个古老意志...",
    "解封到关键时刻，天空突然乌云密布，雷声隆隆...",
    "物品表面渗出暗红色的液体，如同鲜血般诡异...",
    "四周突然陷入绝对的黑暗，连神识都无法感知...",
    "你感受到一股充满恶意的视线正从物品内部窥视着你...",
    "解封法诀引发了小型灵气漩涡，周围的物品都被卷起...",
    "物品突然发出刺耳的尖啸声，几乎要震破耳膜..."
]

# 共享事件类型
SHARING_EVENTS = [
    {
        "title": "劫修团伙",
        "desc": "你解封时引发的灵气波动引来了劫修团伙！",
        "effect": lambda cost: int(cost * runtime_random.uniform(0.4, 0.6)),  # 40%-60%损失
        "message": "这群劫修顺着灵气波动又袭击了附近的其他道友！"
    },
    {
        "title": "灵气污染",
        "desc": "解封过程中产生了危险的灵气污染！",
        "effect": lambda cost: int(cost * runtime_random.uniform(0.3, 0.5)),  # 30%-50%损失
        "message": "污染的灵气扩散开来，影响了附近修炼的其他道友！"
    },
    {
        "title": "诅咒蔓延",
        "desc": "物品上的古老诅咒开始向外扩散！",
        "effect": lambda cost: int(cost * runtime_random.uniform(0.35, 0.55)),  # 35%-55%损失
        "message": "诅咒之力蔓延，不幸波及了附近的其他道友！"
    },
    {
        "title": "福泽共享",
        "desc": "解封产生的祥瑞之气扩散开来！",
        "effect": lambda cost: int(cost * runtime_random.uniform(0.3, 0.5)),  # 30%-50%收益
        "message": "祥瑞之气惠及了附近的其他道友！"
    }
]

unseal_migrate = on_command("同步鉴石", permission=SUPERUSER, priority=25, block=True)


def _migrate_unseal_data_sync(players_dir: Path, sharing_data_path: Path) -> tuple[int, int, int]:
    total = 0
    ok = 0
    fail = 0

    if not players_dir.exists():
        return total, ok, fail

    for user_dir in players_dir.iterdir():
        if not user_dir.is_dir():
            continue
        total += 1
        user_id = user_dir.name
        file_path = user_dir / "unseal_data.json"
        if not file_path.exists():
            continue

        try:
            content = file_path.read_text(encoding="utf-8").strip()
            if not content:
                continue
            raw = json.loads(content)
            if not isinstance(raw, dict):
                raise TypeError("鉴石数据根节点必须是对象")
            unseal_info = raw.get("unseal_info", {})
            sharing_info = raw.get("sharing_info", {})
            if not isinstance(unseal_info, dict) or not isinstance(sharing_info, dict):
                raise TypeError("鉴石统计节点必须是对象")
            data = {
                "unseal_info": {
                    "count": int(unseal_info.get("count", 0)),
                    "total_cost": int(unseal_info.get("total_cost", 0)),
                    "profit": int(unseal_info.get("profit", 0)),
                    "loss": int(unseal_info.get("loss", 0)),
                },
                "sharing_info": {
                    "shared_profit": int(sharing_info.get("shared_profit", 0)),
                    "shared_loss": int(sharing_info.get("shared_loss", 0)),
                    "received_profit": int(sharing_info.get("received_profit", 0)),
                    "received_loss": int(sharing_info.get("received_loss", 0)),
                },
                "last_update": raw.get("last_update", runtime_clock.now().strftime("%Y-%m-%d %H:%M:%S")),
            }
            if save_unseal_data(user_id, data) is None:
                raise RuntimeError("鉴石统计数据 schema 尚未就绪")
            ok += 1
        except (OSError, json.JSONDecodeError, TypeError, ValueError, AttributeError, RuntimeError) as exc:
            fail += 1
            logger.warning(f"鉴石数据同步失败 {user_id}: {exc}")

    try:
        if sharing_data_path.exists():
            content = sharing_data_path.read_text(encoding="utf-8").strip()
            if content:
                sharing_data = json.loads(content)
                if not isinstance(sharing_data, dict):
                    raise TypeError("旧共享名单根节点必须是对象")
                users = sharing_data.get("users", [])
                if isinstance(users, list):
                    if save_sharing_users(users) is None:
                        raise RuntimeError("鉴石共享配置 schema 尚未就绪")
                else:
                    raise TypeError("旧共享名单 users 必须是列表")
    except (OSError, json.JSONDecodeError, TypeError, AttributeError, RuntimeError) as exc:
        fail += 1
        logger.warning(f"鉴石旧共享名单同步失败: {exc}")

    return total, ok, fail


@unseal_migrate.handle(parameterless=[Cooldown(cd_time=0)])
async def unseal_migrate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    players_dir = get_paths().players
    if not players_dir.exists():
        await handle_send(bot, event, "未找到players目录，无需同步。")
        return

    total, ok, fail = await asyncio.to_thread(
        _migrate_unseal_data_sync, players_dir, SHARING_DATA_PATH
    )

    await handle_send(bot, event, f"鉴石同步完成！扫描:{total}，成功:{ok}，失败:{fail}")
