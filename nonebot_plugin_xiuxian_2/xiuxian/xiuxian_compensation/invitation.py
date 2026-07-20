import time
from pathlib import Path

from ..on_compat import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, Message, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.json_store import load_json_file, save_json_file
from ..xiuxian_utils.utils import check_user, send_msg_handler, handle_send, number_to, send_help_message
from ..xiuxian_config import XiuConfig
from ...paths import get_paths

from .common import (
    DATA_PATH,
    sql_message,
    get_item_list,
    create_item_message,
)
from .transaction_service import InvitationRewardClaimService

INVITATION_DATA_PATH = DATA_PATH / "invitation_data"
INVITATION_REWARDS_FILE = INVITATION_DATA_PATH / "invitation_rewards.json"
INVITATION_RECORDS_FILE = INVITATION_DATA_PATH / "invitation_records.json"
INVITATION_CLAIMED_FILE = INVITATION_DATA_PATH / "invitation_claimed.json"
invitation_reward_service = InvitationRewardClaimService(get_paths().game_db)

INVITATION_DATA_PATH.mkdir(parents=True, exist_ok=True)


def init_file(path: Path):
    if not path.exists():
        save_json_file(path, {})


init_file(INVITATION_REWARDS_FILE)
init_file(INVITATION_RECORDS_FILE)
init_file(INVITATION_CLAIMED_FILE)


def load_json(path: Path):
    return load_json_file(path, {}, dict)


def save_json(path: Path, data):
    save_json_file(path, data)


def load_invitation_rewards():
    return load_json(INVITATION_REWARDS_FILE)


def save_invitation_rewards(data):
    save_json(INVITATION_REWARDS_FILE, data)


def load_invitation_records():
    return load_json(INVITATION_RECORDS_FILE)


def save_invitation_records(data):
    save_json(INVITATION_RECORDS_FILE, data)


def load_claimed_records():
    return load_json(INVITATION_CLAIMED_FILE)


def get_user_invitation_count(inviter_id):
    records = load_invitation_records()
    return len(records.get(str(inviter_id), []))


def add_invitation_record(inviter_id, invited_id):
    records = load_invitation_records()

    inviter_id = str(inviter_id)
    invited_id = str(invited_id)

    if inviter_id not in records:
        records[inviter_id] = []

    if invited_id in records[inviter_id]:
        return False

    records[inviter_id].append(invited_id)
    save_invitation_records(records)

    return True


def has_invitation_code(user_id):
    records = load_invitation_records()

    for _, invited_list in records.items():
        if str(user_id) in invited_list:
            return True

    return False


def get_inviter_id(user_id):
    records = load_invitation_records()

    for inviter_id, invited_list in records.items():
        if str(user_id) in invited_list:
            return inviter_id

    return None


invitation_use_cmd = on_command("邀请码", priority=5, block=True)
invitation_check_cmd = on_command("邀请人", priority=5, block=True)
invitation_info_cmd = on_command("我的邀请", priority=5, block=True)
invitation_claim_cmd = on_command("邀请奖励领取", priority=5, block=True)
invitation_reward_list_cmd = on_command("邀请奖励列表", priority=5, block=True)
invitation_help_cmd = on_command("邀请帮助", priority=7, block=True)

invitation_set_reward_cmd = on_command("邀请奖励设置", permission=SUPERUSER, priority=5, block=True)
invitation_admin_help_cmd = on_command("邀请管理", permission=SUPERUSER, priority=5, block=True)


def _invitation_operation_id(event, user_id: str) -> str:
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    return f"invitation:claim:{user_id}:{event_id or time.time_ns()}"


@invitation_use_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    inviter_id = args.extract_plain_text().strip()

    if not inviter_id:
        await handle_send(bot, event, "请输入邀请人的ID。格式：邀请码 [邀请人ID]")
        return

    if user_id == inviter_id:
        await handle_send(bot, event, "不能邀请自己")
        return

    if has_invitation_code(user_id):
        await handle_send(bot, event, "你已经填写过邀请码，不能重复填写")
        return

    inviter_info = sql_message.get_user_info_with_id(inviter_id)

    if not inviter_info:
        await handle_send(bot, event, "邀请人不存在")
        return

    success = add_invitation_record(inviter_id, user_id)

    if not success:
        await handle_send(bot, event, "邀请记录添加失败，可能已经绑定过")
        return

    await handle_send(
        bot,
        event,
        f"成功绑定邀请人：{inviter_info['user_name']}(ID:{inviter_id})",
    )


@invitation_check_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    inviter_id = get_inviter_id(user_info["user_id"])

    if not inviter_id:
        await handle_send(bot, event, "你还没有填写邀请码")
        return

    inviter_info = sql_message.get_user_info_with_id(inviter_id)

    if not inviter_info:
        await handle_send(bot, event, "邀请人信息不存在")
        return

    await handle_send(
        bot,
        event,
        f"你的邀请人是：{inviter_info['user_name']}(ID:{inviter_id})",
    )


@invitation_info_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    count = get_user_invitation_count(user_id)
    rewards = load_invitation_rewards()
    claimed = {
        str(value) for value in load_claimed_records().get(user_id, [])
    } | {
        str(value) for value in invitation_reward_service.claimed_thresholds(user_id)
    }

    available = []

    for threshold_str in sorted(rewards.keys(), key=lambda x: int(x)):
        if count >= int(threshold_str) and threshold_str not in claimed:
            available.append(threshold_str)

    msg = [
        "我的邀请信息",
        f"邀请人数：{count}人",
        f"可领取奖励：{', '.join(available) if available else '无'}",
    ]

    await handle_send(bot, event, "\n".join(msg))


@invitation_claim_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    arg = args.extract_plain_text().strip()
    operation_id = _invitation_operation_id(event, user_id)
    # 先回放：成功后门槛已领会变 no_available，挡住同事件幂等。
    prior = invitation_reward_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        rewards = load_invitation_rewards()
        claimed_msgs = [
            f"邀请{threshold}人奖励："
            f"{', '.join(create_item_message(rewards[str(threshold)]))}"
            for threshold in prior.thresholds
            if str(threshold) in rewards
        ]
        body = "\n".join(f"- {line}" for line in claimed_msgs) if claimed_msgs else f"门槛：{', '.join(map(str, prior.thresholds))}"
        await handle_send(
            bot,
            event,
            f"邀请奖励领取成功\n{body}\n该邀请奖励请求已经处理，无需重复提交。",
        )
        return

    invitation_records = load_invitation_records()
    invited_user_ids = invitation_records.get(user_id, [])
    rewards = load_invitation_rewards()

    if not rewards:
        await handle_send(bot, event, "目前没有设置邀请奖励")
        return

    thresholds = []

    if arg:
        try:
            threshold = int(arg)
        except ValueError:
            await handle_send(bot, event, "门槛人数必须是数字")
            return

        thresholds = [threshold]
    else:
        thresholds = sorted([int(x) for x in rewards.keys()])

    result = invitation_reward_service.claim(
        operation_id=operation_id,
        user_id=user_id,
        invited_user_ids=invited_user_ids,
        rewards_by_threshold=rewards,
        requested_thresholds=thresholds,
        legacy_claimed_thresholds=load_claimed_records().get(user_id, []),
        max_goods_num=XiuConfig().max_goods_num,
    )
    if result.status == "duplicate":
        claimed_msgs = [
            f"邀请{threshold}人奖励："
            f"{', '.join(create_item_message(rewards[str(threshold)]))}"
            for threshold in result.thresholds
            if str(threshold) in rewards
        ]
        body = "\n".join(f"- {line}" for line in claimed_msgs) if claimed_msgs else f"门槛：{', '.join(map(str, result.thresholds))}"
        await handle_send(
            bot,
            event,
            f"邀请奖励领取成功\n{body}\n该邀请奖励请求已经处理，无需重复提交。",
        )
        return
    if not result.succeeded:
        await handle_send(bot, event, "没有可领取的邀请奖励")
        return

    claimed_msgs = [
        f"邀请{threshold}人奖励："
        f"{', '.join(create_item_message(rewards[str(threshold)]))}"
        for threshold in result.thresholds
    ]

    await handle_send(
        bot,
        event,
        "邀请奖励领取成功\n" + "\n".join(f"- {line}" for line in claimed_msgs),
    )


@invitation_set_reward_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    arg_str = args.extract_plain_text().strip()
    parts = arg_str.split(maxsplit=1)

    if len(parts) < 2:
        await handle_send(
            bot,
            event,
            "格式：邀请奖励设置 [门槛人数] [奖励物品]\n示例：邀请奖励设置 5 渡厄丹x5,灵石x10000000",
        )
        return

    try:
        threshold = int(parts[0])
        if threshold <= 0:
            raise ValueError
    except ValueError:
        await handle_send(bot, event, "门槛人数必须是正整数")
        return

    reward_items = get_item_list(parts[1])

    rewards = load_invitation_rewards()
    rewards[str(threshold)] = reward_items
    save_invitation_rewards(rewards)

    await handle_send(
        bot,
        event,
        f"成功设置邀请{threshold}人奖励：\n{', '.join(create_item_message(reward_items))}",
    )


@invitation_reward_list_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    rewards = load_invitation_rewards()

    if not rewards:
        await handle_send(bot, event, "当前没有设置邀请奖励")
        return

    lines = ["邀请奖励列表"]

    for threshold in sorted([int(k) for k in rewards.keys()]):
        reward_items = rewards[str(threshold)]
        lines.extend([
            "",
            f"- 门槛：邀请{threshold}人",
            f"奖励：{', '.join(create_item_message(reward_items))}",
        ])
    
    await send_msg_handler(
        bot,
        event,
        "邀请奖励列表",
        bot.self_id,
        lines,
        title="邀请奖励列表",
    )


@invitation_help_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, INVITATION_HELP,
        k1="我的邀请", v1="我的邀请",
        k2="奖励列表", v2="邀请奖励列表",
        k3="领取", v3="邀请奖励领取"
    )


@invitation_admin_help_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, INVITATION_ADMIN_HELP,
        k1="设置", v1="邀请奖励设置",
        k2="列表", v2="邀请奖励列表",
        k3="帮助", v3="邀请帮助"
    )


INVITATION_HELP = """
**邀请同修**
---
**指令**
- 邀请码 [邀请人ID]
- 邀请人 / 我的邀请
- 邀请奖励列表
- 邀请奖励领取 [门槛]

> 邀请码仅可填写一次。
> 不填门槛时，尝试尽领可领之赏。
""".strip()


INVITATION_ADMIN_HELP = """
邀请系统管理帮助

管理指令
- 邀请奖励设置 [门槛人数] [奖励物品]
- 邀请奖励列表

示例
邀请奖励设置 5 渡厄丹x5,灵石x10000000
""".strip()
