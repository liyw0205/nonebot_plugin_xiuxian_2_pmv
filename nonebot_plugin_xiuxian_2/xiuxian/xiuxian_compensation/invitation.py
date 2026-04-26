import json
from pathlib import Path

from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, Message, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, send_msg_handler, handle_send, number_to

from .common import (
    DATA_PATH,
    sql_message,
    get_item_list,
    create_item_message,
    send_reward_to_user,
)

INVITATION_DATA_PATH = DATA_PATH / "invitation_data"
INVITATION_REWARDS_FILE = INVITATION_DATA_PATH / "invitation_rewards.json"
INVITATION_RECORDS_FILE = INVITATION_DATA_PATH / "invitation_records.json"
INVITATION_CLAIMED_FILE = INVITATION_DATA_PATH / "invitation_claimed.json"

INVITATION_DATA_PATH.mkdir(parents=True, exist_ok=True)


def init_file(path: Path):
    if not path.exists():
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=4)


init_file(INVITATION_REWARDS_FILE)
init_file(INVITATION_RECORDS_FILE)
init_file(INVITATION_CLAIMED_FILE)


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


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


def save_claimed_records(data):
    save_json(INVITATION_CLAIMED_FILE, data)


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


def has_claimed_reward(user_id, threshold):
    claimed = load_claimed_records()
    return str(threshold) in claimed.get(str(user_id), [])


def mark_reward_claimed(user_id, threshold):
    claimed = load_claimed_records()

    user_id = str(user_id)
    threshold = str(threshold)

    if user_id not in claimed:
        claimed[user_id] = []

    if threshold not in claimed[user_id]:
        claimed[user_id].append(threshold)

    save_claimed_records(claimed)


invitation_use_cmd = on_command("邀请码", priority=5, block=True)
invitation_check_cmd = on_command("邀请人", priority=5, block=True)
invitation_info_cmd = on_command("我的邀请", priority=5, block=True)
invitation_claim_cmd = on_command("邀请奖励领取", priority=5, block=True)
invitation_reward_list_cmd = on_command("邀请奖励列表", priority=5, block=True)
invitation_help_cmd = on_command("邀请帮助", priority=7, block=True)

invitation_set_reward_cmd = on_command("邀请奖励设置", permission=SUPERUSER, priority=5, block=True)
invitation_admin_help_cmd = on_command("邀请管理", permission=SUPERUSER, priority=5, block=True)


@invitation_use_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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


@invitation_check_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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


@invitation_info_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    count = get_user_invitation_count(user_id)
    rewards = load_invitation_rewards()
    claimed = load_claimed_records().get(user_id, [])

    available = []

    for threshold_str in sorted(rewards.keys(), key=lambda x: int(x)):
        if count >= int(threshold_str) and threshold_str not in claimed:
            available.append(threshold_str)

    msg = [
        "☆------我的邀请信息------☆",
        f"邀请人数：{count}人",
        f"可领取奖励：{', '.join(available) if available else '无'}",
    ]

    await handle_send(bot, event, "\n".join(msg))


@invitation_claim_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    arg = args.extract_plain_text().strip()

    count = get_user_invitation_count(user_id)
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

    claimed_msgs = []

    for threshold in thresholds:
        threshold_str = str(threshold)

        if threshold_str not in rewards:
            continue

        if count < threshold:
            continue

        if has_claimed_reward(user_id, threshold):
            continue

        reward_items = rewards[threshold_str]
        send_reward_to_user(user_id, reward_items)
        mark_reward_claimed(user_id, threshold)

        claimed_msgs.append(
            f"邀请{threshold}人奖励：{', '.join(create_item_message(reward_items))}"
        )

    if not claimed_msgs:
        await handle_send(bot, event, "没有可领取的邀请奖励")
        return

    await handle_send(bot, event, "成功领取以下奖励：\n" + "\n".join(claimed_msgs))


@invitation_set_reward_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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


@invitation_reward_list_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    rewards = load_invitation_rewards()

    if not rewards:
        await handle_send(bot, event, "当前没有设置邀请奖励")
        return

    lines = [
        "🎁 邀请奖励列表 🎁",
        "====================",
    ]

    for threshold in sorted([int(k) for k in rewards.keys()]):
        reward_items = rewards[str(threshold)]
        lines.extend([
            f"门槛：邀请{threshold}人",
            f"奖励：{', '.join(create_item_message(reward_items))}",
            "------------------",
        ])
    
    await send_msg_handler(
        bot,
        event,
        "邀请奖励列表",
        bot.self_id,
        lines,
        title="邀请奖励列表",
    )


@invitation_help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, INVITATION_HELP)


@invitation_admin_help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, INVITATION_ADMIN_HELP)


INVITATION_HELP = """
🤝 邀请系统帮助 🤝
═════════════
1. 邀请码 [邀请人ID]
2. 邀请人
3. 我的邀请
4. 邀请奖励列表
5. 邀请奖励领取 [门槛]

说明：
- 邀请码只能填写一次。
- 不填门槛时，会尝试领取所有可领取奖励。
""".strip()


INVITATION_ADMIN_HELP = """
👑 邀请系统管理帮助 👑
═════════════
1. 邀请奖励设置 [门槛人数] [奖励物品]
   示例：
   邀请奖励设置 5 渡厄丹x5,灵石x10000000

2. 邀请奖励列表
""".strip()