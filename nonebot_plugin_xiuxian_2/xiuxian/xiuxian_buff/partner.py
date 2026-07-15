import random
import asyncio
import re
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from ...paths import get_paths

from ..on_compat import on_command
from nonebot.log import logger
from nonebot.params import Command, CommandArg

from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent, MessageSegment, get_at_user_id
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.player_fight import Player_fight
from ..xiuxian_utils.utils import check_user, check_user_type, get_msg_pic, handle_send, log_message, number_to, send_msg_handler, update_statistics_value, send_help_message
from ..xiuxian_utils.xiuxian2_handle import (
    XIUXIAN_IMPART_BUFF,
    OtherSet,
    PlayerDataManager,
    UserBuffDate,
    XiuxianDateManage,
    get_base_attributes,
    get_final_attributes,
    get_player_info,
    save_player_info,
)
from .mentor_exp_cd import mentor_exp_cd
from .transaction_service import MentorBindService
from .transaction_service import MentorApplicationService
from .transaction_service import MentorExpelService
from .transaction_service import MentorBreakthroughRewardService
from .transaction_service import ApprenticeLeaveService
from .transaction_service import MentorGraduationService
from .transaction_service import MentorTransmissionService
from .transaction_service import PartnerBreakthroughService
from .transaction_service import PartnerCultivationService
from .transaction_service import PartnerInviteService
from .transaction_service import PartnerProtectionService
from .transaction_service import PartnerTokenUseService
from .transaction_service import PartnerBindService
from .transaction_service import PartnerUnbindService
from .partner_storage import (
    PLAYERSDATA,
    bind_partner_storage,
    load_mentor,
    load_partner,
    save_mentor,
    save_partner,
)
from .relation_utils import (
    _config_rate,
    _format_seconds,
    _is_none_like,
    _normalize_dict,
    _normalize_history,
    _normalize_id_list,
    _parse_datetime,
    _rank_value,
    get_mentor_required_gap,
    get_realm_gap,
    is_wujie_or_above,
    safe_int,
)
from .two_exp_cd import two_exp_cd

partner_invite_cache = {}
sql_message = XiuxianDateManage()
xiuxian_impart = XIUXIAN_IMPART_BUFF()
player_data_manager = PlayerDataManager()
partner_cultivation_service = PartnerCultivationService(get_paths().game_db, get_paths().player_db)
partner_token_service = PartnerTokenUseService(get_paths().game_db, get_paths().player_db)
partner_bind_service = PartnerBindService(get_paths().game_db, get_paths().player_db)
partner_unbind_service = PartnerUnbindService(get_paths().game_db, get_paths().player_db)
partner_breakthrough_service = PartnerBreakthroughService(get_paths().game_db, get_paths().player_db)
mentor_bind_service = MentorBindService(get_paths().game_db, get_paths().player_db)
mentor_application_service = MentorApplicationService(get_paths().player_db)
partner_invite_service = PartnerInviteService(get_paths().player_db)
partner_protection_service = PartnerProtectionService(get_paths().player_db)
mentor_expel_service = MentorExpelService(get_paths().game_db, get_paths().player_db)
mentor_breakthrough_reward_service = MentorBreakthroughRewardService(get_paths().game_db, get_paths().player_db)
apprentice_leave_service = ApprenticeLeaveService(get_paths().game_db, get_paths().player_db)
mentor_graduation_service = MentorGraduationService(get_paths().game_db, get_paths().player_db)
mentor_transmission_service = MentorTransmissionService(get_paths().game_db, get_paths().player_db)
two_exp_limit = 3
mentor_config = XiuConfig()
mentor_transmission_limit = getattr(mentor_config, "mentor_transmission_limit", two_exp_limit)
MENTOR_MAX_APPRENTICES = getattr(mentor_config, "mentor_max_apprentices", 5)
MENTOR_COOLDOWN_DAYS = getattr(mentor_config, "mentor_cooldown_days", 7)
APPRENTICE_COOLDOWN_DAYS = getattr(mentor_config, "mentor_apprentice_cooldown_days", MENTOR_COOLDOWN_DAYS * 2)
MENTOR_MAX_EFFECT_GAP = getattr(mentor_config, "mentor_max_effect_gap", 6)
MENTOR_NEW_BIND_TRANSMISSION_WAIT_HOURS = getattr(mentor_config, "mentor_new_bind_transmission_wait_hours", 24)
MENTOR_SAME_PAIR_REBIND_COOLDOWN_DAYS = getattr(mentor_config, "mentor_same_pair_rebind_cooldown_days", 30)
MENTOR_GRADUATE_PAIR_REBIND_COOLDOWN_DAYS = getattr(mentor_config, "mentor_graduate_pair_rebind_cooldown_days", 7)
MENTOR_GRADUATE_APPRENTICE_STONE_REWARD = getattr(mentor_config, "mentor_graduate_apprentice_stone_reward", 5000000)
MENTOR_GRADUATE_MENTOR_STONE_REWARD = getattr(mentor_config, "mentor_graduate_mentor_stone_reward", 10000000)
MENTOR_HISTORY_LIMIT = getattr(mentor_config, "mentor_history_limit", 50)
MENTOR_APPLY_LIMIT_HOURS = getattr(mentor_config, "mentor_apply_limit_hours", 24)
MENTOR_BREAKTHROUGH_REWARD_LIMIT = 27
MENTOR_BREAKTHROUGH_REWARD_BASE_RATE = getattr(mentor_config, "mentor_breakthrough_reward_base_rate", 0.005)
MENTOR_BREAKTHROUGH_REWARD_MIN_RATE = getattr(mentor_config, "mentor_breakthrough_reward_min_rate", 0.001)
MENTOR_BREAKTHROUGH_REWARD_MAX_RATE = getattr(mentor_config, "mentor_breakthrough_reward_max_rate", 0.01)
try:
    MENTOR_APPLY_LIMIT_HOURS = int(MENTOR_APPLY_LIMIT_HOURS)
except (ValueError, TypeError):
    MENTOR_APPLY_LIMIT_HOURS = 24
if MENTOR_APPLY_LIMIT_HOURS < 0:
    MENTOR_APPLY_LIMIT_HOURS = 0
MENTOR_TITLE_IDS = {
    "apprentice": "30115",
    "mentor": "30116",
    "graduate": "30117",
    "mentor_graduate": "30118",
    "mentor_graduate_5": "30119",
    "transmission_100": "30120",
    "receive_transmission_50": "30121",
}
TITLE_JSONPATH = get_paths().data / "修炼物品" / "称号.json"
_mentor_title_cache = None
bind_partner_storage(
    player_data_manager,
    mentor_history_limit=MENTOR_HISTORY_LIMIT,
    mentor_breakthrough_reward_limit=MENTOR_BREAKTHROUGH_REWARD_LIMIT,
)


def _relation_operation_id(event, action, *user_ids):
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    suffix = ":".join(str(user_id) for user_id in user_ids)
    return f"relation:{action}:{event_id or time.time_ns()}:{suffix}"


def _relation_power(user_info, new_exp):
    root_rate = sql_message.get_root_rate(user_info["root_type"], user_info["user_id"])
    return round(int(new_exp) * root_rate * jsondata.level_data()[user_info["level"]]["spend"], 0)


def _recovered_attributes(user_info, new_exp):
    max_hp, max_mp = int(new_exp / 2), int(new_exp)
    return (
        min(int(user_info["hp"]) + int(new_exp / 10), max_hp),
        min(int(user_info["mp"]) + int(new_exp / 20), max_mp),
        int(new_exp / 10),
    )

two_exp_invite = on_command("双修", priority=6, block=True)
two_exp_accept = on_command("同意双修", priority=5, block=True)
two_exp_reject = on_command("拒绝双修", priority=5, block=True)
two_exp_protect = on_command("双修保护", priority=5, block=True)
double_cultivation_help = on_command("关系帮助", aliases={"道侣帮助", "双修帮助", "师徒帮助"}, priority=5, block=True)
my_exp_num = on_command("我的双修次数", priority=9, block=True)
my_partner = on_command("我的道侣", priority=5, block=True)
bind_partner = on_command("绑定道侣", aliases={"结为道侣"}, priority=5, block=True)
agree_bind = on_command("同意道侣", aliases={"接受道侣"}, priority=5, block=True)
unbind_partner = on_command("解除道侣", aliases={"断绝关系"}, priority=5, block=True)
partner_rank = on_command("道侣排行榜", priority=5, block=True)
apply_mentor = on_command("拜师", aliases={"申请拜师"}, priority=5, block=True)
mentor_protect = on_command("拜师保护", aliases={"师徒保护", "收徒保护"}, priority=5, block=True)
agree_mentor = on_command("同意拜师", aliases={"接受拜师", "收徒", "同意收徒"}, priority=5, block=True)
reject_mentor = on_command("拒绝拜师", aliases={"拒绝收徒"}, priority=5, block=True)
my_mentor = on_command("我的师徒", aliases={"我的师父", "我的师傅", "我的徒弟"}, priority=5, block=True)
mentor_record = on_command("师徒记录", aliases={"师门记录"}, priority=5, block=True)
mentor_rank = on_command("师徒排行榜", aliases={"名师榜", "师门榜"}, priority=5, block=True)
unbind_mentor = on_command("解除师徒", aliases={"叛出师门", "逐出师门", "出师"}, priority=5, block=True)
mentor_transmission = on_command("师徒传功", aliases={"传功"}, priority=5, block=True)


__double_cultivation_help__ = f"""
**关系系统**

---

**双修入口**
  • 双修 [道友QQ/道号] [次数]
  • 同意双修 / 拒绝双修
  • 我的双修次数
  • 双修保护 [开启/关闭/拒绝/状态]

**道侣入口**
  • 绑定道侣 [道号/QQ]
  • 同意道侣
  • 我的道侣
  • 解除道侣
  • 道侣排行榜

**师徒入口**
  • 拜师 [道号/QQ]
  • 同意拜师 [徒弟道号/QQ]
  • 拒绝拜师 [徒弟道号/QQ]
  • 拜师保护 [开启/关闭/状态]
  • 我的师徒 / 我的师父 / 我的徒弟
  • 师徒传功 [徒弟道号/QQ]
  • 师徒记录 / 师徒排行榜
  • 解除师徒 / 出师 / 逐出师门

**关键提示**
  • 徒弟每{MENTOR_APPLY_LIMIT_HOURS}小时只能向一位道友发起拜师申请
  • 师父开启拜师保护后，会自动拒绝新的拜师申请
  • 双修次数每日{two_exp_limit}次，师徒传功每日{mentor_transmission_limit}次
"""


@double_cultivation_help.handle(parameterless=[Cooldown(cd_time=0)])
async def double_cultivation_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, __double_cultivation_help__,
        k1="双修", v1="双修",
        k2="道侣", v2="我的道侣",
        k3="师徒", v3="我的师徒",
        k4="保护", v4="拜师保护 状态"
    )

async def two_exp_cd_up():
    two_exp_cd.re_data()
    mentor_exp_cd.re_data()
    logger.opt(colors=True).info(f"<green>双修次数已更新！</green>")
    logger.opt(colors=True).info(f"<green>师徒传功次数已更新！</green>")

def load_player_user(user_id):
    """加载用户数据，如果不存在或为空，返回默认数据"""
    return partner_protection_service.get_status(user_id)

@two_exp_invite.handle(parameterless=[Cooldown(stamina_cost=10)])
async def two_exp_invite_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """双修邀请"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    global two_exp_limit
    isUser, user_1, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await two_exp_invite.finish()

    user_id = user_1['user_id']

    existing_invite = partner_invite_service.pending_for_user(user_id)
    if existing_invite is not None:
        other_id = existing_invite.target_id if existing_invite.inviter_id == str(user_id) else existing_invite.inviter_id
        target_info = sql_message.get_user_real_info(other_id)
        remaining_time = existing_invite.expires_at - datetime.now().timestamp()
        msg = f"你已经向{target_info['user_name']}发送了双修邀请，请等待{int(remaining_time)}秒后邀请过期或对方回应后再发送新邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()

    two_qq = get_at_user_id(args)
    exp_count = 1  # 默认双修次数

    arg_text = args.extract_plain_text().strip()
    # 尝试解析次数
    count_match = re.search(r'(\d+)次', arg_text)
    if count_match:
        exp_count = int(count_match.group(1))
        # 移除次数信息，保留道号
        arg_text = re.sub(r'\d+次', '', arg_text).strip()

    if not two_qq and arg_text:
        user_info = sql_message.get_user_info_with_name(arg_text)
        if user_info:
            two_qq = user_info['user_id']

    if two_qq is None:
        msg = "请指定双修对象！格式：双修 道号 [次数]"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_invite.finish()

    if str(user_id) == str(two_qq):
        msg = "道友无法与自己双修！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_invite.finish()

    user_2_info = sql_message.get_user_real_info(two_qq)
    if not user_2_info:
        msg = "未找到指定道友，对方可能尚未踏入修仙界。"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_invite.finish()

    if partner_invite_service.pending_for_user(two_qq) is not None:
        msg = "对方已有未处理的双修邀请，请稍后再试！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()

    # 检查自己的双修次数限制
    limt_1 = two_exp_cd.find_user(user_id)
    impart_data_1 = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_two_exp_1 = impart_data_1['impart_two_exp'] if impart_data_1 else 0
    main_two_data_1 = UserBuffDate(user_id).get_user_main_buff_data()
    main_two_1 = main_two_data_1['two_buff'] if main_two_data_1 else 0
    max_count_1 = two_exp_limit + impart_two_exp_1 + main_two_1 - limt_1

    if max_count_1 <= 0:
        msg = "你的双修次数已用尽，无法发送邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_invite.finish()

    # 判断是否为道侣
    is_partner = await check_is_partner(user_id, two_qq)
    if is_partner:
        await direct_two_exp(bot, event, user_id, two_qq, exp_count, is_partner=is_partner)
        await two_exp_invite.finish()

    # 检查对方修为是否比自己高
    if user_2_info['exp'] > user_1['exp']:
        msg = "修仙大能看了看你，不屑一顾，扬长而去！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_invite.finish()

    # 检查对方的双修保护状态
    protection_status = load_player_user(two_qq)

    if protection_status == "refusal":
        msg = "对方已设置拒绝所有双修邀请，无法进行双修！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_invite.finish()
    elif protection_status == "on":
        # 对方开启保护，需要发送持久邀请。
        # 检查对方双修次数是否足够
        limt_2 = two_exp_cd.find_user(two_qq)
        impart_data_2 = xiuxian_impart.get_user_impart_info_with_id(two_qq)
        impart_two_exp_2 = impart_data_2['impart_two_exp'] if impart_data_2 else 0
        main_two_data_2 = UserBuffDate(two_qq).get_user_main_buff_data()
        main_two_2 = main_two_data_2['two_buff'] if main_two_data_2 else 0
        max_count_2 = two_exp_limit + impart_two_exp_2 + main_two_2 - limt_2

        if max_count_2 <= 0:
            msg = "对方今日双修次数已用尽，无法邀请！"
            await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
            await two_exp_invite.finish()
        
        exp_count = max(exp_count, 1)
        # 创建邀请
        invite_id = _relation_operation_id(
            event, "cultivation-invite", user_id, two_qq
        )
        created = partner_invite_service.create(
            invite_id, user_id, two_qq, min(exp_count, max_count_2),
            expected_target_protection="on",
        )
        if not created.succeeded:
            if created.status == "protection_changed":
                msg = "对方的双修保护状态已经变化，请重新发起。"
            else:
                msg = "双方已有待处理的双修邀请，请稍后再试！"
            await handle_send(bot, event, msg, md_type="buff")
            await two_exp_invite.finish()

        # 设置60秒过期
        asyncio.create_task(expire_invite(two_qq, invite_id, bot, event))

        msg = f"已向{user_2_info['user_name']}发送双修邀请（{min(exp_count, max_count_2)}次），等待对方回应..."
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()
    else:
        # 对方关闭保护，直接进行双修
        await direct_two_exp(
            bot, event, user_id, two_qq, exp_count, is_partner=is_partner,
            expected_target_protection="off",
        )
        await two_exp_invite.finish()

async def check_is_partner(user_id_1, user_id_2):
    """检查两个用户是否互为道侣关系"""
    user_id_1 = str(user_id_1)
    user_id_2 = str(user_id_2)

    partner_data_1 = load_partner(user_id_1)
    partner_data_2 = load_partner(user_id_2)

    return (
        partner_data_1
        and partner_data_2
        and str(partner_data_1.get("partner_id")) == user_id_2
        and str(partner_data_2.get("partner_id")) == user_id_1
    )

async def direct_two_exp(
    bot, event, user_id_1, user_id_2, exp_count=1, is_partner=False,
    invite_id=None, expected_target_protection=None,
):
    """
    直接进行双修。

    修复点：
    1. user_id 全部转 str，避免道侣判断失败。
    2. 多次双修时，每一次都用临时累计修为重新计算上限。
    3. 不再按开始时的修为一直算到最后。
    4. 只有实际双修成功才消耗次数。
    5. 道侣亲密度只在双方互相绑定时增加。
    """

    user_id_1 = str(user_id_1)
    user_id_2 = str(user_id_2)

    try:
        exp_count = int(exp_count)
    except (ValueError, TypeError):
        exp_count = 1
    exp_count = max(1, exp_count)

    user_1 = sql_message.get_user_info_with_id(user_id_1)
    user_2 = sql_message.get_user_info_with_id(user_id_2)

    if not user_1 or not user_2:
        msg = "无法获取玩家信息，无法进行双修。"
        await handle_send(
            bot, event, msg,
            md_type="buff",
            k1="双修", v1="双修",
            k2="次数", v2="我的双修次数",
            k3="修为", v3="我的修为"
        )
        return

    # 检查双方双修次数
    limt_1 = two_exp_cd.find_user(user_id_1)
    limt_2 = two_exp_cd.find_user(user_id_2)

    impart_data_1 = xiuxian_impart.get_user_impart_info_with_id(user_id_1)
    impart_data_2 = xiuxian_impart.get_user_impart_info_with_id(user_id_2)

    impart_two_exp_1 = impart_data_1["impart_two_exp"] if impart_data_1 else 0
    impart_two_exp_2 = impart_data_2["impart_two_exp"] if impart_data_2 else 0

    main_two_data_1 = UserBuffDate(user_id_1).get_user_main_buff_data()
    main_two_data_2 = UserBuffDate(user_id_2).get_user_main_buff_data()

    main_two_1 = main_two_data_1["two_buff"] if main_two_data_1 else 0
    main_two_2 = main_two_data_2["two_buff"] if main_two_data_2 else 0

    max_count_1 = two_exp_limit + impart_two_exp_1 + main_two_1 - limt_1
    max_count_2 = two_exp_limit + impart_two_exp_2 + main_two_2 - limt_2

    if max_count_1 <= 0:
        msg = "你的双修次数不足，无法进行双修！"
        await handle_send(
            bot, event, msg,
            md_type="buff",
            k1="双修", v1="双修",
            k2="次数", v2="我的双修次数",
            k3="修为", v3="我的修为"
        )
        return

    if max_count_2 <= 0:
        msg = "对方的双修次数不足，无法进行双修！"
        await handle_send(
            bot, event, msg,
            md_type="buff",
            k1="双修", v1="双修",
            k2="次数", v2="我的双修次数",
            k3="修为", v3="我的修为"
        )
        return

    actual_count = min(exp_count, max_count_1, max_count_2)

    if actual_count <= 0:
        msg = "没有足够的双修次数进行双修！"
        await handle_send(
            bot, event, msg,
            md_type="buff",
            k1="双修", v1="双修",
            k2="次数", v2="我的双修次数",
            k3="修为", v3="我的修为"
        )
        return

    total_exp_1 = 0
    total_exp_2 = 0
    event_descriptions = []
    actual_used_count = 0

    # 关键：临时修为，用于多次双修逐次重新计算上限
    temp_exp_1 = int(user_1["exp"])
    temp_exp_2 = int(user_2["exp"])

    for _ in range(actual_count):
        exp_1, exp_2, event_desc = await process_two_exp(
            user_id_1,
            user_id_2,
            is_partner=is_partner,
            current_exp_1=temp_exp_1,
            current_exp_2=temp_exp_2
        )

        # 双方都无法获得修为时停止
        if exp_1 == 0 and exp_2 == 0:
            break

        total_exp_1 += exp_1
        total_exp_2 += exp_2

        # 更新临时修为，下一次按新修为重新算上限
        temp_exp_1 += exp_1
        temp_exp_2 += exp_2

        if event_desc:
            event_descriptions.append(event_desc)

        actual_used_count += 1

    if actual_used_count == 0:
        msg = "双修过程中修为已达上限，无法进行双修！"
        await handle_send(
            bot, event, msg,
            md_type="buff",
            k1="双修", v1="双修",
            k2="次数", v2="我的双修次数",
            k3="修为", v3="我的修为"
        )
        return

    partner_data_1 = load_partner(user_id_1) if is_partner else None
    partner_data_2 = load_partner(user_id_2) if is_partner else None
    valid_partner = bool(
        partner_data_1 and partner_data_2
        and str(partner_data_1.get("partner_id")) == user_id_2
        and str(partner_data_2.get("partner_id")) == user_id_1
    )
    add_affection_1 = 20 * actual_used_count if valid_partner else 0
    add_affection_2 = 10 * actual_used_count if valid_partner else 0
    new_exp_1, new_exp_2 = temp_exp_1, temp_exp_2
    hp_1, mp_1, atk_1 = _recovered_attributes(user_1, new_exp_1)
    hp_2, mp_2, atk_2 = _recovered_attributes(user_2, new_exp_2)
    special_count = sum("天降异象" in desc for desc in event_descriptions)
    settlement = partner_cultivation_service.apply(
        _relation_operation_id(event, "cultivation", user_id_1, user_id_2),
        user_id_1, user_id_2,
        expected_exp_1=user_1["exp"], expected_exp_2=user_2["exp"],
        exp_1=total_exp_1, exp_2=total_exp_2, used_count=actual_used_count,
        power_1=_relation_power(user_1, new_exp_1), power_2=_relation_power(user_2, new_exp_2),
        hp_1=hp_1, mp_1=mp_1, atk_1=atk_1, hp_2=hp_2, mp_2=mp_2, atk_2=atk_2,
        level_rate_1=special_count * 2, level_rate_2=special_count * 2,
        expected_affection_1=safe_int(partner_data_1.get("affection"), 0) if valid_partner else None,
        expected_affection_2=safe_int(partner_data_2.get("affection"), 0) if valid_partner else None,
        affection_1=add_affection_1, affection_2=add_affection_2,
        invite_id=invite_id,
        expected_used_count_1=limt_1 if invite_id else None,
        expected_used_count_2=limt_2 if invite_id else None,
        expected_target_protection=expected_target_protection,
    )
    if not settlement.succeeded:
        await handle_send(bot, event, "双修状态发生变化，本次未结算，请重新发起。", md_type="buff")
        return
    if settlement.status == "applied" and not invite_id:
        for _ in range(actual_used_count):
            two_exp_cd.add_user(user_id_1)
            two_exp_cd.add_user(user_id_2)

    user_1_info = sql_message.get_user_real_info(user_id_1)
    user_2_info = sql_message.get_user_real_info(user_id_2)

    log_message(
        user_id_1,
        f"与{user_2_info['user_name']}进行{'道侣' if is_partner else ''}双修，"
        f"获得修为{number_to(total_exp_1)}，共{actual_used_count}次"
    )
    log_message(
        user_id_2,
        f"与{user_1_info['user_name']}进行{'道侣' if is_partner else ''}双修，"
        f"获得修为{number_to(total_exp_2)}，共{actual_used_count}次"
    )

    affection_msg = ""
    if is_partner:
        if valid_partner:
            affection_msg = (
                f"\n\n道侣双修亲密度增加："
                f"\n{user_1_info['user_name']} +{add_affection_1}"
                f"\n{user_2_info['user_name']} +{add_affection_2}"
            )
        else:
            affection_msg = "\n\n道侣名册暂未理顺，本次亲密度未增加。"

    if event_descriptions:
        msg = f"{random.choice(event_descriptions)}\n\n"
    else:
        msg = "两位道友气机交融，功法互补，修为有所精进。\n\n"

    msg += f"{user_1_info['user_name']}获得修为：{number_to(total_exp_1)}\n"
    msg += f"{user_2_info['user_name']}获得修为：{number_to(total_exp_2)}"
    msg += affection_msg

    await handle_send(
        bot, event, msg,
        md_type="buff",
        k1="双修", v1="双修",
        k2="次数", v2="我的双修次数",
        k3="修为", v3="我的修为"
    )

async def process_two_exp(
    user_id_1,
    user_id_2,
    is_partner=False,
    current_exp_1=None,
    current_exp_2=None
):
    """
    处理单次双修收益。
    """

    user_id_1 = str(user_id_1)
    user_id_2 = str(user_id_2)

    user_1 = sql_message.get_user_real_info(user_id_1)
    user_2 = sql_message.get_user_real_info(user_id_2)

    if not user_1 or not user_2:
        return 0, 0, "无法获取玩家信息，无法进行双修。"

    user_mes_1 = sql_message.get_user_info_with_id(user_id_1)
    user_mes_2 = sql_message.get_user_info_with_id(user_id_2)

    if not user_mes_1 or not user_mes_2:
        return 0, 0, "无法获取玩家信息，无法进行双修。"

    level_1 = user_mes_1["level"]
    level_2 = user_mes_2["level"]

    # 多次双修时使用临时修为重新计算
    calc_exp_1 = int(current_exp_1) if current_exp_1 is not None else int(user_mes_1["exp"])
    calc_exp_2 = int(current_exp_2) if current_exp_2 is not None else int(user_mes_2["exp"])

    max_exp_1_limit = int(OtherSet().set_closing_type(level_1)) * XiuConfig().closing_exp_upper_limit
    max_exp_2_limit = int(OtherSet().set_closing_type(level_2)) * XiuConfig().closing_exp_upper_limit

    remaining_exp_1 = max_exp_1_limit - calc_exp_1
    remaining_exp_2 = max_exp_2_limit - calc_exp_2

    # 非道侣：任意一方到上限就停止
    if not is_partner and (remaining_exp_1 <= 0 or remaining_exp_2 <= 0):
        return 0, 0, "修为已达上限，无法继续双修。"

    user_buff_data_1 = UserBuffDate(user_id_1)
    user_buff_data_2 = UserBuffDate(user_id_2)

    mainbuffdata_1 = user_buff_data_1.get_user_main_buff_data()
    mainbuffdata_2 = user_buff_data_2.get_user_main_buff_data()

    mainbuffratebuff_1 = mainbuffdata_1["ratebuff"] if mainbuffdata_1 else 0
    mainbuffcloexp_1 = mainbuffdata_1["clo_exp"] if mainbuffdata_1 else 0

    mainbuffratebuff_2 = mainbuffdata_2["ratebuff"] if mainbuffdata_2 else 0
    mainbuffcloexp_2 = mainbuffdata_2["clo_exp"] if mainbuffdata_2 else 0

    user_blessed_spot_data_1 = (
        user_buff_data_1.BuffInfo["blessed_spot"] * 0.5
        if user_buff_data_1.BuffInfo else 0
    )
    user_blessed_spot_data_2 = (
        user_buff_data_2.BuffInfo["blessed_spot"] * 0.5
        if user_buff_data_2.BuffInfo else 0
    )

    # 基础修为计算使用当前临时修为
    exp_base = int((calc_exp_1 + calc_exp_2) * 0.005)

    exp_limit_1 = int(
        exp_base
        * (1 + mainbuffratebuff_1)
        * (1 + mainbuffcloexp_1)
        * (1 + user_blessed_spot_data_1)
    )
    exp_limit_2 = int(
        exp_base
        * (1 + mainbuffratebuff_2)
        * (1 + mainbuffcloexp_2)
        * (1 + user_blessed_spot_data_2)
    )

    user1_rank = max(convert_rank(level_1)[0] // 3, 1)
    user2_rank = max(convert_rank(level_2)[0] // 3, 1)

    max_exp_1 = int((calc_exp_1 * 0.001) * min(0.1 * user1_rank, 1))
    max_exp_2 = int((calc_exp_2 * 0.001) * min(0.1 * user2_rank, 1))

    max_two_exp = 10_0000_0000

    if max_exp_1 >= max_two_exp:
        exp_limit_1 = min(exp_limit_1, max_exp_1, max(0, remaining_exp_1))
    else:
        exp_limit_1 = min(exp_limit_1, max_exp_1_limit * 0.1, max(0, remaining_exp_1))

    if max_exp_2 >= max_two_exp:
        exp_limit_2 = min(exp_limit_2, max_exp_2, max(0, remaining_exp_2))
    else:
        exp_limit_2 = min(exp_limit_2, max_exp_2_limit * 0.1, max(0, remaining_exp_2))

    exp_limit_1 = int(max(0, exp_limit_1))
    exp_limit_2 = int(max(0, exp_limit_2))

    # 道侣倍率
    if is_partner:
        if remaining_exp_1 <= 0:
            exp_limit_1 = 1
        else:
            exp_limit_1 = int(exp_limit_1 * 1.2)

        if remaining_exp_2 <= 0:
            exp_limit_2 = 1
        else:
            exp_limit_2 = int(exp_limit_2 * 1.2)

    # 特殊事件概率
    is_special = random.randint(1, 100) <= 6
    event_desc = ""

    if is_special:
        special_events = [
            "突然天降异象，七彩祥云笼罩两人，修为大增！",
            "意外发现一处灵脉，两人共同吸收，修为精进！",
            "功法意外产生共鸣，引发天地灵气倒灌！",
            "两人心意相通，功法运转达到完美契合！",
            "顿悟时刻来临，两人同时进入玄妙境界！"
        ]
        event_desc = random.choice(special_events)

        exp_limit_1 = int(exp_limit_1 * 1.5)
        exp_limit_2 = int(exp_limit_2 * 1.5)

        event_desc += "\n💫道友同心，天降异象！"
        event_desc += "\n💝离开时双方互相赠送信物，双方各增加突破概率2%。"
    else:
        event_descriptions = [
            f"月明星稀之夜，{user_1['user_name']}与{user_2['user_name']}在灵山之巅相对而坐，双手相抵，周身灵气环绕如雾。",
            f"洞府之中，{user_1['user_name']}与{user_2['user_name']}盘膝对坐，真元交融，形成阴阳鱼图案在两人之间流转。",
            f"瀑布之下，{user_1['user_name']}与{user_2['user_name']}沐浴灵泉，水汽蒸腾间功法共鸣，修为精进。",
            f"竹林小筑内，{user_1['user_name']}与{user_2['user_name']}共饮灵茶，茶香氤氲中功法相互印证。",
            f"云端之上，{user_1['user_name']}与{user_2['user_name']}脚踏飞剑，剑气交织间功法互补，修为大涨。",
        ]
        event_desc = random.choice(event_descriptions)

    # 最终再次裁剪，防止道侣倍率 / 特殊事件倍率后超过上限
    if is_partner:
        if remaining_exp_1 > 0:
            exp_limit_1 = min(exp_limit_1, remaining_exp_1)
        else:
            exp_limit_1 = 1

        if remaining_exp_2 > 0:
            exp_limit_2 = min(exp_limit_2, remaining_exp_2)
        else:
            exp_limit_2 = 1
    else:
        exp_limit_1 = min(exp_limit_1, max(0, remaining_exp_1))
        exp_limit_2 = min(exp_limit_2, max(0, remaining_exp_2))

    exp_limit_1 = int(max(0, exp_limit_1))
    exp_limit_2 = int(max(0, exp_limit_2))

    if not is_partner and exp_limit_1 <= 0 and exp_limit_2 <= 0:
        return 0, 0, "修为已达上限，无法继续双修。"

    return exp_limit_1, exp_limit_2, event_desc

@two_exp_accept.handle(parameterless=[Cooldown(cd_time=0)])
async def two_exp_accept_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """同意双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await two_exp_accept.finish()
        
    user_id = user_info['user_id']
    
    invite = partner_invite_service.pending_for_target(user_id)
    if invite is None:
        msg = "没有待处理的双修邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_accept.finish()
        
    await direct_two_exp(bot, event, invite.inviter_id, user_id, invite.count, invite_id=invite.invite_id)
    await two_exp_accept.finish()

async def expire_invite(user_id, invite_id, bot, event):
    """邀请过期处理"""
    await asyncio.sleep(60)
    result = partner_invite_service.resolve(invite_id, user_id, "expired")
    if result.status == "applied":
        # 发送过期提示
        msg = f"双修邀请已过期！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")

@two_exp_reject.handle(parameterless=[Cooldown(cd_time=0)])
async def two_exp_reject_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """拒绝双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await two_exp_reject.finish()
        
    user_id = user_info['user_id']
    
    invite = partner_invite_service.pending_for_target(user_id)
    if invite is None:
        msg = "没有待处理的双修邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_reject.finish()
        
    inviter_id = invite.inviter_id
    
    inviter_info = sql_message.get_user_real_info(inviter_id)
    msg = f"你拒绝了{inviter_info['user_name']}的双修邀请！"
    
    partner_invite_service.resolve(invite.invite_id, user_id, "rejected")
    
    await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
    await two_exp_reject.finish()

@two_exp_protect.handle(parameterless=[Cooldown(cd_time=0)])
async def two_exp_protect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """双修保护设置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await two_exp_protect.finish()
        
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip().lower()
    
    # 默认双修保护状态为关闭
    expected_status = load_player_user(user_id)
    current_status = expected_status
    
    if arg in ['开启', 'on']:
        current_status = "on"
        msg = "双修保护已开启！其他玩家可以向你发送双修邀请。"
    elif arg in ['关闭', 'off']:
        current_status = "off"
        msg = "双修保护已关闭！其他玩家可以直接和你双修。"
    elif arg in ['拒绝', 'refusal']:
        current_status = "refusal"
        msg = "双修保护已设置为拒绝！其他玩家无法与你双修。"
    elif arg in ['状态', 'status']:
        status_map = {
            "on": "已开启 (需要邀请)",
            "off": "已关闭 (允许直接双修)", 
            "refusal": "已拒绝 (拒绝所有双修)"
        }
        current_status_display = status_map.get(current_status, "已关闭 (允许直接双修)")
        msg = f"双修保护状态：{current_status_display}"
        await handle_send(bot, event, msg, md_type="buff", k1="开启", v1="双修保护 开启", k2="关闭", v2="双修保护 关闭", k3="拒绝", v3="双修保护 拒绝", k4="状态", v4="双修保护 状态")
        await two_exp_protect.finish()
    else:
        msg = "请使用：双修保护 开启/关闭/拒绝/状态"
        await handle_send(bot, event, msg, md_type="buff", k1="开启", v1="双修保护 开启", k2="关闭", v2="双修保护 关闭", k3="拒绝", v3="双修保护 拒绝", k4="状态", v4="双修保护 状态")
        await two_exp_protect.finish()
    
    changed = partner_protection_service.set_status(
        _relation_operation_id(event, "cultivation-protection", user_id),
        user_id,
        expected_status,
        current_status,
    )
    if not changed.succeeded:
        await handle_send(bot, event, "双修保护状态已经变化，请重新设置。")
        await two_exp_protect.finish()
    await handle_send(bot, event, msg, md_type="buff", k1="开启", v1="双修保护 开启", k2="关闭", v2="双修保护 关闭", k3="拒绝", v3="双修保护 拒绝", k4="状态", v4="双修保护 状态")
    await two_exp_protect.finish()


@mentor_protect.handle(parameterless=[Cooldown(cd_time=0)])
async def mentor_protect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """拜师保护设置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mentor_protect.finish()

    user_id = str(user_info["user_id"])
    arg = args.extract_plain_text().strip().lower()
    expected_status = mentor_application_service.get_protection(user_id)
    buttons = {
        "md_type": "buff",
        "k1": "开启", "v1": "拜师保护 开启",
        "k2": "关闭", "v2": "拜师保护 关闭",
        "k3": "状态", "v3": "拜师保护 状态",
        "k4": "师徒", "v4": "我的师徒",
    }

    if arg in ["开启", "on"]:
        current_status = "on"
        msg = "拜师保护已开启，新的拜师申请会被自动拒绝。"
    elif arg in ["关闭", "off"]:
        current_status = "off"
        msg = "拜师保护已关闭，其他道友可以向你发起拜师申请。"
    elif arg in ["", "状态", "status"]:
        status_msg = "已开启（自动拒绝新的拜师申请）" if expected_status == "on" else "已关闭（允许拜师申请）"
        msg = f"拜师保护状态：{status_msg}"
    else:
        msg = "请使用：拜师保护 开启/关闭/状态"

    if arg in ["开启", "on", "关闭", "off"]:
        changed = mentor_application_service.set_protection(
            _relation_operation_id(event, "mentor-protection", user_id),
            user_id,
            expected_status,
            current_status,
        )
        if not changed.succeeded:
            await handle_send(bot, event, "拜师保护状态已经变化，请重新设置。", **buttons)
            await mentor_protect.finish()
        if changed.rejected_apprentice_ids:
            pending_names = _format_mentor_applicant_ids(
                changed.rejected_apprentice_ids, limit=10
            )
            msg += (
                f"\n已自动拒绝当前{len(changed.rejected_apprentice_ids)}条"
                f"待处理拜师申请：{pending_names}。"
            )

    await handle_send(bot, event, msg, **buttons)
    await mentor_protect.finish()


@my_exp_num.handle(parameterless=[Cooldown(cd_time=0)])
async def my_exp_num_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的双修次数"""
    global two_exp_limit
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_exp_num.finish()
    user_id = user_info['user_id']
    limt = two_exp_cd.find_user(user_id)
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_two_exp = impart_data['impart_two_exp'] if impart_data is not None else 0
    
    main_two_data = UserBuffDate(user_id).get_user_main_buff_data()
    main_two = main_two_data['two_buff'] if main_two_data is not None else 0
    
    num = (two_exp_limit + impart_two_exp + main_two) - limt
    if num <= 0:
        num = 0
    msg = f"道友剩余双修次数{num}次！"
    await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="我的修为", v2="我的修为", k3="存档", v3="我的存档")
    await my_exp_num.finish()

async def use_two_exp_token(bot, event, item_id, num):
    """增加双修次数"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
        
    user_id = user_info['user_id']
    
    current_count = two_exp_cd.find_user(user_id)
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    result = partner_token_service.apply(
        f"partner-token:{user_id}:{event_id or time.time_ns()}", user_id, item_id,
        requested_count=num, expected_item_count=sql_message.goods_num(user_id, item_id),
        expected_used_count=current_count,
    )
    tokens_used = result.used_tokens
    if result.succeeded and tokens_used > 0:
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        impart_two_exp = impart_data['impart_two_exp'] if impart_data is not None else 0
        main_two_data = UserBuffDate(user_id).get_user_main_buff_data()
        main_two = main_two_data['two_buff'] if main_two_data is not None else 0
        remaining_count = (two_exp_limit + impart_two_exp + main_two) - result.used_count
        msg = f"增加{tokens_used}次双修！\n"
        msg += f"当前剩余双修次数：{remaining_count}次"
    elif result.status == "limit_full":
        msg = "当前剩余双修次数已满！"
    else:
        msg = "双修令牌或次数状态已变化，请刷新背包后重试！"
    
    await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="我的修为", v2="我的修为", k3="次数", v3="我的双修次数")

@bind_partner.handle(parameterless=[Cooldown(cd_time=0)])
async def bind_partner_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """绑定道侣"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await bind_partner.finish()
    
    user_id = user_info['user_id']
    
    # 检查是否已经有道侣
    partner_data = load_partner(user_id)
    if partner_data and partner_data.get('partner_id') is not None:
        msg = "你已经有了道侣，请先解除道侣关系再绑定新的道侣！"
        await handle_send(bot, event, msg, md_type="buff", k1="绑定", v1="绑定道侣", k2="解除", v2="断绝关系", k3="道侣", v3="我的道侣")
        await bind_partner.finish()
    
    arg = args.extract_plain_text().strip()
    
    # 尝试解析道号或艾特
    partner_user_id = get_at_user_id(args)
    if not partner_user_id:
        # 解析道号
        partner_info = sql_message.get_user_info_with_name(arg)
        if partner_info:
            partner_user_id = partner_info['user_id']
    
    if not partner_user_id:
        msg = "未找到指定的道侣，请检查道号或艾特是否正确！"
        await handle_send(bot, event, msg, md_type="buff", k1="绑定", v1="绑定道侣", k2="解除", v2="断绝关系", k3="道侣", v3="我的道侣")
        await bind_partner.finish()
    
    # 检查对方是否已经有道侣
    partner_partner_data = load_partner(partner_user_id)
    if partner_partner_data and partner_partner_data.get('partner_id') is not None:
        msg = "对方已经有道侣了，无法绑定新的道侣！"
        await handle_send(bot, event, msg, md_type="buff", k1="绑定", v1="绑定道侣", k2="解除", v2="断绝关系", k3="道侣", v3="我的道侣")
        await bind_partner.finish()
    
    # 检查是否已经有未处理的邀请（作为被邀请者）
    if str(user_id) in partner_invite_cache:
        inviter_id = partner_invite_cache[str(user_id)]['inviter']
        inviter_info = sql_message.get_user_real_info(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - partner_invite_cache[str(user_id)]['timestamp'])
        msg = f"你已有来自{inviter_info['user_name']}的道侣绑定邀请（剩余{int(remaining_time)}秒），请先处理！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意道侣", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
        await bind_partner.finish()
    
    # 检查是否已经发出过邀请（作为邀请者）
    existing_invite = None
    for target_id, invite_data in partner_invite_cache.items():
        if invite_data['inviter'] == user_id:
            existing_invite = target_id
            break
    
    if existing_invite is not None:
        target_info = sql_message.get_user_real_info(existing_invite)
        remaining_time = 60 - (datetime.now().timestamp() - partner_invite_cache[existing_invite]['timestamp'])
        msg = f"你已经向{target_info['user_name']}发送了道侣绑定邀请，请等待{int(remaining_time)}秒后邀请过期或对方回应后再发送新邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意道侣", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
        await bind_partner.finish()
    
    # 创建绑定邀请
    invite_id = f"{user_id}_{partner_user_id}_{datetime.now().timestamp()}"
    partner_invite_cache[str(partner_user_id)] = {
        'inviter': user_id,
        'timestamp': datetime.now().timestamp(),
        'invite_id': invite_id
    }
    
    # 设置60秒过期
    asyncio.create_task(expire_partner_invite(partner_user_id, invite_id, bot, event))
    
    partner_info = sql_message.get_user_real_info(partner_user_id)
    msg = f"已向{partner_info['user_name']}发送道侣绑定邀请，等待对方回应..."
    await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意道侣", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
    await bind_partner.finish()

async def expire_partner_invite(user_id, invite_id, bot, event):
    """道侣绑定邀请过期处理"""
    await asyncio.sleep(60)
    if str(user_id) in partner_invite_cache and partner_invite_cache[str(user_id)]['invite_id'] == invite_id:
        inviter_id = partner_invite_cache[str(user_id)]['inviter']
        # 发送过期提示
        msg = f"道侣绑定邀请已过期！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意道侣", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
        # 删除过期的邀请
        del partner_invite_cache[str(user_id)]

@agree_bind.handle(parameterless=[Cooldown(cd_time=0)])
async def agree_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """同意道侣绑定"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await agree_bind.finish()
    
    user_id = user_info['user_id']
    
    # 检查是否有邀请
    if str(user_id) not in partner_invite_cache:
        msg = "没有待处理的道侣绑定邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意道侣", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
        await agree_bind.finish()
        
    bind_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    invitee_partner = load_partner(user_id).get("partner_id")
    inviter_partner = load_partner(inviter_id).get("partner_id")
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    result = partner_bind_service.apply(
        f"partner-bind:{user_id}:{invite_data.get('invite_id', event_id or time.time_ns())}",
        user_id, inviter_id, bind_time=bind_time,
        expected_invitee_partner=invitee_partner, expected_inviter_partner=inviter_partner,
    )
    if not result.succeeded:
        await handle_send(bot, event, "道侣邀请或双方关系状态已变化，请重新发起邀请。", md_type="buff")
        await agree_bind.finish()
    if result.status == "applied" and str(user_id) in partner_invite_cache:
        del partner_invite_cache[str(user_id)]

    msg = f"你已与{inviter_info['user_name']}结为道侣，绑定时间为{result.bind_time}。"
    await handle_send(bot, event, msg)
    await agree_bind.finish()
    # 删除邀请
    del partner_invite_cache[str(user_id)]
    
    msg = f"你已与{inviter_info['user_name']}结为道侣，绑定时间为{partner_data['bind_time']}。"
    await handle_send(bot, event, msg)    
    await agree_bind.finish()

@unbind_partner.handle(parameterless=[Cooldown(cd_time=0)])
async def unbind_partner_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """解除道侣关系"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await unbind_partner.finish()
    
    user_id = user_info['user_id']
    
    # 获取当前道侣数据
    partner_data = load_partner(user_id)
    
    if not partner_data or partner_data.get('partner_id') is None:
        msg = "你还没有道侣！"
        await handle_send(bot, event, msg, md_type="buff", k1="解除", v1="断绝关系", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
        await unbind_partner.finish()
    
    partner_user_id = str(partner_data["partner_id"])
    partner_side = load_partner(partner_user_id)
    checked_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    result = partner_unbind_service.apply(
        f"partner-unbind:{user_id}:{event_id or time.time_ns()}", user_id, partner_user_id,
        expected_user_bind_time=partner_data.get("bind_time"),
        expected_partner_bind_time=partner_side.get("bind_time"),
        expected_user_affection=partner_data.get("affection", 0),
        expected_partner_affection=partner_side.get("affection", 0),
        checked_at=checked_at, minimum_days=7,
    )
    if result.status == "too_early":
        await handle_send(bot, event, "你与道侣的绑定时间不足7天，暂时不能解除关系。", md_type="buff")
        await unbind_partner.finish()
    if not result.succeeded:
        await handle_send(bot, event, "道侣关系状态已变化，请重新查看后再试。", md_type="buff")
        await unbind_partner.finish()
    msg = "你已与道侣断绝关系。"
    await handle_send(bot, event, msg, md_type="buff", k1="绑定", v1="绑定道侣", k2="解除", v2="断绝关系", k3="道侣", v3="我的道侣")
    await unbind_partner.finish()

def get_affection_level(affection):
    affection = safe_int(affection)
    if affection >= 10000:
        affection_level = "深情厚谊"
    elif affection >= 5000:
        affection_level = "心有灵犀"
    elif affection >= 1000:
        affection_level = "初识情愫"
    else:
        affection_level = "缘分伊始"
    return affection_level

@my_partner.handle(parameterless=[Cooldown(cd_time=0)])
async def my_partner_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看我的道侣信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_partner.finish()
    
    user_id = user_info['user_id']
    
    # 获取道侣数据
    partner_data = load_partner(user_id)
    
    if not partner_data or partner_data.get('partner_id') is None:
        msg = "你还没有道侣！"
        await handle_send(bot, event, msg, md_type="buff", k1="绑定", v1="绑定道侣", k2="解除", v2="断绝关系", k3="道侣", v3="我的道侣")
        await my_partner.finish()
    
    partner_user_id = partner_data["partner_id"]
    partner_info = sql_message.get_user_real_info(partner_user_id)
    
    bind_time = partner_data["bind_time"]
    affection = partner_data["affection"]
    bound_days = (datetime.now() - datetime.strptime(bind_time, '%Y-%m-%d %H:%M:%S')).days
    affection_level = get_affection_level(affection)
    partner_user_info = sql_message.get_user_info_with_id(partner_user_id)
    msg = f"""【我的道侣】
道侣：{partner_info['user_name']}
境界：{partner_user_info['level']}
修为：{number_to(partner_user_info['exp'])}

结契信息：
- 绑定时间：{bind_time}
- 相伴天数：{bound_days}天
- 亲密度：{affection}（{affection_level}）"""
    await handle_send(bot, event, msg)
    await my_partner.finish()


def _get_mentor_apply_remaining(apprentice_id):
    if MENTOR_APPLY_LIMIT_HOURS <= 0:
        return 0, None

    data = load_mentor(apprentice_id)
    apply_time = _parse_datetime(data.get("mentor_apply_time"))
    apply_target = data.get("mentor_apply_target")
    if apply_time is None:
        return 0, apply_target

    available_time = apply_time + timedelta(hours=MENTOR_APPLY_LIMIT_HOURS)
    now = datetime.now()
    if now >= available_time:
        return 0, None

    return int((available_time - now).total_seconds()), apply_target


def _get_pending_mentor_invites(mentor_id):
    return {
        app.apprentice_id: {
            "timestamp": app.created_at,
            "expires_at": app.expires_at,
            "invite_id": app.invite_id,
        }
        for app in mentor_application_service.list_pending(mentor_id)
    }


def _remove_pending_mentor_invite(mentor_id, apprentice_id):
    mentor_id = str(mentor_id)
    apprentice_id = str(apprentice_id)
    invite = _get_pending_mentor_invites(mentor_id).get(apprentice_id)
    if invite:
        mentor_application_service.resolve(invite["invite_id"], mentor_id, apprentice_id, "cancelled")


def _find_pending_mentor_invite_by_apprentice(apprentice_id):
    apprentice_id = str(apprentice_id)
    app = mentor_application_service.find_pending_by_apprentice(apprentice_id)
    if app:
        return app.mentor_id, {"timestamp": app.created_at, "expires_at": app.expires_at, "invite_id": app.invite_id}
    return None, None


def _format_mentor_applicant_ids(apprentice_ids, limit=5):
    names = []
    apprentice_ids = list(apprentice_ids)
    for apprentice_id in apprentice_ids[:limit]:
        apprentice_info = sql_message.get_user_real_info(apprentice_id)
        names.append(apprentice_info["user_name"] if apprentice_info else str(apprentice_id))
    if len(apprentice_ids) > limit:
        names.append(f"等{len(apprentice_ids)}人")
    return "、".join(names)


def _format_pending_mentor_applicants(mentor_id, limit=5):
    return _format_mentor_applicant_ids(
        _get_pending_mentor_invites(mentor_id).keys(), limit=limit
    )


def _count_pending_mentor_invites(mentor_id):
    return len(_get_pending_mentor_invites(mentor_id))


def _cooldown_remaining(user_id, field):
    data = load_mentor(user_id)
    until = _parse_datetime(data.get(field))
    if until is None:
        return 0
    now = datetime.now()
    if now >= until:
        return 0
    return int((until - now).total_seconds())


def _set_mentor_cooldown(user_id, field, days):
    data = load_mentor(user_id)
    data[field] = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    save_mentor(user_id, data)


def _load_mentor_title_data():
    global _mentor_title_cache
    if _mentor_title_cache is not None:
        return _mentor_title_cache
    try:
        with open(TITLE_JSONPATH, "r", encoding="UTF-8") as f:
            _mentor_title_cache = json.load(f)
    except Exception as e:
        logger.warning(f"师徒称号数据读取失败: {e}")
        _mentor_title_cache = {}
    return _mentor_title_cache


def _get_mentor_title_by_id(title_id):
    return _load_mentor_title_data().get(str(title_id))


def _get_user_title_ids(user_id):
    unlocked = player_data_manager.get_field_data(str(user_id), "title", "unlocked")
    if not unlocked:
        return []
    if isinstance(unlocked, list):
        return [str(title_id) for title_id in unlocked]
    if isinstance(unlocked, str):
        try:
            return [str(title_id) for title_id in json.loads(unlocked)]
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _grant_title_to_user(user_id, title_id):
    title_data = _get_mentor_title_by_id(title_id)
    if not title_data:
        return False, "称号ID不存在"
    unlocked_set = set(_get_user_title_ids(user_id))
    if str(title_id) in unlocked_set:
        return False, f"用户已拥有称号【{title_data['name']}】"
    unlocked_set.add(str(title_id))
    player_data_manager.update_or_write_data(
        str(user_id), "title", "unlocked", list(unlocked_set), data_type="TEXT"
    )
    return True, f"已赠送称号【{title_data['name']}】"


def _grant_mentor_title(user_id, title_key):
    title_id = MENTOR_TITLE_IDS.get(title_key)
    if not title_id or not _get_mentor_title_by_id(title_id):
        return ""
    ok, _ = _grant_title_to_user(str(user_id), title_id)
    if not ok:
        return ""
    title_data = _get_mentor_title_by_id(title_id)
    title_name = title_data["name"] if title_data else title_id
    log_message(str(user_id), f"[师徒称号] 解锁称号【{title_name}】")
    return f"解锁称号【{title_name}】"


def _grant_mentor_titles_by_stats(user_id):
    stats = player_data_manager.get_fields(str(user_id), "statistics") or {}
    granted = []
    if safe_int(stats.get("师徒传功次数")) >= 100:
        msg = _grant_mentor_title(user_id, "transmission_100")
        if msg:
            granted.append(msg)
    if safe_int(stats.get("接受传功次数")) >= 50:
        msg = _grant_mentor_title(user_id, "receive_transmission_50")
        if msg:
            granted.append(msg)
    if safe_int(stats.get("培养出师徒弟")) >= 5:
        msg = _grant_mentor_title(user_id, "mentor_graduate_5")
        if msg:
            granted.append(msg)
    return granted


def _add_mentor_history(user_id, event_type, description, related_id=None):
    data = load_mentor(user_id)
    history = _normalize_history(data.get("mentor_history"))
    history.append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type": str(event_type),
        "related_id": str(related_id) if related_id is not None else "",
        "description": str(description),
    })
    data["mentor_history"] = history[-MENTOR_HISTORY_LIMIT:]
    save_mentor(user_id, data)


def _record_mentor_event(mentor_id, apprentice_id, event_type, mentor_desc, apprentice_desc):
    _add_mentor_history(mentor_id, event_type, mentor_desc, apprentice_id)
    _add_mentor_history(apprentice_id, event_type, apprentice_desc, mentor_id)


def _set_pair_rebind_cooldown(apprentice_id, mentor_id, days):
    data = load_mentor(apprentice_id)
    rebind_cd = _normalize_dict(data.get("mentor_rebind_cd"))
    rebind_cd[str(mentor_id)] = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    data["mentor_rebind_cd"] = rebind_cd
    save_mentor(apprentice_id, data)


def _get_pair_rebind_remaining(apprentice_id, mentor_id):
    data = load_mentor(apprentice_id)
    rebind_cd = _normalize_dict(data.get("mentor_rebind_cd"))
    until = _parse_datetime(rebind_cd.get(str(mentor_id)))
    if until is None:
        return 0
    now = datetime.now()
    if now >= until:
        rebind_cd.pop(str(mentor_id), None)
        data["mentor_rebind_cd"] = rebind_cd
        save_mentor(apprentice_id, data)
        return 0
    return int((until - now).total_seconds())


def _get_bind_wait_remaining(apprentice_id):
    data = load_mentor(apprentice_id)
    bind_time = _parse_datetime(data.get("bind_time"))
    if bind_time is None:
        return 0
    available_time = bind_time + timedelta(hours=MENTOR_NEW_BIND_TRANSMISSION_WAIT_HOURS)
    now = datetime.now()
    if now >= available_time:
        return 0
    return int((available_time - now).total_seconds())


def _grant_graduation_rewards(mentor_id, apprentice_id):
    reward_lines = []
    mentor_info = sql_message.get_user_real_info(mentor_id)
    apprentice_info = sql_message.get_user_real_info(apprentice_id)

    if MENTOR_GRADUATE_APPRENTICE_STONE_REWARD > 0:
        sql_message.update_ls(apprentice_id, MENTOR_GRADUATE_APPRENTICE_STONE_REWARD, 1)
        update_statistics_value(apprentice_id, "灵石获取", increment=MENTOR_GRADUATE_APPRENTICE_STONE_REWARD)
        reward_lines.append(f"徒弟获得灵石{number_to(MENTOR_GRADUATE_APPRENTICE_STONE_REWARD)}")

    if MENTOR_GRADUATE_MENTOR_STONE_REWARD > 0:
        sql_message.update_ls(mentor_id, MENTOR_GRADUATE_MENTOR_STONE_REWARD, 1)
        update_statistics_value(mentor_id, "灵石获取", increment=MENTOR_GRADUATE_MENTOR_STONE_REWARD)
        reward_lines.append(f"师父获得灵石{number_to(MENTOR_GRADUATE_MENTOR_STONE_REWARD)}")

    update_statistics_value(apprentice_id, "正常出师次数", increment=1)
    update_statistics_value(mentor_id, "培养出师徒弟", increment=1)

    title_lines = []
    for title_msg in (
        _grant_mentor_title(apprentice_id, "graduate"),
        _grant_mentor_title(mentor_id, "mentor_graduate"),
    ):
        if title_msg:
            title_lines.append(title_msg)
    title_lines.extend(_grant_mentor_titles_by_stats(mentor_id))

    mentor_name = mentor_info["user_name"] if mentor_info else str(mentor_id)
    apprentice_name = apprentice_info["user_name"] if apprentice_info else str(apprentice_id)
    log_message(mentor_id, f"[师徒出师] 徒弟{apprentice_name}出师，奖励{number_to(MENTOR_GRADUATE_MENTOR_STONE_REWARD)}灵石")
    log_message(apprentice_id, f"[师徒出师] 从师父{mentor_name}门下出师，奖励{number_to(MENTOR_GRADUATE_APPRENTICE_STONE_REWARD)}灵石")

    return reward_lines, title_lines


def _resolve_user_id_from_args(args: Message):
    target_id = get_at_user_id(args)
    arg = args.extract_plain_text().strip()

    if target_id:
        return str(target_id)

    if arg.isdigit():
        return arg

    if arg:
        target_info = sql_message.get_user_info_with_name(arg)
        if target_info:
            return str(target_info["user_id"])

    return None


def get_valid_apprentices(mentor_id):
    mentor_data = load_mentor(mentor_id)
    apprentice_ids = []
    for apprentice_id in mentor_data.get("apprentice_ids", []):
        apprentice_data = load_mentor(apprentice_id)
        if str(apprentice_data.get("mentor_id")) == str(mentor_id):
            apprentice_ids.append(str(apprentice_id))
    return apprentice_ids


def check_is_mentor_pair(mentor_id, apprentice_id):
    mentor_id = str(mentor_id)
    apprentice_id = str(apprentice_id)
    mentor_data = load_mentor(mentor_id)
    apprentice_data = load_mentor(apprentice_id)
    return (
        apprentice_id in _normalize_id_list(mentor_data.get("apprentice_ids"))
        and str(apprentice_data.get("mentor_id")) == mentor_id
    )


def get_mentor_team_attack_buffs(member_ids):
    """返回副本组队中的师徒攻击加成。"""
    member_set = {str(member_id) for member_id in member_ids}
    buff_user_ids = set()

    for mentor_id in member_set:
        for apprentice_id in get_valid_apprentices(mentor_id):
            if apprentice_id in member_set:
                buff_user_ids.add(mentor_id)
                buff_user_ids.add(apprentice_id)

    return {
        user_id: {
            "name": "薪火相承",
            "attack_multiplier": 1.10,
        }
        for user_id in buff_user_ids
    }


def _validate_mentor_application(apprentice_id, mentor_id):
    apprentice_id = str(apprentice_id)
    mentor_id = str(mentor_id)

    apprentice_info = sql_message.get_user_info_with_id(apprentice_id)
    mentor_info = sql_message.get_user_info_with_id(mentor_id)
    if not apprentice_info or not mentor_info:
        return False, "未找到指定道友，对方可能尚未踏入修仙界。"

    apprentice_data = load_mentor(apprentice_id)
    mentor_data = load_mentor(mentor_id)

    if apprentice_data.get("mentor_id"):
        return False, "你已经有师父了，不能再拜其他师父。请先出师或解除当前师徒关系。"

    if str(mentor_data.get("mentor_id")) == apprentice_id:
        return False, "不可向自己的徒弟拜师。"

    mentor_cd = _cooldown_remaining(mentor_id, "mentor_cd_until")
    if mentor_cd > 0:
        return False, f"对方处于收徒冷却中，剩余{_format_seconds(mentor_cd)}，冷却结束后才能继续收徒。"

    apprentice_cd = _cooldown_remaining(apprentice_id, "apprentice_cd_until")
    if apprentice_cd > 0:
        return False, f"你处于拜师冷却中，剩余{_format_seconds(apprentice_cd)}。"

    pair_rebind_cd = _get_pair_rebind_remaining(apprentice_id, mentor_id)
    if pair_rebind_cd > 0:
        return False, f"你暂不能再次拜入对方门下，剩余{_format_seconds(pair_rebind_cd)}。"

    apprentices = get_valid_apprentices(mentor_id)
    if len(apprentices) >= MENTOR_MAX_APPRENTICES:
        return False, f"对方门下徒弟已达上限（{MENTOR_MAX_APPRENTICES}人），需有徒弟出师，或逐出师门并等待收徒冷却结束后才能继续收徒。"

    gap = get_realm_gap(mentor_info["level"], apprentice_info["level"])
    required_gap = get_mentor_required_gap(mentor_info["level"])
    if gap < required_gap:
        return (
            False,
            f"拜师条件不足：你的境界需低于对方{required_gap}个小境界或以上，当前相差{max(gap, 0)}个小境界。",
        )

    return True, ""


def _bind_mentor_relation(mentor_id, apprentice_id):
    mentor_id = str(mentor_id)
    apprentice_id = str(apprentice_id)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    mentor_data = load_mentor(mentor_id)
    apprentice_ids = get_valid_apprentices(mentor_id)
    if apprentice_id not in apprentice_ids:
        apprentice_ids.append(apprentice_id)
    mentor_data["apprentice_ids"] = apprentice_ids
    save_mentor(mentor_id, mentor_data)

    apprentice_data = load_mentor(apprentice_id)
    apprentice_data["mentor_id"] = mentor_id
    apprentice_data["bind_time"] = now
    save_mentor(apprentice_id, apprentice_data)

    mentor_info = sql_message.get_user_real_info(mentor_id)
    apprentice_info = sql_message.get_user_real_info(apprentice_id)
    mentor_name = mentor_info["user_name"] if mentor_info else str(mentor_id)
    apprentice_name = apprentice_info["user_name"] if apprentice_info else str(apprentice_id)

    update_statistics_value(mentor_id, "收徒次数", increment=1)
    update_statistics_value(apprentice_id, "拜师次数", increment=1)
    _record_mentor_event(
        mentor_id,
        apprentice_id,
        "bind",
        f"收{apprentice_name}为徒",
        f"拜{mentor_name}为师",
    )
    log_message(mentor_id, f"[师徒] 收{apprentice_name}为徒")
    log_message(apprentice_id, f"[师徒] 拜{mentor_name}为师")

    title_lines = []
    for title_msg in (
        _grant_mentor_title(apprentice_id, "apprentice"),
        _grant_mentor_title(mentor_id, "mentor"),
    ):
        if title_msg:
            title_lines.append(title_msg)

    return now, title_lines


def _remove_mentor_relation(mentor_id, apprentice_id):
    mentor_id = str(mentor_id)
    apprentice_id = str(apprentice_id)

    mentor_data = load_mentor(mentor_id)
    mentor_data["apprentice_ids"] = [
        uid for uid in _normalize_id_list(mentor_data.get("apprentice_ids")) if uid != apprentice_id
    ]
    save_mentor(mentor_id, mentor_data)

    apprentice_data = load_mentor(apprentice_id)
    if str(apprentice_data.get("mentor_id")) == mentor_id:
        apprentice_data["mentor_id"] = None
        apprentice_data["bind_time"] = None
        apprentice_data["breakthrough_reward_count"] = 0
        save_mentor(apprentice_id, apprentice_data)


def _get_remaining_mentor_transmission(user_id):
    used_count = mentor_exp_cd.find_user(user_id)
    return max(mentor_transmission_limit - used_count, 0)


def _build_mentor_help_buttons():
    return {
        "md_type": "buff",
        "k1": "拜师", "v1": "拜师",
        "k2": "师徒", "v2": "我的师徒",
        "k3": "传功", "v3": "师徒传功",
        "k4": "保护", "v4": "拜师保护 状态",
    }


def _mentor_application_result_message(result, requested_mentor_id):
    if result.status == "protected":
        return "对方已开启拜师保护，自动拒绝了你的拜师申请。"
    if result.status == "invite_conflict":
        return "该事件已用于其他拜师申请。"
    application = result.application
    if result.status == "already_pending" and application is not None:
        mentor_info = sql_message.get_user_real_info(application.mentor_id)
        mentor_name = (
            mentor_info["user_name"] if mentor_info else application.mentor_id
        )
        return f"你已有向{mentor_name}发出的待处理拜师申请。"
    if result.status == "duplicate" and application is not None:
        mentor_info = sql_message.get_user_real_info(application.mentor_id)
        mentor_name = (
            mentor_info["user_name"] if mentor_info else application.mentor_id
        )
        status_messages = {
            "pending": f"你已向{mentor_name}发送拜师申请，正在等待回应。",
            "accepted": f"你向{mentor_name}发出的拜师申请已经被同意。",
            "rejected": f"你向{mentor_name}发出的拜师申请已经被拒绝。",
            "expired": f"你向{mentor_name}发出的拜师申请已经过期。",
            "cancelled": f"你向{mentor_name}发出的拜师申请已经取消。",
        }
        return status_messages.get(
            application.status, "这条拜师申请已经处理。"
        )
    mentor_info = sql_message.get_user_real_info(requested_mentor_id)
    mentor_name = mentor_info["user_name"] if mentor_info else str(requested_mentor_id)
    return f"向{mentor_name}发起拜师申请时状态发生变化，请稍后重试。"


@apply_mentor.handle(parameterless=[Cooldown(cd_time=0)])
async def apply_mentor_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """申请拜师"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await apply_mentor.finish()

    user_id = str(user_info["user_id"])
    mentor_id = _resolve_user_id_from_args(args)
    buttons = _build_mentor_help_buttons()

    if not mentor_id:
        await handle_send(bot, event, "请指定拜师对象！格式：拜师 道号/QQ", **buttons)
        await apply_mentor.finish()

    invite_id = _relation_operation_id(event, "mentor-application", user_id)
    replayed = mentor_application_service.replay_create(
        invite_id, mentor_id, user_id
    )
    if replayed is not None:
        await handle_send(
            bot, event,
            _mentor_application_result_message(replayed, mentor_id),
            **buttons,
        )
        await apply_mentor.finish()

    if mentor_id == user_id:
        await handle_send(bot, event, "道友不能拜自己为师。", **buttons)
        await apply_mentor.finish()

    ok, reason = _validate_mentor_application(user_id, mentor_id)
    if not ok:
        await handle_send(bot, event, reason, **buttons)
        await apply_mentor.finish()

    apply_remaining, apply_target = _get_mentor_apply_remaining(user_id)
    if apply_remaining > 0 and str(apply_target) != str(mentor_id):
        target_info = sql_message.get_user_real_info(apply_target) if apply_target else None
        target_name = target_info["user_name"] if target_info else "其他道友"
        await handle_send(
            bot,
            event,
            f"你在{MENTOR_APPLY_LIMIT_HOURS}小时内已经向{target_name}发起过拜师申请，"
            f"剩余{_format_seconds(apply_remaining)}后才能拜其他师父。",
            **buttons,
        )
        await apply_mentor.finish()

    mentor_info = sql_message.get_user_real_info(mentor_id)
    created = mentor_application_service.create(invite_id, mentor_id, user_id)
    if not created.succeeded:
        await handle_send(
            bot, event,
            _mentor_application_result_message(created, mentor_id),
            **buttons,
        )
        await apply_mentor.finish()
    if created.status == "duplicate":
        await handle_send(
            bot, event,
            _mentor_application_result_message(created, mentor_id),
            **buttons,
        )
        await apply_mentor.finish()
    asyncio.create_task(expire_mentor_invite(mentor_id, user_id, invite_id, bot, event))

    msg = f"已向{mentor_info['user_name']}发送拜师申请，等待对方回应。"
    await handle_send(
        bot,
        event,
        msg,
        md_type="buff",
        k1="同意", v1=f"同意拜师 {user_id}",
        k2="拒绝", v2=f"拒绝拜师 {user_id}",
        k3="师徒", v3="我的师徒",
    )
    await apply_mentor.finish()


async def expire_mentor_invite(mentor_id, apprentice_id, invite_id, bot, event):
    """拜师申请过期处理"""
    await asyncio.sleep(60)
    mentor_id = str(mentor_id)
    apprentice_id = str(apprentice_id)
    expired = mentor_application_service.resolve(
        invite_id, mentor_id, apprentice_id, "expired",
        operation_id=f"mentor-application-expire:{invite_id}",
    )
    if expired.status == "applied":
        await handle_send(
            bot,
            event,
            "拜师申请已过期！",
            md_type="buff",
            k1="拜师", v1="拜师",
            k2="师徒", v2="我的师徒",
            k3="关系", v3="关系帮助",
        )


async def _send_mentor_bind_success(
    bot, event, mentor_id, mentor_info, apprentice_id, apprentice_info, result
):
    title_lines = []
    if result.status == "applied":
        log_message(mentor_id, f"[师徒] 收{apprentice_info['user_name']}为徒")
        log_message(apprentice_id, f"[师徒] 拜{mentor_info['user_name']}为师")
        title_lines = (
            _grant_mentor_titles_by_stats(mentor_id)
            + _grant_mentor_titles_by_stats(apprentice_id)
        )
    title_msg = "\n" + "\n".join(title_lines) if title_lines else ""
    msg = (
        f"你已收{apprentice_info['user_name']}为徒，拜师时间为{result.bind_time}。\n"
        f"新拜师后{MENTOR_NEW_BIND_TRANSMISSION_WAIT_HOURS}小时内不能传功。"
        f"{title_msg}"
    )
    await handle_send(
        bot,
        event,
        msg,
        md_type="buff",
        k1="传功", v1=f"师徒传功 {apprentice_info['user_name']}",
        k2="师徒", v2="我的师徒",
        k3="关系", v3="关系帮助",
    )


@agree_mentor.handle(parameterless=[Cooldown(cd_time=0)])
async def agree_mentor_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """同意拜师"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await agree_mentor.finish()

    mentor_id = str(user_info["user_id"])
    buttons = _build_mentor_help_buttons()
    apprentice_id = _resolve_user_id_from_args(args)

    operation_id = None
    if apprentice_id:
        apprentice_id = str(apprentice_id)
        operation_id = _relation_operation_id(event, "mentor-bind", mentor_id)
        replayed = mentor_bind_service.replay(
            operation_id, mentor_id, apprentice_id
        )
        if replayed is not None:
            if not replayed.succeeded:
                await handle_send(bot, event, "该事件已用于其他拜师确认。", **buttons)
                await agree_mentor.finish()
            apprentice_info = sql_message.get_user_real_info(apprentice_id)
            await _send_mentor_bind_success(
                bot, event, mentor_id, user_info, apprentice_id,
                apprentice_info, replayed,
            )
            await agree_mentor.finish()

    pending_invites = _get_pending_mentor_invites(mentor_id)
    if not pending_invites:
        await handle_send(bot, event, "没有待处理的拜师申请！", **buttons)
        await agree_mentor.finish()

    if not apprentice_id:
        pending_names = _format_pending_mentor_applicants(mentor_id)
        await handle_send(
            bot,
            event,
            f"请指定要收下的徒弟！格式：同意拜师 徒弟道号/QQ\n当前待处理申请：{pending_names}",
            **buttons,
        )
        await agree_mentor.finish()

    apprentice_id = str(apprentice_id)
    if apprentice_id not in pending_invites:
        await handle_send(bot, event, "该道友没有向你发起待处理的拜师申请。", **buttons)
        await agree_mentor.finish()

    ok, reason = _validate_mentor_application(apprentice_id, mentor_id)
    if not ok:
        _remove_pending_mentor_invite(mentor_id, apprentice_id)
        await handle_send(bot, event, f"拜师失败：{reason}", **buttons)
        await agree_mentor.finish()

    invite_data = pending_invites[apprentice_id]
    apprentice_info = sql_message.get_user_real_info(apprentice_id)
    bind_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = mentor_bind_service.apply(
        operation_id, mentor_id, apprentice_id,
        invite_data["invite_id"], bind_time=bind_time,
        expected_mentor_level=user_info["level"], expected_apprentice_level=apprentice_info["level"],
        max_apprentices=MENTOR_MAX_APPRENTICES, history_limit=MENTOR_HISTORY_LIMIT,
        mentor_desc=f"收{apprentice_info['user_name']}为徒",
        apprentice_desc=f"拜{user_info['user_name']}为师",
    )
    if not result.succeeded:
        await handle_send(bot, event, "拜师邀请或双方状态已变化，请重新申请。", **buttons)
        await agree_mentor.finish()
    await _send_mentor_bind_success(
        bot, event, mentor_id, user_info, apprentice_id, apprentice_info, result
    )
    await agree_mentor.finish()


@reject_mentor.handle(parameterless=[Cooldown(cd_time=0)])
async def reject_mentor_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """拒绝拜师"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await reject_mentor.finish()

    mentor_id = str(user_info["user_id"])
    buttons = _build_mentor_help_buttons()
    apprentice_id = _resolve_user_id_from_args(args)

    operation_id = None
    if apprentice_id:
        apprentice_id = str(apprentice_id)
        operation_id = _relation_operation_id(
            event, "mentor-application-reject", mentor_id
        )
        replayed = mentor_application_service.replay_resolution(
            operation_id, mentor_id, apprentice_id, "rejected"
        )
        if replayed is not None:
            if replayed.status == "operation_conflict":
                await handle_send(bot, event, "该事件已用于其他拜师申请处理。", **buttons)
            elif replayed.succeeded:
                apprentice_info = sql_message.get_user_real_info(apprentice_id)
                apprentice_name = (
                    apprentice_info["user_name"] if apprentice_info else apprentice_id
                )
                await handle_send(
                    bot, event, f"你拒绝了{apprentice_name}的拜师申请。",
                    **buttons,
                )
            else:
                await handle_send(bot, event, "拜师申请状态已经变化，未执行拒绝。", **buttons)
            await reject_mentor.finish()

    pending_invites = _get_pending_mentor_invites(mentor_id)
    if not pending_invites:
        await handle_send(bot, event, "没有待处理的拜师申请！", **buttons)
        await reject_mentor.finish()
    if not apprentice_id:
        pending_names = _format_pending_mentor_applicants(mentor_id)
        await handle_send(
            bot,
            event,
            f"请指定要拒绝的徒弟！格式：拒绝拜师 徒弟道号/QQ\n当前待处理申请：{pending_names}",
            **buttons,
        )
        await reject_mentor.finish()

    apprentice_id = str(apprentice_id)
    if apprentice_id not in pending_invites:
        await handle_send(bot, event, "该道友没有向你发起待处理的拜师申请。", **buttons)
        await reject_mentor.finish()

    apprentice_info = sql_message.get_user_real_info(apprentice_id)
    apprentice_name = apprentice_info["user_name"] if apprentice_info else apprentice_id
    rejected = mentor_application_service.resolve(
        pending_invites[apprentice_id]["invite_id"],
        mentor_id,
        apprentice_id,
        "rejected",
        operation_id=operation_id,
    )
    if not rejected.succeeded:
        await handle_send(bot, event, "拜师申请状态已经变化，未执行拒绝。", **buttons)
        await reject_mentor.finish()

    await handle_send(bot, event, f"你拒绝了{apprentice_name}的拜师申请。", **buttons)
    await reject_mentor.finish()


@my_mentor.handle(parameterless=[Cooldown(cd_time=0)])
async def my_mentor_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, command: tuple = Command()):
    """查看师徒信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_mentor.finish()

    user_id = str(user_info["user_id"])
    mentor_data = load_mentor(user_id)
    mentor_id = mentor_data.get("mentor_id")

    if mentor_id:
        mentor_info = sql_message.get_user_real_info(mentor_id)
        bind_time = mentor_data.get("bind_time") or "未知"
        mentor_line = f"{mentor_info['user_name']}（{mentor_info['level']}，拜师时间：{bind_time}）" if mentor_info else "数据异常"
    else:
        mentor_line = "无"

    apprentice_lines = []
    for apprentice_id in get_valid_apprentices(user_id):
        apprentice_info = sql_message.get_user_real_info(apprentice_id)
        if apprentice_info:
            apprentice_lines.append(f"- {apprentice_info['user_name']}（{apprentice_info['level']}）")

    apprentice_msg = "\n".join(apprentice_lines) if apprentice_lines else "无"

    mentor_cd = _cooldown_remaining(user_id, "mentor_cd_until")
    apprentice_cd = _cooldown_remaining(user_id, "apprentice_cd_until")
    cd_lines = []
    if mentor_cd > 0:
        cd_lines.append(f"收徒冷却：{_format_seconds(mentor_cd)}")
    if apprentice_cd > 0:
        cd_lines.append(f"拜师冷却：{_format_seconds(apprentice_cd)}")
    cd_msg = "\n".join(cd_lines) if cd_lines else "无"

    stats_data = player_data_manager.get_fields(user_id, "statistics") or {}
    remain = _get_remaining_mentor_transmission(user_id)
    mentor_protect_status = "开启（自动拒绝拜师）" if mentor_data.get("mentor_protect") == "on" else "关闭（允许拜师申请）"
    pending_count = _count_pending_mentor_invites(user_id)
    command_name = str(command[0]) if command else "我的师徒"

    if command_name in {"我的师父", "我的师傅"}:
        msg = f"""【我的师父】
师父：{mentor_line}

今日剩余传功次数：{remain}/{mentor_transmission_limit}
接受传功：{safe_int(stats_data.get("接受传功次数"))}
拜师冷却：{_format_seconds(apprentice_cd) if apprentice_cd > 0 else "无"}"""
        await handle_send(bot, event, msg, md_type="buff", k1="师徒", v1="我的师徒", k2="拜师", v2="拜师", k3="徒弟", v3="我的徒弟", k4="记录", v4="师徒记录")
        await my_mentor.finish()

    if command_name == "我的徒弟":
        msg = f"""【我的徒弟】
徒弟（{len(apprentice_lines)}/{MENTOR_MAX_APPRENTICES}）：
{apprentice_msg}

拜师保护：{mentor_protect_status}
待处理拜师申请：{pending_count}条
今日剩余传功次数：{remain}/{mentor_transmission_limit}
累计收徒：{safe_int(stats_data.get("收徒次数"))}，培养出师：{safe_int(stats_data.get("培养出师徒弟"))}
累计传功：{safe_int(stats_data.get("师徒传功次数"))}
收徒冷却：{_format_seconds(mentor_cd) if mentor_cd > 0 else "无"}"""
        await handle_send(bot, event, msg, md_type="buff", k1="师徒", v1="我的师徒", k2="传功", v2="师徒传功", k3="师父", v3="我的师父", k4="保护", v4="拜师保护 状态")
        await my_mentor.finish()

    msg = f"""【我的师徒】
师父：{mentor_line}

徒弟（{len(apprentice_lines)}/{MENTOR_MAX_APPRENTICES}）：
{apprentice_msg}

拜师保护：{mentor_protect_status}
待处理拜师申请：{pending_count}条
今日剩余传功次数：{remain}/{mentor_transmission_limit}
累计收徒：{safe_int(stats_data.get("收徒次数"))}，培养出师：{safe_int(stats_data.get("培养出师徒弟"))}
累计传功：{safe_int(stats_data.get("师徒传功次数"))}，接受传功：{safe_int(stats_data.get("接受传功次数"))}
冷却状态：{cd_msg}"""

    await handle_send(bot, event, msg, md_type="buff", k1="拜师", v1="拜师", k2="传功", v2="师徒传功", k3="记录", v3="师徒记录", k4="保护", v4="拜师保护 状态")
    await my_mentor.finish()


@mentor_record.handle(parameterless=[Cooldown(cd_time=0)])
async def mentor_record_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看师徒记录"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mentor_record.finish()

    user_id = str(user_info["user_id"])
    history = list(reversed(load_mentor(user_id).get("mentor_history", [])))
    if not history:
        await handle_send(bot, event, "暂无师徒记录。", md_type="buff", k1="师徒", v1="我的师徒", k2="拜师", v2="拜师", k3="关系", v3="关系帮助")
        await mentor_record.finish()

    lines = ["【师徒记录】"]
    for record in history[:10]:
        lines.append(
            f"- {record.get('time', '未知时间')} | {record.get('description', '未知事件')}"
        )
    lines.append("")
    lines.append(f"仅显示最近10条，最多保留{MENTOR_HISTORY_LIMIT}条。")
    await handle_send(bot, event, "\n".join(lines), md_type="buff", k1="师徒", v1="我的师徒", k2="榜单", v2="师徒排行榜", k3="关系", v3="关系帮助")
    await mentor_record.finish()


@mentor_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def mentor_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """师徒排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mentor_rank.finish()

    all_stats = player_data_manager.get_all_records("statistics")
    rank_rows = []
    for stats in all_stats:
        user_id = str(stats.get("user_id", ""))
        if not user_id:
            continue
        user_real_info = sql_message.get_user_real_info(user_id)
        if not user_real_info:
            continue
        apprentice_count = len(get_valid_apprentices(user_id))
        graduate_count = safe_int(stats.get("培养出师徒弟"))
        transmission_count = safe_int(stats.get("师徒传功次数"))
        receive_count = safe_int(stats.get("接受传功次数"))
        score = graduate_count * 1000 + apprentice_count * 100 + transmission_count
        if score <= 0 and apprentice_count <= 0:
            continue
        rank_rows.append({
            "user_name": user_real_info["user_name"],
            "level": user_real_info["level"],
            "apprentice_count": apprentice_count,
            "graduate_count": graduate_count,
            "transmission_count": transmission_count,
            "receive_count": receive_count,
            "score": score,
        })

    rank_rows.sort(
        key=lambda row: (
            row["graduate_count"],
            row["apprentice_count"],
            row["transmission_count"],
            row["receive_count"],
        ),
        reverse=True,
    )

    if not rank_rows:
        await handle_send(bot, event, "暂无师徒榜数据。", md_type="buff", k1="师徒", v1="我的师徒", k2="拜师", v2="拜师", k3="关系", v3="关系帮助")
        await mentor_rank.finish()

    lines = ["【师徒排行榜】"]
    for idx, row in enumerate(rank_rows[:20], start=1):
        lines.append(
            f"{idx}. {row['user_name']}（{row['level']}）\n"
            f"   徒弟：{row['apprentice_count']} | 出师：{row['graduate_count']} | 传功：{row['transmission_count']}"
        )
    await handle_send(bot, event, "\n".join(lines), md_type="buff", k1="师徒", v1="我的师徒", k2="记录", v2="师徒记录", k3="关系", v3="关系帮助")
    await mentor_rank.finish()


@unbind_mentor.handle(parameterless=[Cooldown(cd_time=0)])
async def unbind_mentor_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """解除师徒关系 / 逐出师门 / 出师"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await unbind_mentor.finish()

    user_id = str(user_info["user_id"])
    target_id = _resolve_user_id_from_args(args)
    buttons = _build_mentor_help_buttons()

    if target_id:
        if not check_is_mentor_pair(user_id, target_id):
            await handle_send(bot, event, "对方不是你的徒弟，无法逐出师门。", **buttons)
            await unbind_mentor.finish()

        target_info = sql_message.get_user_real_info(target_id)
        mentor_name = user_info["user_name"]
        target_name = target_info["user_name"] if target_info else str(target_id)
        now = datetime.now()
        occurred_at = now.strftime("%Y-%m-%d %H:%M:%S")
        mentor_cd_until = (now + timedelta(days=MENTOR_COOLDOWN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        apprentice_cd_until = (now + timedelta(days=APPRENTICE_COOLDOWN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        pair_rebind_until = (now + timedelta(days=MENTOR_SAME_PAIR_REBIND_COOLDOWN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        settlement = mentor_expel_service.apply(
            _relation_operation_id(event, "expel", user_id, target_id), user_id, target_id,
            occurred_at=occurred_at, mentor_cd_until=mentor_cd_until,
            apprentice_cd_until=apprentice_cd_until, pair_rebind_until=pair_rebind_until,
            history_limit=MENTOR_HISTORY_LIMIT, mentor_desc=f"将{target_name}逐出师门",
            apprentice_desc=f"被师父{mentor_name}逐出师门",
        )
        if not settlement.succeeded:
            await handle_send(bot, event, "师徒关系状态已变化，本次逐出未执行。", **buttons)
            await unbind_mentor.finish()
        if settlement.status == "applied":
            log_message(user_id, f"[师徒] 将{target_name}逐出师门")
            log_message(target_id, f"[师徒] 被师父{mentor_name}逐出师门")
        msg = (
            f"你已将{target_name}逐出师门。\n"
            f"你进入{MENTOR_COOLDOWN_DAYS}天收徒冷却，对方进入{APPRENTICE_COOLDOWN_DAYS}天拜师冷却。\n"
            f"对方{MENTOR_SAME_PAIR_REBIND_COOLDOWN_DAYS}天内不能再次拜入你门下。"
        )
        await unbind_mentor.finish()

    mentor_data = load_mentor(user_id)
    mentor_id = mentor_data.get("mentor_id")
    if not mentor_id:
        apprentices = get_valid_apprentices(user_id)
        if apprentices:
            await handle_send(bot, event, "你是师父身份，如需解除关系请使用：逐出师门 道号", **buttons)
        else:
            await handle_send(bot, event, "你当前没有师徒关系。", **buttons)
        await unbind_mentor.finish()

    mentor_info = sql_message.get_user_real_info(mentor_id)
    mentor_name = mentor_info["user_name"] if mentor_info else str(mentor_id)
    if is_wujie_or_above(user_info["level"]):
        mentor_stats = player_data_manager.get_fields(mentor_id, "statistics") or {}
        mentor_titles = [MENTOR_TITLE_IDS["mentor_graduate"]]
        if safe_int(mentor_stats.get("培养出师徒弟"), 0) + 1 >= 5:
            mentor_titles.append(MENTOR_TITLE_IDS["mentor_graduate_5"])
        settlement = mentor_graduation_service.apply(
            _relation_operation_id(event, "graduate", mentor_id, user_id), mentor_id, user_id,
            expected_mentor_stone=mentor_info["stone"], expected_apprentice_stone=user_info["stone"],
            apprentice_reward=MENTOR_GRADUATE_APPRENTICE_STONE_REWARD,
            mentor_reward=MENTOR_GRADUATE_MENTOR_STONE_REWARD,
            cooldown_days=MENTOR_GRADUATE_PAIR_REBIND_COOLDOWN_DAYS,
            history_limit=MENTOR_HISTORY_LIMIT,
            mentor_desc=f"徒弟{user_info['user_name']}修至无界境出师",
            apprentice_desc=f"从师父{mentor_name}门下出师",
            apprentice_title_ids=[MENTOR_TITLE_IDS["graduate"]], mentor_title_ids=mentor_titles,
        )
        if not settlement.succeeded:
            await handle_send(bot, event, "师徒状态发生变化，本次出师未结算，请重试。", **buttons)
            await unbind_mentor.finish()
        reward_lines = [
            f"徒弟获得灵石{number_to(settlement.apprentice_stone)}",
            f"师父获得灵石{number_to(settlement.mentor_stone)}",
        ]
        title_lines = ["出师称号奖励已发放"]
        extra_lines = reward_lines + title_lines
        extra_msg = "\n" + "\n".join(extra_lines) if extra_lines else ""
        msg = (
            f"你已修至无界境，正式从{mentor_name}门下出师。\n"
            f"{MENTOR_GRADUATE_PAIR_REBIND_COOLDOWN_DAYS}天内不能再次拜入同一位师父门下。"
            f"{extra_msg}"
        )
        await handle_send(bot, event, msg, **buttons)
        await unbind_mentor.finish()

    now = datetime.now()
    occurred_at = now.strftime("%Y-%m-%d %H:%M:%S")
    apprentice_cd_until = (now + timedelta(days=APPRENTICE_COOLDOWN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    pair_rebind_until = (now + timedelta(days=MENTOR_SAME_PAIR_REBIND_COOLDOWN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    settlement = apprentice_leave_service.apply(
        _relation_operation_id(event, "leave", mentor_id, user_id), mentor_id, user_id,
        occurred_at=occurred_at, expected_apprentice_level=user_info["level"],
        graduation_eligible=False, apprentice_cd_until=apprentice_cd_until,
        pair_rebind_until=pair_rebind_until, history_limit=MENTOR_HISTORY_LIMIT,
        mentor_desc=f"徒弟{user_info['user_name']}离开师门",
        apprentice_desc=f"离开师父{mentor_name}门下",
    )
    if not settlement.succeeded:
        await handle_send(bot, event, "师徒关系状态已变化，本次离师未执行。", **buttons)
        await unbind_mentor.finish()
    if settlement.status == "applied":
        log_message(user_id, f"[师徒] 离开师父{mentor_name}门下")
        log_message(mentor_id, f"[师徒] 徒弟{user_info['user_name']}离开师门")
    msg = (
        f"你已离开{mentor_name}门下，进入{APPRENTICE_COOLDOWN_DAYS}天拜师冷却。\n"
        f"{MENTOR_SAME_PAIR_REBIND_COOLDOWN_DAYS}天内不能再次拜入同一位师父门下。"
    )
    await handle_send(bot, event, msg, **buttons)
    await unbind_mentor.finish()


@mentor_transmission.handle(parameterless=[Cooldown(cd_time=0)])
async def mentor_transmission_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """师徒传功"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, mentor_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mentor_transmission.finish()

    mentor_id = str(mentor_info["user_id"])
    apprentices = get_valid_apprentices(mentor_id)
    target_id = _resolve_user_id_from_args(args)
    buttons = _build_mentor_help_buttons()

    if not target_id:
        if len(apprentices) == 1:
            target_id = apprentices[0]
        else:
            await handle_send(bot, event, "请指定传功对象！格式：师徒传功 徒弟道号/QQ", **buttons)
            await mentor_transmission.finish()

    target_id = str(target_id)
    if target_id == mentor_id:
        await handle_send(bot, event, "不能给自己传功。", **buttons)
        await mentor_transmission.finish()

    if not check_is_mentor_pair(mentor_id, target_id):
        await handle_send(bot, event, "对方不是你的徒弟，无法传功。", **buttons)
        await mentor_transmission.finish()

    apprentice_info = sql_message.get_user_info_with_id(target_id)
    if not apprentice_info:
        await handle_send(bot, event, "徒弟信息异常，无法传功。", **buttons)
        await mentor_transmission.finish()

    gap = get_realm_gap(mentor_info["level"], apprentice_info["level"])
    if gap <= 0:
        await handle_send(bot, event, "徒弟境界已与你相同或在你之上，无法继续传功。", **buttons)
        await mentor_transmission.finish()

    bind_wait_remaining = _get_bind_wait_remaining(target_id)
    if bind_wait_remaining > 0:
        await handle_send(
            bot,
            event,
            f"新拜师后{MENTOR_NEW_BIND_TRANSMISSION_WAIT_HOURS}小时内不能传功，剩余{_format_seconds(bind_wait_remaining)}。",
            **buttons,
        )
        await mentor_transmission.finish()

    mentor_remain = _get_remaining_mentor_transmission(mentor_id)
    apprentice_remain = _get_remaining_mentor_transmission(target_id)
    if mentor_remain <= 0:
        await handle_send(bot, event, "你的今日传功次数已用尽。", **buttons)
        await mentor_transmission.finish()
    if apprentice_remain <= 0:
        await handle_send(bot, event, "徒弟今日可接受传功次数已用尽。", **buttons)
        await mentor_transmission.finish()

    apprentice_exp = int(apprentice_info["exp"])
    max_exp_limit = int(OtherSet().set_closing_type(apprentice_info["level"])) * XiuConfig().closing_exp_upper_limit
    remaining_exp = int(max_exp_limit - apprentice_exp)
    if remaining_exp <= 0:
        await handle_send(bot, event, "徒弟当前修为已达境界上限，无法继续传功。", **buttons)
        await mentor_transmission.finish()

    effect_ratio = min(gap, MENTOR_MAX_EFFECT_GAP) / MENTOR_MAX_EFFECT_GAP
    once_cap = max(1, int(apprentice_exp * 0.01))
    give_exp = max(1, int(once_cap * effect_ratio))
    give_exp = min(give_exp, once_cap, remaining_exp)

    if give_exp <= 0:
        await handle_send(bot, event, "本次传功未能获得修为。", **buttons)
        await mentor_transmission.finish()

    new_exp = apprentice_exp + give_exp
    hp, mp, atk = _recovered_attributes(apprentice_info, new_exp)
    settlement = mentor_transmission_service.apply(
        _relation_operation_id(event, "transmission", mentor_id, target_id), mentor_id, target_id,
        expected_apprentice_exp=apprentice_exp, reward_exp=give_exp,
        power=_relation_power(apprentice_info, new_exp), hp=hp, mp=mp, atk=atk,
        mentor_used=mentor_transmission_limit - mentor_remain,
        apprentice_used=mentor_transmission_limit - apprentice_remain,
        daily_limit=mentor_transmission_limit, history_limit=MENTOR_HISTORY_LIMIT,
        mentor_desc=f"向徒弟{apprentice_info['user_name']}传功，授予修为{number_to(give_exp)}",
        apprentice_desc=f"师父{mentor_info['user_name']}传功，获得修为{number_to(give_exp)}",
    )
    if not settlement.succeeded:
        await handle_send(bot, event, "师徒状态发生变化，本次传功未结算，请重试。", **buttons)
        await mentor_transmission.finish()
    if settlement.status == "applied":
        mentor_exp_cd.add_user(mentor_id)
        mentor_exp_cd.add_user(target_id)
    title_lines = _grant_mentor_titles_by_stats(mentor_id) + _grant_mentor_titles_by_stats(target_id)
    log_message(mentor_id, f"向徒弟{apprentice_info['user_name']}传功，授予修为{number_to(give_exp)}")
    log_message(target_id, f"师父{mentor_info['user_name']}传功，获得修为{number_to(give_exp)}")

    title_msg = "\n" + "\n".join(title_lines) if title_lines else ""
    msg = (
        f"师父{mentor_info['user_name']}运转真元，为徒弟{apprentice_info['user_name']}传功。\n"
        f"境界差：{gap}个小境界，效果系数：{effect_ratio * 100:.0f}%\n"
        f"{apprentice_info['user_name']}获得修为：{number_to(give_exp)}\n"
        f"今日剩余传功次数：师父{_get_remaining_mentor_transmission(mentor_id)}/{mentor_transmission_limit}，"
        f"徒弟{_get_remaining_mentor_transmission(target_id)}/{mentor_transmission_limit}"
        f"{title_msg}"
    )
    await handle_send(bot, event, msg, md_type="buff", k1="传功", v1=f"师徒传功 {apprentice_info['user_name']}", k2="师徒", v2="我的师徒", k3="修为", v3="我的修为")
    await mentor_transmission.finish()

@partner_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def partner_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """道侣排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await partner_rank.finish()

    # 获取所有用户的affection数据
    all_user_integral = player_data_manager.get_all_field_data("partner", "affection")
    
    # 排序数据
    sorted_integral = sorted(all_user_integral, key=lambda x: safe_int(x[1]), reverse=True)
    
    # 生成排行榜
    rank_lines = ["【道侣排行榜】"]
    for i, (user_id, affection) in enumerate(sorted_integral[:50], start=1):
        user_info = sql_message.get_user_info_with_id(user_id)
        partner_id = player_data_manager.get_field_data(str(user_id), "partner", "partner_id")
        partner_info = sql_message.get_user_info_with_id(partner_id)
        if partner_info is None:
            continue
        rank_lines.append(
            f"{i}. {user_info['user_name']} & {partner_info['user_name']}\n"
            f"   亲密度：{number_to(affection)}"
        )
    
    if len(rank_lines) == 1:
        rank_lines.append("暂无道侣榜数据。")

    await handle_send(bot, event, "\n".join(rank_lines))
    await partner_rank.finish()

def trigger_partner_exp_share(user_id, new_level):
    user_id = str(user_id)
    partner_data = load_partner(user_id)
    if partner_data and partner_data.get('partner_id'):
        partner_id = str(partner_data['partner_id'])
        user_info = sql_message.get_user_info_with_id(user_id)
        partner_info = sql_message.get_user_info_with_id(partner_id)
        if not user_info or not partner_info or not awaitable_check_partner_pair(user_id, partner_id):
            return ""
        self_exp = int(user_info['exp'])
        partner_exp = int(partner_info['exp'])
        partner_name = partner_info['user_name']
    
        # 计算可赠送的修为量：突破者当前修为的1%
        give_exp = int(self_exp * 0.01)
    
        # 上限：不得超过道侣当前修为的10%
        max_give = int(partner_exp * 0.10)
        give_exp = min(give_exp, max_give)
    
        if give_exp > 0:
            # 随机触发概率（基础5%，每1000亲密度+1%，上限50%）
            affection = partner_data.get('affection', 0)
            trigger_rate = min(40 + (affection // 1000), 50)
        
            if random.randint(1, 100) <= trigger_rate:
                result = partner_breakthrough_service.apply(
                    f"partner-breakthrough:{user_id}:{new_level}", user_id, partner_id, new_level,
                    expected_user_exp=self_exp, expected_partner_exp=partner_exp,
                    expected_affection=affection, reward_exp=give_exp,
                    partner_power=_relation_power(partner_info, partner_exp + give_exp),
                )
                if not result.succeeded:
                    return ""
            
                # 记录日志
                log_message(user_id, f"突破{new_level}，道侣共享修为：{number_to(give_exp)}")
                log_message(partner_id, f"道侣突破{new_level}，获得共享修为：{number_to(give_exp)}")
                return f"\n道侣{partner_name}感受到你的突破，获得{number_to(give_exp)}修为！"
    return ""


def awaitable_check_partner_pair(user_id, partner_id):
    reciprocal = load_partner(partner_id)
    return str(reciprocal.get("partner_id")) == str(user_id)


def _mentor_breakthrough_reward_rate(apprentice_level):
    """
    参考双修按境界段缩放：低境界拿更低比例，高境界最多到配置上限。
    """
    base_rate = _config_rate(MENTOR_BREAKTHROUGH_REWARD_BASE_RATE, 0.005)
    min_rate = _config_rate(MENTOR_BREAKTHROUGH_REWARD_MIN_RATE, 0.001)
    max_rate = _config_rate(MENTOR_BREAKTHROUGH_REWARD_MAX_RATE, 0.01)
    if min_rate > max_rate:
        min_rate, max_rate = max_rate, min_rate

    try:
        apprentice_rank = convert_rank(apprentice_level)[0]
        max_rank = convert_rank("江湖好手")[0]
        rank_progress = max(max_rank - apprentice_rank, 0)
        apprentice_stage = (max(rank_progress - 1, 0) // 3) + 1
    except (TypeError, ValueError):
        apprentice_stage = 1

    level_factor = min(apprentice_stage * 0.2, 2)
    return max(min_rate, min(max_rate, base_rate * level_factor))


def trigger_mentor_breakthrough_reward(apprentice_id, new_level):
    apprentice_id = str(apprentice_id)
    apprentice_data = load_mentor(apprentice_id)
    mentor_id = apprentice_data.get("mentor_id")
    if not mentor_id or not check_is_mentor_pair(mentor_id, apprentice_id):
        return ""

    reward_count = safe_int(apprentice_data.get("breakthrough_reward_count"), 0)
    if reward_count >= MENTOR_BREAKTHROUGH_REWARD_LIMIT:
        return ""

    mentor_info = sql_message.get_user_info_with_id(mentor_id)
    apprentice_info = sql_message.get_user_info_with_id(apprentice_id)
    if not mentor_info or not apprentice_info:
        return ""

    mentor_exp = safe_int(mentor_info.get("exp"))
    if mentor_exp <= 0:
        return ""

    max_exp_limit = int(OtherSet().set_closing_type(mentor_info["level"])) * XiuConfig().closing_exp_upper_limit
    remaining_exp = int(max_exp_limit - mentor_exp)
    if remaining_exp <= 0:
        return ""

    reward_rate = _mentor_breakthrough_reward_rate(new_level)
    give_exp = min(int(mentor_exp * reward_rate), remaining_exp)
    if give_exp <= 0:
        return ""

    mentor_name = mentor_info["user_name"]
    apprentice_name = apprentice_info["user_name"]
    business_event_id = f"mentor-breakthrough:{apprentice_id}:{new_level}:{apprentice_info['exp']}"
    result = mentor_breakthrough_reward_service.apply(
        business_event_id, mentor_id, apprentice_id, new_level, business_event_id,
        expected_mentor_exp=mentor_exp, expected_apprentice_exp=apprentice_info["exp"],
        expected_reward_count=reward_count, reward_limit=MENTOR_BREAKTHROUGH_REWARD_LIMIT,
        reward_exp=give_exp, max_mentor_exp=max_exp_limit,
        mentor_power=_relation_power(mentor_info, mentor_exp + give_exp),
        history_limit=MENTOR_HISTORY_LIMIT,
        mentor_desc=f"徒弟{apprentice_name}突破{new_level}，获得返修{number_to(give_exp)}（{reward_count + 1}/{MENTOR_BREAKTHROUGH_REWARD_LIMIT}）",
        apprentice_desc=f"突破{new_level}，师父{mentor_name}获得返修{number_to(give_exp)}（{reward_count + 1}/{MENTOR_BREAKTHROUGH_REWARD_LIMIT}）",
    )
    if not result.succeeded:
        return ""
    if result.status == "applied":
        log_message(
            mentor_id,
            f"[师徒] 徒弟{apprentice_name}突破{new_level}，获得返修{number_to(give_exp)}（{reward_count + 1}/{MENTOR_BREAKTHROUGH_REWARD_LIMIT}）",
        )
        log_message(
            apprentice_id,
            f"[师徒] 突破{new_level}，师父{mentor_name}获得返修{number_to(give_exp)}（{reward_count + 1}/{MENTOR_BREAKTHROUGH_REWARD_LIMIT}）",
        )
    return f"\n师父{mentor_name}因徒弟突破{new_level}，获得{number_to(give_exp)}修为返修！"
