import random
import asyncio
import re
import json
from datetime import datetime
from pathlib import Path

from ..on_compat import on_command
from nonebot.log import logger
from nonebot.params import CommandArg

from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent, MessageSegment
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
from .two_exp_cd import two_exp_cd

invite_cache = {}
partner_invite_cache = {}
sql_message = XiuxianDateManage()
xiuxian_impart = XIUXIAN_IMPART_BUFF()
player_data_manager = PlayerDataManager()
two_exp_limit = 3
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

two_exp_invite = on_command("双修", priority=6, block=True)
two_exp_accept = on_command("同意双修", priority=5, block=True)
two_exp_reject = on_command("拒绝双修", priority=5, block=True)
two_exp_protect = on_command("双修保护", priority=5, block=True)
double_cultivation_help = on_command("道侣帮助", aliases={"双修帮助"}, priority=5, block=True)
my_exp_num = on_command("我的双修次数", priority=9, block=True)
my_partner = on_command("我的道侣", priority=5, block=True)
bind_partner = on_command("绑定道侣", aliases={"结为道侣"}, priority=5, block=True)
agree_bind = on_command("同意道侣", aliases={"接受道侣"}, priority=5, block=True)
unbind_partner = on_command("解除道侣", aliases={"断绝关系"}, priority=5, block=True)
partner_rank = on_command("道侣排行榜", priority=5, block=True)


__double_cultivation_help__ = f"""
【双修与道侣系统】🌸

💕 双修系统：
  • 双修 [道友QQ/道号] [次数] - 邀请他人双修
  • 同意双修 - 接受双修邀请
  • 拒绝双修 - 拒绝双修邀请
  • 双修保护 [开启/关闭/拒绝/状态] - 设置双修保护

  ⚙️ 双修规则：
  • 基础双修次数：每人每天{two_exp_limit}次
  • 修为限制：修为低者无法向修为高者发起双修
  • 保护机制：可设置拒绝所有双修、仅接受邀请、或完全开放
  • 特殊事件：双修时有6%概率触发特殊事件，获得额外修为和突破概率

  🌟 双修效果：
  • 获得修为提升
  • 增加突破概率
  • 道侣双修有额外加成
  • 可能触发天降异象等特殊效果

🔗 道侣系统：
  • 绑定道侣 [道号] - 向道友发送道侣绑定邀请
  • 同意道侣 - 接受道侣绑定
  • 我的道侣 - 查看当前道侣信息
  • 解除道侣 - 断绝与道侣的关系


✨ 温馨提示：
  • 双修是修仙界提升修为的重要方式之一
  • 与道侣双修可获得额外的修为收益和情感体验
  • 合理使用双修保护功能，管理好自己的修炼时光
  • 道侣关系需要双方共同维护，珍惜每一次的双修机会
"""


@double_cultivation_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def double_cultivation_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, __double_cultivation_help__,
        k1="双修", v1="双修",
        k2="道侣", v2="我的道侣",
        k3="绑定", v3="绑定道侣"
    )

async def two_exp_cd_up():
    two_exp_cd.re_data()
    logger.opt(colors=True).info(f"<green>双修次数已更新！</green>")

def load_player_user(user_id):
    """加载用户数据，如果不存在或为空，返回默认数据"""
    user_id_str = str(user_id)
    status = player_data_manager.get_field_data(user_id_str, "status", "two_exp_protect")
    if status is None:
        status = "off"  # 默认值为 False
    return status

def save_player_user(user_id, status):
    """保存用户数据，确保目录存在"""
    user_id_str = str(user_id)
    player_data_manager.update_or_write_data(user_id_str, "status", "two_exp_protect", status)

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

    # 检查是否已经发出过邀请（作为邀请者）
    existing_invite = None
    for target_id, invite_data in invite_cache.items():
        if invite_data['inviter'] == user_id:
            existing_invite = target_id
            break

    if existing_invite is not None:
        # 已经发出过邀请，提示用户等待
        target_info = sql_message.get_user_real_info(existing_invite)
        remaining_time = 60 - (datetime.now().timestamp() - invite_cache[existing_invite]['timestamp'])
        msg = f"你已经向{target_info['user_name']}发送了双修邀请，请等待{int(remaining_time)}秒后邀请过期或对方回应后再发送新邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()

    # 检查是否有未处理的邀请（作为被邀请者）
    if str(user_id) in invite_cache:
        # 有未处理的邀请，提示用户
        inviter_id = invite_cache[str(user_id)]['inviter']
        inviter_info = sql_message.get_user_real_info(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - invite_cache[str(user_id)]['timestamp'])
        msg = f"道友已有来自{inviter_info['user_name']}的双修邀请（剩余{int(remaining_time)}秒），请先处理！\n发送【同意双修】或【拒绝双修】"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()

    two_qq = None
    exp_count = 1  # 默认双修次数

    for arg in args:
        if arg.type == "at":
            two_qq = arg.data.get("qq", "")
        else:
            arg_text = args.extract_plain_text().strip()
            # 尝试解析次数
            count_match = re.search(r'(\d+)次', arg_text)
            if count_match:
                exp_count = int(count_match.group(1))
                # 移除次数信息，保留道号
                arg_text = re.sub(r'\d+次', '', arg_text).strip()
            
            if arg_text:
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

    # 检查对方是否已经作为邀请者发出过邀请
    target_existing_invite = None
    for target_id, invite_data in invite_cache.items():
        if invite_data['inviter'] == two_qq:
            target_existing_invite = target_id
            break

    if target_existing_invite is not None:
        # 对方已经发出过邀请，提示用户
        target_info = sql_message.get_user_real_info(target_existing_invite)
        remaining_time = 60 - (datetime.now().timestamp() - invite_cache[target_existing_invite]['timestamp'])
        msg = f"对方已经向{target_info['user_name']}发送了双修邀请，请等待{int(remaining_time)}秒后再试！"
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()

    # 检查对方是否有未处理的邀请（作为被邀请者）
    if str(two_qq) in invite_cache:
        # 对方有未处理的邀请，提示用户
        inviter_id = invite_cache[str(two_qq)]['inviter']
        inviter_info = sql_message.get_user_real_info(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - invite_cache[str(two_qq)]['timestamp'])
        msg = f"对方已有来自{inviter_info['user_name']}的双修邀请（剩余{int(remaining_time)}秒），请稍后再试！"
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
    user_2_info = sql_message.get_user_real_info(two_qq)
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
        # 对方开启保护，需要发送邀请
        # 检查邀请是否已存在（再次确认，防止并发）
        if str(two_qq) in invite_cache:
            msg = "对方已有未处理的双修邀请，请稍后再试！"
            await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
            await two_exp_invite.finish()
        
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
        invite_id = f"{user_id}_{two_qq}_{datetime.now().timestamp()}"
        invite_cache[str(two_qq)] = {
            'inviter': user_id,
            'count': min(exp_count, max_count_2),  # 取最小值
            'timestamp': datetime.now().timestamp(),
            'invite_id': invite_id
        }

        # 设置60秒过期
        asyncio.create_task(expire_invite(two_qq, invite_id, bot, event))

        user_2_info = sql_message.get_user_real_info(two_qq)
        msg = f"已向{user_2_info['user_name']}发送双修邀请（{min(exp_count, max_count_2)}次），等待对方回应..."
        await handle_send(bot, event, msg, md_type="buff", k1="同意", v1="同意双修", k2="拒绝", v2="拒绝双修", k3="双修", v3="双修")
        await two_exp_invite.finish()
    else:
        # 对方关闭保护，直接进行双修
        await direct_two_exp(bot, event, user_id, two_qq, exp_count, is_partner=is_partner)
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

async def direct_two_exp(bot, event, user_id_1, user_id_2, exp_count=1, is_partner=False):
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

        # 只有实际进行了双修才消耗次数
        two_exp_cd.add_user(user_id_1)
        two_exp_cd.add_user(user_id_2)

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

    # 统一写入最终获得修为
    sql_message.update_exp(user_id_1, total_exp_1)
    sql_message.update_power2(user_id_1)

    user_1_info_before_recover = sql_message.get_user_real_info(user_id_1)
    result_msg_1, result_hp_mp_1 = OtherSet().send_hp_mp(
        user_id_1,
        int(user_1_info_before_recover["exp"] / 10),
        int(user_1_info_before_recover["exp"] / 20)
    )
    sql_message.update_user_attribute(
        user_id_1,
        result_hp_mp_1[0],
        result_hp_mp_1[1],
        int(result_hp_mp_1[2] / 10)
    )

    sql_message.update_exp(user_id_2, total_exp_2)
    sql_message.update_power2(user_id_2)

    user_2_info_before_recover = sql_message.get_user_real_info(user_id_2)
    result_msg_2, result_hp_mp_2 = OtherSet().send_hp_mp(
        user_id_2,
        int(user_2_info_before_recover["exp"] / 10),
        int(user_2_info_before_recover["exp"] / 20)
    )
    sql_message.update_user_attribute(
        user_id_2,
        result_hp_mp_2[0],
        result_hp_mp_2[1],
        int(result_hp_mp_2[2] / 10)
    )

    user_1_info = sql_message.get_user_real_info(user_id_1)
    user_2_info = sql_message.get_user_real_info(user_id_2)

    update_statistics_value(user_id_1, "双修次数", increment=actual_used_count)
    update_statistics_value(user_id_2, "双修次数", increment=actual_used_count)

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
        partner_data_1 = load_partner(user_id_1)
        partner_data_2 = load_partner(user_id_2)

        if (
            partner_data_1
            and partner_data_2
            and str(partner_data_1.get("partner_id")) == str(user_id_2)
            and str(partner_data_2.get("partner_id")) == str(user_id_1)
        ):
            current_affection_1 = safe_int(partner_data_1.get("affection"), 0)
            current_affection_2 = safe_int(partner_data_2.get("affection"), 0)

            add_affection_1 = 20 * actual_used_count
            add_affection_2 = 10 * actual_used_count

            partner_data_1["affection"] = current_affection_1 + add_affection_1
            partner_data_2["affection"] = current_affection_2 + add_affection_2

            save_partner(user_id_1, partner_data_1)
            save_partner(user_id_2, partner_data_2)

            affection_msg = (
                f"\n\n💕道侣双修亲密度增加："
                f"\n{user_1_info['user_name']} +{add_affection_1}"
                f"\n{user_2_info['user_name']} +{add_affection_2}"
            )
        else:
            affection_msg = "\n\n⚠️检测到道侣关系数据异常，本次未增加亲密度。"

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

        sql_message.update_levelrate(user_id_1, user_mes_1["level_up_rate"] + 2)
        sql_message.update_levelrate(user_id_2, user_mes_2["level_up_rate"] + 2)

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

@two_exp_accept.handle(parameterless=[Cooldown(cd_time=1.4)])
async def two_exp_accept_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """同意双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await two_exp_accept.finish()
        
    user_id = user_info['user_id']
    
    # 检查是否有邀请
    if str(user_id) not in invite_cache:
        msg = "没有待处理的双修邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_accept.finish()
        
    invite_data = invite_cache[str(user_id)]
    inviter_id = invite_data['inviter']
    exp_count = invite_data['count']
    
    # 删除邀请
    del invite_cache[str(user_id)]
    
    await direct_two_exp(bot, event, inviter_id, user_id, exp_count)
    await two_exp_accept.finish()

async def expire_invite(user_id, invite_id, bot, event):
    """邀请过期处理"""
    await asyncio.sleep(60)
    if str(user_id) in invite_cache and invite_cache[str(user_id)]['invite_id'] == invite_id:
        inviter_id = invite_cache[str(user_id)]['inviter']
        # 发送过期提示
        msg = f"双修邀请已过期！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        # 删除过期的邀请
        del invite_cache[str(user_id)]

@two_exp_reject.handle(parameterless=[Cooldown(cd_time=1.4)])
async def two_exp_reject_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """拒绝双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await two_exp_reject.finish()
        
    user_id = user_info['user_id']
    
    if str(user_id) not in invite_cache:
        msg = "没有待处理的双修邀请！"
        await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
        await two_exp_reject.finish()
        
    invite_data = invite_cache[str(user_id)]
    inviter_id = invite_data['inviter']
    
    inviter_info = sql_message.get_user_real_info(inviter_id)
    msg = f"你拒绝了{inviter_info['user_name']}的双修邀请！"
    
    # 删除邀请
    del invite_cache[str(user_id)]
    
    await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="次数", v2="我的双修次数", k3="修为", v3="我的修为")
    await two_exp_reject.finish()

@two_exp_protect.handle(parameterless=[Cooldown(cd_time=1.4)])
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
    current_status = load_player_user(user_id)
    
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
    
    # 保存用户数据
    save_player_user(user_id, current_status)
    await handle_send(bot, event, msg, md_type="buff", k1="开启", v1="双修保护 开启", k2="关闭", v2="双修保护 关闭", k3="拒绝", v3="双修保护 拒绝", k4="状态", v4="双修保护 状态")
    await two_exp_protect.finish()

@my_exp_num.handle(parameterless=[Cooldown(cd_time=1.4)])
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
    tokens_used = min(num, current_count)
    if tokens_used > 0:
        two_exp_cd.remove_user(user_id, tokens_used)
        
        sql_message.update_back_j(user_id, item_id, tokens_used)
        
        # 计算剩余双修次数
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        impart_two_exp = impart_data['impart_two_exp'] if impart_data is not None else 0
        main_two_data = UserBuffDate(user_id).get_user_main_buff_data()
        main_two = main_two_data['two_buff'] if main_two_data is not None else 0
        remaining_count = (two_exp_limit + impart_two_exp + main_two) - two_exp_cd.find_user(user_id)
        
        msg = f"增加{tokens_used}次双修！\n"
        msg += f"当前剩余双修次数：{remaining_count}次"
    else:
        msg = "当前剩余双修次数已满！"
    
    await handle_send(bot, event, msg, md_type="buff", k1="双修", v1="双修", k2="我的修为", v2="我的修为", k3="次数", v3="我的双修次数")

@bind_partner.handle(parameterless=[Cooldown(cd_time=1.4)])
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
    partner_user_id = None
    if arg.startswith("@"):
        # 解析艾特
        for arg_item in args:
            if arg_item.type == "at":
                partner_user_id = arg_item.data.get("qq", "")
                break
    else:
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

@agree_bind.handle(parameterless=[Cooldown(cd_time=1.4)])
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
        
    invite_data = partner_invite_cache[str(user_id)]
    inviter_id = invite_data['inviter']
    
    # 获取双方信息
    inviter_info = sql_message.get_user_real_info(inviter_id)
    user_info = sql_message.get_user_real_info(user_id)
    
    # 创建道侣数据
    partner_data = {
        'partner_id': inviter_id,
        'bind_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'affection': 0  # 初始化亲密度
    }
    
    # 保存用户道侣数据
    save_partner(user_id, partner_data)
    
    # 创建对方道侣数据
    partner_data_inviter = {
        'partner_id': user_id,
        'bind_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'affection': 0  # 初始化亲密度
    }
    
    # 保存邀请者道侣数据
    save_partner(inviter_id, partner_data_inviter)
    
    # 删除邀请
    del partner_invite_cache[str(user_id)]
    
    msg = f"你已与{inviter_info['user_name']}结为道侣，绑定时间为{partner_data['bind_time']}。"
    await handle_send(bot, event, msg)    
    await agree_bind.finish()

@unbind_partner.handle(parameterless=[Cooldown(cd_time=1.4)])
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
    
    partner_user_id = partner_data["partner_id"]
    bind_time_str = partner_data.get("bind_time")
    
    if not bind_time_str:
        # 如果没有绑定时间，视为异常情况，允许解绑
        msg = "检测到绑定时间异常，允许解绑道侣。"
        await handle_send(bot, event, msg)
        # 继续执行解绑逻辑
    else:
        try:
            bind_time = datetime.strptime(bind_time_str, '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()
            time_difference = current_time - bind_time
            days_difference = time_difference.days
            
            if days_difference < 7:
                remaining_days = 7 - days_difference
                msg = f"你与道侣的绑定时间不足7天，还需等待{remaining_days}天才能解绑道侣。"
                await handle_send(bot, event, msg, md_type="buff", k1="解除", v1="断绝关系", k2="绑定", v2="绑定道侣", k3="道侣", v3="我的道侣")
                await unbind_partner.finish()
        except ValueError:
            # 如果 bind_time 格式不正确，视为异常，允许解绑
            msg = "检测到绑定时间格式异常，允许解绑道侣。"
            await handle_send(bot, event, msg)
            # 继续执行解绑逻辑
    
    # 继续执行解绑逻辑
    partner_user_id = partner_data["partner_id"]
    
    # 解除双方道侣关系
    save_partner(user_id, {'partner_id': None, 'bind_time': None, 'affection': None})
    save_partner(partner_user_id, {'partner_id': None, 'bind_time': None, 'affection': None})
    
    msg = f"你已与道侣断绝关系。"
    await handle_send(bot, event, msg, md_type="buff", k1="绑定", v1="绑定道侣", k2="解除", v2="断绝关系", k3="道侣", v3="我的道侣")
    
    await unbind_partner.finish()

def get_affection_level(affection):
    affection = safe_int(affection)
    if affection >= 10000:
        affection_level = "💖 深情厚谊"
    elif affection >= 5000:
        affection_level = "💕 心有灵犀"
    elif affection >= 1000:
        affection_level = "💗 初识情愫"
    else:
        affection_level = "💓 缘分伊始"
    return affection_level

@my_partner.handle(parameterless=[Cooldown(cd_time=1.4)])
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
    msg = f"""💕 我的道侣信息 💕
🏮 道侣道号：{partner_info['user_name']}
🌟 当前境界：{sql_message.get_user_info_with_id(partner_user_id)['level']}
💫 当前修为：{number_to(sql_message.get_user_info_with_id(partner_user_id)['exp'])}
🤝 绑定时间：{bind_time}
⏳ 相伴天数：{bound_days} 天
💖 亲密度：{affection} ({affection_level})"""
    await handle_send(bot, event, msg)
    await my_partner.finish()

# 加载和保存道侣数据的函数
def _is_none_like(value):
    """
    兼容历史脏数据：
    None / "" / "None" / "null" / "NULL" 都视为无值。
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in ["", "none", "null"]:
        return True
    return False


def safe_int(value, default=0):
    try:
        if _is_none_like(value):
            return default
        return int(value)
    except (ValueError, TypeError):
        return default

def load_partner(user_id):
    """
    加载用户自己的道侣数据。

    修复点：
    1. 不再读取对方的 partner 表，避免亲密度、绑定时间读错。
    2. 兼容历史 "None" / "null" / "" 脏数据。
    """
    info = player_data_manager.get_fields(str(user_id), "partner")

    if not info:
        return {
            "partner_id": None,
            "bind_time": None,
            "affection": 0
        }

    partner_id = info.get("partner_id")
    bind_time = info.get("bind_time")
    affection = info.get("affection")

    if _is_none_like(partner_id):
        partner_id = None
    else:
        partner_id = str(partner_id)

    if _is_none_like(bind_time):
        bind_time = None
    else:
        bind_time = str(bind_time)

    affection = safe_int(affection, 0)

    return {
        "partner_id": partner_id,
        "bind_time": bind_time,
        "affection": affection
    }


def save_partner(user_id, data):
    """
    保存用户道侣数据。

    注意：
    如果你已经修复 PlayerDataManager.update_or_write_data，使 None 写入 SQL NULL，
    这里可以直接传 None。
    """
    partner_id = data.get("partner_id")
    bind_time = data.get("bind_time")
    affection = data.get("affection")

    if _is_none_like(partner_id):
        partner_id = None
    else:
        partner_id = str(partner_id)

    if _is_none_like(bind_time):
        bind_time = None
    else:
        bind_time = str(bind_time)

    affection = safe_int(affection, 0)

    player_data_manager.update_or_write_data(
        str(user_id), "partner", "partner_id", partner_id, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "partner", "bind_time", bind_time, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "partner", "affection", affection, data_type="INTEGER"
    )

@partner_rank.handle(parameterless=[Cooldown(cd_time=1.4)])
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
    rank_msg = "✨【道侣排行榜】✨\n"
    rank_msg += "-----------------------------------\n"
    for i, (user_id, affection) in enumerate(sorted_integral[:50], start=1):
        user_info = sql_message.get_user_info_with_id(user_id)
        partner_id = player_data_manager.get_field_data(str(user_id), "partner", "partner_id")
        partner_info = sql_message.get_user_info_with_id(partner_id)
        if partner_info is None:
            continue
        rank_msg += f"第{i}位 | {user_info['user_name']}&{partner_info['user_name']}\n亲密度：{number_to(affection)}\n"
    
    await handle_send(bot, event, rank_msg)
    await partner_rank.finish()

def trigger_partner_exp_share(user_id, new_level):
    partner_data = load_partner(user_id)
    if partner_data and partner_data.get('partner_id'):
        partner_id = partner_data['partner_id']
    
        # 获取双方当前修为
        self_exp = sql_message.get_user_info_with_id(user_id)['exp']
        partner_info = sql_message.get_user_info_with_id(partner_id)
        partner_exp = partner_info['exp']
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
                # 给道侣加修为
                sql_message.update_exp(partner_id, give_exp)
                sql_message.update_power2(partner_id)  # 更新战力
            
                # 记录日志
                log_message(user_id, f"突破{new_level}，道侣共享修为：{number_to(give_exp)}")
                log_message(partner_id, f"道侣突破{new_level}，获得共享修为：{number_to(give_exp)}")
                return f"\n道侣{partner_name}感受到你的突破，获得{number_to(give_exp)}修为！"
    return ""
