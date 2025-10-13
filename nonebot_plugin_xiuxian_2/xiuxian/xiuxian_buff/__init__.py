import random
import asyncio
import re
import json
from nonebot.log import logger
from datetime import datetime
from pathlib import Path
from nonebot import on_command, on_fullmatch
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, OtherSet, get_player_info, 
    save_player_info,UserBuffDate, get_main_info_msg, 
    get_user_buff, get_sec_msg, get_sub_info_msg, get_effect_info_msg,
    XIUXIAN_IMPART_BUFF, leave_harm_time
)
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata
from nonebot.params import CommandArg
from ..xiuxian_utils.player_fight import Player_fight
from ..xiuxian_utils.utils import (
    number_to, check_user, send_msg_handler,
    check_user_type, get_msg_pic, CommandObjectID, handle_send, log_message, update_statistics_value
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from .two_exp_cd import two_exp_cd


cache_help = {}
invite_cache = {}
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
BLESSEDSPOTCOST = 3500000 # 洞天福地购买消耗
two_exp_limit = 3 # 默认双修次数上限，修仙之人一天3次也不奇怪（
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

buffinfo = on_fullmatch("我的功法", priority=25, block=True)
out_closing = on_command("出关", aliases={"灵石出关"}, priority=5, block=True)
in_closing = on_fullmatch("闭关", priority=5, block=True)
up_exp = on_command("修炼", priority=5, block=True)
reset_exp = on_command("重置修炼状态", priority=5, block=True)
stone_exp = on_command("灵石修炼", aliases={"灵石修仙"}, priority=5, block=True)
two_exp_invite = on_command("双修", priority=5, block=True)
two_exp_accept = on_fullmatch("同意双修", priority=5, block=True)
two_exp_reject = on_fullmatch("拒绝双修", priority=5, block=True)
two_exp_protect = on_command("双修保护", priority=5, block=True)
mind_state = on_fullmatch("我的状态", priority=7, block=True)
qc = on_command("切磋", priority=6, block=True)
buff_help = on_command("功法帮助", aliases={"灵田帮助", "洞天福地帮助"}, priority=5, block=True)
blessed_spot_creat = on_fullmatch("洞天福地购买", priority=10, block=True)
blessed_spot_info = on_fullmatch("洞天福地查看", priority=11, block=True)
blessed_spot_rename = on_command("洞天福地改名", priority=7, block=True)
ling_tian_up = on_fullmatch("灵田开垦", priority=5, block=True)
del_exp_decimal = on_fullmatch("抑制黑暗动乱", priority=9, block=True)
my_exp_num = on_fullmatch("我的双修次数", priority=9, block=True)

__buff_help__ = f"""
【修仙功法系统】📜

🌿 功法修炼：
  我的功法 - 查看当前修炼的功法详情
  抑制黑暗动乱 - 清除修为浮点数(稳定境界)

🏡 洞天福地：
  洞天福地购买 - 获取专属修炼福地
  洞天福地查看 - 查看福地状态
  洞天福地改名+名字 - 为福地命名

🌱 灵田管理：
  灵田开垦 - 提升灵田等级(增加药材产量)
  当前最高等级：9级

👥 双修系统：
  双修 道号 [x次] - 发送双修邀请
  同意双修 - 接受双修邀请
  拒绝双修 - 拒绝双修邀请
  我的双修次数 - 查看剩余双修机会
  双修保护 开启/关闭 - 管理双修邀请权限
  切磋@道友 - 友好比试(不消耗气血)

💡 小贴士：
  1. 洞天福地可加速修炼
  2. 灵田每23小时可收获
  3. 双修邀请60秒后过期
""".strip()

async def two_exp_cd_up():
    two_exp_cd.re_data()
    logger.opt(colors=True).info(f"<green>双修次数已更新！</green>")


@buff_help.handle(parameterless=[Cooldown(at_sender=False)])
async def buff_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """功法帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        msg = cache_help[session_id]
        await handle_send(bot, event, msg)
        await buff_help.finish()
    else:
        msg = __buff_help__
        await handle_send(bot, event, msg)
        await buff_help.finish()


@blessed_spot_creat.handle(parameterless=[Cooldown(at_sender=False)])
async def blessed_spot_creat_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """洞天福地购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) != 0:
        msg = f"道友已经拥有洞天福地了，请发送洞天福地查看吧~"
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()
    if user_info['stone'] < BLESSEDSPOTCOST:
        msg = f"道友的灵石不足{BLESSEDSPOTCOST}枚，无法购买洞天福地"
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()
    else:
        sql_message.update_ls(user_id, BLESSEDSPOTCOST, 2)
        sql_message.update_user_blessed_spot_flag(user_id)
        mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
        mix_elixir_info['收取时间'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')
        msg = f"恭喜道友拥有了自己的洞天福地，请收集聚灵旗来提升洞天福地的等级吧~\n"
        msg += f"默认名称为：{user_info['user_name']}道友的家"
        sql_message.update_user_blessed_spot_name(user_id, f"{user_info['user_name']}道友的家")
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()


@blessed_spot_info.handle(parameterless=[Cooldown(at_sender=False)])
async def blessed_spot_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """洞天福地信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await blessed_spot_info.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买来购买吧~"
        await handle_send(bot, event, msg)
        await blessed_spot_info.finish()
    msg = f"\n道友的洞天福地:\n"
    user_buff_data = UserBuffDate(user_id).BuffInfo
    if user_info['blessed_spot_name'] == 0:
        blessed_spot_name = "尚未命名"
    else:
        blessed_spot_name = user_info['blessed_spot_name']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    msg += f"名字：{blessed_spot_name}\n"
    msg += f"修炼速度：增加{user_buff_data['blessed_spot'] * 0.5 * 100}%\n"
    msg += f"药材速度：增加{mix_elixir_info['药材速度'] * 100}%\n"
    msg += f"灵田数量：{mix_elixir_info['灵田数量']}"
    await handle_send(bot, event, msg)
    await blessed_spot_info.finish()


@ling_tian_up.handle(parameterless=[Cooldown(at_sender=False)])
async def ling_tian_up_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """洞天福地灵田升级"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await ling_tian_up.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        await handle_send(bot, event, msg)
        await ling_tian_up.finish()
    LINGTIANCONFIG = {
        "1": {
            "level_up_cost": 3500000
        },
        "2": {
            "level_up_cost": 5000000
        },
        "3": {
            "level_up_cost": 7000000
        },
        "4": {
            "level_up_cost": 10000000
        },
        "5": {
            "level_up_cost": 15000000
        },
        "6": {
            "level_up_cost": 23000000
        },
        "7": {
            "level_up_cost": 30000000
        },
        "8": {
            "level_up_cost": 40000000
        },
        "9": {
            "level_up_cost": 50000000
        }
    }
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    now_num = mix_elixir_info['灵田数量']
    if now_num == len(LINGTIANCONFIG) + 1:
        msg = f"道友的灵田已全部开垦完毕，无法继续开垦了！"
    else:
        cost = LINGTIANCONFIG[str(now_num)]['level_up_cost']
        if int(user_info['stone']) < cost:
            msg = f"本次开垦需要灵石：{cost}，道友的灵石不足！"
        else:
            msg = f"道友成功消耗灵石：{cost}，灵田数量+1,目前数量:{now_num + 1}"
            mix_elixir_info['灵田数量'] = now_num + 1
            save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')
            sql_message.update_ls(user_id, cost, 2)
    await handle_send(bot, event, msg)
    await ling_tian_up.finish()


@blessed_spot_rename.handle(parameterless=[Cooldown(at_sender=False)])
async def blessed_spot_rename_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """洞天福地改名"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await blessed_spot_rename.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        await handle_send(bot, event, msg)
        await blessed_spot_rename.finish()
    arg = args.extract_plain_text().strip()
    arg = str(arg)
    if arg == "":
        msg = "请输入洞天福地的名字！"
        await handle_send(bot, event, msg)
        await blessed_spot_rename.finish()
    if len(arg) > 9:
        msg = f"洞天福地的名字不可大于9位,请重新命名"
    else:
        msg = f"道友的洞天福地成功改名为：{arg}"
        sql_message.update_user_blessed_spot_name(user_id, arg)
    await handle_send(bot, event, msg)
    await blessed_spot_rename.finish()


@qc.handle(parameterless=[Cooldown(cd_time=60, stamina_cost=1)])
async def qc_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """切磋，不会掉血"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await qc.finish()
    user_id = user_info['user_id']

    user1 = sql_message.get_user_real_info(user_id)
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        if give_qq == str(user_id):
            msg = "道友不会左右互搏之术！"
            await handle_send(bot, event, msg)
            await qc.finish()
    else:
        arg = args.extract_plain_text().strip()
        give_info = sql_message.get_user_info_with_name(str(arg))
        give_qq = give_info.get('user_id')
    
    user2 = sql_message.get_user_real_info(give_qq)
    
    if user_info['hp'] is None or user_info['hp'] == 0:
    # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！"
        msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg)
        await qc.finish()
        
    if user1 and user2:
        player1 = sql_message.get_player_data(user1['user_id'])
        player2 = sql_message.get_player_data(user2['user_id'])

        result, victor = Player_fight(player1, player2, 1, bot.self_id)
        await send_msg_handler(bot, event, result)
        msg = f"获胜的是{victor}"
        if victor == "没有人":
            msg = f"{victor}获胜"
        else:
            if victor == player1['道号']:
                update_statistics_value(player1['user_id'], "切磋胜利")
                update_statistics_value(player2['user_id'], "切磋失败")
            else:
                update_statistics_value(player2['user_id'], "切磋胜利")
                update_statistics_value(player1['user_id'], "切磋失败")
        await handle_send(bot, event, msg)
        await qc.finish()
    else:
        msg = "修仙界没有对方的信息，快邀请对方加入修仙界吧！"
        await handle_send(bot, event, msg)
        await qc.finish()


def load_player_user(user_id):
    """加载用户数据，如果文件不存在或为空，返回默认数据"""
    user_file = PLAYERSDATA / str(user_id) / "user_data.json"
    
    if not user_file.exists():
        return {}
    
    try:
        with open(user_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}

def save_player_user(user_id, data):
    """保存用户数据，确保目录存在"""
    user_dir = PLAYERSDATA / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    
    user_file = user_dir / "user_data.json"
    with open(user_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

@two_exp_invite.handle(parameterless=[Cooldown(stamina_cost = 10, at_sender=False)])
async def two_exp_invite_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    global two_exp_limit
    isUser, user_1, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    # 检查是否已经发出过邀请（作为邀请者）
    user_id = user_1['user_id']
    
    # 查找当前用户是否已经作为邀请者存在于invite_cache中
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
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    # 检查是否有未处理的邀请（作为被邀请者）
    if str(user_id) in invite_cache:
        # 有未处理的邀请，提示用户
        inviter_id = invite_cache[str(user_id)]['inviter']
        inviter_info = sql_message.get_user_real_info(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - invite_cache[str(user_id)]['timestamp'])
        msg = f"道友已有来自{inviter_info['user_name']}的双修邀请（剩余{int(remaining_time)}秒），请先处理！\n发送【同意双修】或【拒绝双修】"
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    if int(user_1['user_id']) == int(two_qq):
        msg = "道友无法与自己双修！"
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    # 检查对方修为是否比自己高
    user_2_info = sql_message.get_user_real_info(two_qq)
    if user_2_info['exp'] > user_1['exp']:
        msg = "修仙大能看了看你，不屑一顾，扬长而去！"
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    # 检查对方是否有未处理的邀请（作为被邀请者）
    if str(two_qq) in invite_cache:
        # 对方有未处理的邀请，提示用户
        inviter_id = invite_cache[str(two_qq)]['inviter']
        inviter_info = sql_message.get_user_real_info(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - invite_cache[str(two_qq)]['timestamp'])
        msg = f"对方已有来自{inviter_info['user_name']}的双修邀请（剩余{int(remaining_time)}秒），请稍后再试！"
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    limt_1 = two_exp_cd.find_user(user_id)
    impart_data_1 = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_two_exp_1 = impart_data_1['impart_two_exp'] if impart_data_1 is not None else 0
    main_two_data_1 = UserBuffDate(user_id).get_user_main_buff_data()
    main_two_1 = main_two_data_1['two_buff'] if main_two_data_1 is not None else 0
    max_count_1 = two_exp_limit + impart_two_exp_1 + main_two_1 - limt_1
    
    if max_count_1 <= 0:
        msg = "你的双修次数已用尽，无法发送邀请！"
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    # 检查对方双修保护状态
    user_data = load_player_user(two_qq)
    if user_data is None:
        user_data = {}
    protection_status = user_data.get('two_exp_protect', False)

    if protection_status == "refusal":
        msg = "对方已设置拒绝所有双修邀请，无法进行双修！"
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()        
    if protection_status:
        # 对方开启保护，需要发送邀请
        # 检查邀请是否已存在（再次确认，防止并发）
        if str(two_qq) in invite_cache:
            msg = "对方已有未处理的双修邀请，请稍后再试！"
            await handle_send(bot, event, msg)
            await two_exp_invite.finish()
        
        # 检查对方双修次数是否足够
        limt_2 = two_exp_cd.find_user(two_qq)
        impart_data_2 = xiuxian_impart.get_user_impart_info_with_id(two_qq)
        impart_two_exp_2 = impart_data_2['impart_two_exp'] if impart_data_2 is not None else 0
        main_two_data_2 = UserBuffDate(two_qq).get_user_main_buff_data()
        main_two_2 = main_two_data_2['two_buff'] if main_two_data_2 is not None else 0
        max_count_2 = two_exp_limit + impart_two_exp_2 + main_two_2 - limt_2
        
        if max_count_2 <= 0:
            msg = "对方今日双修次数已用尽，无法邀请！"
            await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()
    else:
            # 对方关闭保护，直接双修
        await direct_two_exp(bot, event, user_id, two_qq, exp_count)
        await two_exp_invite.finish()

async def direct_two_exp(bot, event, user_id_1, user_id_2, exp_count=1):
    """直接双修"""
    # 检查双方是否达到修为上限
    user_1 = sql_message.get_user_info_with_id(user_id_1)
    user_2 = sql_message.get_user_info_with_id(user_id_2)
    
    level_1 = user_1['level']
    level_2 = user_2['level']
    
    max_exp_1_limit = int(OtherSet().set_closing_type(level_1)) * XiuConfig().closing_exp_upper_limit
    max_exp_2_limit = int(OtherSet().set_closing_type(level_2)) * XiuConfig().closing_exp_upper_limit
    
    if user_1['exp'] >= max_exp_1_limit:
        msg = f"{user_1['user_name']}修为已达上限，无法双修！"
        await handle_send(bot, event, msg)
        return
        
    if user_2['exp'] >= max_exp_2_limit:
        msg = f"{user_2['user_name']}修为已达上限，无法双修！"
        await handle_send(bot, event, msg)
        return
    
    # 检查次数限制
    limt_1 = two_exp_cd.find_user(user_id_1)
    limt_2 = two_exp_cd.find_user(user_id_2)
    
    impart_data_1 = xiuxian_impart.get_user_impart_info_with_id(user_id_1)
    impart_data_2 = xiuxian_impart.get_user_impart_info_with_id(user_id_2)
    impart_two_exp_1 = impart_data_1['impart_two_exp'] if impart_data_1 is not None else 0
    impart_two_exp_2 = impart_data_2['impart_two_exp'] if impart_data_2 is not None else 0
    
    main_two_data_1 = UserBuffDate(user_id_1).get_user_main_buff_data()
    main_two_data_2 = UserBuffDate(user_id_2).get_user_main_buff_data()
    main_two_1 = main_two_data_1['two_buff'] if main_two_data_1 is not None else 0
    main_two_2 = main_two_data_2['two_buff'] if main_two_data_2 is not None else 0
    
    max_count_1 = two_exp_limit + impart_two_exp_1 + main_two_1 - limt_1
    max_count_2 = two_exp_limit + impart_two_exp_2 + main_two_2 - limt_2
    
    if max_count_1 <= 0:
        msg = "你的双修次数不足，无法进行双修！"
        await handle_send(bot, event, msg)
        return
        
    if max_count_2 <= 0:
        msg = "对方双修次数不足，无法进行双修！"
        await handle_send(bot, event, msg)
        return
        
    # 取最小可用次数
    actual_count = min(exp_count, max_count_1, max_count_2)
    
    # 进行双修
    total_exp_1 = 0
    total_exp_2 = 0
    event_descriptions = []
    actual_used_count = 0  # 实际消耗的双修次数
    
    for i in range(actual_count):
        # 每次双修前检查修为上限
        user_1_current = sql_message.get_user_info_with_id(user_id_1)
        user_2_current = sql_message.get_user_info_with_id(user_id_2)
        
        max_exp_1_limit = int(OtherSet().set_closing_type(level_1)) * XiuConfig().closing_exp_upper_limit
        max_exp_2_limit = int(OtherSet().set_closing_type(level_2)) * XiuConfig().closing_exp_upper_limit
        
        # 如果任意一方达到上限，停止双修
        if user_1_current['exp'] >= max_exp_1_limit or user_2_current['exp'] >= max_exp_2_limit:
            if i == 0:  # 第一次就达到上限
                msg = "修为已达上限，无法进行双修！"
                await handle_send(bot, event, msg)
                return
            break  # 中途达到上限，停止双修
            
        exp_1, exp_2, event_desc = await process_two_exp(user_id_1, user_id_2)
        
        # 如果双修过程中获得0修为（可能因为其他原因），也视为一次有效双修
        if exp_1 == 0 and exp_2 == 0:
            break
            
        total_exp_1 += exp_1
        total_exp_2 += exp_2
        event_descriptions.append(event_desc)
        actual_used_count += 1
        
        # 只有实际进行了双修才消耗次数
        two_exp_cd.add_user(user_id_1)
        two_exp_cd.add_user(user_id_2)
    
    user_1_info = sql_message.get_user_real_info(user_id_1)
    user_2_info = sql_message.get_user_real_info(user_id_2)
    
    if actual_used_count == 0:
        msg = "双修过程中修为已达上限，无法进行双修！"
    else:
        msg = f"{random.choice(event_descriptions)}\n"
        msg += f"{user_1_info['user_name']}获得修为：{number_to(total_exp_1)}\n"
        msg += f"{user_2_info['user_name']}获得修为：{number_to(total_exp_2)}"

    # 记录实际双修次数
    update_statistics_value(user_id_1, "双修次数", increment=actual_used_count)
    update_statistics_value(user_id_2, "双修次数", increment=actual_used_count)
    log_message(user_id_1, f"与{user_2_info['user_name']}进行双修，获得修为{number_to(total_exp_1)}，共{actual_used_count}次")
    log_message(user_id_2, f"与{user_1_info['user_name']}进行双修，获得修为{number_to(total_exp_2)}，共{actual_used_count}次")
    
    await handle_send(bot, event, msg)

@two_exp_accept.handle(parameterless=[Cooldown(at_sender=False)])
async def two_exp_accept_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """同意双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_accept.finish()
        
    user_id = user_info['user_id']
    
    # 检查是否有邀请
    if str(user_id) not in invite_cache:
        msg = "没有待处理的双修邀请！"
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        # 删除过期的邀请
        del invite_cache[str(user_id)]

async def process_two_exp(user_id_1, user_id_2):
    """处理单次双修逻辑"""
    user_1 = sql_message.get_user_real_info(user_id_1)
    user_2 = sql_message.get_user_real_info(user_id_2)
    
    # 获取双方当前修为和上限
    user_mes_1 = sql_message.get_user_info_with_id(user_id_1)
    user_mes_2 = sql_message.get_user_info_with_id(user_id_2)
    
    level_1 = user_mes_1['level']
    level_2 = user_mes_2['level']
    
    max_exp_1_limit = int(OtherSet().set_closing_type(level_1)) * XiuConfig().closing_exp_upper_limit
    max_exp_2_limit = int(OtherSet().set_closing_type(level_2)) * XiuConfig().closing_exp_upper_limit
    
    # 计算剩余可获得的修为
    remaining_exp_1 = max_exp_1_limit - user_mes_1['exp']
    remaining_exp_2 = max_exp_2_limit - user_mes_2['exp']
    
    # 如果已经达到上限，无法双修
    if remaining_exp_1 <= 0 or remaining_exp_2 <= 0:
        return 0, 0, "修为已达上限，无法继续双修"
    
    # 原有的双修事件描述
    event_descriptions = [
        f"月明星稀之夜，{user_1['user_name']}与{user_2['user_name']}在灵山之巅相对而坐，双手相抵，周身灵气环绕如雾。",
        f"洞府之中，{user_1['user_name']}与{user_2['user_name']}盘膝对坐，真元交融，形成阴阳鱼图案在两人之间流转。",
        f"瀑布之下，{user_1['user_name']}与{user_2['user_name']}沐浴灵泉，水汽蒸腾间功法共鸣，修为精进。",
        f"竹林小筑内，{user_1['user_name']}与{user_2['user_name']}共饮灵茶，茶香氤氲中功法相互印证。",
        f"云端之上，{user_1['user_name']}与{user_2['user_name']}脚踏飞剑，剑气交织间功法互补，修为大涨。",
    ]
    
    special_events = [
        f"突然天降异象，七彩祥云笼罩两人，修为大增！",
        f"意外发现一处灵脉，两人共同吸收，修为精进！",
        f"功法意外产生共鸣，引发天地灵气倒灌！",
        f"两人心意相通，功法运转达到完美契合！",
        f"顿悟时刻来临，两人同时进入玄妙境界！"
    ]
    
    # 随机选择描述
    event_desc = random.choice(event_descriptions)
    is_special = random.randint(1, 100) <= 6  # 6%概率特殊事件

    exp_1 = user_1['exp']
    exp_2 = user_2['exp']
    user1_rank = convert_rank(user_1['level'])[0]
    
    max_exp_1 = int((exp_1 * 0.001) * min(0.1 * user1_rank, 1))# 最大获得修为为当前修为的0.1%同时境界越高获得比例越少
    max_exp_2 = int((exp_2 * 0.001) * min(0.1 * user1_rank, 1))
    # 修为相加的0.5%
    exp = int((exp_1 + exp_2) * 0.005)
    max_two_exp = 10000000
    
    # 计算实际可获得的修为
    exp_limit_1 = min(exp, max_exp_1, remaining_exp_1) if max_exp_1 >= max_two_exp else min(exp, remaining_exp_1, exp_1 * 0.1)
    exp_limit_2 = min(exp, max_exp_2, remaining_exp_2) if max_exp_2 >= max_two_exp else min(exp, min(remaining_exp_2, exp_2 * 0.1))
    
    # 特殊事件加成
    if is_special:
        exp_limit_1 = int(min(exp_limit_1 * 1.5, remaining_exp_1))
        exp_limit_2 = int(min(exp_limit_2 * 1.5, remaining_exp_2))
        
        # 增加突破概率
        sql_message.update_levelrate(user_id_1, user_mes_1['level_up_rate'] + 2)
        sql_message.update_levelrate(user_id_2, user_mes_2['level_up_rate'] + 2)
        
        event_desc += f"\n{random.choice(special_events)}"
        event_desc += f"\n离开时双方互相留法宝为对方护道,双方各增加突破概率2%。"
    
    # 确保修为不为负数
    exp_limit_1 = max(exp_limit_1, 1)
    exp_limit_2 = max(exp_limit_2, 1)
    
    # 更新修为
    sql_message.update_exp(user_id_1, exp_limit_1)
    sql_message.update_power2(user_id_1)
    sql_message.update_exp(user_id_2, exp_limit_2)
    sql_message.update_power2(user_id_2)
    
    return exp_limit_1, exp_limit_2, event_desc

@two_exp_reject.handle(parameterless=[Cooldown(at_sender=False)])
async def two_exp_reject_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """拒绝双修"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_reject.finish()
        
    user_id = user_info['user_id']
    
    if str(user_id) not in invite_cache:
        msg = "没有待处理的双修邀请！"
        await handle_send(bot, event, msg)
        await two_exp_reject.finish()
        
    invite_data = invite_cache[str(user_id)]
    inviter_id = invite_data['inviter']
    
    # 通知邀请者
    inviter_info = sql_message.get_user_real_info(inviter_id)
    msg = f"{user_info['user_name']}拒绝了你的双修邀请！"
    
    # 删除邀请
    del invite_cache[str(user_id)]
    
    await handle_send(bot, event, msg)
    await two_exp_reject.finish()

@two_exp_protect.handle(parameterless=[Cooldown(at_sender=False)])
async def two_exp_protect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """双修保护设置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_protect.finish()
        
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip().lower()
    
    user_data = load_player_user(user_id)
    if user_data is None:
        user_data = {}
    
    # 默认双修保护状态为关闭
    current_status = user_data.get('two_exp_protect', False)
    
    if arg in ['开启', 'on']:
        user_data['two_exp_protect'] = True
        msg = "双修保护已开启！其他玩家可以向你发送双修邀请。"
    elif arg in ['关闭', 'off']:
        user_data['two_exp_protect'] = False
        msg = "双修保护已关闭！其他玩家可以直接和你双修。"
    elif arg in ['拒绝', 'refusal']:
        user_data['two_exp_protect'] = "refusal"
        msg = "双修保护已设置为拒绝！其他玩家无法与你双修。"
    elif arg in ['状态', 'status']:
        status_map = {
            True: "已开启 (需要邀请)",
            False: "已关闭 (允许直接双修)", 
            "refusal": "已拒绝 (拒绝所有双修)"
        }
        current_status_display = status_map.get(current_status, "已关闭 (允许直接双修)")
        msg = f"双修保护状态：{current_status_display}"
        await handle_send(bot, event, msg)
        await two_exp_protect.finish()
    else:
        msg = "请使用：双修保护 开启/关闭/拒绝/状态"
        await handle_send(bot, event, msg)
        await two_exp_protect.finish()
    
    # 保存用户数据
    save_player_user(user_id, user_data)
    await handle_send(bot, event, msg)
    await two_exp_protect.finish()

@reset_exp.handle(parameterless=[Cooldown(at_sender=False, cd_time=60)])
async def reset_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置修炼状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 5  # 状态5为修炼
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await reset_exp.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, user_type)
    if not is_type:
        await handle_send(bot, event, msg)
        await reset_exp.finish()
    msg = "请等待一分钟生效即可！"
    await handle_send(bot, event, msg)
    await asyncio.sleep(60)
    is_type, msg = check_user_type(user_id, user_type)
    if is_type:
        sql_message.in_closing(user_id, 0)
        msg = "已重置修炼状态！"
        await handle_send(bot, event, msg)
    await reset_exp.finish()
        
    
@up_exp.handle(parameterless=[Cooldown(at_sender=False, cd_time=60)])
async def up_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 5  # 状态5为修炼
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await up_exp.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']

    max_exp = (
            int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    now_time = datetime.now()
    user_cd_message = sql_message.get_user_cd(user_id)
    is_type, msg = check_user_type(user_id, 0)
    if not is_type:
        await handle_send(bot, event, msg)
        await up_exp.finish()
    else:
        level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id)  # 灵根倍率
        realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
        user_buff_data = UserBuffDate(user_id)
        user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5
        mainbuffdata = user_buff_data.get_user_main_buff_data()
        mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata != None else 0  # 功法修炼倍率
        mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata != None else 0  # 功法闭关经验
        mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata != None else 0  # 功法闭关回复
        
        exp = int(
            XiuConfig().closing_exp * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + user_blessed_spot_data)))
            # 洞天福地为加法
        )  # 本次闭关获取的修为
        # 计算传承增益
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
        exp = int(exp * (1 + impart_exp_up))
        exp_rate = random.uniform(0.9, 1.3)
        exp = int(exp * exp_rate)
        sql_message.in_closing(user_id, user_type)
        if user_info['root_type'] == '伪灵根':
            msg = f"开始挖矿⛏️！【{user_info['user_name']}开始挖矿】\n挥起玄铁镐砸向发光岩壁\n碎石里蹦出带灵气的矿石\j预计时间：60秒"
            await handle_send(bot, event, msg)
            await asyncio.sleep(60)
            give_stone = random.randint(10000, 300000)
            give_stone_num = int(give_stone * exp_rate)
            sql_message.update_ls(user_info['user_id'], give_stone_num, 1)  # 增加用户灵石
            msg = f"挖矿结束，增加灵石：{give_stone_num}"
            await handle_send(bot, event, msg)
            await up_exp.finish()
        else:
            msg = f"【{user_info['user_name']}开始修炼】\n盘膝而坐，五心朝天，闭目凝神，渐入空明之境...\n周身灵气如涓涓细流汇聚，在经脉中缓缓流转\n丹田内真元涌动，与天地灵气相互呼应\n渐入佳境，物我两忘，进入深度修炼状态\n预计修炼时间：60秒"
        await handle_send(bot, event, msg)
        await asyncio.sleep(60)
        update_statistics_value(user_id, "修炼次数")
        user_type = 0  # 状态0为无事件
        if exp >= user_get_exp_max:
            # 用户获取的修为到达上限
            sql_message.in_closing(user_id, user_type)
            sql_message.update_exp(user_id, user_get_exp_max)
            sql_message.update_power2(user_id)  # 更新战力

            result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10), int(use_exp / 20))
            sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
            msg = f"修炼结束，本次修炼到达上限，共增加修为：{number_to(user_get_exp_max)}{result_msg[0]}{result_msg[1]}"
            await handle_send(bot, event, msg)
            await up_exp.finish()
        else:
            # 用户获取的修为没有到达上限
            sql_message.in_closing(user_id, user_type)
            sql_message.update_exp(user_id, exp)
            sql_message.update_power2(user_id)  # 更新战力
            result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10), int(use_exp / 20))
            sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
            msg = f"修炼结束，增加修为：{number_to(exp)}{result_msg[0]}{result_msg[1]}"
            await handle_send(bot, event, msg)
            await up_exp.finish()

 
@stone_exp.handle(parameterless=[Cooldown(at_sender=False)])
async def stone_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """灵石修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await stone_exp.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']
    use_stone = user_mes['stone']
    max_exp = (
            int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    msg = args.extract_plain_text().strip()
    stone_num = re.findall(r"\d+", msg)  # 灵石数

    if stone_num:
        pass
    else:
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await stone_exp.finish()
    stone_num = int(stone_num[0])
    if use_stone <= stone_num:
        msg = "你的灵石还不够呢，快去赚点灵石吧！"
        await handle_send(bot, event, msg)
        await stone_exp.finish()

    exp = int(stone_num / 10)
    if exp >= user_get_exp_max:
        # 用户获取的修为到达上限
        sql_message.update_exp(user_id, user_get_exp_max)
        sql_message.update_power2(user_id)  # 更新战力
        msg = f"修炼结束，本次修炼到达上限，共增加修为：{user_get_exp_max},消耗灵石：{user_get_exp_max * 10}"
        sql_message.update_ls(user_id, int(user_get_exp_max * 10), 2)
        update_statistics_value(user_id, "灵石修炼", increment=user_get_exp_max * 10)
        await handle_send(bot, event, msg)
        await stone_exp.finish()
    else:
        sql_message.update_exp(user_id, exp)
        sql_message.update_power2(user_id)  # 更新战力
        msg = f"修炼结束，本次修炼共增加修为：{exp},消耗灵石：{stone_num}"
        sql_message.update_ls(user_id, int(stone_num), 2)
        update_statistics_value(user_id, "灵石修炼", increment=stone_num)
        await handle_send(bot, event, msg)
        await stone_exp.finish()


@in_closing.handle(parameterless=[Cooldown(at_sender=False)])
async def in_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """闭关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 1  # 状态0为无事件
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await in_closing.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)
    if user_info['root_type'] == '伪灵根':
        msg = "凡人无法闭关！"
        await handle_send(bot, event, msg)
        await in_closing.finish()
    if is_type:  # 符合
        sql_message.in_closing(user_id, user_type)
        msg = "进入闭关状态，如需出关，发送【出关】！"
        await handle_send(bot, event, msg)
        await in_closing.finish()
    else:
        await handle_send(bot, event, msg)
        await in_closing.finish()


@out_closing.handle(parameterless=[Cooldown(at_sender=False)])
async def out_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """出关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 0  # 状态0为无事件
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await out_closing.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']

    max_exp = (
            int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    now_time = datetime.now()
    user_cd_message = sql_message.get_user_cd(user_id)
    is_type, msg = check_user_type(user_id, 1)
    if not is_type:
        await handle_send(bot, event, msg)
        await out_closing.finish()
    else:
        # 用户状态为1
        in_closing_time = datetime.strptime(
            user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
        )  # 进入闭关的时间
        exp_time = (
                OtherSet().date_diff(now_time, in_closing_time) // 60
        )  # 闭关时长计算(分钟) = second // 60
        level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id)  # 灵根倍率
        realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
        user_buff_data = UserBuffDate(user_id)
        user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5
        mainbuffdata = user_buff_data.get_user_main_buff_data()
        mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata != None else 0  # 功法修炼倍率
        mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata != None else 0  # 功法闭关经验
        mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata != None else 0  # 功法闭关回复
        
        exp = int(
            (exp_time * XiuConfig().closing_exp) * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + user_blessed_spot_data)))
            # 洞天福地为加法
        )  # 本次闭关获取的修为
        # 计算传承增益
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
        exp = int(exp * (1 + impart_exp_up))
        base_exp_rate = f"{int((level_rate + mainbuffratebuff + mainbuffcloexp + user_blessed_spot_data + impart_exp_up) * 100)}%"
        if exp >= user_get_exp_max:
            # 用户获取的修为到达上限
            sql_message.in_closing(user_id, user_type)
            sql_message.update_exp(user_id, user_get_exp_max)
            sql_message.update_power2(user_id)  # 更新战力

            result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
            sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
            msg = f"闭关结束，本次闭关到达上限，共增加修为：{number_to(user_get_exp_max)}{result_msg[0]}{result_msg[1]}"
            update_statistics_value(user_id, "闭关时长", increment=exp_time)
            await handle_send(bot, event, msg)
            await out_closing.finish()
        else:
            # 用户获取的修为没有到达上限
            if str(event.message) == "灵石出关":
                user_stone = user_mes['stone']  # 用户灵石数
                if user_stone <= 0:
                    user_stone = 0
                if exp <= user_stone:
                    exp = exp * 2
                    sql_message.in_closing(user_id, user_type)
                    sql_message.update_exp(user_id, exp)
                    sql_message.update_ls(user_id, int(exp / 2), 2)
                    sql_message.update_power2(user_id)  # 更新战力

                    result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
                    sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1],
                                                      int(result_hp_mp[2] / 10))
                    msg = f"闭关结束，共闭关{exp_time}分钟，本次闭关增加修为：{number_to(exp)}(修炼效率：{base_exp_rate})，消耗灵石{int(exp / 2)}枚{result_msg[0]}{result_msg[1]}"
                    update_statistics_value(user_id, "闭关时长", increment=exp_time)
                    await handle_send(bot, event, msg)
                    await out_closing.finish()
                else:
                    exp = exp + user_stone
                    sql_message.in_closing(user_id, user_type)
                    sql_message.update_exp(user_id, exp)
                    sql_message.update_ls(user_id, user_stone, 2)
                    sql_message.update_power2(user_id)  # 更新战力
                    result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
                    sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1],
                                                      int(result_hp_mp[2] / 10))
                    msg = f"闭关结束，共闭关{exp_time}分钟，本次闭关增加修为：{number_to(exp)}(修炼效率：{base_exp_rate})，消耗灵石{user_stone}枚{result_msg[0]}{result_msg[1]}"
                    update_statistics_value(user_id, "闭关时长", increment=exp_time)
                    await handle_send(bot, event, msg)
                    await out_closing.finish()
            else:
                sql_message.in_closing(user_id, user_type)
                sql_message.update_exp(user_id, exp)
                sql_message.update_power2(user_id)  # 更新战力
                result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
                sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
                msg = f"闭关结束，共闭关{exp_time}分钟，本次闭关增加修为：{number_to(exp)}(修炼效率：{base_exp_rate}){result_msg[0]}{result_msg[1]}"
                update_statistics_value(user_id, "闭关时长", increment=exp_time)
                await handle_send(bot, event, msg)
                await out_closing.finish()

@mind_state.handle(parameterless=[Cooldown(at_sender=False)])
async def mind_state_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的状态信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_msg, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await mind_state.finish()
    user_id = user_msg['user_id']
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    
    player_data = sql_message.get_player_data(user_id)
    if not player_data:
        msg = "获取用户状态信息失败！"
        await handle_send(bot, event, msg)
        await mind_state.finish()
    
    user_info = sql_message.get_user_info_with_id(user_id)
    
    user_data = load_player_user(user_id)
    if user_data is None:
        user_data = {}
    
    current_status = user_data.get('two_exp_protect', False)
    
    # 状态映射
    status_map = {
        True: "开启",
        False: "关闭", 
        "refusal": "拒绝"
    }
    current_status_display = status_map.get(current_status, "关闭")

    level_rate = sql_message.get_root_rate(user_info['root_type'], user_id)  # 灵根倍率
    realm_rate = jsondata.level_data()[user_info['level']]["spend"]  # 境界倍率
    user_buff_data = UserBuffDate(user_id)
    user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5
    main_buff_data = user_buff_data.get_user_main_buff_data()
    
    # 获取传承数据
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_atk_per = impart_data['impart_atk_per'] if impart_data is not None else 0
    impart_hp_per = impart_data['impart_hp_per'] if impart_data is not None else 0
    impart_mp_per = impart_data['impart_mp_per'] if impart_data is not None else 0
    impart_know_per = impart_data['impart_know_per'] if impart_data is not None else 0
    impart_burst_per = impart_data['impart_burst_per'] if impart_data is not None else 0
    boss_atk = impart_data['boss_atk'] if impart_data is not None else 0
    
    base_attack = player_data['攻击']
    user_attack = int(base_attack * (1 + impart_atk_per))
    
    # 获取其他buff数据
    user_armor_crit_data = user_buff_data.get_user_armor_buff_data()
    user_weapon_data = UserBuffDate(user_id).get_user_weapon_data()
    user_main_crit_data = UserBuffDate(user_id).get_user_main_buff_data()
    user_main_data = UserBuffDate(user_id).get_user_main_buff_data()
    
    if user_main_data is not None:
        main_def = user_main_data['def_buff'] * 100
    else:
        main_def = 0
    
    if user_armor_crit_data is not None:
        armor_crit_buff = ((user_armor_crit_data['crit_buff']) * 100)
    else:
        armor_crit_buff = 0
        
    if user_weapon_data is not None:
        crit_buff = ((user_weapon_data['crit_buff']) * 100)
    else:
        crit_buff = 0

    user_armor_data = user_buff_data.get_user_armor_buff_data()
    if user_armor_data is not None:
        def_buff = int(user_armor_data['def_buff'] * 100)
    else:
        def_buff = 0
    
    if user_weapon_data is not None:
        weapon_def = user_weapon_data['def_buff'] * 100
    else:
        weapon_def = 0

    if user_main_crit_data is not None:
        main_crit_buff = ((user_main_crit_data['crit_buff']) * 100)
    else:
        main_crit_buff = 0
    
    # 计算会心率（包含传承加成）
    base_crit_rate = player_data['会心']
    total_crit_rate = base_crit_rate + (impart_know_per * 100)
    
    list_all = len(OtherSet().level) - 1
    now_index = OtherSet().level.index(user_info['level'])
    if list_all == now_index:
        exp_meg = f"位面至高"
    else:
        is_updata_level = OtherSet().level[now_index + 1]
        need_exp = sql_message.get_level_power(is_updata_level)
        get_exp = need_exp - user_info['exp']
        if get_exp > 0:
            exp_meg = f"还需{number_to(get_exp)}修为可突破！"
        else:
            exp_meg = f"可突破！"
    
    main_buff_rate_buff = main_buff_data['ratebuff'] if main_buff_data is not None else 0
    main_hp_buff = main_buff_data['hpbuff'] if main_buff_data is not None else 0
    main_mp_buff = main_buff_data['mpbuff'] if main_buff_data is not None else 0
    
    hppractice = user_info['hppractice'] * 0.05 if user_info['hppractice'] is not None else 0
    mppractice = user_info['mppractice'] * 0.05 if user_info['mppractice'] is not None else 0  
    
    weapon_critatk_data = UserBuffDate(user_id).get_user_weapon_data()
    weapon_critatk = weapon_critatk_data['critatk'] if weapon_critatk_data is not None else 0
    user_main_critatk = UserBuffDate(user_id).get_user_main_buff_data()
    main_critatk = user_main_critatk['critatk'] if user_main_critatk is not None else 0
    
    user_js = def_buff + weapon_def + main_def
    leveluprate = int(user_info['level_up_rate'])
    number = user_main_critatk["number"] if user_main_critatk is not None else 0
    
    max_hp = int((user_info['exp'] / 2) * (1 + main_hp_buff + impart_hp_per + hppractice))
    max_mp = int(user_info['exp'] * (1 + main_mp_buff + impart_mp_per + mppractice))
    
    msg = f"""
道号：{player_data['道号']}
气血:{number_to(player_data['气血'])}/{number_to(max_hp)}({((player_data['气血'] / max_hp) * 100):.2f}%)
真元:{number_to(player_data['真元'])}/{number_to(max_mp)}({((player_data['真元'] / max_mp) * 100):.2f}%)
攻击:{number_to(user_attack)}
突破状态: {exp_meg}(概率：{jsondata.level_rate_data()[user_info['level']] + leveluprate + number}%)
攻击修炼:{user_info['atkpractice']}级(提升攻击力{user_info['atkpractice'] * 4}%)
元血修炼:{user_info['hppractice']}级(提升气血{user_info['hppractice'] * 8}%)
灵海修炼:{user_info['mppractice']}级(提升真元{user_info['mppractice'] * 5}%)
修炼效率:{int(((level_rate * realm_rate) * (1 + main_buff_rate_buff) * (1+ user_blessed_spot_data)) * 100)}%
会心:{total_crit_rate:.1f}%
减伤率:{user_js}%
boss战增益:{int(boss_atk * 100)}%
会心伤害增益:{int((1.5 + impart_burst_per + weapon_critatk + main_critatk) * 100)}%
双修保护状态：{current_status_display}
"""
    sql_message.update_last_check_info_time(user_id)
    await handle_send(bot, event, msg)
    await mind_state.finish()

@buffinfo.handle(parameterless=[Cooldown(at_sender=False)])
async def buffinfo_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的功法"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await buffinfo.finish()

    user_id = user_info['user_id']
    mainbuffdata = UserBuffDate(user_id).get_user_main_buff_data()
    if mainbuffdata != None:
        s, mainbuffmsg = get_main_info_msg(str(get_user_buff(user_id)['main_buff']))
    else:
        mainbuffmsg = ''
        
    subbuffdata = UserBuffDate(user_id).get_user_sub_buff_data()#辅修功法13
    if subbuffdata != None:
        sub, subbuffmsg = get_sub_info_msg(str(get_user_buff(user_id)['sub_buff']))
    else:
        subbuffmsg = ''
        
    effect1buffdata = UserBuffDate(user_id).get_user_effect1_buff_data()
    if effect1buffdata != None:
        effect1, effect1buffmsg = get_effect_info_msg(str(get_user_buff(user_id)['effect1_buff']))
    else:
        effect1buffmsg = ''
        
    effect2buffdata = UserBuffDate(user_id).get_user_effect2_buff_data()
    if effect2buffdata != None:
        effect2, effect2buffmsg = get_effect_info_msg(str(get_user_buff(user_id)['effect2_buff']))
    else:
        effect2buffmsg = ''
        
    secbuffdata = UserBuffDate(user_id).get_user_sec_buff_data()
    secbuffmsg = get_sec_msg(secbuffdata) if get_sec_msg(secbuffdata) != '无' else ''
    msg = f"""
主功法：{mainbuffdata["name"] if mainbuffdata != None else '无'}
{mainbuffmsg}

辅修功法：{subbuffdata["name"] if subbuffdata != None else '无'}
{subbuffmsg}

神通：{secbuffdata["name"] if secbuffdata != None else '无'}
{secbuffmsg}

身法：{effect1buffdata["name"] if effect1buffdata != None else '无'}
{effect1buffmsg}

瞳术：{effect2buffdata["name"] if effect2buffdata != None else '无'}
{effect2buffmsg}
"""

    await handle_send(bot, event, msg)
    await buffinfo.finish()


@del_exp_decimal.handle(parameterless=[Cooldown(at_sender=False)])
async def del_exp_decimal_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清除修为浮点数"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await del_exp_decimal.finish()
    user_id = user_info['user_id']
    exp = user_info['exp']
    sql_message.del_exp_decimal(user_id, exp)
    msg = f"黑暗动乱暂时抑制成功！"
    await handle_send(bot, event, msg)
    await del_exp_decimal.finish()


@my_exp_num.handle(parameterless=[Cooldown(at_sender=False)])
async def my_exp_num_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的双修次数"""
    global two_exp_limit
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
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
    await handle_send(bot, event, msg)
    await my_exp_num.finish()

async def use_two_exp_token(bot, event, item_id, num):
    """增加双修次数"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
