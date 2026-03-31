import random
from datetime import datetime
from nonebot import on_command
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    is_channel_event,
    MessageSegment
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager, OtherSet, UserBuffDate
from ..xiuxian_utils.data_source import jsondata
from .draw_user_info import draw_user_info_img, draw_user_info_img_with_default_bg
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send, number_to, handle_pic_send, handle_send_md, get_impersonating_target, call_upload_api
from ..xiuxian_config import XiuConfig
from ..xiuxian_buff import load_partner
from .draw_changelog import get_commits, create_changelog_image
from nonebot.log import logger
from nonebot.params import CommandArg
from io import BytesIO
from pathlib import Path

# 导入本命法宝数据管理类
from ..xiuxian_natal_treasure.natal_data import NatalTreasure # 新增：导入 NatalTreasure

xiuxian_message = on_command("我的修仙信息", aliases={"我的存档", "存档", "修仙信息"}, priority=23, block=True)
xiuxian_message_img = on_command("我的修仙信息图片版", aliases={"我的存档图片版", "存档图片版", "修仙信息图片版"}, priority=23, block=True)
avatar_switch_cmd = on_command("身外化身", priority=5, block=True)
my_id_cmd = on_command("我的ID", aliases={"我的id", "myid", "id"}, priority=5, block=True)
changelog = on_command("更新日志", priority=5, aliases={"更新记录"}, block=True)

sql_message = XiuxianDateManage()  # sql类
player_data_manager = PlayerDataManager()

async def get_user_xiuxian_info(user_id):
    """获取用户修仙信息的公共函数"""
    user_info = sql_message.get_user_real_info(user_id)
    user_name = user_info['user_name']
    
    user_num = user_info['id']
    rank = sql_message.get_exp_rank(user_id)
    user_rank = int(rank[0])
    stone = sql_message.get_stone_rank(user_id)
    user_stone = int(stone[0])

    if not user_name:
        user_name = f"无名氏(发送修仙改名+道号更新)"

    level_rate = sql_message.get_root_rate(user_info['root_type'], user_id)  # 灵根倍率
    realm_rate = jsondata.level_data()[user_info['level']]["spend"]  # 境界倍率
    sect_id = user_info['sect_id']
    if sect_id:
        sect_info = sql_message.get_sect_info(sect_id)
        sectmsg = sect_info['sect_name']
        sectzw = jsondata.sect_config_data()[f"{user_info['sect_position']}"]["title"]
    else:
        sectmsg = f"无宗门"
        sectzw = f"无"

    # 判断突破的修为
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
    
    partner_data = load_partner(user_id)
    if not partner_data or partner_data.get('partner_id') is None:
        partner_info = "无"
    else:
        partner_user_id = partner_data["partner_id"]
        affection = partner_data["affection"]
        partner_info = sql_message.get_user_real_info(partner_user_id)
        if affection >= 10000:
            affection_level = "💖 深情厚谊"
        elif affection >= 5000:
            affection_level = "💕 心有灵犀"
        elif affection >= 1000:
            affection_level = "💗 初识情愫"
        else:
            affection_level = "💓 缘分伊始"
        partner_info = f"{partner_info['user_name']} ({affection_level})" if partner_info else "无"
    
    user_buff_data = UserBuffDate(user_id)
    user_main_buff_date = user_buff_data.get_user_main_buff_data()
    user_sub_buff_date = user_buff_data.get_user_sub_buff_data()
    user_sec_buff_date = user_buff_data.get_user_sec_buff_data()
    user_effect1_buff_date = user_buff_data.get_user_effect1_buff_data()
    user_effect2_buff_date = user_buff_data.get_user_effect2_buff_data()
    user_weapon_data = user_buff_data.get_user_weapon_data()
    user_armor_data = user_buff_data.get_user_armor_buff_data()
    
    main_buff_name = f"无"
    sub_buff_name = f"无"
    sec_buff_name = f"无"
    effect1_buff_buff_name = f"无"
    effect2_buff_buff_name = f"无"
    weapon_name = f"无"
    armor_name = f"无"
    
    if user_main_buff_date is not None:
        main_buff_name = f"{user_main_buff_date['name']}({user_main_buff_date['level']})"
    if user_sub_buff_date != None:
        sub_buff_name = f"{user_sub_buff_date['name']}({user_sub_buff_date['level']})"   
    if user_sec_buff_date is not None:
        sec_buff_name = f"{user_sec_buff_date['name']}({user_sec_buff_date['level']})"
    if user_effect1_buff_date is not None:
        effect1_buff_buff_name = f"{user_effect1_buff_date['name']}({user_effect1_buff_date['level']})"
    if user_effect2_buff_date is not None:
        effect2_buff_buff_name = f"{user_effect2_buff_date['name']}({user_effect2_buff_date['level']})"
    if user_weapon_data is not None:
        weapon_name = f"{user_weapon_data['name']}({user_weapon_data['level']})"
    if user_armor_data is not None:
        armor_name = f"{user_armor_data['name']}({user_armor_data['level']})"
        
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data() # 功法突破概率提升
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    leveluprate = int(user_info['level_up_rate'])  # 用户失败次数加成
    number =  main_rate_buff["number"] if main_rate_buff is not None else 0
    
    nt = NatalTreasure(user_id)
    natal_name_level = "无" # 默认显示无
    if nt.exists():
        natal_data = nt.get_data()
        natal_name_level = f"{natal_data.get('name', '未知法宝')} (Lv.{natal_data.get('level', 0)})"

    DETAIL_MAP = {
        "ID": f"{user_id}",
        "道号": f"{user_name}",
        "境界": f"{user_info['level']}",
        "修为": f"{number_to(user_info['exp'])}",
        "灵石": f"{number_to(user_info['stone'])}",
        "战力": f"{number_to(int(user_info['exp'] * level_rate * realm_rate))}",
        "灵根": f"{user_info['root']}({user_info['root_type']}+{int(level_rate * 100)}%)",
        "突破状态": f"{exp_meg}概率：{jsondata.level_rate_data()[user_info['level']] + leveluprate + number}%",
        "修炼等级": f"攻修{user_info['atkpractice']}级，元血{user_info['hppractice']}级，灵海{user_info['mppractice']}级",
        "攻击力": f"{number_to(user_info['atk'])}",
        "所在宗门": sectmsg,
        "宗门职位": sectzw,
        "主修功法": main_buff_name,
        "辅修功法": sub_buff_name,
        "副修神通": sec_buff_name,
        "法器": weapon_name,
        "防具": armor_name,
        "道侣": partner_info,
        "本命法宝": natal_name_level, # 添加本命法宝名称和等级
        "注册位数": f"第{int(user_num)}人",
        "修为排行": f"第{int(user_rank)}位",
        "灵石排行": f"第{int(user_stone)}位",
    }
    
    # 格式化文本消息，本命法宝只显示名称和等级
    text_msg = f"""
道号: {user_name}
境界: {user_info['level']}
修为: {number_to(user_info['exp'])}
灵石: {number_to(user_info['stone'])}
战力: {number_to(int(user_info['exp'] * level_rate * realm_rate))}
灵根: {user_info['root']}({user_info['root_type']}+{int(level_rate * 100)}%)
突破状态: {exp_meg}概率：{jsondata.level_rate_data()[user_info['level']] + leveluprate + number}%
攻击力: {number_to(user_info['atk'])}
攻修等级: {user_info['atkpractice']}级
元血等级: {user_info['hppractice']}级
灵海等级: {user_info['mppractice']}级
所在宗门: {sectmsg}
宗门职位: {sectzw}
主修功法: {main_buff_name}
辅修功法: {sub_buff_name}
副修神通: {sec_buff_name}
身法: {effect1_buff_buff_name}
瞳术: {effect2_buff_buff_name}
法器: {weapon_name}
防具: {armor_name}
道侣：{partner_info}
本命法宝: {natal_name_level}
注册位数: 第{int(user_num)}人
修为排行: 第{int(user_rank)}位
灵石排行: 第{int(user_stone)}位"""
    
    return DETAIL_MAP, text_msg

@xiuxian_message.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiuxian_message_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """普通文本版修仙信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xiuxian_message.finish()
    
    _, text_msg = await get_user_xiuxian_info(user_info['user_id'])
    if XiuConfig().user_info_image:
        await xiuxian_message_img_(bot, event)
    else:
        await handle_send(bot, event, text_msg, md_type="修仙信息", k1="图片版", v1="我的修仙信息图片版", k2="修为", v2="我的修为", k3="状态", v3="我的状态")
    await xiuxian_message.finish()

@xiuxian_message_img.handle(parameterless=[Cooldown(cd_time=30)])
async def xiuxian_message_img_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """图片版修仙信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xiuxian_message_img.finish()
    
    detail_map, _ = await get_user_xiuxian_info(user_info['user_id'])
    
    if XiuConfig().xiuxian_info_img:
        img_res = await draw_user_info_img(user_info['user_id'], detail_map)
    else:
        img_res = await draw_user_info_img_with_default_bg(user_info['user_id'], detail_map)
    if XiuConfig().markdown_status:
        if XiuConfig().markdown_id and XiuConfig().web_link:
            msg_param = {
            "key": "t1",
            "values": ["](mqqapi://aio/inlinecmd?command=我的修仙信息&enter=false&reply=false)\r![",f"img #1100px #2450px]({XiuConfig().web_link}/download/user_xiuxian_info_{user_info['user_id']}.png)\r",f"道号：[{user_info['user_name']}"]
            }
            await handle_send_md(bot, event, " ", markdown_id=XiuConfig().markdown_id, msg_param=msg_param, at_msg=None)
            await xiuxian_message_img.finish()
        else:
            if not is_channel_event(event):
                link = call_upload_api(img_res)
                logger.error(f"web图片返回: {link}")
                if link:
                    img_data = f"[img #1100px #2450px]({link})"
                    await bot.send(event=event, message=MessageSegment.markdown(bot, img_data))
                    await xiuxian_message_img.finish()
    await handle_pic_send(bot, event, img_res)
    await xiuxian_message_img.finish()

@avatar_switch_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def avatar_switch_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    身外化身命令
    用法：
    - 身外化身         -> 本号/化身互相切换
    - 身外化身 本体    -> 强制切回本体
    """
    # 分配bot
    bot, _ = await assign_bot(bot=bot, event=event)

    main_id = str(event.user_id)
    arg_text = args.extract_plain_text().strip()

    # 强制切回本体
    if arg_text in ["本体", "回来", "返回", "切回"]:
        info = init_avatar_if_needed(main_id)
        player_data_manager.update_or_write_data(main_id, "avatar", "active_id", str(main_id))
        await handle_send(
            bot, event,
            f"🔁 已切回本体！\n当前为【本号】状态\n（后续修仙指令将作用于本号）"
        )
        await avatar_switch_cmd.finish()

    is_user, user_info, msg = check_user(str(event.user_id))
    if not is_user:
        await handle_send(bot, event, "请先使用【我要修仙】进入修仙世界后再开启身外化身！\n切换回来：身外化身 本体")
        await avatar_switch_cmd.finish()

    role, info = toggle_avatar(main_id)

    if role == "avatar":
        avatar_id = info.get("avatar_id")
        await handle_send(
            bot, event,
            f"✨ 身外化身已启用！\n已从【本号】切换至【化身】\n化身ID：{avatar_id}\n（后续修仙指令将作用于化身）"
        )
    else:
        await handle_send(
            bot, event,
            f"🔁 已收回化身，回归本体！\n当前为【本号】状态\n（后续修仙指令将作用于本号）"
        )

    await avatar_switch_cmd.finish()

@my_id_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def my_id_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查询当前ID信息（含伪装/化身状态）"""
    bot, _ = await assign_bot(bot=bot, event=event)

    real_user_id = str(event.get_user_id())

    # 群号
    if isinstance(event, GroupMessageEvent):
        group_id = str(event.group_id)
    else:
        group_id = "私聊无群号"

    # 伪装目标（若有）
    impersonated_id = get_impersonating_target(real_user_id)

    # 化身激活ID（若有）
    avatar_active_id = player_data_manager.get_field_data(real_user_id, "avatar", "active_id")
    avatar_active_id = str(avatar_active_id) if avatar_active_id else real_user_id

    # 生效ID优先级：伪装 > 化身 > 本体（与现有逻辑一致）
    effective_user_id = impersonated_id if impersonated_id else avatar_active_id

    # 身份状态描述
    status_list = []
    if impersonated_id:
        status_list.append(f"伪装中 -> {impersonated_id}")
    if avatar_active_id != real_user_id:
        status_list.append(f"化身中 -> {avatar_active_id}")
    if not status_list:
        status_list.append("正常（本体）")

    msg = (
        f"你的ID信息如下：\n"
        f"用户ID：{real_user_id}\n"
        f"当前ID：{effective_user_id}\n"
        f"群ID：{group_id}\n"
        f"状态：{'；'.join(status_list)}"
    )

    await handle_send(bot, event, msg)
    await my_id_cmd.finish()

@changelog.handle(parameterless=[Cooldown(cd_time=30)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """处理更新日志命令"""
    page_arg = args.extract_plain_text().strip()
    page = 1
    if page_arg and page_arg.isdigit():
        page = int(page_arg)

    if page <= 0:
        page = 1

    msg = "正在获取更新日志，请稍候..."
    await handle_send(bot, event, msg)
    try:
        commits = get_commits(page=page)
        if commits:
            image_path = create_changelog_image(commits, page)
            
            await handle_pic_send(bot, event, image_path)
            
            try:
                if image_path.exists():
                    image_path.unlink()
            except Exception as e:
                logger.error(f"删除更新日志图片失败: {e}")
            
        else:
            msg = "无法获取更新日志，可能已到达最后一页或请求失败。"
            await handle_send(bot, event, msg)
            await changelog.finish()
    except Exception as e:
        msg = f"生成更新日志图片时出错: {e}"
        await handle_send(bot, event, msg)
        await changelog.finish()

def _generate_unique_avatar_id() -> str:
    """生成不与现有修仙用户冲突的化身ID"""
    # 可按需调整范围，尽量大一些避免碰撞
    while True:
        new_id = str(random.randint(10_000_000, 9_999_999_999))
        # 不能和已有修仙用户重复
        if not sql_message.get_user_info_with_id(new_id):
            return new_id

def get_active_user_id(user_id: str) -> str:
    """获取当前激活ID（本号或化身）"""
    active_id = player_data_manager.get_field_data(user_id, "avatar", "active_id")
    return str(active_id) if active_id else str(user_id)

def get_avatar_info(user_id: str) -> dict:
    """获取玩家化身信息（以本号ID为键）"""
    info = player_data_manager.get_fields(user_id, "avatar")
    return info if info else {}

def init_avatar_if_needed(main_id: str) -> dict:
    """初始化化身信息（首次使用时创建）"""
    info = get_avatar_info(main_id)
    if info and info.get("avatar_id"):
        return info

    avatar_id = _generate_unique_avatar_id()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    player_data_manager.update_or_write_data(main_id, "avatar", "main_id", str(main_id))
    player_data_manager.update_or_write_data(main_id, "avatar", "avatar_id", str(avatar_id))
    player_data_manager.update_or_write_data(main_id, "avatar", "active_id", str(main_id))
    player_data_manager.update_or_write_data(main_id, "avatar", "create_time", now_str)

    return get_avatar_info(main_id)

def toggle_avatar(main_id: str) -> tuple[str, dict]:
    """切换本号/化身，返回(当前激活身份, info)"""
    info = init_avatar_if_needed(main_id)
    main_id = str(info.get("main_id", main_id))
    avatar_id = str(info.get("avatar_id"))
    active_id = str(info.get("active_id", main_id))

    # active是本号就切到化身，否则切回本号
    if active_id == main_id:
        new_active = avatar_id
        role = "avatar"
    else:
        new_active = main_id
        role = "main"

    player_data_manager.update_or_write_data(main_id, "avatar", "active_id", new_active)
    info["active_id"] = new_active
    return role, info