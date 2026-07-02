from ..on_compat import on_command
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    is_channel_event,
    is_group_event,
    MessageSegment
)
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, handle_send, number_to,
    handle_pic_send, handle_pic_msg_send, handle_send_md,
    call_upload_api_async,
    optimize_md
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager, OtherSet, UserBuffDate, get_base_attributes, get_final_attributes
from ..xiuxian_utils.data_source import jsondata
from .draw_user_info import draw_user_info_img, draw_user_info_img_with_default_bg
from ..xiuxian_config import XiuConfig
from ..xiuxian_buff import load_mentor, load_partner
from nonebot.log import logger
from urllib.parse import quote

# 导入本命法宝数据管理类
from ..xiuxian_natal_treasure.natal_data import NatalTreasure # 新增：导入 NatalTreasure
# 称号
from ..xiuxian_title.title_data import (
    get_equipped_title_display, check_and_unlock_titles,
    get_user_equipped_title, get_title_by_id
)

xiuxian_message = on_command("我的修仙信息", aliases={"我的存档", "存档", "修仙信息"}, priority=23, block=True)
xiuxian_message_img = on_command("我的修仙信息图片版", aliases={"我的存档图片版", "存档图片版", "修仙信息图片版"}, priority=23, block=True)

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

    # 统一属性口径
    final_attr = get_final_attributes(user_id)
    final_atk = final_attr["final_atk"] if final_attr else user_info['atk']

    level_rate = sql_message.get_root_rate(user_info['root_type'], user_id)
    realm_rate = jsondata.level_data()[user_info['level']]["spend"]
    sect_id = user_info['sect_id']
    if sect_id:
        sect_info = sql_message.get_sect_info(sect_id)
        sectmsg = sect_info['sect_name']
        sectzw = jsondata.sect_config_data()[f"{user_info['sect_position']}"]["title"]
    else:
        sectmsg = f"无宗门"
        sectzw = f"无"

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
        partner_info_data = sql_message.get_user_real_info(partner_user_id)
        if affection >= 10000:
            affection_level = "💖 深情厚谊"
        elif affection >= 5000:
            affection_level = "💕 心有灵犀"
        elif affection >= 1000:
            affection_level = "💗 初识情愫"
        else:
            affection_level = "💓 缘分伊始"
        partner_info = f"{partner_info_data['user_name']} ({affection_level})" if partner_info_data else "无"

    mentor_data = load_mentor(user_id)
    mentor_id = mentor_data.get("mentor_id")
    if mentor_id:
        mentor_info_data = sql_message.get_user_real_info(mentor_id)
        mentor_info = mentor_info_data["user_name"] if mentor_info_data else "数据异常"
    else:
        mentor_info = "无"

    apprentice_names = []
    for apprentice_id in mentor_data.get("apprentice_ids", []):
        apprentice_data = load_mentor(apprentice_id)
        if str(apprentice_data.get("mentor_id")) != str(user_id):
            continue
        apprentice_info_data = sql_message.get_user_real_info(apprentice_id)
        if apprentice_info_data:
            apprentice_names.append(apprentice_info_data["user_name"])
    apprentice_info = "、".join(apprentice_names[:3]) if apprentice_names else "无"
    if len(apprentice_names) > 3:
        apprentice_info += f"等{len(apprentice_names)}人"
    relationship_info = f"道侣：{partner_info}；师父：{mentor_info}；徒弟：{apprentice_info}"

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
    if user_sub_buff_date is not None:
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

    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()
    sql_message.update_last_check_info_time(user_id)
    leveluprate = int(user_info['level_up_rate'])
    number = main_rate_buff["number"] if main_rate_buff is not None else 0

    nt = NatalTreasure(user_id)
    natal_name_level = "无"
    if nt.exists():
        natal_data = nt.get_data()
        natal_name_level = f"{natal_data.get('name', '未知法宝')} (Lv.{natal_data.get('level', 0)})"

    # ===== 获取称号信息 =====
    title_name = get_equipped_title_display(user_id)

    DETAIL_MAP = {
        "ID": f"{user_id}",
        "道号": f"{user_name}",
        "称号": f"{title_name}" if title_name else "无",
        "境界": f"{user_info['level']}",
        "修为": f"{number_to(user_info['exp'])}",
        "灵石": f"{number_to(user_info['stone'])}",
        "战力": f"{number_to(int(user_info['exp'] * level_rate * realm_rate))}",
        "灵根": f"{user_info['root']}({user_info['root_type']}+{int(level_rate * 100)}%)",
        "突破状态": f"{exp_meg}概率：{jsondata.level_rate_data()[user_info['level']] + leveluprate + number}%",
        "修炼等级": f"攻修{user_info['atkpractice']}级，元血{user_info['hppractice']}级，灵海{user_info['mppractice']}级",
        "攻击力": f"{number_to(final_atk)}",
        "所在宗门": sectmsg,
        "宗门职位": sectzw,
        "主修功法": main_buff_name,
        "辅修功法": sub_buff_name,
        "副修神通": sec_buff_name,
        "身法": effect1_buff_buff_name,
        "瞳术": effect2_buff_buff_name,
        "法器": weapon_name,
        "防具": armor_name,
        "道侣": partner_info,
        "师父": mentor_info,
        "徒弟": apprentice_info,
        "关系": relationship_info,
        "本命法宝": natal_name_level,
        "注册位数": f"第{int(user_num)}人",
        "修为排行": f"第{int(user_rank)}位",
        "灵石排行": f"第{int(user_stone)}位",
    }

    title_line = f"\n称号: {title_name}" if title_name else ""

    text_msg = f"""
道号: {user_name}{title_line}
境界: {user_info['level']}
修为: {number_to(user_info['exp'])}
灵石: {number_to(user_info['stone'])}
战力: {number_to(int(user_info['exp'] * level_rate * realm_rate))}
灵根: {user_info['root']}({user_info['root_type']}+{int(level_rate * 100)}%)
突破状态: {exp_meg}概率：{jsondata.level_rate_data()[user_info['level']] + leveluprate + number}%
攻击力: {number_to(final_atk)}
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
关系：{relationship_info}
本命法宝: {natal_name_level}
注册位数: 第{int(user_num)}人
修为排行: 第{int(user_rank)}位
灵石排行: 第{int(user_stone)}位"""

    return DETAIL_MAP, text_msg

@xiuxian_message.handle(parameterless=[Cooldown(cd_time=0)])
async def xiuxian_message_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """普通文本版修仙信息"""
    from urllib.parse import quote

    bot, send_group_id = await assign_bot(bot=bot, event=event)

    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xiuxian_message.finish()

    detail_map, text_msg = await get_user_xiuxian_info(user_info["user_id"])

    if XiuConfig().user_info_image:
        await xiuxian_message_img_(bot, event)
        await xiuxian_message.finish()

    config = XiuConfig()
    is_channel = is_channel_event(event)

    def _md_cmd_link(text: str, command: str) -> str:
        """
        生成 QQ 原生 Markdown 蓝色点击命令。
        """
        text = str(text) if text is not None else " "
        command = str(command) if command is not None else " "

        # 防止显示文本破坏 Markdown 结构
        text = text.replace("[", "").replace("]", "")
        text = text.replace("\r", " ").replace("\n", " ")

        # 中文、空格等需要编码
        command = quote(command, safe="")

        return f"[{text}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false)"

    def _get_effect_name(value: str) -> str:
        """
        从装备/功法显示名中提取用于“查看效果”的名称。
        """
        value = str(value) if value is not None else "无"

        if not value or value == "无":
            return ""

        # 去掉括号内等级
        effect_name = value.split("(")[0].strip()
        return effect_name

    def _effect_link(value: str, command: str = "查看效果 {name}") -> str:
        value = str(value) if value is not None else "无"
    
        if not value or value == "无":
            return "无"
    
        effect_name = _get_effect_name(value)
        if not effect_name:
            return value
    
        click_command = command.format(name=effect_name)
    
        return _md_cmd_link(value, click_command)

    def _build_native_md_info(title_url: str = "") -> str:
        """
        构造“我的修仙信息”原生 Markdown。
        """
        user_name = detail_map.get("道号", "无名氏")
        title_name = detail_map.get("称号", "无")

        md_lines = []

        if title_url:
            md_lines.append(f"![img #256px #64px]({title_url})")

        md_lines.extend([
            f"道号: {_md_cmd_link(user_name, '修仙改名')}",
        ])

        if title_name and title_name != "无":
            md_lines.append(f"称号: {title_name}")

        md_lines.extend([
            f"境界: {detail_map.get('境界', '无')}",
            f"修为: {_effect_link(detail_map.get('修为', '0'), '我的修为')}",
            f"灵石: {detail_map.get('灵石', '0')}",
            f"战力: {detail_map.get('战力', '0')}",
            f"灵根: {detail_map.get('灵根', '无')}",
            f"突破状态: {_effect_link(detail_map.get('突破状态', '无'), '突破')}",
            f"攻击力: {detail_map.get('攻击力', '0')}",
            f"修炼等级: {detail_map.get('修炼等级', '无')}",
            f"所在宗门: {_effect_link(detail_map.get('所在宗门', '无宗门'), '我的宗门')}",
            f"宗门职位: {detail_map.get('宗门职位', '无')}",
            f"主修功法: {_effect_link(detail_map.get('主修功法', '无'))}",
            f"辅修功法: {_effect_link(detail_map.get('辅修功法', '无'))}",
            f"副修神通: {_effect_link(detail_map.get('副修神通', '无'))}",
            f"身法: {_effect_link(detail_map.get('身法', '无'))}",
            f"瞳术: {_effect_link(detail_map.get('瞳术', '无'))}",
            f"法器: {_effect_link(detail_map.get('法器', '无'))}",
            f"防具: {_effect_link(detail_map.get('防具', '无'))}",
            f"关系: {_effect_link(detail_map.get('关系', '无'), '关系帮助')}",
            f"本命法宝: {_effect_link(detail_map.get('本命法宝', '无'), '我的本命法宝')}",
            f"注册位数: {detail_map.get('注册位数', '无')}",
            f"修为排行: {detail_map.get('修为排行', '无')}",
            f"灵石排行: {detail_map.get('灵石排行', '无')}",
            "",
            "---",
            f"{_md_cmd_link('图片版', '我的修仙信息图片版')} | "
            f"{_md_cmd_link('我的修为', '我的修为')} | "
            f"{_md_cmd_link('我的状态', '我的状态')}"
        ])

        return "\r".join(md_lines)

    # 获取称号信息
    title_id = get_user_equipped_title(user_info["user_id"])
    title_url = ""
    title_name = ""

    if title_id:
        title_data = get_title_by_id(title_id)
        if title_data:
            title_url = title_data.get("url", "")
            title_name = title_data.get("name", "")

    # ==================================================
    # 有称号图片
    # ==================================================
    if title_url:
        # ------------------------------
        # 1. 开启 Markdown 且设置了模板 ID：走原模板逻辑
        # ------------------------------
        if config.markdown_status and config.markdown_id and not is_channel:
            try:
                optimized_msg = optimize_md(text_msg)
                msg_param = {
                    "key": "t1",
                    "values": [
                        f"](mqqapi://aio/inlinecmd?command=我的修仙信息&enter=false&reply=false)\r![",
                        f"img #256px #64px]({title_url})\r",
                        f"{optimized_msg}\r\r---\r\r[",
                        f"图片版](mqqapi://aio/inlinecmd?command=我的修仙信息图片版&enter=false&reply=false) | [",
                        f"我的修为](mqqapi://aio/inlinecmd?command=我的修为&enter=false&reply=false) | [",
                        f"我的状态](mqqapi://aio/inlinecmd?command=我的状态&enter=false&reply=false)\r",
                    ]
                }
                await handle_send_md(
                    bot,
                    event,
                    " ",
                    markdown_id=config.markdown_id,
                    msg_param=msg_param,
                    at_msg=None
                )
            except Exception as e:
                logger.warning(f"存档称号模板MD发送失败，降级处理: {e}")
            await xiuxian_message.finish()

        # ------------------------------
        # 2. 开启 Markdown 但没有模板 ID：走原生 Markdown 蓝字
        # ------------------------------
        if config.markdown_status and not config.markdown_id and not config.markdown_id2 and not is_channel:
            try:
                md_msg = _build_native_md_info(title_url=title_url)
                await handle_send(
                    bot,
                    event,
                    md_msg,
                    native_markdown=True,
                    fallback_msg=text_msg,
                    at_msg=False,
                )
                await xiuxian_message.finish()
            except Exception as e:
                logger.warning(f"存档称号原生MD蓝字发送失败，降级普通图文: {e}")

        # ------------------------------
        # 3. 未开启 Markdown：普通图文模式
        # ------------------------------
        if not config.markdown_status:
            try:
                pic_text = f"🏅 称号：{title_name}\n{text_msg}"
                await handle_pic_msg_send(bot, event, title_url, pic_text)
            except Exception as e:
                logger.warning(f"存档称号图文发送失败，降级普通文本: {e}")
            await xiuxian_message.finish()

    if config.markdown_status and not config.markdown_id and not config.markdown_id2 and not is_channel:
        try:
            md_msg = _build_native_md_info()
            await handle_send(
                bot,
                event,
                md_msg,
                native_markdown=True,
                fallback_msg=text_msg,
                at_msg=False,
            )
        except Exception as e:
            logger.warning(f"我的修仙信息原生MD蓝字发送失败，降级普通文本: {e}")
        await xiuxian_message.finish()

    await handle_send(
        bot,
        event,
        text_msg,
        md_type="修仙信息",
        k1="图片版",
        v1="我的修仙信息图片版",
        k2="修为",
        v2="我的修为",
        k3="状态",
        v3="我的状态"
    )

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
    equipped_title_name = detail_map.get("称号", "")
    if equipped_title_name == "无":
        equipped_title_name = ""
    
    if XiuConfig().xiuxian_info_img:
        img_res = await draw_user_info_img(user_info['user_id'], detail_map)
    else:
        img_res = await draw_user_info_img_with_default_bg(user_info['user_id'], detail_map)
    
    if XiuConfig().markdown_status:
        if XiuConfig().markdown_id and XiuConfig().web_link:
            # 模板MD - 称号显示在标题中
            title_display = f"🏅{equipped_title_name}\r" if equipped_title_name else ""
            msg_param = {
                "key": "t1",
                "values": [
                    f"](mqqapi://aio/inlinecmd?command=我的修仙信息&enter=false&reply=false)\r",
                    f"![img #1100px #2680px]({XiuConfig().web_link}/download/user_xiuxian_info_{user_info['user_id']}.png)\r",
                    f"{title_display}道号：[{user_info['user_name']}"
                ]
            }
            await handle_send_md(bot, event, " ", markdown_id=XiuConfig().markdown_id, msg_param=msg_param, at_msg=None)
            await xiuxian_message_img.finish()
        else:
            if not is_channel_event(event):
                link = await call_upload_api_async(img_res)
                if link:
                    # 原生MD - 称号显示在标题中
                    title_display = f"**{equipped_title_name}**\r" if equipped_title_name else ""
                    img_data = f"{title_display}![img #1100px #2680px]({link})"
                    await bot.send(event=event, message=MessageSegment.markdown(bot, img_data))
                    await xiuxian_message_img.finish()
    
    await handle_pic_send(bot, event, img_res)
    await xiuxian_message_img.finish()
