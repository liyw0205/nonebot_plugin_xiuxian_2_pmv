from nonebot import on_command
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, OtherSet, UserBuffDate
from ..xiuxian_utils.data_source import jsondata
from .draw_user_info import draw_user_info_img, draw_user_info_img_with_default_bg
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send, number_to
from ..xiuxian_config import XiuConfig

xiuxian_message = on_command("我的修仙信息", aliases={"我的存档", "存档", "修仙信息"}, priority=23, block=True)
xiuxian_message_img = on_command("我的修仙信息图片版", aliases={"我的存档图片版", "存档图片版", "修仙信息图片版"}, priority=23, block=True)
sql_message = XiuxianDateManage()  # sql类

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
        "注册位数": f"第{int(user_num)}人",
        "修为排行": f"第{int(user_rank)}位",
        "灵石排行": f"第{int(user_stone)}位",
    }
    
    text_msg = f"""
ID：{user_id}
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
注册位数: 第{int(user_num)}人
修为排行: 第{int(user_rank)}位
灵石排行: 第{int(user_stone)}位
\n【我的修仙信息图片版】"""
    
    return DETAIL_MAP, text_msg

@xiuxian_message.handle(parameterless=[Cooldown(at_sender=False)])
async def xiuxian_message_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """普通文本版修仙信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await xiuxian_message.finish()
    
    _, text_msg = await get_user_xiuxian_info(user_info['user_id'])
    await handle_send(bot, event, text_msg)

@xiuxian_message_img.handle(parameterless=[Cooldown(at_sender=False, cd_time=30)])
async def xiuxian_message_img_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """图片版修仙信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await xiuxian_message_img.finish()
    
    detail_map, _ = await get_user_xiuxian_info(user_info['user_id'])
    
    if XiuConfig().xiuxian_info_img:
        img_res = await draw_user_info_img(user_info['user_id'], detail_map)
    else:
        img_res = await draw_user_info_img_with_default_bg(user_info['user_id'], detail_map)
    
    if isinstance(event, GroupMessageEvent):
        await bot.send_group_msg(group_id=event.group_id, message=MessageSegment.image(img_res))
    else:
        await bot.send_private_msg(user_id=event.user_id, message=MessageSegment.image(img_res))
