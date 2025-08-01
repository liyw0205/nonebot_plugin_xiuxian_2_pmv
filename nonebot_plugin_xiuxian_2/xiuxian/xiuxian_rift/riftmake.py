import random
from .riftconfig import get_rift_config
from ..xiuxian_utils.xiuxian2_handle import OtherSet
from .jsondata import read_f
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, UserBuffDate, XIUXIAN_IMPART_BUFF
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank

sql_message = XiuxianDateManage()
xiuxian_impart = XIUXIAN_IMPART_BUFF()
items = Items()
skill_data = read_f()

NONEMSG = [
    "道友在秘境中晕头转向，等到清醒时竟然发现已被秘境踢出，毫无所获！",
    "道友进入秘境发现此地烟雾缭绕，无法前行，只能原路而返空手而归！",
]

TREASUREMSG = [
    "道友进入秘境后误入一处兵冢，仔细查探一番后竟然找到了{}",
    "在秘境最深处与神秘势力大战，底牌尽出总算是抢到了{}"
]

TREASUREMSG_1 = [
    "道友进入秘境后闯过了重重试炼，拿到了{}",
    "道友进入秘境后闯过了重重试炼，拿到了{}"
]

TREASUREMSG_2 = [
    "道友进入秘境后偶遇了一位修为深不可测的大能，大能见与你有缘，留下一本{}后飘然离去",
    "道友进入秘境后偶遇了一位修为深不可测的大能，大能见与你有缘，留下一本{}后飘然离去"
]

TREASUREMSG_3 = [
    "道友进入秘境后四处寻宝许久却两手空空，正当秘境快要关闭时你瞥见石缝里的一本灰色书籍，打开一看居然是{}",
    "道友进入秘境后四处寻宝许久却两手空空，正当秘境快要关闭时你瞥见石缝里的一本灰色书籍，打开一看居然是{}"
]

TREASUREMSG_4 = [
    "道友进入秘境后搜刮了一番，{}",
    "道友进入秘境后竟然发现了一位前辈坐化于此，{}"
]

TREASUREMSG_5 = [
    "道友在秘境里探索险境，突然感觉一阵天旋地转，清醒过来时已被踢出秘境！但手里多了一本书籍，竟然是失传已久的{}！",
    "道友在秘境里探索险境，突然感觉一阵天旋地转，清醒过来时已被踢出秘境！但手里多了一本书籍，竟然是失传已久的{}！"
]


STORY = {
    "宝物": {
        "type_rate": 300,
        "功法": {
            "type_rate": 50,
        },
        "辅修功法": {
            "type_rate": 45,
        },
        "神通": {
            "type_rate": 50,
        },
        "法器": {
            "type_rate": 35,
        },
        "防具": {
            "type_rate": 40,
        },
        "灵石": {
            "type_rate": 70,
            "stone": 3000000
        }
    },
    "战斗": {
        "type_rate": 100,
        "Boss战斗": {
            "type_rate": 200,
            "Boss数据": {
                "name": ["墨蛟", "婴鲤兽", "千目妖", "鸡冠蛟", "妖冠蛇", "铁火蚁", "天晶蚁", "银光鼠", "紫云鹰", "狗青"],
                "hp": [1.2, 1.4, 1.6, 1.8, 2, 3, 5, 10],
                "mp": 10,
                "atk": [0.1, 0.12, 0.14, 0.16, 0.18, 0.5, 1, 2],
            },
            "success": {
                "desc": "道友大战一番成功战胜{}!",
                "give": {
                    "exp": [0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.09],
                    "stone": 500000
                }
            },
            "fail": {
                "desc": "道友大战一番不敌{}，仓皇逃窜！",
            }
        },
        "掉血事件": {
            "type_rate": 50,
            "desc": [
                "秘境内竟然散布着浓烈的毒气，道友贸然闯入！{}!",
                "秘境内竟然藏着一群未知势力，道友被打劫了！{}!"
            ],
            "cost": {
                "exp": {
                    "type_rate": 50,
                    "value": [0.003, 0.004, 0.005]
                },
                "hp": {
                    "type_rate": 100,
                    "value": [0.3, 0.5, 0.7]
                },
                "stone": {
                    "type_rate": 50,
                    "value": [5000000, 10000000, 15000000]
                },
            }
        },
    },
    "无事": {
        "type_rate": 50,
    }
}


async def get_boss_battle_info(user_info, rift_rank, bot_id):
    """获取Boss战事件的内容"""
    boss_data = STORY['战斗']['Boss战斗']["Boss数据"]
    player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
    userinfo = sql_message.get_user_real_info(user_info['user_id'])
    user1_weapon_data = UserBuffDate(user_info['user_id']).get_user_weapon_data()
    user_armor_data = UserBuffDate(user_info['user_id']).get_user_armor_buff_data()#秘境战斗防具会心
    user_main_crit_data = UserBuffDate(user_info['user_id']).get_user_main_buff_data() #秘境战斗功法会心
    
    if user_main_crit_data is not None: #秘境战斗功法会心
        main_crit_buff = ((user_main_crit_data['crit_buff']) * 100)
    else:
        main_crit_buff = 0
    
    if  user_armor_data != None: #秘境战斗防具会心
        armor_crit_buff = user_armor_data['crit_buff']
    else:
        armor_crit_buff = 0

    if user1_weapon_data is not None:
        player['会心'] = int(((user1_weapon_data['crit_buff']) + armor_crit_buff +  main_crit_buff) * 100) 
    else:
        player['会心'] = (armor_crit_buff + main_crit_buff) * 100

    player['user_id'] = userinfo['user_id']
    player['道号'] = userinfo['user_name']
    player['气血'] = userinfo['hp']
    player['攻击'] = userinfo['atk']
    player['真元'] = userinfo['mp']
    player['exp'] = userinfo['exp']

    base_exp = userinfo['exp']
    boss_info = {
        "name": random.choice(boss_data["name"]),
        "气血": int(base_exp * random.choice(boss_data["hp"])),
        "总血量": int(base_exp * random.choice(boss_data["hp"])),
        "攻击": int(base_exp * random.choice(boss_data["atk"])),
        "真元": base_exp * boss_data["mp"],
        "jj":"斩我境",
        'stone': 1
    }


    result, victor, bossinfo_new, stone = await Boss_fight(player, boss_info, bot_id=bot_id)  # 未开启，1不写入，2写入

    if victor == "群友赢了":  # 获胜
        user_rank = convert_rank('练气境圆满')[0] - convert_rank(user_info['level'])[0] # 60-用户当前等级 原50
        success_info = STORY['战斗']['Boss战斗']['success']
        msg = success_info['desc'].format(boss_info['name'])
        give_exp = int(random.choice(success_info["give"]["exp"]) * user_info['exp'])
        give_stone = (rift_rank + user_rank) * success_info["give"]["stone"]
        sql_message.update_exp(user_info['user_id'], give_exp)
        sql_message.update_ls(user_info['user_id'], give_stone, 1)  # 负数也挺正常
        msg += f"获得了修为：{give_exp}点，灵石：{give_stone}枚！"
    else:  # 输了
        fail_info = STORY['战斗']['Boss战斗']["fail"]
        msg = fail_info['desc'].format(boss_info["name"])
    return result, msg


def get_dxsj_info(rift_type, user_info):
    """获取掉血事件的内容"""
    msg = None
    battle_data = STORY['战斗']
    cost_type = get_dict_type_rate(battle_data[rift_type]['cost'])
    value = random.choice(battle_data[rift_type]['cost'][cost_type]['value'])
    if cost_type == "exp":
        exp = int(user_info['exp'] * value)
        sql_message.update_j_exp(user_info['user_id'], exp)

        nowhp = user_info['hp'] - (exp / 2) if (user_info['hp'] - (exp / 2)) > 0 else 1
        nowmp = user_info['mp'] - exp if (user_info['mp'] - exp) > 0 else 1
        sql_message.update_user_hp_mp(user_info['user_id'], nowhp, nowmp)  # 修为掉了，血量、真元也要掉

        msg = random.choice(battle_data[rift_type]['desc']).format(f"修为减少了：{exp}点！")
    elif cost_type == "hp":
        cost_hp = int((user_info['exp'] / 2) * value)
        now_hp = user_info['hp'] - cost_hp
        if now_hp < 0:
            now_hp = 1
        sql_message.update_user_hp_mp(user_info['user_id'], now_hp, user_info['mp'])
        msg = random.choice(battle_data[rift_type]['desc']).format(f"气血减少了：{cost_hp}点！")
    elif cost_type == "stone":
        cost_stone = value
        sql_message.update_ls(user_info['user_id'], cost_stone, 2)  # 负数也挺正常
        msg = random.choice(battle_data[rift_type]['desc']).format(f"灵石减少了：{cost_stone}枚！")
    return msg


def get_treasure_info(user_info, rift_rank):
    rift_type = get_goods_type()  # 功法、神通、法器、防具、法宝#todo
    msg = None
    if rift_type == "法器":
        weapon_info = get_weapon(user_info, rift_rank)
        temp_msg = f"{weapon_info[1]['level']}:{weapon_info[1]['name']}!"
        msg = random.choice(TREASUREMSG).format(temp_msg)
        sql_message.send_back(user_info['user_id'], weapon_info[0], weapon_info[1]['name'], weapon_info[1]['type'], 1, 0)
        # 背包sql

    elif rift_type == "防具":  # todo
        armor_info = get_armor(user_info, rift_rank)
        temp_msg = f"{armor_info[1]['level']}防具：{armor_info[1]['name']}!"
        msg = random.choice(TREASUREMSG_1).format(temp_msg)
        sql_message.send_back(user_info['user_id'], armor_info[0], armor_info[1]['name'], armor_info[1]['type'], 1, 0)
        # 背包sql

    elif rift_type == "功法":
        give_main_info = get_main_info(user_info['level'], rift_rank)
        if give_main_info[0]:  # 获得了
            main_buff_id = give_main_info[1]
            main_buff = items.get_data_by_item_id(main_buff_id)
            temp_msg = f"{main_buff['level']}功法：{main_buff['name']}"
            msg = random.choice(TREASUREMSG_2).format(temp_msg)
            sql_message.send_back(user_info['user_id'], main_buff_id, main_buff['name'], main_buff['type'], 1, 0)
        else:
            msg = '道友在秘境中获得一本书籍，翻开一看居然是绿野仙踪...'

    elif rift_type == "神通":
        give_sec_info = get_sec_info(user_info['level'], rift_rank)
        if give_sec_info[0]:  # 获得了
            sec_buff_id = give_sec_info[1]
            sec_buff = items.get_data_by_item_id(sec_buff_id)
            temp_msg = f"{sec_buff['level']}神通：{sec_buff['name']}!"
            msg = random.choice(TREASUREMSG_3).format(temp_msg)
            sql_message.send_back(user_info['user_id'], sec_buff_id, sec_buff['name'], sec_buff['type'], 1, 0)
            # 背包sql
        else:
            msg = '道友在秘境中获得一本书籍，翻开一看居然是金瓶梅...'

    elif rift_type == "辅修功法":
        give_sub_info = get_sub_info(user_info['level'], rift_rank)
        if give_sub_info[0]:  # 获得了
            sub_buff_id = give_sub_info[1]
            sub_buff = items.get_data_by_item_id(sub_buff_id)
            temp_msg = f"{sub_buff['level']}辅修功法：{sub_buff['name']}!"
            msg = random.choice(TREASUREMSG_5).format(temp_msg)
            sql_message.send_back(user_info['user_id'], sub_buff_id, sub_buff['name'], sub_buff['type'], 1, 0)
            # 背包sql
        else:
            msg = '道友在秘境中获得一本书籍，翻开一看居然是四库全书...'

    
    elif rift_type == "灵石":
        stone_base = STORY['宝物']['灵石']['stone']
        user_rank = random.randint(1, 10)  # 随机等级
        give_stone = (rift_rank + user_rank) * stone_base
        sql_message.update_ls(user_info['user_id'], give_stone, 1)
        temp_msg = f"竟然获得了灵石：{give_stone}枚！"
        msg = random.choice(TREASUREMSG_4).format(temp_msg)

    return msg



def get_dict_type_rate(data_dict):
    """根据字典内概率,返回字典key"""
    temp_dict = {}
    for i, v in data_dict.items():
        try:
            temp_dict[i] = v["type_rate"]
        except:
            continue
    key = OtherSet().calculated(temp_dict)
    return key


def get_rift_type():
    """根据概率返回秘境等级"""
    data_dict = get_rift_config()['rift']
    return get_dict_type_rate(data_dict)


def get_story_type():
    """根据概率返回事件类型"""
    data_dict = STORY
    return get_dict_type_rate(data_dict)


def get_battle_type():
    """根据概率返回战斗事件的类型"""
    data_dict = STORY['战斗']
    return get_dict_type_rate(data_dict)


def get_goods_type():
    data_dict = STORY['宝物']
    return get_dict_type_rate(data_dict)


def get_id_by_rank(dict_data, user_level, rift_rank=0):
    """根据字典的rank、用户等级、秘境等级随机获取key"""
    l_temp = []
    zx_rank = max(convert_rank(user_level)[0] - 17 - rift_rank, 16)
    # 秘境等级会增幅用户等级
    zx_rank = min(random.randint(zx_rank, zx_rank + 20), 55)
    for k, v in dict_data.items():
        if zx_rank <= v['rank']:
            l_temp.append(k)

    return random.choice(l_temp)


def get_weapon(user_info, rift_rank=0):
    """
    随机获取一个法器
    :param user_info:用户信息类
    :param rift_rank:秘境等级
    :return 法器ID, 法器信息json
    """
    weapon_data = items.get_data_by_item_type(['法器'])
    weapon_id = get_id_by_rank(weapon_data, user_info['level'], rift_rank)
    weapon_info = items.get_data_by_item_id(weapon_id)
    return weapon_id, weapon_info


def get_armor(user_info, rift_rank=0):
    """
    随机获取一个防具
    :param user_info:用户信息类
    :param rift_rank:秘境等级
    :return 防具ID, 防具信息json
    """
    armor_data = items.get_data_by_item_type(['防具'])
    armor_id = get_id_by_rank(armor_data, user_info['level'], rift_rank)
    armor_info = items.get_data_by_item_id(armor_id)
    return armor_id, armor_info


def get_main_info(user_level, rift_rank):
    """获取功法的信息"""
    main_buff_type = get_skill_by_rank(user_level, rift_rank)  # 天地玄黄
    main_buff_id_list = skill_data[main_buff_type]['gf_list']
    init_rate = 60  # 初始概率为60
    finall_rate = init_rate + rift_rank * 5
    finall_rate = finall_rate if finall_rate <= 100 else 100
    is_success = False
    main_buff_id = 0
    if random.randint(0, 100) <= finall_rate:  # 成功
        is_success = True
        main_buff_id = random.choice(main_buff_id_list)
        return is_success, main_buff_id
    return is_success, main_buff_id


def get_sec_info(user_level, rift_rank):
    """获取神通的信息"""
    sec_buff_type = get_skill_by_rank(user_level, rift_rank)  # 天地玄黄
    sec_buff_id_list = skill_data[sec_buff_type]['st_list']
    init_rate = 60  # 初始概率为60
    finall_rate = init_rate + rift_rank * 5
    finall_rate = finall_rate if finall_rate <= 100 else 100
    is_success = False
    sec_buff_id = 0
    if random.randint(0, 100) <= finall_rate:  # 成功
        is_success = True
        sec_buff_id = random.choice(sec_buff_id_list)
        return is_success, sec_buff_id
    return is_success, sec_buff_id

def get_sub_info(user_level, rift_rank):
    """获取辅修功法的信息"""
    sub_buff_type = get_skill_by_rank(user_level, rift_rank)  # 天地玄黄
    sub_buff_id_list = skill_data[sub_buff_type]['fx_list']
    init_rate = 60  # 初始概率为60
    finall_rate = init_rate + rift_rank * 5
    finall_rate = finall_rate if finall_rate <= 100 else 100
    is_success = False
    sub_buff_id = 0
    if random.randint(0, 100) <= finall_rate:  # 成功
        is_success = True
        sub_buff_id = random.choice(sub_buff_id_list)
        return is_success, sub_buff_id
    return is_success, sub_buff_id



def get_skill_by_rank(user_level, rift_rank):
    """根据用户等级、秘境等级随机获取一个技能"""
    zx_rank = max(convert_rank(user_level)[0] - 17, 16)
    # 秘境等级会增幅用户等级
    zx_rank = min(random.randint(zx_rank, zx_rank + 20 - rift_rank), 55)
    temp_dict = []
    for k, v in skill_data.items():
        if zx_rank <= v['rank']:
            temp_dict.append(k)
    return random.choice(temp_dict)


class Rift:
    def __init__(self) -> None:
        self.name = ''
        self.rank = 0
        self.l_user_id = []
        self.time = 0
