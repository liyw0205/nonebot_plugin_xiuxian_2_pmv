import random
from .riftconfig import get_rift_config
from ..xiuxian_utils.utils import number_to
from .jsondata import read_f
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, XIUXIAN_IMPART_BUFF, OtherSet
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank, base_rank
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.numeric_bind import percent_exp_reward
from ...features.rift.domain import RiftBossBattleResolver, RiftDamageEventResolver

_sql_message_instance = None
xiuxian_impart = XIUXIAN_IMPART_BUFF()
items = Items()
skill_data = read_f()


def _sql_message():
    global _sql_message_instance
    if _sql_message_instance is None:
        _sql_message_instance = XiuxianDateManage()
    return _sql_message_instance

NONEMSG = [
    "道友在秘境中晕头转向，等到清醒时已被秘境踢出，毫无所获！",
    "道友进入秘境发现此地烟雾缭绕，无法前行，只能空手而归！",
    "秘境中突然出现空间裂缝，道友被迫提前离开，一无所获！",
    "道友在秘境中遭遇时间乱流，转眼间已被传送出来，两手空空！",
    "秘境守护兽突然苏醒，道友不得不仓皇逃命，未能取得任何宝物！",
    "道友在秘境中迷路三天三夜，等找到出口时秘境已经关闭！",
    "秘境突然坍塌，道友险些丧命，最终狼狈逃出！",
    "道友被秘境中的幻阵所困，等破阵而出时秘境已经关闭！",
]

TREASUREMSG = [
    "道友进入秘境后误入一处兵冢，仔细查探后找到了{}",
    "在秘境最深处与神秘势力大战，底牌尽出后抢到了{}",
    "道友破解了秘境中的古老机关，获得了{}",
    "道友在秘境废墟中搜寻多时，终于在一处暗格中发现了{}",
    "秘境守护兽的巢穴中闪烁着奇异光芒，道友冒险潜入后得到了{}",
    "道友击败了守护宝物的傀儡，成功夺取了{}",
    "秘境中的神秘商人看你有缘，以低价出售给你{}",
    "道友在秘境祭坛上完成献祭仪式，获得了{}",
]

TREASUREMSG_1 = [
    "道友进入秘境后闯过了重重试炼，最终获得了{}",
    "道友在秘境中历经九死一生，终于得到了{}",
    "道友解开了秘境中的上古谜题，获得了{}",
    "秘境中的古老传承选择了你，赐予了{}",
    "道友在秘境核心区域发现了{}",
    "道友用智慧通过了秘境智者的考验，被赠予了{}",
    "秘境中的时间流速异常，道友苦修百日，最终得到了{}",
    "道友在秘境灵泉中沐浴时，意外获得了{}",
]

TREASUREMSG_2 = [
    "道友进入秘境后偶遇一位修为深不可测的大能，获赠{}",
    "秘境中的藏书阁突然门户大开，道友趁机取得了{}",
    "一位垂死的前辈将毕生所学的{}传授于你后坐化",
    "道友在秘境古籍中发现藏宝图，按图索骥找到了{}",
    "秘境中的石碑突然发光，将{}的内容印入你的脑海",
    "道友救助了秘境中的灵兽，灵兽衔来{}作为报答",
    "秘境中的幻象不断演示着{}的修炼法门，道友默默记下",
    "道友在秘境参悟时突然顿悟，自创出了{}",
]

TREASUREMSG_3 = [
    "道友在秘境中寻宝许久无果，却在石缝里发现了一本{}",
    "道友失望准备离开时，踢到一块石头，下面压着{}",
    "秘境关闭的震动使岩壁剥落，露出了藏在其中的{}",
    "道友在最后一刻发现秘境中的暗门，匆忙带出了{}",
    "离开秘境时空间扭曲，一本{}突然掉入你怀中",
    "道友在秘境出口处被神秘力量指引，获得{}后离开",
    "秘境中的幻象消散后，原地留下了{}",
    "道友的储物袋突然震动，不知何时多了一本{}",
]

TREASUREMSG_4 = [
    "道友在秘境中仔细搜寻，找到了{}",
    "道友发现一位前辈坐化于此，得到了{}",
    "秘境中的灵田已经荒废，但仍有一些{}幸存",
    "道友破解了秘境药园的禁制，采集到了{}",
    "秘境丹房中的丹炉尚有余温，里面藏着{}",
    "道友在秘境灵脉节点修炼时，意外获得了{}",
    "秘境中的灵兽园早已破败，但仍寻得{}",
    "道友在秘境厨房发现了上古食谱和{}",
]

TREASUREMSG_5 = [
    "道友在秘境探索时天旋地转，被踢出秘境时手中多了一本{}",
    "秘境中的时空乱流将你卷入，稳定下来时手中握着{}",
    "道友被秘境强制传送出来时，储物袋中多了一本{}",
    "秘境关闭的最后一刻，一道灵光飞入你怀中，正是{}",
    "道友在秘境昏迷期间，似乎有人将{}塞入了你的衣襟",
    "离开秘境后整理收获时，才发现得到了{}",
    "秘境中的记忆逐渐模糊，唯有{}的内容清晰异常",
    "道友的识海中突然多了一段关于{}的完整记忆",
]


STORY = {
    "宝物": {
        "type_rate": 70,
        "功法": {
            "type_rate": 50,
        },
        "辅修功法": {
            "type_rate": 10,
        },
        "神通": {
            "type_rate": 50,
        },
        "法器": {
            "type_rate": 15,
        },
        "防具": {
            "type_rate": 20,
        },
        "灵石": {
            "type_rate": 55,
            "stone": 3000000
        }
    },
    "战斗": {
        "type_rate": 10,
        "Boss战斗": {
            "type_rate": 50,
            "Boss数据": {
                "name": ["墨蛟", "婴鲤兽", "千目妖", "鸡冠蛟", "妖冠蛇", "铁火蚁", "天晶蚁", "银光鼠", "紫云鹰", "狗青"],
                "hp": [1.2, 1.4, 1.6, 1.8, 2, 3, 5, 10],
                "mp": 10,
                "atk": [0.1, 0.12, 0.14, 0.16, 0.18, 0.5, 1, 2],
            },
            "success": {
                "desc": [
                    "道友大战三百回合，终于将{}斩于剑下！",
                    "道友智计百出，设下陷阱成功击杀{}！",
                    "经过惨烈战斗，道友以重伤代价击毙了{}！",
                    "道友与{}斗法三日三夜，最终险胜一筹！",
                    "道友临阵突破，实力暴涨后击败了{}！",
                    "道友召唤天雷地火，将{}轰成齑粉！",
                    "道友的绝招正中{}要害，取得了胜利！",
                    "{}不敌道友的猛烈攻势，最终败亡！",
                ],
                "give": {
                    "exp": [0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.09],
                    "stone": 500000
                }
            },
            "fail": {
                "desc": [
                    "道友不敌{}凶威，重伤逃遁！",
                    "{}突然狂暴，道友见势不妙立即撤退！",
                    "道友的法宝被{}击碎，不得不败走！",
                    "{}召唤援军，道友双拳难敌四手！",
                    "道友中了{}的诡计，险些丧命！",
                    "{}施展秘法，道友难以抵挡！",
                    "道友被{}的毒雾所困，勉强突围！",
                    "{}设下阵法，道友陷入苦战后撤退！",
                ]
            }
        },
        "掉血事件": {
            "type_rate": 50,
            "desc": [
                "秘境内竟然散布着浓烈的毒气，道友贸然闯入！{}!",
                "秘境内竟然藏着一群未知势力，道友被打劫了！{}!",
                "道友触发秘境机关，遭到暗器袭击！{}!",
                "秘境中的幻阵使道友自残！{}!",
                "道友误入空间裂缝，遭受空间之力撕扯！{}!",
                "秘境突然地震，道友被落石砸中！{}!",
                "道友被秘境中的凶兽追杀！{}!",
                "秘境中的怨灵缠上了道友！{}!",
                "道友贪心触动禁制，遭到反噬！{}!",
                "秘境中的时间乱流加速了道友的衰老！{}!",
            ],
            "cost": {
                "exp": {
                    "type_rate": 10,
                    "value": [0.003, 0.004, 0.005]
                },
                "hp": {
                    "type_rate": 70,
                    "value": [0.3, 0.5, 0.7]
                },
                "stone": {
                    "type_rate": 20,
                    "value": [3000000, 5000000, 1000000]
                },
            }
        },
    },
    "无事": {
        "type_rate": 20,
    }
}


def _boss_battle_resolver() -> RiftBossBattleResolver:
    return RiftBossBattleResolver(
        boss_config=STORY['战斗']['Boss战斗'],
        battle_runner=Boss_fight,
        rank_score=lambda level: convert_rank(level)[0],
        level_power=lambda level: jsondata.level_data()[level]["power"],
        max_exp_factor=XiuConfig().closing_exp_upper_limit * 0.1,
        exp_reward=percent_exp_reward,
        format_number=number_to,
    )


async def get_boss_battle_info(user_info, rift_rank, bot_id, persist=True):
    """Compatibility adapter for the feature-owned Boss battle resolver."""
    event = await _boss_battle_resolver().roll(
        user_info,
        rift_rank,
        bot_id,
        random_source=random,
        battle_mode=2 if persist else 0,
    )
    if persist and event.victory:
        delta = event.outcome["delta"]
        _sql_message().update_exp(user_info['user_id'], delta.get("exp", 0))
        _sql_message().update_ls(user_info['user_id'], delta.get("stone", 0), 1)
    if persist:
        return event.battle_result, event.message
    return event.battle_result, event.message, event.outcome


def get_dxsj_info(rift_type, user_info):
    """Compatibility adapter for the feature-owned damage event resolver."""
    event = RiftDamageEventResolver(
        battle_config=STORY['战斗'],
        exp_reward=percent_exp_reward,
        format_number=number_to,
    ).roll(rift_type, user_info, random_source=random)
    return event.message, event.as_outcome()


def get_treasure_info(user_info, rift_rank):
    rift_type = get_goods_type()  # 功法、神通、法器、防具、法宝#todo
    msg = None
    item_name = None
    outcome = {"delta": {}, "items": []}
    if rift_type == "法器":
        weapon_info = get_weapon(user_info, rift_rank)
        temp_msg = f"{weapon_info[1]['name']}!"
        msg = random.choice(TREASUREMSG).format(temp_msg)
        item_name = weapon_info[1]['name']
        outcome["items"].append({"id": weapon_info[0], "name": weapon_info[1]['name'], "type": weapon_info[1]['type'], "amount": 1})
        # 背包sql

    elif rift_type == "防具":  # todo
        armor_info = get_armor(user_info, rift_rank)
        temp_msg = f"{armor_info[1]['name']}!"
        msg = random.choice(TREASUREMSG_1).format(temp_msg)
        item_name = armor_info[1]['name']
        outcome["items"].append({"id": armor_info[0], "name": armor_info[1]['name'], "type": armor_info[1]['type'], "amount": 1})
        # 背包sql

    elif rift_type == "功法":
        give_main_info = get_main_info(user_info['level'], rift_rank)
        if give_main_info[0]:  # 获得了
            main_buff_id = give_main_info[1]
            main_buff = items.get_data_by_item_id(main_buff_id)
            temp_msg = f"{main_buff['name']}"
            msg = random.choice(TREASUREMSG_2).format(temp_msg)
            item_name = main_buff['name']
            outcome["items"].append({"id": main_buff_id, "name": main_buff['name'], "type": main_buff['type'], "amount": 1})
        else:
            msg = '道友在秘境中获得一本书籍，翻开一看居然是绿野仙踪...'

    elif rift_type == "神通":
        give_sec_info = get_sec_info(user_info['level'], rift_rank)
        if give_sec_info[0]:  # 获得了
            sec_buff_id = give_sec_info[1]
            sec_buff = items.get_data_by_item_id(sec_buff_id)
            temp_msg = f"{sec_buff['name']}!"
            msg = random.choice(TREASUREMSG_3).format(temp_msg)
            item_name = sec_buff['name']
            outcome["items"].append({"id": sec_buff_id, "name": sec_buff['name'], "type": sec_buff['type'], "amount": 1})
            # 背包sql
        else:
            msg = '道友在秘境中获得一本书籍，翻开一看居然是戏书...'

    elif rift_type == "辅修功法":
        give_sub_info = get_sub_info(user_info['level'], rift_rank)
        if give_sub_info[0]:  # 获得了
            sub_buff_id = give_sub_info[1]
            sub_buff = items.get_data_by_item_id(sub_buff_id)
            temp_msg = f"{sub_buff['name']}!"
            msg = random.choice(TREASUREMSG_5).format(temp_msg)
            item_name = sub_buff['name']
            outcome["items"].append({"id": sub_buff_id, "name": sub_buff['name'], "type": sub_buff['type'], "amount": 1})
            # 背包sql
        else:
            msg = '道友在秘境中获得一本书籍，翻开一看居然是四库全书...'

    
    elif rift_type == "灵石":
        stone_base = STORY['宝物']['灵石']['stone']
        user_rank = random.randint(1, 3)  # 随机等级
        give_stone = (rift_rank + user_rank) * stone_base
        outcome["delta"] = {"stone": give_stone}
        temp_msg = f"灵石：{number_to(give_stone)}枚！"
        msg = random.choice(TREASUREMSG_4).format(temp_msg)

    outcome["message"] = msg
    return item_name, msg, outcome



def get_dict_type_rate(data_dict):
    """根据字典内概率,返回字典key"""
    temp_dict = {}
    for i, v in data_dict.items():
        try:
            temp_dict[i] = v["type_rate"]
        except Exception:
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
    zx_rank = base_rank(user_level, 5, up=rift_rank + 10)
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
    init_rate = 70  # 初始概率为70
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
    init_rate = 70  # 初始概率为70
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
    init_rate = 70  # 初始概率为70
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
    zx_rank = base_rank(user_level, 5, up=rift_rank + 10)
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
        self.target_realm = ''
        self.target_heaven = ''
        self.target_node_id = ''
        self.target_node_name = ''
        self.target_nodes = []
