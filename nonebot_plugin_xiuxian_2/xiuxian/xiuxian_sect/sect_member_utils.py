import random
from typing import List
from urllib.parse import quote

from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .sectconfig import get_config
from .sect_tasks import sect_task_state_manager

items = Items()
sql_message = XiuxianDateManage()
config = get_config()
userstask = {}


def bind_sect_member_dependencies(task_store=None, sql_manager=None, item_manager=None, sect_config=None):
    """绑定 __init__.py 中已有的共享对象，保持迁移前的运行状态。"""
    global userstask, sql_message, items, config

    if task_store is not None:
        userstask = task_store
    if sql_manager is not None:
        sql_message = sql_manager
    if item_manager is not None:
        items = item_manager
    if sect_config is not None:
        config = sect_config


def _md_cmd_link(text: str, cmd: str) -> str:
    """生成 QQ 原生 Markdown 快捷指令链接"""
    return f"[{text}](mqqapi://aio/inlinecmd?command={quote(cmd)}&enter=false&reply=false)"


def create_user_sect_task(user_id, sect_id=None, operation_id=None, replace_existing=False,
                          membership_service=None):
    tasklist = config["宗门任务"]
    if sect_id is None:
        user_info = sql_message.get_user_info_with_id(user_id) or {}
        sect_id = user_info.get("sect_id")
    if sect_id and membership_service is not None:
        key = random.choice(list(tasklist))
        claim = membership_service.claim_task(
            operation_id, user_id, sect_id, sect_task_state_manager._period(),
            key, tasklist[key], config["每日宗门任务次上限"], replace_existing,
        )
        if not claim.applied:
            return None
        task = {"任务名称": claim.task_key, "任务内容": dict(claim.task_data or {}),
                "sect_id": claim.sect_id, "period": claim.period, "status": "accepted",
                "progress": 0, "target": 1}
    elif sect_id:
        task = sect_task_state_manager.accept_task(user_id, sect_id, tasklist)
    else:
        key = random.choices(list(tasklist))[0]
        task = {"任务名称": key, "任务内容": tasklist[key]}
    userstask[user_id] = dict(task)
    return userstask[user_id]



def refresh_user_sect_task(user_id, sect_id, operation_id, membership_service):
    current = sect_task_state_manager.get_active_task(user_id)
    if not current:
        return None
    tasklist = config["宗门任务"]
    key = random.choice(list(tasklist))
    refreshed = membership_service.refresh_task(
        operation_id, user_id, sect_id, sect_task_state_manager._period(),
        current["任务名称"], current["任务内容"], key, tasklist[key],
        config["每日宗门任务次上限"],
    )
    if not refreshed.applied:
        return None
    task = {"任务名称": refreshed.task_key, "任务内容": dict(refreshed.task_data or {}),
            "sect_id": refreshed.sect_id, "period": refreshed.period, "status": "accepted",
            "progress": 0, "target": 1}
    userstask[user_id] = task
    return task

def isUserTask(user_id):
    """判断用户是否已有任务 True:有任务"""
    task = sect_task_state_manager.get_active_task(user_id)
    if task:
        userstask[user_id] = dict(task)
        return True

    if user_id not in userstask:
        userstask[user_id] = {}
    return userstask[user_id] != {}


def get_sect_mainbuff_id_list(sect_id):
    """获取宗门功法id列表"""
    sect_info = sql_message.get_sect_info(sect_id)
    mainbufflist = str(sect_info['mainbuff'])[1:-1].split(',')
    return mainbufflist


def get_sect_secbuff_id_list(sect_id):
    """获取宗门神通id列表"""
    sect_info = sql_message.get_sect_info(sect_id)
    secbufflist = str(sect_info['secbuff'])[1:-1].split(',')
    return secbufflist


def set_sect_list(bufflist):
    """传入ID列表,返回[ID列表]"""
    sqllist1 = ''
    for buff in bufflist:
        if buff == '':
            continue
        sqllist1 += f'{buff},'
    sqllist = f"[{sqllist1[:-1]}]"
    return sqllist


def get_mainname_list(bufflist):
    """根据传入的功法列表，返回功法名字列表"""
    namelist = []
    for buff in bufflist:
        mainbuff = items.get_data_by_item_id(buff)
        namelist.append(mainbuff['name'])
    return namelist


def get_secname_list(bufflist):
    """根据传入的神通列表，返回神通名字列表"""
    namelist = []
    for buff in bufflist:
        secbuff = items.get_data_by_item_id(buff)
        namelist.append(secbuff['name'])
    return namelist


def get_mainnameid(buffname, bufflist):
    """根据传入的功法名字,获取到功法的id"""
    tempdict = {}
    buffid = 0
    for buff in bufflist:
        mainbuff = items.get_data_by_item_id(buff)
        tempdict[mainbuff['name']] = buff
    for k, v in tempdict.items():
        if buffname == k:
            buffid = v
    return buffid


def get_secnameid(buffname, bufflist):
    tempdict = {}
    buffid = 0
    for buff in bufflist:
        secbuff = items.get_data_by_item_id(buff)
        tempdict[secbuff['name']] = buff
    for k, v in tempdict.items():
        if buffname == k:
            buffid = v
    return buffid


def get_sectbufftxt(sect_scale, config_):
    """
    获取宗门当前可搜寻的功法/神通品阶列表（包含当前及以下所有品阶）
    参数:
        sect_scale: 宗门建设度
        config_: 宗门主功法/神通参数
    返回: (当前档位, 可搜寻品阶列表)
    """
    buff_gear_map = {
        1: '人阶下品',
        2: '人阶上品',
        3: '黄阶下品',
        4: '黄阶上品',
        5: '玄阶下品',
        6: '玄阶上品',
        7: '地阶下品',
        8: '地阶上品',
        90: '天阶下品',
        100: '天阶上品',
        500: '仙阶下品',
        1000: '仙阶上品'
    }

    # 计算当前档位
    current_gear = min(max(1, sect_scale // config_['建设度']), 1000)

    # 特殊处理仙阶档位
    if current_gear >= 1000:
        current_gear = 1000
    elif current_gear >= 500:
        current_gear = 500
    elif current_gear >= 100:
        current_gear = 100
    elif current_gear >= 90:
        current_gear = 90

    # 获取所有<=当前档位的品阶
    available_gears = [g for g in buff_gear_map.keys() if g <= current_gear]

    # 去重并排序
    available_gears = sorted(list(set(available_gears)))

    # 转换为品阶名称列表
    available_tiers = [buff_gear_map[g] for g in available_gears]

    return current_gear, available_tiers


def get_sect_level(sect_id):
    sect = sql_message.get_sect_info(sect_id)
    return divmod(sect['sect_scale'], config["等级建设度"])


def get_sect_contribution_level(sect_contribution):
    return divmod(sect_contribution, config["等级建设度"])


def generate_random_sect_name(count: int = 1) -> List[str]:
    """随机生成多样化的宗门名称（包含正邪佛魔妖鬼等各类宗门）"""
    # 基础前缀词库（按字数分类，已大幅扩充）
    base_prefixes = {
        # 单字（1字） - 权重10%
        1: [
            # 天象类
            "天", "昊", "穹", "霄", "星", "月", "日", "辰", "云", "霞",
            "风", "雷", "电", "雨", "雪", "霜", "露", "雾", "虹", "霓",
            # 地理类
            "山", "海", "川", "河", "江", "湖", "泉", "溪", "渊", "崖",
            "峰", "岭", "谷", "洞", "岛", "洲", "泽", "野", "原", "林",
            # 五行类
            "金", "木", "水", "火", "土", "阴", "阳", "乾", "坤", "艮",
            # 仙道类
            "玄", "虚", "太", "清", "灵", "真", "元", "始", "极", "妙",
            "神", "仙", "圣", "佛", "魔", "妖", "鬼", "邪", "煞", "冥",
            # 数字类
            "一", "三", "五", "七", "九", "十", "百", "千", "万", "亿"
        ],
        # 双字（2字） - 权重30%
        2: [
            # 天象组合
            "九天", "凌霄", "太虚", "玄天", "紫霄", "青冥", "碧落", "黄泉",
            "星河", "月华", "日曜", "云海", "风雷", "霜雪", "虹霓", "霞光",
            # 地理组合
            "昆仑", "蓬莱", "方丈", "瀛洲", "岱舆", "员峤", "峨眉", "青城",
            "天山", "沧海", "长河", "大江", "五湖", "四海", "八荒", "六合",
            # 五行组合
            "太阴", "太阳", "少阴", "少阳", "玄黄", "洪荒", "混沌", "鸿蒙",
            "乾坤", "坎离", "震巽", "艮兑", "两仪", "四象", "八卦", "五行",
            # 仙道组合
            "太上", "玉清", "上清", "太清", "玄都", "紫府", "瑶池", "琼台",
            "菩提", "般若", "金刚", "罗汉", "天魔", "血煞", "幽冥", "黄泉",
            # 数字组合
            "一元", "两仪", "三才", "四象", "五行", "六合", "七星", "八卦",
            "九宫", "十方", "百炼", "千幻", "万法", "亿劫"
        ],
        # 三字（3字） - 权重40%
        3: [
            # 天象三字
            "九霄云", "凌霄殿", "太虚境", "玄天宫", "紫霄阁", "青冥峰", "碧落泉", "黄泉路",
            "星河转", "月华轮", "日曜光", "云海潮", "风雷动", "霜雪寒", "虹霓现", "霞光漫",
            # 地理三字
            "昆仑山", "蓬莱岛", "方丈洲", "瀛洲境", "岱舆峰", "员峤谷", "峨眉顶", "青城山",
            "天山雪", "沧海月", "长河落", "大江流", "五湖烟", "四海平", "八荒寂", "六合清",
            # 五行三字
            "太阴月", "太阳星", "少阴寒", "少阳暖", "玄黄气", "洪荒初", "混沌开", "鸿蒙始",
            "乾坤转", "坎离合", "震巽动", "艮兑静", "两仪生", "四象变", "八卦演", "五行轮",
            # 仙道三字
            "太上道", "玉清宫", "上清观", "太清殿", "玄都府", "紫府天", "瑶池宴", "琼台会",
            "菩提树", "般若智", "金刚身", "罗汉果", "天魔舞", "血煞阵", "幽冥界", "黄泉河",
            # 数字三字
            "一元始", "两仪分", "三才立", "四象成", "五行生", "六合聚", "七星列", "八卦演",
            "九宫变", "十方界", "百炼钢", "千幻影", "万法归", "亿劫渡"
        ],
        # 四字（4字） - 权重20%
        4: [
            "九霄云外", "太虚仙境", "玄天无极", "紫霄神宫", "青冥之上", "碧落黄泉", "星河倒悬", "月华如水",
            "日曜中天", "云海翻腾", "风雷激荡", "霜雪漫天", "虹霓贯日", "霞光万道", "昆仑之巅", "蓬莱仙岛",
            "方丈神山", "瀛洲幻境", "岱舆悬圃", "员峤仙山", "峨眉金顶", "青城洞天", "天山雪莲", "沧海月明",
            "长河落日", "大江东去", "五湖烟雨", "四海升平", "八荒六合", "洪荒宇宙", "混沌初开", "鸿蒙未判",
            "乾坤无极", "坎离既济", "震巽相薄", "艮兑相成", "两仪四象", "五行八卦", "太上忘情", "玉清圣境",
            "上清灵宝", "太清道德", "玄都紫府", "瑶池仙境", "琼台玉宇", "菩提般若", "金刚不坏", "罗汉金身",
            "天魔乱舞", "血煞冲天", "幽冥鬼域", "黄泉路上"
        ]
    }

    # 特色宗门类型（正派）
    righteous_types = {
        "剑修": ["剑", "剑阁", "剑宗", "剑派", "剑宫", "剑山", "剑域", "天剑", "神剑", "仙剑", "御剑", "飞剑", "心剑"],
        "丹修": ["丹", "丹阁", "丹宗", "丹派", "丹鼎", "丹霞", "丹元", "丹心", "灵丹", "仙丹", "神丹", "药王"],
        "器修": ["器", "器阁", "器宗", "器派", "器殿", "器魂", "器灵", "神工", "天工", "炼器", "铸剑", "百炼"],
        "符修": ["符", "符阁", "符宗", "符派", "符殿", "符箓", "符道", "天符", "神符", "灵符", "咒印", "真言"],
        "阵修": ["阵", "阵阁", "阵宗", "阵派", "阵殿", "阵法", "阵玄", "天阵", "神阵", "灵阵", "奇门", "遁甲"],
        "道修": ["道", "道观", "道宫", "道宗", "道院", "道德", "天道", "真武", "玄门", "妙法", "无为", "自然"],
        "佛修": ["佛", "佛寺", "佛院", "佛宗", "禅院", "禅林", "菩提", "金刚", "般若", "罗汉", "明王", "如来"]
    }

    # 邪魔外道类型
    evil_types = {
        "魔修": ["魔", "魔宫", "魔宗", "魔教", "魔殿", "天魔", "血魔", "心魔", "真魔", "幻魔", "阴魔", "煞魔"],
        "妖修": ["妖", "妖宫", "妖宗", "妖盟", "妖殿", "天妖", "万妖", "百妖", "真妖", "幻妖", "灵妖", "大妖"],
        "鬼修": ["鬼", "鬼门", "鬼宗", "鬼教", "鬼殿", "幽冥", "黄泉", "阴司", "夜叉", "罗刹", "无常", "判官"],
        "邪修": ["邪", "邪门", "邪宗", "邪派", "邪殿", "极乐", "合欢", "血煞", "噬魂", "夺魄", "摄心", "炼尸"]
    }

    # 王朝类名称
    dynasty_names = [
        "仙朝", "仙廷", "神朝", "天朝", "圣朝", "皇朝", "帝朝", "仙国",
        "神国", "天国", "圣国", "皇庭", "帝庭", "仙庭", "神庭", "天宫",
        "天庭", "玉京", "紫府", "瑶台", "琼楼", "金阙", "银汉", "碧城"
    ]

    # 通用后缀词库
    common_suffixes = [
        "门", "派", "宗", "宫", "殿", "阁", "轩", "楼", "观", "院",
        "堂", "居", "斋", "舍", "苑", "坊", "亭", "台", "榭", "坞",
        "谷", "山", "峰", "岛", "洞", "府", "林", "海", "渊", "崖",
        "境", "界", "天", "地", "台", "坛", "塔", "庙", "庵", "祠"
    ]

    # 邪派专用后缀
    evil_suffixes = [
        "窟", "洞", "渊", "狱", "殿", "教", "门", "派", "宗", "宫",
        "血池", "魔窟", "鬼域", "妖巢", "邪殿", "煞地", "阴间", "炼狱",
        "魔渊", "妖洞", "鬼窟", "邪巢", "血海", "骨山", "尸林", "魂冢"
    ]

    # 权重分配：基础40%，正派30%，邪派20%，王朝10%
    type_weights = [0.4, 0.3, 0.2, 0.1]

    # 获取已有宗门名称避免重复
    used_names = {sect['sect_name'] for sect in sql_message.get_all_sects()}
    options = []

    while len(options) < count:
        # 随机选择名称类型
        name_type = random.choices(["base", "righteous", "evil", "dynasty"], weights=type_weights, k=1)[0]

        if name_type == "base":  # 基础宗门名称
            prefix_length = random.choices([1, 2, 3, 4], weights=[0.1, 0.3, 0.4, 0.2], k=1)[0]
            prefix = random.choice(base_prefixes[prefix_length])
            suffix = random.choice(common_suffixes)
            while prefix.endswith(suffix):
                suffix = random.choice(common_suffixes)
            name = f"{prefix}{suffix}"

        elif name_type == "righteous":  # 正派特色宗门
            spec_type = random.choice(list(righteous_types.keys()))
            spec_suffixes = righteous_types[spec_type]

            if random.random() < 0.5:  # 50%单字前缀+特色后缀
                prefix = random.choice(base_prefixes[1])
                suffix = random.choice(spec_suffixes)
            else:  # 50%双字前缀+特色后缀
                prefix = random.choice(base_prefixes[2])
                suffix = random.choice(spec_suffixes[1:])  # 跳过单字特色后缀

            name = f"{prefix}{suffix}"

        elif name_type == "evil":  # 邪魔外道宗门
            spec_type = random.choice(list(evil_types.keys()))
            spec_suffixes = evil_types[spec_type]

            if random.random() < 0.7:  # 70%使用邪派专用后缀
                prefix = random.choice(base_prefixes[1 if random.random() < 0.5 else 2])
                suffix = random.choice(evil_suffixes)
            else:  # 30%使用特色后缀
                prefix = random.choice(base_prefixes[1 if random.random() < 0.5 else 2])
                suffix = random.choice(spec_suffixes)

            name = f"{prefix}{suffix}"

        else:  # 王朝类名称
            prefix = random.choice(base_prefixes[1 if random.random() < 0.5 else 2])
            suffix = random.choice(dynasty_names)
            name = f"{prefix}{suffix}"

        # 检查是否已存在
        if name not in used_names and name not in options:
            options.append(name)

    return options if count > 1 else options[0]


def get_sect_member_limit(sect_scale):
    """获取宗门人数上限"""
    base_member_limit = 20
    additional_members = sect_scale // 50000000
    return min(base_member_limit + additional_members, 100)


def can_join_sect(sect_id):
    """检查宗门是否可以加入"""
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info:
        return False, "宗门不存在"

    if sect_info['closed']:
        return False, "宗门已封闭"

    if not sect_info['join_open']:
        return False, "宗门关闭加入"

    # 检查人数上限
    max_members = get_sect_member_limit(sect_info['sect_scale'])
    current_members = len(sql_message.get_all_users_by_sect_id(sect_id))

    if current_members >= max_members:
        return False, f"人数已满 ({current_members}/{max_members})"

    return True, f"可加入 ({current_members}/{max_members})"
