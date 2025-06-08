import random
from pathlib import Path
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .bossconfig import get_boss_config
import json

config = get_boss_config()
JINGJIEEXP = {  # 数值为中期和圆满的平均值
    "感气境": [1000, 2000, 3000],
    "练气境": [6000, 8000, 10000],
    "筑基境": [30000, 60000, 90000],
    "结丹境": [144000, 160000, 176000],
    "金丹境": [284000, 352000, 416000],
    "元神境": [832000, 896000, 960000],
    "化神境": [1920000, 2048000, 2176000],
    "炼神境": [4352000, 4608000, 4864000],
    "返虚境": [9728000, 12348000, 14968000],
    "大乘境": [30968000, 35968000, 40968000],
    "虚道境": [60968000, 70968000, 80968000],
    "斩我境": [120968000, 140968000, 160968000],
    "遁一境": [321936000, 450710400, 579484800],
    "至尊境": [1158969600, 1622557440, 2086145280],
    "微光境": [4172290560, 5841206784, 7510123008],
    "星芒境": [15020246016, 21028344422, 27036442828],
    "月华境": [54072885657, 75702039920, 97331194180],
    "耀日境": [194662388360, 372527343704, 550392299048],
    "祭道境": [2721543800000, 4873629500000, 13579246800000],
    "自在境": [38254628000000, 45280915000000, 82467315000000],
    "破虚境": [91827364500000, 163847290000000, 224681357000000],
    "无界境": [315926530000000, 523847560000000, 682741950000000],
    "混元境": [918273645000000, 1368421700000000, 2675438200000000],
    "造化境": [3159265300000000, 4528091500000000, 8246731500000000],
    "永恒境": [9182736450000000, 13684217000000000, 22468135700000000]
}


jinjie_list = [k for k, v in JINGJIEEXP.items()]
sql_message = XiuxianDateManage()  # sql类

def get_boss_jinjie_dict():
    CONFIGJSONPATH = Path() / "data" / "xiuxian" / "境界.json"
    with open(CONFIGJSONPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    temp_dict = {}
    data = json.loads(data)
    for k, v in data.items():
        temp_dict[k] = v['exp']
    return temp_dict


def get_boss_exp(boss_jj):
    if boss_jj in JINGJIEEXP:
        bossexp = random.choice(JINGJIEEXP[boss_jj])
        bossinfo = {
            '气血': bossexp * config["Boss倍率"]["气血"],
            '总血量': bossexp * config["Boss倍率"]["气血"],
            '真元': bossexp * config["Boss倍率"]["真元"],
            '攻击': int(bossexp * config["Boss倍率"]["攻击"])
        }
        return bossinfo
    else:
        return None


def createboss():
    top_user_info = sql_message.get_realm_top1_user() # 改成了境界第一
    top_user_level = top_user_info['level']
    if len(top_user_level) == 5:
        level = top_user_level[:3] 
    elif len(top_user_level) == 4: # 对江湖好手判断
        level = "感气境"
    elif len(top_user_level) == 2: # 对至高判断
        level = "永恒境"

    boss_jj = random.choice(jinjie_list[:jinjie_list.index(level) + 1])
    bossinfo = get_boss_exp(boss_jj)
    bossinfo['name'] = random.choice(config["Boss名字"])
    bossinfo['jj'] = boss_jj
    bossinfo['max_stone'] = random.choice(config["Boss灵石"][boss_jj])
    bossinfo['stone'] = bossinfo['max_stone']
    return bossinfo


def createboss_jj(boss_jj, boss_name=None):
    bossinfo = get_boss_exp(boss_jj)
    if bossinfo:
        bossinfo['name'] = boss_name if boss_name else random.choice(config["Boss名字"])
        bossinfo['jj'] = boss_jj
        bossinfo['max_stone'] = random.choice(config["Boss灵石"][boss_jj])
        bossinfo['stone'] = bossinfo['max_stone']
        return bossinfo
    else:
        return None


