try:
    import ujson as json
except ImportError:
    import json
import os
from pathlib import Path
from datetime import datetime, timedelta
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

configkey = [
    "LEVLECOST",
    "等级建设度",
    "发放宗门资材",
    "每日宗门任务次上限",
    "宗门任务完成cd",
    "宗门任务刷新cd",
    "宗门任务",
    "宗门主功法参数",
    "宗门神通参数",
    "宗门丹房参数"
]
CONFIG = {
    "LEVLECOST": {
        '0': 5000000, '1': 5000000, '2': 5000000, '3': 5000000, '4': 5000000,
        '5': 5000000, '6': 5000000, '7': 5000000, '8': 5000000, '9': 5000000,
        '10': 10000000, '11': 10000000, '12': 10000000, '13': 10000000, '14': 10000000,
        '15': 10000000, '16': 10000000, '17': 10000000, '18': 10000000, '19': 10000000,
        '20': 20000000, '21': 20000000, '22': 20000000, '23': 20000000, '24': 20000000,
        '25': 20000000, '26': 20000000, '27': 20000000, '28': 20000000, '29': 20000000,
        '30': 80000000, '31': 80000000, '32': 80000000, '33': 80000000, '34': 80000000,
        '35': 80000000, '36': 80000000, '37': 80000000, '38': 80000000, '39': 80000000,
        '40': 160000000, '41': 160000000, '42': 160000000, '43': 160000000, '44': 160000000,
        '45': 160000000, '46': 160000000, '47': 160000000, '48': 160000000, '49': 160000000,
        '50': 280000000,
        '51': 280000000, '52': 280000000, '53': 280000000, '54': 280000000,
        '55': 280000000, '56': 280000000, '57': 280000000, '58': 280000000, '59': 280000000,
        '60': 490000000,
        '61': 490000000, '62': 490000000, '63': 490000000, '64': 490000000,
        '65': 490000000, '66': 490000000, '67': 490000000, '68': 490000000, '69': 490000000,
        '70': 857500000,
        '71': 857500000, '72': 857500000, '73': 857500000, '74': 857500000,
        '75': 857500000, '76': 857500000, '77': 857500000, '78': 857500000, '79': 857500000,
        '80': 1500625000,
        '81': 1500625000, '82': 1500625000, '83': 1500625000, '84': 1500625000,
        '85': 1500625000, '86': 1500625000, '87': 1500625000, '88': 1500625000, '89': 1500625000,
        '90': 2626093750,
        '91': 2626093750, '92': 2626093750, '93': 2626093750, '94': 2626093750,
        '95': 2626093750, '96': 2626093750, '97': 2626093750, '98': 2626093750, '99': 2626093750,
        '100': 4595664062
    },
    "等级建设度": 5000000,  # 决定宗门修炼上限等级的参数，500万贡献度每级
    "发放宗门资材": {
        "时间": "11-12",  # 定时任务发放宗门资材，每日11-12点根据 对应宗门贡献度的 * 倍率 发放资材
        "倍率": 1,  # 倍率
    },
    "每日宗门任务次上限": 3,
    "宗门任务完成cd": 60,  # 宗门任务每次完成间隔，单位秒
    "宗门任务刷新cd": 10,  # 宗门任务刷新间隔，单位秒
    "宗门主功法参数": {
        "获取消耗的资材": 1000000,  # 最终消耗会乘档位
        "获取消耗的灵石": 3000000,  # 最终消耗会乘档位
        "获取到功法的概率": 10,
        "建设度": 10000000,  # 建设度除以此参数，一共10档（10档目前无法配置,对应天地玄黄人上下）
        "学习资材消耗": 10000000,  # 最终消耗会乘档位
    },
    "宗门神通参数": {
        "获取消耗的资材": 1000000,  # 最终消耗会乘档位
        "获取消耗的灵石": 3000000,  # 最终消耗会乘档位
        "获取到神通的概率": 10,
        "建设度": 10000000,  # 建设度除以此参数，一共10档（10档目前无法配置,对应天地玄黄人上下）
        "学习资材消耗": 10000000,  # 最终消耗会乘档位
    },
    "宗门丹房参数": {
        "领取贡献度要求": 10000000,
        "elixir_room_level": {
            "1": {
                "name": "黄级",
                "level_up_cost": {
                    "建设度": 50000000,  # 升级消耗建设度
                    "stone": 200000  # 升级消耗宗门灵石
                },
                "give_level": {
                    "give_num": 5,  # 每日领取的丹药数量
                    "rank_up": 1  # rank增幅等级，影响丹药获取品质，目前高于6会无法获得丹药
                }
            },
            "2": {
                "name": "玄级",
                "level_up_cost": {
                    "建设度": 60000000,
                    "stone": 400000
                },
                "give_level": {
                    "give_num": 6,
                    "rank_up": 1
                }
            },
            "3": {
                "name": "地级",
                "level_up_cost": {
                    "建设度": 70000000,
                    "stone": 800000
                },
                "give_level": {
                    "give_num": 7,
                    "rank_up": 1
                }
            },
            "4": {
                "name": "天级",
                "level_up_cost": {
                    "建设度": 80000000,
                    "stone": 1600000
                },
                "give_level": {
                    "give_num": 8,
                    "rank_up": 2
                }
            },
            "5": {
                "name": "仙级",
                "level_up_cost": {
                    "建设度": 100000000,
                    "stone": 3200000
                },
                "give_level": {
                    "give_num": 9,
                    "rank_up": 2
                }
            },
            "6": {
                "name": "仙王级",
                "level_up_cost": {
                    "建设度": 1000000000,
                    "stone": 100000000
                },
                "give_level": {
                    "give_num": 11,
                    "rank_up": 3
                }
            },
            "7": {
                "name": "仙帝级",
                "level_up_cost": {
                    "建设度": 2000000000,
                    "stone": 150000000
                },
                "give_level": {
                    "give_num": 13,
                    "rank_up": 4
                }
            },
            "8": {
                "name": "无上",
                "level_up_cost": {
                    "建设度": 5000000000,
                    "stone": 300000000
                },
                "give_level": {
                    "give_num": 15,
                    "rank_up": 5
                }
            }
        }
    },
    "宗门任务": {
        # type=1：需要扣气血，type=2：需要扣灵石
        # cost：消耗，type=1时，气血百分比，type=2时，消耗灵石
        # give：给与玩家当前修为的百分比修为
        # sect：给与所在宗门 储备的灵石，同时会增加灵石 * 10 的建设度
        "狩猎邪修": {
            "desc": "传言山外村庄有邪修抢夺灵石，请道友下山为民除害",
            "type": 1,
            "cost": 0.45,
            "give": 0.012,
            "sect": 1400000
        },
        "查抄窝点": {
            "desc": "有少量弟子不在金银阁消费，私自架设小型窝点，请道友前去查抄",
            "type": 1,
            "cost": 0.35,
            "give": 0.01,
            "sect": 1000000
        },
        "九转仙丹": {
            "desc": "山门将开，宗门急缺一批药草熬制九转丹，请道友下山购买",
            "type": 2,
            "cost": 900000,
            "give": 0.008,
            "sect": 1500000
        },
        "仗义疏财": {
            "desc": "在宗门外见到师弟欠了别人灵石被追打催债，请道友帮助其还清赌债",
            "type": 2,
            "cost": 1000000,
            "give": 0.0085,
            "sect": 1200000
        },
        "红尘寻宝": {
            "desc": "山下一月一度的市场又开张了，其中虽凡物较多，但是请道友慷慨解囊，为宗门购买一些蒙尘奇宝",
            "type": 2,
            "cost": 1500000,
            "give": 0.009,
            "sect": 1800000
        },
        "镇守仙阵": {
            "desc": "宗门仙阵出现不稳迹象，请道友协助镇守，确保阵法正常运行",
            "type": 1,
            "cost": 0.40,
            "give": 0.011,
            "sect": 1000000
        },
        "斩妖除魔": {
            "desc": "近期妖魔活动频繁，请道友前往镇压，保护百姓安宁",
            "type": 1,
            "cost": 0.50,
            "give": 0.014,
            "sect": 1600000
        },
        "寻访名师": {
            "desc": "宗门仙阵出现不稳迹象，请道友协助镇守，确保阵法正常运行",
            "type": 2,
            "cost": 500000,
            "give": 0.007,
            "sect": 1100000
        },
        "采集灵草": {
            "desc": "宗门药园灵草短缺，请道友外出采集，以供炼丹之用",
            "type": 1,
            "cost": 0.30,
            "give": 0.0095,
            "sect": 900000
        },
        "护送物资": {
            "desc": "宗门有一批重要物资需要护送至盟友门派，途中可能会遭遇劫匪",
            "type": 1,
            "cost": 0.25,
            "give": 0.008,
            "sect": 800000
        },
        "修复阵法": {
            "desc": "护山大阵年久失修，需要注入灵力进行修复",
            "type": 1,
            "cost": 0.35,
            "give": 0.010,
            "sect": 1200000
        },
        "教导弟子": {
            "desc": "新入门的弟子需要指导修行，请道友传授修炼心得",
            "type": 1,
            "cost": 0.20,
            "give": 0.007,
            "sect": 700000
        },
        "采购材料": {
            "desc": "炼器堂急需一批炼器材料，请道友前往集市采购",
            "type": 2,
            "cost": 800000,
            "give": 0.008,
            "sect": 1300000
        },
        "探索秘境": {
            "desc": "附近发现一处小型秘境，请道友前往探索并带回有价值的东西",
            "type": 1,
            "cost": 0.55,
            "give": 0.015,
            "sect": 2000000
        },
        "调解纠纷": {
            "desc": "两位同门因修炼资源分配产生矛盾，请道友出面调解",
            "type": 1,
            "cost": 0.15,
            "give": 0.006,
            "sect": 600000
        },
        "炼制丹药": {
            "desc": "丹房缺少人手，请道友帮忙炼制一批基础丹药",
            "type": 2,
            "cost": 700000,
            "give": 0.009,
            "sect": 1100000
        },
        "守卫矿脉": {
            "desc": "宗门灵石矿脉近期频遭骚扰，需要加强守卫力量",
            "type": 1,
            "cost": 0.40,
            "give": 0.012,
            "sect": 1500000
        }
    },
    "商店商品": {
        "1999": {
            "name": "渡厄丹",
            "cost": 100000000,
            "weekly_limit": 10
        }
    }
}


def get_config():
    try:
        config = readf()
        for key in configkey:
            if key not in list(config.keys()):
                config[key] = CONFIG[key]
        savef(config)
    except:
        config = CONFIG
        savef(config)
    return config


CONFIGJSONPATH = Path(__file__).parent
FILEPATH = CONFIGJSONPATH / 'config.json'


def readf():
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def savef(data):
    data = json.dumps(data, ensure_ascii=False, indent=3)
    savemode = "w" if os.path.exists(FILEPATH) else "x"
    with open(FILEPATH, mode=savemode, encoding="UTF-8") as f:
        f.write(data)
        f.close()
    return True

def get_sect_weekly_purchases(user_id, item_id):
    """获取用户本周已购买宗门商店某商品的数量"""
    user_id = str(user_id)
    file_path = PLAYERSDATA / user_id / "sect_purchases.json"

    if not file_path.exists():
        # 初始化文件并设置重置日期
        init_sect_purchase_file(user_id)
        return 0

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            # 检查是否需要重置
            if "_last_reset" in data:
                last_reset = datetime.strptime(data["_last_reset"], "%Y-%m-%d")
                current_week = datetime.now().isocalendar()[1]
                last_week = last_reset.isocalendar()[1]
                current_year = datetime.now().year
                last_year = last_reset.year

                if current_week != last_week or current_year != last_year:
                    # 重置购买记录
                    init_sect_purchase_file(user_id)
                    return 0
            else:
                # 没有重置日期，初始化
                init_sect_purchase_file(user_id)
                return 0

            return data.get(str(item_id), 0)
        except:
            # 文件损坏，重新初始化
            init_sect_purchase_file(user_id)
            return 0

def init_sect_purchase_file(user_id):
    """初始化宗门商店购买记录文件"""
    user_id = str(user_id)
    file_path = PLAYERSDATA / user_id / "sect_purchases.json"

    data = {
        "_last_reset": datetime.now().strftime("%Y-%m-%d")
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def update_sect_weekly_purchase(user_id, item_id, quantity):
    """更新用户本周购买宗门商店某商品的数量"""
    user_id = str(user_id)
    file_path = PLAYERSDATA / user_id / "sect_purchases.json"

    data = {}
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except:
                pass

    # 确保有重置日期
    if "_last_reset" not in data:
        data["_last_reset"] = datetime.now().strftime("%Y-%m-%d")

    current = data.get(str(item_id), 0)
    data[str(item_id)] = current + quantity

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
