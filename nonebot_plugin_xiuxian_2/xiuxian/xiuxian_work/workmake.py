from .reward_data_source import *
import random
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.xiuxian2_handle import OtherSet


def workmake(work_level, exp, user_level):
    if work_level == '江湖好手':
        work_level = '江湖好手'
    else:
        work_level = work_level[:3]  # 取境界前3位，补全初期、中期、圆满任务可不取

    jsondata_ = reward()
    item_s = Items()
    yaocai_data = jsondata_.reward_yaocai_data()
    levelpricedata = jsondata_.reward_levelprice_data()
    ansha_data = jsondata_.reward_ansa_data()
    zuoyao_data = jsondata_.reward_zuoyao_data()
    work_json = {}
    work_list = [yaocai_data[work_level], ansha_data[work_level], zuoyao_data[work_level]]
    i = 1
    for w in work_list:
        work_name_list = []
        for k, v in w.items():
            work_name_list.append(k)
        work_name = random.choice(work_name_list)
        work_info = w[work_name]
        level_price_data = levelpricedata[work_level][work_info['level']]
        rate, isOut = countrate(exp, level_price_data["needexp"])
        success_msg = work_info['succeed']
        fail_msg = work_info['fail']
        item_type = get_random_item_type()
        zx_rank = max(convert_rank(user_level)[0] - 17, 16)
        zx_rank = min(random.randint(zx_rank, zx_rank + 20), 55)
        item_id = item_s.get_random_id_list_by_rank_and_item_type((zx_rank), item_type)
        if not item_id:
            item_id = 0
        else:
            item_id = random.choice(item_id)
        work_json[work_name] = [rate, level_price_data["award"], int(level_price_data["time"] * isOut), item_id,
                                success_msg, fail_msg]
        i += 1
    return work_json


def get_random_item_type():
    type_rate = {
        "功法": {
            "type_rate": 500,
        },
        "神通": {
            "type_rate": 250,
        },
        "药材": {
            "type_rate": 250,
        },
        "法器": {
            "type_rate": 5,
        },
        "防具": {
            "type_rate": 5,
        }
    }
    temp_dict = {}
    for i, v in type_rate.items():
        try:
            temp_dict[i] = v["type_rate"]
        except:
            continue
    key = [OtherSet().calculated(temp_dict)]
    return key


def countrate(exp, needexp):
    rate = int(exp / needexp * 100)
    isOut = 1
    if rate >= 100:
        tp = 1
        flag = True
        while flag:
            r = exp / needexp * 100
            if r > 100:
                tp += 1
                exp /= 1.5
            else:
                flag = False

        rate = 100
        isOut = float(1 - tp * 0.05)
        if isOut < 0.5:
            isOut = 0.5
    return rate, round(isOut, 2)
