import os
from pathlib import Path

import numpy
from nonebot.adapters.onebot.v11 import (
    MessageSegment,
)

from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF
from .impart_data import impart_data_json

xiuxian_impart = XIUXIAN_IMPART_BUFF()
img_path = Path() / "data" / "xiuxian" / "卡图"

def random_int():
    return numpy.random.randint(low=0, high=10000, size=None, dtype="l")


# 抽卡概率来自https://www.bilibili.com/read/cv10468091
# 角色抽卡概率
def character_probability(count):
    count += 1
    if count <= 73:
        ret = 60
    else:
        ret = 60 + 600 * (count - 73)
    return ret


def get_rank(user_id):
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    value = random_int()
    num = int(impart_data["wish"])
    for x in range(num, num + 10):
        index_5 = character_probability(x)
        if value <= index_5:
            return True
        if x >= 89:
            return True
    return False


async def impart_check(user_id):
    impart_data_json.find_user_impart(user_id)
    if xiuxian_impart.get_user_impart_info_with_id(user_id) is None:
        xiuxian_impart._create_user(user_id)
        return xiuxian_impart.get_user_impart_info_with_id(user_id)
    else:
        return xiuxian_impart.get_user_impart_info_with_id(user_id)

async def re_impart_data(user_id):
    """重新计算传承属性"""
    list_tp = impart_data_json.data_person_list(user_id)
    if list_tp is None:
        return False
    
    all_data = impart_data_json.data_all_()
    impart_two_exp = 0
    impart_exp_up = 0
    impart_atk_per = 0
    impart_hp_per = 0
    impart_mp_per = 0
    boss_atk = 0
    impart_know_per = 0
    impart_burst_per = 0
    impart_mix_per = 0
    impart_reap_per = 0
    
    # 统计每种卡片的数量
    card_counts = {}
    for x in list_tp:
        card_counts[x] = card_counts.get(x, 0) + 1
    
    # 计算加成
    for card_name, count in card_counts.items():
        card_data = all_data[card_name]
        card_type = card_data["type"]
        base_value = card_data["vale"]
        
        # 计算加成，最多5倍（25张）
        effective_count = min(count, 25)
        bonus = base_value * (1 + (effective_count // 5))
        
        if card_type == "impart_two_exp":
            impart_two_exp += bonus
        elif card_type == "impart_exp_up":
            impart_exp_up += bonus
        elif card_type == "impart_atk_per":
            impart_atk_per += bonus
        elif card_type == "impart_hp_per":
            impart_hp_per += bonus
        elif card_type == "impart_mp_per":
            impart_mp_per += bonus
        elif card_type == "boss_atk":
            boss_atk += bonus
        elif card_type == "impart_know_per":
            impart_know_per += bonus
        elif card_type == "impart_burst_per":
            impart_burst_per += bonus
        elif card_type == "impart_mix_per":
            impart_mix_per += bonus
        elif card_type == "impart_reap_per":
            impart_reap_per += bonus
    
    # 更新属性
    xiuxian_impart.update_impart_two_exp(impart_two_exp, user_id)
    xiuxian_impart.update_impart_exp_up(impart_exp_up, user_id)
    xiuxian_impart.update_impart_atk_per(impart_atk_per, user_id)
    xiuxian_impart.update_impart_hp_per(impart_hp_per, user_id)
    xiuxian_impart.update_impart_mp_per(impart_mp_per, user_id)
    xiuxian_impart.update_boss_atk(boss_atk, user_id)
    xiuxian_impart.update_impart_know_per(impart_know_per, user_id)
    xiuxian_impart.update_impart_burst_per(impart_burst_per, user_id)
    xiuxian_impart.update_impart_mix_per(impart_mix_per, user_id)
    xiuxian_impart.update_impart_reap_per(impart_reap_per, user_id)
    
    return True

async def update_user_impart_data(user_id, time: int):
    """更新用户传承数据

    Args:
        user_id: 用户QQ号
        time: 传承时间
    """
    xiuxian_impart.add_impart_exp_day(time, user_id)
    # 更新传承数据
    await re_impart_data(user_id)

def get_star_rating(count):
    """将卡片数量转换为星级显示"""
    full_stars = count // 5  # 每5张卡1颗实星
    half_stars = count % 5   # 剩余数量用空星表示
    
    stars = '★' * full_stars + '☆' * half_stars
    return stars.ljust(5, ' ')  # 固定5个字符宽度

def get_image_representation(image_name: str):
    """根据获取对应卡图地址
    """
    return img_path / f"{image_name}.webp"