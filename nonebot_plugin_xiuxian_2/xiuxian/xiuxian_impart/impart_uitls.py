import os
from pathlib import Path
import numpy
from ...paths import get_paths
from .impart_all import impart_all
from .card_bonus import BONUS_FIELDS, calculate_card_bonuses

img_path = get_paths().data / "卡图"


def _impart_manager():
    from ..xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF

    return XIUXIAN_IMPART_BUFF()


def _impart_data_manager():
    from .impart_data import impart_data_json

    return impart_data_json

def random_int():
    return numpy.random.randint(low=0, high=10000, size=None, dtype="l")

def character_probability(count):
    """角色抽卡概率"""
    count += 1
    if count <= 73:
        ret = 60
    else:
        ret = 60 + 600 * (count - 73)
    return ret

def get_rank(user_id):
    """获取抽卡结果"""
    impart_data = _impart_manager().get_user_impart_info_with_id(user_id)
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
    """检查用户传承数据"""
    data_manager = _impart_data_manager()
    manager = _impart_manager()
    data_manager.find_user_impart(user_id)
    if manager.get_user_impart_info_with_id(user_id) is None:
        manager._create_user(user_id)
        return manager.get_user_impart_info_with_id(user_id)
    else:
        return manager.get_user_impart_info_with_id(user_id)

async def re_impart_data(user_id):
    """重新计算传承属性"""
    data_manager = _impart_data_manager()
    card_dict = data_manager.data_person_list(user_id)
    if card_dict is None:
        return False

    all_data = data_manager.data_all_()
    bonuses = calculate_card_bonuses(card_dict, all_data)
    manager = _impart_manager()
    updates = {
        "impart_two_exp": "update_impart_two_exp",
        "impart_exp_up": "update_impart_exp_up",
        "impart_atk_per": "update_impart_atk_per",
        "impart_hp_per": "update_impart_hp_per",
        "impart_mp_per": "update_impart_mp_per",
        "boss_atk": "update_boss_atk",
        "impart_know_per": "update_impart_know_per",
        "impart_burst_per": "update_impart_burst_per",
        "impart_mix_per": "update_impart_mix_per",
        "impart_reap_per": "update_impart_reap_per",
    }
    for field in BONUS_FIELDS:
        getattr(manager, updates[field])(bonuses[field], user_id)

    return True

async def update_user_impart_data(user_id, time: int):
    """更新用户传承数据"""
    _impart_manager().add_impart_exp_day(time, user_id)
    await re_impart_data(user_id)

def get_star_rating(count):
    """将卡片数量转换为星级显示"""
    effective_count = min(count, 25)
    full_stars = effective_count // 5
    half_stars = effective_count % 5
    
    stars = '★' * full_stars + '☆' * half_stars
    return stars.ljust(5, ' ')

def get_image_representation(image_name: str):
    """获取对应卡图地址"""
    return img_path / f"{image_name}.webp"

def get_impart_card_description(card_name: str) -> str:
    """根据卡图名称描述首张与重复卡的属性加成。"""
    card_info = impart_all.get(card_name)
    if not card_info:
        return f"未找到卡图「{card_name}」的属性信息。"
    
    card_type = card_info["type"]
    value = card_info["vale"]

    type_mapping = {
        "impart_atk_per": "攻击提升",
        "impart_hp_per": "气血提升",
        "impart_mp_per": "真元提升",
        "impart_know_per": "会心提升",
        "impart_burst_per": "会心伤害提升",
        "impart_exp_up": "闭关经验提升",
        "impart_mix_per": "炼丹收获数量提升",
        "impart_reap_per": "灵田收取数量提升",
        "impart_two_exp": "每日双修次数提升",
        "boss_atk": "boss战攻击提升",
    }

    type_text = type_mapping.get(card_type)
    if not type_text:
        return f"卡图「{card_name}」的属性类型暂不支持显示。"
    
    duplicate_value = float(value) / 2
    if card_type in ["impart_two_exp", "impart_reap_per", "impart_mix_per"]:
        unit = {
            "impart_two_exp": "次",
            "impart_reap_per": "颗",
            "impart_mix_per": "颗",
        }[card_type]
        first_text = _format_number(value)
        duplicate_text = _format_number(duplicate_value)
        return (
            f"{type_text}：首张+{first_text}{unit}，"
            f"重复卡每张+{duplicate_text}{unit}"
        )
    return (
        f"{type_text}：首张+{format_impart_percent(value)}，"
        f"重复卡每张+{format_impart_percent(duplicate_value)}"
    )


def _format_number(value) -> str:
    return f"{float(value):.3f}".rstrip("0").rstrip(".") or "0"


def format_impart_percent(value) -> str:
    """格式化传承属性百分比，保留重复卡产生的小数百分比。"""
    return f"{_format_number(float(value) * 100)}%"
