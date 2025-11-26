try:
    import ujson as json
except ImportError:
    import json
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, UserBuffDate, 
    get_weapon_info_msg, get_armor_info_msg,
    get_player_info, save_player_info, 
    get_sec_msg, get_main_info_msg, get_sub_info_msg, get_effect_info_msg
)
from datetime import datetime
import os
from pathlib import Path
from ..xiuxian_config import convert_rank

items = Items()
sql_message = XiuxianDateManage()

sign = lambda x: (x > 0) - (x < 0)
YAOCAIINFOMSG = {
    "-1": "性寒",
    "0": "性平",
    "1": "性热",
    "2": "生息",
    "3": "养气",
    "4": "炼气",
    "5": "聚元",
    "6": "凝神",
}


def check_equipment_can_use(user_id, goods_id):
    """
    装备数据库字段：
        good_type -> '装备'
        state -> 0-未使用， 1-已使用
        goods_num -> '目前数量'
        all_num -> '总数量'
        update_time ->使用的时候更新
        action_time ->使用的时候更新
    判断:
        state = 0, goods_num = 1, all_num =1  可使用
        state = 1, goods_num = 1, all_num =1  已使用
        state = 1, goods_num = 2, all_num =2  已装备，多余的，不可重复使用
    顶用：
    """
    flag = False
    back_equipment = sql_message.get_item_by_good_id_and_user_id(user_id, goods_id)
    if back_equipment['state'] == 0:
        flag = True
    return flag


def get_use_equipment_sql(user_id, goods_id):
    """
    使用装备
    返回sql,和法器或防具
    """
    sql_str = []
    item_info = items.get_data_by_item_id(goods_id)
    user_buff_info = UserBuffDate(user_id).BuffInfo
    now_time = datetime.now()
    item_type = ''
    if item_info['item_type'] == "法器":
        item_type = "法器"
        in_use_id = user_buff_info['faqi_buff']
        sql_str.append(
            f"UPDATE back set update_time='{now_time}',action_time='{now_time}',state=1 WHERE user_id={user_id} and goods_id={goods_id}")  # 装备
        if in_use_id != 0:
            sql_str.append(
                f"UPDATE back set update_time='{now_time}',action_time='{now_time}',state=0 WHERE user_id={user_id} and goods_id={in_use_id}")  # 取下原有的

    if item_info['item_type'] == "防具":
        item_type = "防具"
        in_use_id = user_buff_info['armor_buff']
        sql_str.append(
            f"UPDATE back set update_time='{now_time}',action_time='{now_time}',state=1 WHERE user_id={user_id} and goods_id={goods_id}")  # 装备
        if in_use_id != 0:
            sql_str.append(
                f"UPDATE back set update_time='{now_time}',action_time='{now_time}',state=0 WHERE user_id={user_id} and goods_id={in_use_id}")  # 取下原有的

    return sql_str, item_type


def get_no_use_equipment_sql(user_id, goods_id):
    """
    卸载装备
    返回sql,和法器或防具
    """
    item_info = items.get_data_by_item_id(goods_id)
    user_buff_info = UserBuffDate(user_id).BuffInfo
    now_time = datetime.now()
    sql_str = []
    item_type = ""

    # 检查装备类型，并确定要卸载的是哪种buff
    if item_info['item_type'] == "法器":
        item_type = "法器"
        in_use_id = user_buff_info['faqi_buff']
    elif item_info['item_type'] == "防具":
        item_type = "防具"
        in_use_id = user_buff_info['armor_buff']
    else:
        return sql_str, item_type

    # 如果当前装备正被使用，或者存在需要卸载的其他装备
    if goods_id == in_use_id or in_use_id != 0:
        # 卸载当前装备
        sql_str.append(
            f"UPDATE back set update_time='{now_time}',action_time='{now_time}',state=0 WHERE user_id={user_id} and goods_id={goods_id}")
        # 如果还有其他装备需要卸载（对于法器和防具的情况）
        if in_use_id != 0 and goods_id != in_use_id:
            sql_str.append(
                f"UPDATE back set update_time='{now_time}',action_time='{now_time}',state=0 WHERE user_id={user_id} and goods_id={in_use_id}")

    return sql_str, item_type



def check_equipment_use_msg(user_id, goods_id):
    """
    检测装备是否已用
    """
    user_back = sql_message.get_item_by_good_id_and_user_id(user_id, goods_id)
    state = user_back['state']
    is_use = False
    if state == 0:
        is_use = False
    if state == 1:
        is_use = True
    return is_use

def get_user_main_back_msg(user_id):
    """
    获取背包内的所有物品信息（已装备的装备会显示在前面）
    """
    l_msg = []
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    if user_backs is None:
        return l_msg
    
    # === 装备分类 ===
    # 按装备类型和品阶分类
    equipment_types = {
        "法器": {
            "已装备": [],
            "未装备": {
                "无上": [],
                "极品仙器": [],
                "上品仙器": [],
                "下品仙器": [],
                "上品通天法器": [],
                "下品通天法器": [],
                "上品纯阳法器": [],
                "下品纯阳法器": [],
                "上品法器": [],
                "下品法器": [],
                "上品符器": [],
                "下品符器": []
            }
        },
        "防具": {
            "已装备": [],
            "未装备": {
                "无上": [],
                "极品仙器": [],
                "上品仙器": [],
                "下品仙器": [],
                "上品通天": [],
                "下品通天": [],
                "上品纯阳": [],
                "下品纯阳": [],
                "上品玄器": [],
                "下品玄器": [],
                "上品符器": [],
                "下品符器": []
            }
        }
    }
    
    # 装备品阶优先级（从高到低）
    equipment_ranks = [
        "无上",
        "极品仙器",
        "上品仙器",
        "下品仙器",
        "上品通天法器",
        "下品通天法器",
        "上品纯阳法器",
        "下品纯阳法器",
        "上品法器",
        "下品法器",
        "上品玄器",
        "下品玄器",
        "上品符器",
        "下品符器"
    ]
    
    # === 技能分类 ===
    # 按技能类型分类
    skill_types = {
        "功法": [],
        "神通": [],
        "辅修功法": [],
        "身法": [],
        "瞳术": []
    }
    
    # 技能品阶优先级（从高到低）
    skill_ranks = [
        "无上",
        "仙阶极品",
        "仙阶上品",
        "仙阶下品",
        "天阶上品",
        "天阶下品",
        "地阶上品",
        "地阶下品",
        "玄阶上品",
        "玄阶下品",
        "黄阶上品",
        "黄阶下品",
        "人阶上品",
        "人阶下品"
    ]
    
    # 其他分类保持不变
    l_shenwu_msg = []
    l_xiulianitem_msg = []
    l_special_msg = []
    l_ldl_msg = []
    l_libao_msg = []
    
    for user_back in user_backs:
        item_info = items.get_data_by_item_id(user_back['goods_id'])
        
        if user_back['goods_type'] == "装备":
            # 检查是否已装备
            is_equipped = check_equipment_use_msg(user_id, user_back['goods_id'])
            equip_data = {
                'id': user_back['goods_id'],
                'name': item_info['name'],
                'level': item_info['level'],
                'num': user_back['goods_num'],
                'bind_num': user_back['bind_num'],
                'is_use': is_equipped
            }
            
            # 按装备类型分类
            if item_info['item_type'] == "法器":
                if is_equipped:
                    equipment_types["法器"]["已装备"].append(equip_data)
                else:
                    # 未装备的按品阶分类
                    level = item_info['level']
                    for rank in equipment_ranks:
                        if rank in level:
                            equipment_types["法器"]["未装备"][rank].append(equip_data)
                            break
            
            elif item_info['item_type'] == "防具":
                if is_equipped:
                    equipment_types["防具"]["已装备"].append(equip_data)
                else:
                    # 未装备的按品阶分类
                    level = item_info['level']
                    for rank in equipment_ranks:
                        if rank in level:
                            equipment_types["防具"]["未装备"][rank].append(equip_data)
                            break
        
        elif user_back['goods_type'] == "技能":
            # 按技能类型分类
            skill_type = item_info['item_type']
            if skill_type in skill_types:
                # 按品阶排序
                level = item_info['level']
                skill_rank_index = len(skill_ranks)  # 默认最低优先级
                for i, rank in enumerate(skill_ranks):
                    if rank in level:
                        skill_rank_index = i
                        break
                
                skill_types[skill_type].append({
                    'id': user_back['goods_id'],
                    'name': item_info['name'],
                    'type': skill_type,
                    'level': level,
                    'num': user_back['goods_num'],
                    'bind_num': user_back['bind_num'],
                    'rank_index': skill_rank_index
                })
        
        # 其他物品类型保持不变
        elif user_back['goods_type'] == "神物":
            l_shenwu_msg = get_shenwu_msg(l_shenwu_msg, user_back['goods_id'], user_back['goods_num'], user_back['bind_num'])
        elif user_back['goods_type'] == "聚灵旗":
            l_xiulianitem_msg = get_jlq_msg(l_xiulianitem_msg, user_back['goods_id'], user_back['goods_num'], user_back['bind_num'])
        elif user_back['goods_type'] == "特殊道具":
            l_special_msg = get_special_msg(l_special_msg, user_back['goods_id'], user_back['goods_num'], user_back['bind_num'])
        elif user_back['goods_type'] == "炼丹炉":
            l_ldl_msg = get_ldl_msg(l_ldl_msg, user_back['goods_id'], user_back['goods_num'], user_back['bind_num'])
        elif user_back['goods_type'] == "礼包":
            l_libao_msg = get_libao_msg(l_libao_msg, user_back['goods_id'], user_back['goods_num'], user_back['bind_num'])
    
    # === 构建装备消息 ===
    # 检查是否有装备
    has_equipment = False
    for equip_type in ["法器", "防具"]:
        if (equipment_types[equip_type]["已装备"] or 
            any(equipment_types[equip_type]["未装备"].values())):
            has_equipment = True
            break
    
    if has_equipment:
        
        # 按装备类型显示：先法器，后防具
        for equip_type in ["法器", "防具"]:
            has_this_type_equipment = (equipment_types[equip_type]["已装备"] or 
                                     any(equipment_types[equip_type]["未装备"].values()))
            
            if has_this_type_equipment:
                l_msg.append(f"☆------{equip_type}------☆")
                
                # 1. 先显示已装备的装备
                if equipment_types[equip_type]["已装备"]:
                    for equip in equipment_types[equip_type]["已装备"]:
                        msg = f"{equip['level']}-{equip['name']}\n"
                        msg += f"拥有数量：{equip['num']}，绑定数量：{equip['bind_num']}"
                        msg += "\n※已装备※"
                        l_msg.append(msg)
                
                # 2. 显示未装备的装备（按品阶从高到低）
                for rank in equipment_ranks:
                    equipments = equipment_types[equip_type]["未装备"][rank]
                    if equipments:
                        for equip in equipments:
                            msg = f"{equip['level']}-{equip['name']}\n"
                            msg += f"拥有数量：{equip['num']}，绑定数量：{equip['bind_num']}"
                            l_msg.append(msg)
    
    # === 构建技能消息 ===
    # 检查是否有技能
    has_skills = any(skill_types.values())
    
    if has_skills:
        
        # 按技能类型显示
        for skill_type in ["功法", "神通", "辅修功法", "身法", "瞳术"]:
            skills = skill_types[skill_type]
            if skills:
                # 按品阶从高到低排序
                skills.sort(key=lambda x: x['rank_index'])
                
                l_msg.append(f"☆------{skill_type}------☆")
                
                for skill in skills:
                    msg = f"{skill['level']}-{skill['name']}\n"
                    msg += f"拥有数量：{skill['num']}，绑定数量：{skill['bind_num']}"
                    l_msg.append(msg)
    
    # === 其他物品消息 ===
    if l_shenwu_msg:
        l_msg.append("☆------神物------☆")
        for msg in l_shenwu_msg:
            l_msg.append(msg)
    if l_xiulianitem_msg:
        l_msg.append("☆------聚灵旗------☆")
        for msg in l_xiulianitem_msg:
            l_msg.append(msg)
    if l_special_msg:
        l_msg.append("☆------特殊道具------☆")
        for msg in l_special_msg:
            l_msg.append(msg)
    if l_ldl_msg:
        l_msg.append("☆------炼丹炉------☆")
        for msg in l_ldl_msg:
            l_msg.append(msg)
    if l_libao_msg:
        l_msg.append("☆------礼包------☆")
        for msg in l_libao_msg:
            l_msg.append(msg)
    
    return l_msg

def get_user_equipment_msg(user_id):
    """
    获取背包内的所有装备及其详细信息
    """
    l_msg = []
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    if user_backs is None:
        return l_msg
    
    # 装备品阶优先级（从高到低）
    equipment_ranks = [
        "无上",
        "极品仙器",
        "上品仙器",
        "下品仙器",
        "上品通天法器",
        "下品通天法器",
        "上品纯阳法器",
        "下品纯阳法器",
        "上品法器",
        "下品法器",
        "上品玄器",
        "下品玄器",
        "上品符器",
        "下品符器"
    ]
    
    # 分类存储装备（已装备和未装备分开）
    equipped_weapons = []
    unequipped_weapons = {rank: [] for rank in equipment_ranks}
    equipped_armors = []
    unequipped_armors = {rank: [] for rank in equipment_ranks}
    
    for user_back in user_backs:
        if user_back['goods_type'] == "装备":
            item_info = items.get_data_by_item_id(user_back['goods_id'])
            is_equipped = check_equipment_use_msg(user_id, user_back['goods_id'])
            equip_data = {
                'id': user_back['goods_id'],
                'info': item_info,
                'num': user_back['goods_num'],
                'bind_num': user_back['bind_num'],
                'is_use': is_equipped
            }
            
            if item_info['item_type'] == "法器":
                if is_equipped:
                    equipped_weapons.append(equip_data)
                else:
                    # 按品阶分类
                    level = item_info['level']
                    for rank in equipment_ranks:
                        if rank in level:
                            unequipped_weapons[rank].append(equip_data)
                            break
            elif item_info['item_type'] == "防具":
                if is_equipped:
                    equipped_armors.append(equip_data)
                else:
                    # 按品阶分类
                    level = item_info['level']
                    for rank in equipment_ranks:
                        if rank in level:
                            unequipped_armors[rank].append(equip_data)
                            break
    
    # === 构建法器消息 ===
    if equipped_weapons or any(unequipped_weapons.values()):
        l_msg.append("☆------法器------☆")
        
        # 1. 先显示已装备的法器
        for weapon in equipped_weapons:
            msg = get_weapon_info_msg(weapon['id'], weapon['info'])
            msg += f"\n拥有数量: {weapon['num']} (绑定: {weapon['bind_num']})"
            msg += "\n※已装备※"
            l_msg.append(msg)
        
        # 2. 显示未装备的法器（按品阶从高到低）
        for rank in equipment_ranks:
            weapons = unequipped_weapons[rank]
            for weapon in weapons:
                msg = get_weapon_info_msg(weapon['id'], weapon['info'])
                msg += f"\n拥有数量: {weapon['num']} (绑定: {weapon['bind_num']})"
                l_msg.append(msg)
    
    # === 构建防具消息 ===
    if equipped_armors or any(unequipped_armors.values()):
        l_msg.append("☆------防具------☆")
        
        # 1. 先显示已装备的防具
        for armor in equipped_armors:
            msg = get_armor_info_msg(armor['id'], armor['info'])
            msg += f"\n拥有数量: {armor['num']} (绑定: {armor['bind_num']})"
            msg += "\n※已装备※"
            l_msg.append(msg)
        
        # 2. 显示未装备的防具（按品阶从高到低）
        for rank in equipment_ranks:
            armors = unequipped_armors[rank]
            for armor in armors:
                msg = get_armor_info_msg(armor['id'], armor['info'])
                msg += f"\n拥有数量: {armor['num']} (绑定: {armor['bind_num']})"
                l_msg.append(msg)
    
    return l_msg

def get_user_danyao_back_msg(user_id):
    """
    获取丹药背包信息
    """
    l_msg = []
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    if user_backs is None:
        return l_msg
    
    # 按buff_type分类存储丹药
    danyao_by_type = {}
    
    for user_back in user_backs:
        if user_back['goods_type'] == "丹药":
            item_info = items.get_data_by_item_id(user_back['goods_id'])
            buff_type = item_info.get('buff_type', '未知')
            
            if buff_type not in danyao_by_type:
                danyao_by_type[buff_type] = []
            
            danyao_by_type[buff_type].append({
                'id': user_back['goods_id'],
                'name': item_info['name'],
                'num': user_back['goods_num'],
                'bind_num': user_back['bind_num'],
                'info': item_info
            })
    
    buff_type_order = {
        'hp': 1,           # 回复状态
        'all': 2,          # 回满状态
        'level_up_rate': 3, # 突破概率
        'level_up_big': 4,  # 大境界突破
        'atk_buff': 5,     # 永久攻击
        'exp_up': 6,       # 增加经验
        'level_up': 7,      # 突破相关
        '未知': 999        # 未知类型
    }
    
    buff_type_names = {
        'hp': '气血回复丹药',
        'all': '全状态回复丹药',
        'level_up_rate': '突破丹药',
        'level_up_big': '大境界突破丹药',
        'atk_buff': '永久攻击丹药',
        'exp_up': '经验增加丹药',
        'level_up': '突破辅助丹药',
        '未知': '未知类型丹药'
    }
    
    # 按定义的顺序排序
    sorted_buff_types = sorted(danyao_by_type.keys(), 
                              key=lambda x: buff_type_order.get(x, 999))
    
    # 构建排序后的消息
    for buff_type in sorted_buff_types:
        danyao_list = danyao_by_type[buff_type]
        if danyao_list:
            # 在每个类型内部按丹药名称排序
            danyao_list.sort(key=lambda x: x['name'])
            
            type_name = buff_type_names.get(buff_type, f"{buff_type}类丹药")
            l_msg.append(f"☆------{type_name}------☆")
            
            for danyao in danyao_list:
                msg = f"名字：{danyao['name']}\n"
                msg += f"拥有数量：{danyao['num']}，绑定数量：{danyao['bind_num']}"
                l_msg.append(msg)
    
    return l_msg

def get_user_yaocai_back_msg(user_id):
    """
    获取药材背包信息
    """
    l_msg = []
    l_yaocai_msg = []
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    if user_backs is None:
        return l_msg
    
    # 按品阶排序的药材字典
    sorted_yaocai = {
        "一品药材": [],
        "二品药材": [],
        "三品药材": [],
        "四品药材": [],
        "五品药材": [],
        "六品药材": [],
        "七品药材": [],
        "八品药材": [],
        "九品药材": []
    }
    
    for user_back in user_backs:
        if user_back['goods_type'] == "药材":
            item_info = items.get_data_by_item_id(user_back['goods_id'])
            level = item_info['level']
            if level in sorted_yaocai:
                sorted_yaocai[level].append({
                    'id': user_back['goods_id'],
                    'name': item_info['name'],
                    'num': user_back['goods_num'],
                    'bind_num': user_back['bind_num']
                })
    
    # 构建排序后的消息
    for level, yaocai_list in sorted_yaocai.items():
        if yaocai_list:
            l_msg.append(f"☆------{level}------☆")
            for yaocai in yaocai_list:
                msg = f"名字：{yaocai['name']}\n"
                msg += f"ID：{yaocai['id']}\n"
                msg += f"拥有数量：{yaocai['num']}，绑定数量：{yaocai['bind_num']}"
                l_msg.append(msg)
    
    return l_msg

def get_user_yaocai_detail_back_msg(user_id):
    """
    获取药材背包详细信息
    """
    l_msg = []
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    if user_backs is None:
        return l_msg
    
    # 按品阶排序的药材字典
    sorted_yaocai = {
        "一品药材": [],
        "二品药材": [],
        "三品药材": [],
        "四品药材": [],
        "五品药材": [],
        "六品药材": [],
        "七品药材": [],
        "八品药材": [],
        "九品药材": []
    }
    
    for user_back in user_backs:
        if user_back['goods_type'] == "药材":
            item_info = items.get_data_by_item_id(user_back['goods_id'])
            level = item_info['level']
            if level in sorted_yaocai:
                # 获取药材详细信息
                yaocai_detail = {
                    'id': user_back['goods_id'],
                    'name': item_info['name'],
                    'num': user_back['goods_num'],
                    'bind_num': user_back['bind_num'],
                    'info': get_yaocai_info_msg(user_back['goods_id'], item_info)  # 使用已有的详细信息函数
                }
                sorted_yaocai[level].append(yaocai_detail)
    
    # 构建排序后的消息
    for level, yaocai_list in sorted_yaocai.items():
        if yaocai_list:
            l_msg.append(f"☆------{level}------☆")
            for yaocai in yaocai_list:
                msg = f"{yaocai['info']}\n"  # 包含完整药材信息
                msg += f"拥有数量：{yaocai['num']}，绑定数量：{yaocai['bind_num']}"
                l_msg.append(msg)
    
    return l_msg

def get_libao_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的礼包信息
    """
    item_info = items.get_data_by_item_id(goods_id)
    msg = f"名字：{item_info['name']}\n"
    msg += f"拥有数量：{goods_num}，绑定数量：{bind_num}"
    l_msg.append(msg)
    return l_msg


def get_yaocai_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的药材信息
    """
    item_info = items.get_data_by_item_id(goods_id)
    msg = f"ID：{goods_id}\n"
    msg += f"名字：{item_info['name']}\n"
    msg += f"品级：{item_info['level']}"
    msg += f"\n拥有数量:{goods_num}，绑定数量:{bind_num}"
    l_msg.append(msg)
    return l_msg


def get_jlq_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的修炼物品信息，聚灵旗
    """
    item_info = items.get_data_by_item_id(goods_id)
    msg = f"名字：{item_info['name']}\n"
    msg += f"效果：{item_info['desc']}"
    msg += f"\n拥有数量:{goods_num}，绑定数量:{bind_num}"
    l_msg.append(msg)
    return l_msg


def get_ldl_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的炼丹炉信息
    """
    item_info = items.get_data_by_item_id(goods_id)
    msg = f"名字：{item_info['name']}\n"
    msg += f"效果：{item_info['desc']}"
    msg += f"\n拥有数量:{goods_num}，绑定数量:{bind_num}"
    l_msg.append(msg)
    return l_msg


def get_yaocai_info(yaocai_info):
    """
    获取药材信息
    """
    msg =  f"主药 {YAOCAIINFOMSG[str(yaocai_info['主药']['type'])]}"
    msg += f"{yaocai_info['主药']['power']}"
    msg += f" {YAOCAIINFOMSG[str(sign(yaocai_info['主药']['h_a_c']))]}"
    msg += f"{abs(yaocai_info['主药']['h_a_c']) or ''}\n"
    msg += f"药引 {YAOCAIINFOMSG[str(yaocai_info['药引']['type'])]}"
    msg += f"{yaocai_info['药引']['power']}"
    msg += f" {YAOCAIINFOMSG[str(sign(yaocai_info['药引']['h_a_c']))]}"
    msg += f"{abs(yaocai_info['药引']['h_a_c']) or ''}\n"
    msg += f"辅药 {YAOCAIINFOMSG[str(yaocai_info['辅药']['type'])]}"
    msg += f"{yaocai_info['辅药']['power']}"

    return msg

def get_skill_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的技能信息
    """
    item_info = items.get_data_by_item_id(goods_id)
    msg = ""
    if item_info['item_type'] == '神通':
        msg = f"{item_info['level']}神通-{item_info['name']}:"
    elif item_info['item_type'] == '功法':
        msg = f"{item_info['level']}功法-"
        msg += f"{item_info['name']}"
    elif item_info['item_type'] == '辅修功法':#辅修功法12
        msg = f"{item_info['level']}辅修功法-"
        msg += f"{item_info['name']}"
    elif item_info['item_type'] == '身法':
        msg = f"{item_info['level']}身法-"
        msg += f"{item_info['name']}"
    elif item_info['item_type'] == '瞳术':
        msg = f"{item_info['level']}瞳术-"
        msg += f"{item_info['name']}"
    msg += f"\n拥有数量:{goods_num}，绑定数量:{bind_num}"
    l_msg.append(msg)
    return l_msg


def get_elixir_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的丹药信息
    """
    item_info = items.get_data_by_item_id(goods_id)
    msg = f"名字：{item_info['name']}"
    msg += f"\n拥有数量：{goods_num}，绑定数量：{bind_num}"
    l_msg.append(msg)
    return l_msg

def get_shenwu_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的神物信息
    """
    item_info = items.get_data_by_item_id(goods_id)
    try:
        desc = item_info['desc']
    except KeyError:
        desc = "十分神秘的东西，谁也不知道它的作用"
    
    msg = f"名字：{item_info['name']}\n"
    msg += f"拥有数量：{goods_num}，绑定数量：{bind_num}"
    l_msg.append(msg)
    return l_msg

def get_special_msg(l_msg, goods_id, goods_num, bind_num):
    """
    获取背包内的特殊道具
    """
    item_info = items.get_data_by_item_id(goods_id)
    try:
        desc = item_info['desc']
    except KeyError:
        desc = "十分神秘的东西，谁也不知道它的作用"
    
    msg = f"名字：{item_info['name']}\n"
    msg += f"拥有数量：{goods_num}，绑定数量：{bind_num}"
    l_msg.append(msg)
    return l_msg

def get_item_msg(goods_id, user_id=None):
    """
    获取单个物品的消息
    """
    item_info = items.get_data_by_item_id(goods_id)
    rank_name_list = convert_rank("江湖好手")[1]
    if int(item_info['rank']) == -5:
        goods_rank = 23
    else:
        goods_rank = int(item_info['rank']) + 21
    required_rank_name = rank_name_list[len(rank_name_list) - goods_rank]
    msg = ''
    if item_info['type'] == '丹药':
        msg = f"名字：{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}"
        if item_info['buff_type'] == 'level_up_big':
            back = sql_message.get_item_by_good_id_and_user_id(user_id, goods_id)
            goods_all_num = back['all_num'] if back is not None else 0
            rank = item_info.get('境界', '')
            msg += f"\n境界：{rank}\n耐药性：{goods_all_num}/{item_info['all_num']}"
    elif item_info['item_type'] == '神物':
        msg = f"名字：{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}\n境界：{item_info['境界']}\n增加{number_to(item_info['buff'])}修为"
    elif item_info['item_type'] == '神通':
        msg += f"神通名字：{item_info['name']}\n"
        msg += f"品阶：{item_info['level']}\n"
        msg += f"效果：{get_sec_msg(item_info)}\n{item_info['desc']}\n境界：{required_rank_name}"
    elif item_info['item_type'] == '身法':
        msg += f"身法名字：{item_info['name']}\n"
        msg += f"品阶：{item_info['level']}\n"
        msg += f"效果：{get_effect_info_msg(goods_id)[1]}\n{item_info['desc']}\n境界：{required_rank_name}"
    elif item_info['item_type'] == '瞳术':
        msg += f"瞳术名字：{item_info['name']}\n"
        msg += f"品阶：{item_info['level']}\n"
        msg += f"效果：{get_effect_info_msg(goods_id)[1]}\n{item_info['desc']}\n境界：{required_rank_name}"
    elif item_info['item_type'] == '功法':
        msg += f"功法名字：{item_info['name']}\n"
        msg += f"品阶：{item_info['level']}\n"
        msg += f"效果：{get_main_info_msg(goods_id)[1]}\n境界：{required_rank_name}"
    elif item_info['item_type'] == '辅修功法':  # 辅修功法
        msg += f"辅修名字：{item_info['name']}\n"
        msg += f"品阶：{item_info['level']}\n"
        msg += f"效果：{get_sub_info_msg(goods_id)[1]}\n境界：{required_rank_name}"
    elif item_info['item_type'] == '防具':
        msg = get_armor_info_msg(goods_id, item_info)
        msg += f"\n境界：{required_rank_name}"
    elif item_info['item_type'] == '法器':
        msg = get_weapon_info_msg(goods_id, item_info)
        msg += f"\n境界：{required_rank_name}"
    elif item_info['item_type'] == "药材":
        msg = get_yaocai_info_msg(goods_id, item_info)
    elif item_info['item_type'] == "聚灵旗":
        msg = f"名字：{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}\n修炼速度：{item_info['修炼速度'] * 100}%\n药材速度：{item_info['药材速度'] * 100}%"
    elif item_info['type'] == '特殊道具':
        msg = f"名字：{item_info['name']}\n"
        msg += f"描述：{item_info['desc']}"
    elif item_info['item_type'] == "炼丹炉":
        msg = f"名字：{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}"
    elif item_info['item_type'] == "礼包":
        msg = f"名字：{item_info['name']}\n"
        msg += f"包含：{item_info['desc']}"
    else:
        msg = '不支持的物品'

    return msg


def get_item_msg_rank(goods_id):
    """
    获取单个物品的rank
    """
    item_info = items.get_data_by_item_id(goods_id)
    if item_info['type'] == '丹药':
        msg = item_info['rank']
    elif item_info['item_type'] == '神通':
        msg = item_info['rank']
    elif item_info['item_type'] == '功法':
        msg = item_info['rank']
    elif item_info['item_type'] == '辅修功法':
        msg = item_info['rank']
    elif item_info['item_type'] == '防具':
        msg = item_info['rank']
    elif item_info['item_type'] == '法器':
        msg = item_info['rank']
    elif item_info['item_type'] == "药材":
        msg = item_info['rank']
    elif item_info['item_type'] == "聚灵旗":
        msg = item_info['rank']
    elif item_info['item_type'] == "炼丹炉":
        msg = item_info['rank']      
    elif item_info['item_type'] == "礼包":
        msg = "54"
    elif item_info['item_type'] == "特殊道具":
        msg = "54"
    else:
        msg = 520
    return int(msg)


def get_yaocai_info_msg(goods_id, item_info):
    msg = f"名字：{item_info['name']}\n"
    msg += f"品级：{item_info['level']}\n"
    msg += get_yaocai_info(item_info)
    return msg


def check_use_elixir(user_id, goods_id, num):
    user_info = sql_message.get_user_info_with_id(user_id)
    user_rank = convert_rank(user_info['level'])[0]
    goods_info = items.get_data_by_item_id(goods_id)
    goods_rank = goods_info['rank'] + 19
    goods_name = goods_info['name']
    back = sql_message.get_item_by_good_id_and_user_id(user_id, goods_id)
    goods_all_num = back['all_num'] # 数据库里的使用数量
    remaining_limit = goods_info['all_num'] - goods_all_num  # 剩余可用数量

    if goods_info['buff_type'] == "level_up_rate":  # 增加突破概率的丹药
        level_up_rate = False
        if goods_id == 15151:  # 太清玉液丹
            if num > remaining_limit:
                num = remaining_limit
                msg = f"道友使用的数量超过了耐药性上限呢，仅使用了{num}颗！"
            level_up_rate = True
        elif goods_rank < user_rank:  # 最低使用限制
            msg = f"丹药：{goods_name}的最低使用境界为{goods_info['境界']}，道友不满足使用条件"
        elif goods_rank - user_rank > 10:  # 最高使用限制
            msg = f"道友当前境界为：{user_info['level']}，丹药：{goods_name}已不能满足道友，请寻找适合道友的丹药吧！"
        else:  # 检查完毕
            level_up_rate = True
        if level_up_rate:
            sql_message.update_back_j(user_id, goods_id, num, 1)
            sql_message.update_levelrate(user_id, user_info['level_up_rate'] + goods_info['buff'] * num)
            msg = f"道友成功使用丹药：{goods_name}{num}颗，下一次突破的成功概率提高{goods_info['buff'] * num}%!"

    elif goods_info['buff_type'] == "level_up_big":  # 增加大境界突破概率的丹药
        if goods_rank != user_rank:  # 使用限制
            msg = f"丹药：{goods_name}的使用境界为{goods_info['境界']}，道友不满足使用条件！"
        else:
            if goods_all_num >= goods_info['all_num']:
                msg = f"道友使用的丹药：{goods_name}已经达到丹药的耐药性上限！已经无法使用该丹药了！"    
            else:
                if num > remaining_limit:
                    num = remaining_limit
                    msg = f"道友使用的数量超过了耐药性上限呢，仅使用了{num}颗！"
                else:
                    msg = f"道友成功使用丹药：{goods_name}{num}颗, 下一次突破的成功概率提高{goods_info['buff'] * num}%!"

                sql_message.update_back_j(user_id, goods_id, num, 1)
                sql_message.update_levelrate(user_id, user_info['level_up_rate'] + goods_info['buff'] * num)

    elif goods_info['buff_type'] == "hp":  # 回复状态的丹药
        if user_info['root'] == "凡人":
            user_max_hp = int(user_info['exp'] / 2)
            user_max_mp = int(user_info['exp'])
            if user_info['hp'] == user_max_hp and user_info['mp'] == user_max_mp:
                msg = f"道友的状态是满的，用不了哦！"
            else:
                buff = goods_info['buff']
                buff = round((0.016 * user_rank + 0.104) * buff , 2)
                recover_hp = int(buff * user_max_hp * num)
                recover_mp = int(buff * user_max_mp * num)
                if user_info['hp'] + recover_hp > user_max_hp:
                    new_hp = user_max_hp  # 超过最大
                else:
                    new_hp = user_info['hp'] + recover_hp
                if user_info['mp'] + recover_mp > user_max_mp:
                    new_mp = user_max_mp
                else:
                    new_mp = user_info['mp'] + recover_mp
                msg = f"道友成功使用丹药：{goods_name}{num}颗，经过境界转化状态恢复了{int(buff * 100 * num)}%!"
                sql_message.update_back_j(user_id, goods_id, num=num ,use_key=1)
                sql_message.update_user_hp_mp(user_id, new_hp, new_mp)
        else:
            if goods_rank < user_rank:  # 使用限制
                msg = f"丹药：{goods_name}的使用境界为{goods_info['境界']}以上，道友不满足使用条件！"
            else:
                user_max_hp = int(user_info['exp'] / 2)
                user_max_mp = int(user_info['exp'])
                if user_info['hp'] == user_max_hp and user_info['mp'] == user_max_mp:
                    msg = f"道友的状态是满的，用不了哦！"
                else:
                    buff = goods_info['buff']
                    buff = round((0.016 * user_rank + 0.104) * buff , 2)
                    recover_hp = int(buff * user_max_hp * num)
                    recover_mp = int(buff * user_max_mp * num)
                    if user_info['hp'] + recover_hp > user_max_hp:
                        new_hp = user_max_hp  # 超过最大
                    else:
                        new_hp = user_info['hp'] + recover_hp
                    if user_info['mp'] + recover_mp > user_max_mp:
                        new_mp = user_max_mp
                    else:
                        new_mp = user_info['mp'] + recover_mp
                    msg = f"道友成功使用丹药：{goods_name}{num}颗，经过境界转化状态恢复了{int(buff * 100 * num)}%!"
                    sql_message.update_back_j(user_id, goods_id, num=num ,use_key=1)
                    sql_message.update_user_hp_mp(user_id, new_hp, new_mp)

    elif goods_info['buff_type'] == "all":  # 回满状态的丹药
        if user_info['root'] == "凡人":
            user_max_hp = int(user_info['exp'] / 2)
            user_max_mp = int(user_info['exp'])
            if user_info['hp'] == user_max_hp and user_info['mp'] == user_max_mp:
                msg = f"道友的状态是满的，用不了哦！"
            else:
                sql_message.update_back_j(user_id, goods_id, use_key=1)
                sql_message.update_user_hp(user_id)
                msg = f"道友成功使用丹药：{goods_name}1颗,状态已全部恢复!"
        else:
            if goods_rank < user_rank:  # 使用限制
                msg = f"丹药：{goods_name}的使用境界为{goods_info['境界']}以上，道友不满足使用条件！"
            else:
                user_max_hp = int(user_info['exp'] / 2)
                user_max_mp = int(user_info['exp'])
                if user_info['hp'] == user_max_hp and user_info['mp'] == user_max_mp:
                    msg = f"道友的状态是满的，用不了哦！"
                else:
                    sql_message.update_back_j(user_id, goods_id, use_key=1)
                    sql_message.update_user_hp(user_id)
                    msg = f"道友成功使用丹药：{goods_name}1颗,状态已全部恢复!"

    elif goods_info['buff_type'] == "atk_buff":  # 永久加攻击buff的丹药
        if user_info['root'] == "凡人":
            buff = goods_info['buff'] * num
            sql_message.updata_user_atk_buff(user_id, buff)
            sql_message.update_back_j(user_id, goods_id,num=num, use_key=1)
            msg = f"道友成功使用丹药：{goods_name}{num}颗，攻击力永久增加{buff}点！"
        else:
            if goods_rank < user_rank:  # 使用限制
                msg = f"丹药：{goods_name}的使用境界为{goods_info['境界']}以上，道友不满足使用条件！"
            else:
                buff = goods_info['buff'] * num
                sql_message.updata_user_atk_buff(user_id, buff)
                sql_message.update_back_j(user_id, goods_id,num=num, use_key=1)
                msg = f"道友成功使用丹药：{goods_name}{num}颗，攻击力永久增加{buff}点！"

    elif goods_info['buff_type'] == "exp_up":  # 加固定经验值的丹药
        if goods_rank < user_rank:  # 使用限制
            msg = f"丹药：{goods_name}的使用境界为{goods_info['境界']}以上，道友不满足使用条件！"
        else:
            exp = goods_info['buff'] * num
            user_hp = int(user_info['hp'] + (exp / 2))
            user_mp = int(user_info['mp'] + exp)
            user_atk = int(user_info['atk'] + (exp / 10))
            sql_message.update_exp(user_id, exp)
            sql_message.update_power2(user_id)  # 更新战力
            sql_message.update_user_attribute(user_id, user_hp, user_mp, user_atk)  # 这种事情要放在update_exp方法里
            sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
            msg = f"道友成功使用丹药：{goods_name}{num}颗,修为增加{exp}点！"
    else:
        msg = f"该类型的丹药目前暂时不支持使用！"
    return msg


def get_use_jlq_msg(user_id, goods_id):
    user_info = sql_message.get_user_info_with_id(user_id)
    if user_info['blessed_spot_flag'] == 0:
        msg = f"道友还未拥有洞天福地，无法使用该物品"
    else:
        item_info = items.get_data_by_item_id(goods_id)
        user_buff_data = UserBuffDate(user_id).BuffInfo
        if int(user_buff_data['blessed_spot']) > item_info['level']:
            msg = f"当前福地聚灵旗等级较高，无需降级"
        elif int(user_buff_data['blessed_spot']) == item_info['level']:
            msg = f"聚灵旗和福地聚灵旗等级相同，无需使用"
        else:
            mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
            mix_elixir_info['药材速度'] = item_info['药材速度']
            save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')
            sql_message.update_back_j(user_id, goods_id)
            sql_message.updata_user_blessed_spot(user_id, item_info['level'])
            msg = f"道友洞天福地的聚灵旗已经替换为：{item_info['name']}"
    return msg

PATH = Path(__file__).parent
FILEPATH = PATH / 'shop.json'

