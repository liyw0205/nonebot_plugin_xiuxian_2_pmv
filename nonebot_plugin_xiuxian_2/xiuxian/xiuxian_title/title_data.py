"""
称号数据管理模块
- 加载称号定义
- 条件解析与判定
- 称号解锁/装备/卸下
"""
try:
    import ujson as json
except ImportError:
    import json
import re
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from nonebot.log import logger

from ..xiuxian_config import XiuConfig, convert_rank

READPATH = Path() / "data" / "xiuxian"
TITLE_JSONPATH = READPATH / "修炼物品" / "称号.json"

# 全局缓存
_TITLE_CACHE: Dict[str, dict] = {}


def load_title_data() -> Dict[str, dict]:
    """加载称号数据"""
    global _TITLE_CACHE
    if _TITLE_CACHE:
        return _TITLE_CACHE
    try:
        with open(TITLE_JSONPATH, "r", encoding="UTF-8") as f:
            _TITLE_CACHE = json.loads(f.read())
        logger.opt(colors=True).info(f"<green>称号数据加载完成，共{len(_TITLE_CACHE)}个称号</green>")
    except FileNotFoundError:
        logger.warning(f"称号数据文件未找到: {TITLE_JSONPATH}")
        _TITLE_CACHE = {}
    except json.JSONDecodeError as e:
        logger.error(f"称号数据JSON解析错误: {e}")
        _TITLE_CACHE = {}
    return _TITLE_CACHE


def get_title_by_id(title_id: str) -> Optional[dict]:
    """通过ID获取称号数据"""
    data = load_title_data()
    return data.get(str(title_id))


def get_all_titles() -> Dict[str, dict]:
    """获取所有称号数据"""
    return load_title_data()


def find_title_id_by_name_or_id(name_or_id: str) -> Optional[str]:
    """通过称号ID或名称查找称号ID"""
    all_titles = load_title_data()
    key = str(name_or_id).strip()

    if key in all_titles:
        return key

    for tid, tdata in all_titles.items():
        if tdata.get("name") == key:
            return tid
    return None


def parse_condition(condition_str: str) -> List[Tuple[str, str, str]]:
    """
    解析条件字符串
    支持格式: "修仙签到>=100" 或 "修仙签到>=100;历练次数>=50"
    返回: [(key, operator, value), ...]
    """
    conditions = []
    for part in condition_str.split(';'):
        part = part.strip()
        if not part:
            continue
        match = re.match(r'(.+?)(>=|<=|!=|>|<|==)(.+)', part)
        if match:
            key = match.group(1).strip()
            op = match.group(2)
            value = match.group(3).strip()
            conditions.append((key, op, value))
    return conditions


def _compare(actual, operator: str, expected) -> bool:
    """通用比较函数"""
    try:
        # 尝试数值比较
        actual_num = float(actual) if actual is not None else 0
        expected_num = float(expected)
        if operator == '>=':
            return actual_num >= expected_num
        elif operator == '<=':
            return actual_num <= expected_num
        elif operator == '>':
            return actual_num > expected_num
        elif operator == '<':
            return actual_num < expected_num
        elif operator == '==':
            return actual_num == expected_num
        elif operator == '!=':
            return actual_num != expected_num
    except (TypeError, ValueError):
        pass

    # 字符串比较
    actual_str = str(actual) if actual is not None else ""
    expected_str = str(expected)
    if operator == '==':
        return actual_str == expected_str
    elif operator == '!=':
        return actual_str != expected_str
    return False


def check_condition_for_user(user_id: str, condition_str: str) -> bool:
    """
    检查用户是否满足指定条件
    condition_str: 如 "修仙签到>=100;历练次数>=50"
    """
    from ..xiuxian_utils.utils import get_statistics_data
    from ..xiuxian_utils.xiuxian2_handle import sql_message

    conditions = parse_condition(condition_str)
    if not conditions:
        return False

    user_info = sql_message.get_user_info_with_id(user_id)

    for key, op, value in conditions:
        # 1. 先尝试从统计数据中查找
        stats_val = get_statistics_data(user_id, key)
        if stats_val is not None:
            if not _compare(stats_val, op, value):
                return False
            continue

        # 2. 境界比较增强
        if key == '境界':
            if not user_info or not user_info.get("level"):
                return False

            user_level = user_info["level"]
            target_level = value.strip()

            # 支持“化神境”这种写法，自动补圆满作门槛
            all_levels = convert_rank("江湖好手")[1]
            if target_level not in all_levels:
                if not target_level.endswith(("初期", "中期", "圆满")):
                    target_level = target_level + "圆满"

            user_rank = convert_rank(user_level)[0]
            target_rank = convert_rank(target_level)[0]

            if user_rank is None or target_rank is None:
                logger.warning(f"境界条件无法识别: user={user_level}, target={value}")
                return False

            if not _compare(user_rank, op, target_rank):
                return False
            continue

        # 3. 尝试从 user_info 字段中查找
        if user_info:
            field_val = user_info.get(key)
            if field_val is not None:
                if not _compare(field_val, op, value):
                    return False
                continue

        # 未知条件键，条件不满足
        logger.warning(f"称号条件未知键: {key}")
        return False

    return True


def check_and_unlock_titles(user_id: str) -> List[dict]:
    """
    检查用户是否有新的可解锁称号
    返回: 新解锁的称号列表
    """
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    newly_unlocked = []

    # 获取已解锁称号
    unlocked_str = player_data_manager.get_field_data(str(user_id), "title", "unlocked")
    unlocked_ids = set()
    if unlocked_str:
        if isinstance(unlocked_str, str):
            try:
                unlocked_ids = set(json.loads(unlocked_str))
            except:
                unlocked_ids = set()
        elif isinstance(unlocked_str, list):
            unlocked_ids = set(unlocked_str)

    # 检查所有称号
    all_titles = load_title_data()
    for title_id, title_data in all_titles.items():
        if str(title_id) in unlocked_ids:
            continue
        condition = title_data.get("condition", "")
        if not condition:
            continue
        if check_condition_for_user(user_id, condition):
            newly_unlocked.append(title_data)
            unlocked_ids.add(str(title_id))

    # 保存更新后的已解锁列表
    if newly_unlocked:
        player_data_manager.update_or_write_data(
            str(user_id), "title", "unlocked",
            json.dumps(list(unlocked_ids), ensure_ascii=False)
        )

    return newly_unlocked


def get_user_unlocked_titles(user_id: str) -> List[str]:
    """获取用户已解锁的称号ID列表"""
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    unlocked_str = player_data_manager.get_field_data(str(user_id), "title", "unlocked")
    if not unlocked_str:
        return []
    if isinstance(unlocked_str, str):
        try:
            return json.loads(unlocked_str)
        except:
            return []
    elif isinstance(unlocked_str, list):
        return unlocked_str
    return []


def get_user_equipped_title(user_id: str) -> Optional[str]:
    """获取用户当前装备的称号ID"""
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    equipped = player_data_manager.get_field_data(str(user_id), "title", "equipped")
    return str(equipped) if equipped else None


def grant_title_to_user(user_id: str, title_id: str) -> Tuple[bool, str]:
    """给用户增加称号（不自动装备）"""
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    title_data = get_title_by_id(str(title_id))
    if not title_data:
        return False, "称号ID不存在"

    unlocked = get_user_unlocked_titles(user_id)
    unlocked_set = set(str(x) for x in unlocked)

    if str(title_id) in unlocked_set:
        return False, f"用户已拥有称号【{title_data['name']}】"

    unlocked_set.add(str(title_id))
    player_data_manager.update_or_write_data(
        str(user_id), "title", "unlocked",
        json.dumps(list(unlocked_set), ensure_ascii=False)
    )
    return True, f"已赠送称号【{title_data['name']}】"


def equip_title(user_id: str, title_id: str) -> Tuple[bool, str]:
    """装备称号"""
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    # 检查称号是否存在
    title_data = get_title_by_id(title_id)
    if not title_data:
        # 尝试通过名称查找
        all_titles = load_title_data()
        for tid, tdata in all_titles.items():
            if tdata['name'] == title_id:
                title_id = tid
                title_data = tdata
                break
        if not title_data:
            return False, "称号不存在！"

    # 检查是否已解锁
    unlocked = get_user_unlocked_titles(user_id)
    if str(title_id) not in unlocked:
        return False, "你还未解锁该称号！"

    # 装备
    player_data_manager.update_or_write_data(
        str(user_id), "title", "equipped", str(title_id)
    )
    return True, f"成功装备称号【{title_data['name']}】！"


def unequip_title(user_id: str) -> Tuple[bool, str]:
    """卸下称号"""
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    equipped = get_user_equipped_title(user_id)
    if not equipped:
        return False, "你当前没有装备任何称号！"

    title_data = get_title_by_id(equipped)
    name = title_data['name'] if title_data else equipped

    player_data_manager.update_or_write_data(
        str(user_id), "title", "equipped", ""
    )
    return True, f"成功卸下称号【{name}】！"


def get_equipped_title_display(user_id: str) -> str:
    """
    获取用户当前装备称号的显示名称
    如果没有装备称号，返回空字符串
    """
    equipped_id = get_user_equipped_title(user_id)
    if not equipped_id:
        return ""
    title_data = get_title_by_id(equipped_id)
    if title_data:
        return title_data['name']
    return ""


def get_equipped_title_id(user_id: str) -> Optional[str]:
    """获取用户当前装备称号的ID"""
    return get_user_equipped_title(user_id)


def get_title_info_text(title_id: str) -> str:
    """获取称号详细信息文本"""
    title_data = get_title_by_id(title_id)
    if not title_data:
        return "未知称号"
    return f"称号：{title_data['name']}\n描述：{title_data['desc']}\n获取条件：{title_data['condition']}"


def refresh_title_cache():
    """刷新称号数据缓存"""
    global _TITLE_CACHE
    _TITLE_CACHE.clear()
    load_title_data()
    logger.info("称号数据缓存已刷新")