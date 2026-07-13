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
from typing import Optional, List, Dict, Tuple, Set
from nonebot.log import logger
from ...paths import get_paths

from ..xiuxian_config import XiuConfig, convert_rank

READPATH = get_paths().data
TITLE_JSONPATH = READPATH / "修炼物品" / "称号.json"

# 全局缓存
_TITLE_CACHE: Dict[str, dict] = {}
_TITLE_CONDITION_KEY_CACHE: Set[str] = set()
_UNKNOWN_CONDITION_KEYS_LOGGED: Set[str] = set()


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


def get_title_condition_keys() -> Set[str]:
    """获取称号配置中声明过的所有条件键。"""
    global _TITLE_CONDITION_KEY_CACHE
    if _TITLE_CONDITION_KEY_CACHE:
        return _TITLE_CONDITION_KEY_CACHE

    keys = set()
    for title_data in load_title_data().values():
        condition = str(title_data.get("condition", "")).strip()
        for key, _, _ in parse_condition(condition):
            if key:
                keys.add(key)
    _TITLE_CONDITION_KEY_CACHE = keys
    return _TITLE_CONDITION_KEY_CACHE


def _log_unknown_condition_key_once(key: str) -> None:
    if key in _UNKNOWN_CONDITION_KEYS_LOGGED:
        return
    _UNKNOWN_CONDITION_KEYS_LOGGED.add(key)
    logger.debug(f"称号条件未知键: {key}")


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
    from ..xiuxian_utils.xiuxian2_handle import sql_message, player_data_manager

    conditions = parse_condition(condition_str)
    if not conditions:
        return False

    user_info = sql_message.get_user_info_with_id(user_id)

    for key, op, value in conditions:
        # 1. 先尝试从统计数据中查找
        stats_val = get_statistics_data(user_id, key)
        if stats_val not in (None, {}):
            if not _compare(stats_val, op, value):
                return False
            continue

        # 2. 兼容已有玩法表中的进度数据
        if key in {"通天塔最高层", "通天塔积分"}:
            field = "max_floor" if key == "通天塔最高层" else "score"
            tower_val = player_data_manager.get_field_data(str(user_id), "tower", field)
            if tower_val is not None:
                if not _compare(tower_val, op, value):
                    return False
                continue

        # 3. 境界比较增强
        if key == '境界':
            if not user_info or not user_info.get("level"):
                return False

            user_level = user_info["level"]
            target_level = _normalize_realm_target(value)
            all_levels = convert_rank("江湖好手")[1]
            if user_level not in all_levels or target_level not in all_levels:
                logger.warning(f"境界条件无法识别: user={user_level}, target={value}")
                return False

            if not _compare_realm(user_level, op, target_level):
                return False
            continue

        # 4. 尝试从 user_info 字段中查找
        if user_info:
            field_val = user_info.get(key)
            if field_val is not None:
                if not _compare(field_val, op, value):
                    return False
                continue

        # 称号配置里的普通计数器缺失时按 0 处理，避免每次刷新成就刷屏 warning。
        if key in get_title_condition_keys():
            if not _compare(0, op, value):
                return False
            continue

        # 真正未声明过的条件键只记录一次 debug 日志，避免污染正常运行日志。
        _log_unknown_condition_key_once(key)
        return False

    return True


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_progress_number(value) -> str:
    number = _safe_float(value)
    if number is None:
        return str(value) if value not in (None, "") else "0"
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def _normalize_realm_target(value: str) -> str:
    target_level = str(value).strip()
    all_levels = convert_rank("江湖好手")[1]
    if target_level not in all_levels and not target_level.endswith(("初期", "中期", "圆满")):
        target_level = target_level + "圆满"
    return target_level


def _compare_realm(user_level: str, operator: str, target_level: str) -> bool:
    all_levels = convert_rank("江湖好手")[1]
    if user_level not in all_levels or target_level not in all_levels:
        return False
    # convert_rank 的数值越大境界越低；列表索引越大境界越高，更适合直观比较。
    return _compare(all_levels.index(user_level), operator, all_levels.index(target_level))


def _get_condition_actual_value(user_id: str, key: str, user_info: Optional[dict]):
    from ..xiuxian_utils.utils import get_statistics_data
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    if key == "境界":
        return user_info.get("level") if user_info else None

    stats_val = get_statistics_data(user_id, key)
    if stats_val not in (None, {}):
        return stats_val

    if key in {"通天塔最高层", "通天塔积分"}:
        field = "max_floor" if key == "通天塔最高层" else "score"
        value = player_data_manager.get_field_data(str(user_id), "tower", field)
        if value is not None:
            return value

    if user_info:
        return user_info.get(key)
    return None


def _infer_achievement_category(condition_str: str) -> str:
    rules = (
        ("境界突破", ("境界", "突破", "渡劫", "心魔劫")),
        ("战斗挑战", ("讨伐世界BOSS", "通天塔", "切磋", "秘境打怪")),
        ("生活修行", ("炼丹", "鉴石", "闭关", "修仙签到", "灵石修炼", "虚神界")),
        ("探索任务", ("历练", "秘境次数", "悬赏", "宗门任务", "寻心")),
        ("社交传承", ("双修", "拜师", "收徒", "传功", "传承")),
        ("资源交互", ("送灵石", "偷灵石", "抢灵石")),
    )
    for category, keys in rules:
        if any(key in condition_str for key in keys):
            return category
    return "综合成就"


def get_condition_progress_for_user(user_id: str, condition_str: str) -> List[dict]:
    """返回条件进度，用于成就列表展示。"""
    from ..xiuxian_utils.xiuxian2_handle import sql_message

    user_info = sql_message.get_user_info_with_id(user_id)
    progress = []

    for key, op, expected in parse_condition(condition_str):
        actual = _get_condition_actual_value(user_id, key, user_info)

        if key == "境界":
            if not actual:
                satisfied = False
                ratio = 0
            else:
                target_level = _normalize_realm_target(expected)
                all_levels = convert_rank("江湖好手")[1]
                if actual not in all_levels or target_level not in all_levels:
                    satisfied = False
                    ratio = 0
                else:
                    actual_index = all_levels.index(actual)
                    target_index = all_levels.index(target_level)
                    satisfied = _compare(actual_index, op, target_index)
                    if satisfied:
                        ratio = 1
                    elif op in (">=", ">") and target_index > 0:
                        ratio = max(min(actual_index / target_index, 1), 0)
                    elif op in ("<=", "<") and actual_index > 0:
                        ratio = max(min(target_index / actual_index, 1), 0)
                    else:
                        ratio = 0

            progress.append({
                "key": key,
                "operator": op,
                "actual": str(actual or "无"),
                "expected": str(expected),
                "display": f"{actual or '无'} / {expected}",
                "ratio": ratio,
                "satisfied": satisfied,
            })
            continue

        actual_value = actual if actual not in (None, "") else 0
        satisfied = _compare(actual_value, op, expected)
        actual_num = _safe_float(actual_value)
        expected_num = _safe_float(expected)

        if actual_num is not None and expected_num not in (None, 0):
            if op in (">=", ">"):
                ratio = max(min(actual_num / expected_num, 1), 0)
            elif op in ("<=", "<"):
                ratio = 1 if satisfied else max(min(expected_num / max(actual_num, 1), 1), 0)
            else:
                ratio = 1 if satisfied else 0
        else:
            ratio = 1 if satisfied else 0

        progress.append({
            "key": key,
            "operator": op,
            "actual": _format_progress_number(actual_value),
            "expected": _format_progress_number(expected),
            "display": f"{_format_progress_number(actual_value)} / {_format_progress_number(expected)}",
            "ratio": ratio,
            "satisfied": satisfied,
        })

    return progress


def get_title_achievement_records(user_id: str) -> List[dict]:
    """将有条件的称号视为成就，返回用户解锁和进度状态。"""
    unlocked_ids = set(str(tid) for tid in get_user_unlocked_titles(user_id))
    records = []

    def sort_key(item):
        title_id = str(item[0])
        return int(title_id) if title_id.isdigit() else title_id

    for title_id, title_data in sorted(load_title_data().items(), key=sort_key):
        condition = str(title_data.get("condition", "")).strip()
        if not condition:
            continue

        progress = get_condition_progress_for_user(user_id, condition)
        satisfied = bool(progress) and all(item["satisfied"] for item in progress)
        unlocked = str(title_id) in unlocked_ids
        if unlocked or satisfied:
            ratio = 1
        elif progress:
            ratio = min(item["ratio"] for item in progress)
        else:
            ratio = 0

        records.append({
            "id": str(title_id),
            "name": title_data.get("name", str(title_id)),
            "desc": title_data.get("desc", ""),
            "condition": condition,
            "category": _infer_achievement_category(condition),
            "progress": progress,
            "ratio": ratio,
            "satisfied": satisfied,
            "unlocked": unlocked,
        })

    return records


def format_new_title_message(newly_unlocked: List[dict]) -> str:
    if not newly_unlocked:
        return ""
    lines = ["", "【新称号解锁】"]
    for title in newly_unlocked:
        lines.append(f"【{title.get('name', '未知称号')}】{title.get('desc', '')}")
    return "\n".join(lines)


def find_unlockable_titles(user_id: str) -> List[dict]:
    """
    检查用户是否有新的可解锁称号
    返回: 新解锁的称号列表
    """
    newly_unlocked = []

    # 获取已解锁称号
    unlocked_str = player_data_manager.get_field_data(str(user_id), "title", "unlocked")
    unlocked_ids = set()
    if unlocked_str:
        if isinstance(unlocked_str, str):
            try:
                unlocked_ids = set(json.loads(unlocked_str))
            except Exception:
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

    return newly_unlocked


def check_and_unlock_titles(user_id: str) -> List[dict]:
    """Compatibility wrapper for non-command callers."""
    from .title_transaction_service import TitleTransactionService

    expected = get_user_unlocked_titles(user_id)
    unlockable = find_unlockable_titles(user_id)
    if not unlockable:
        return []
    title_ids = [str(title["id"]) for title in unlockable]
    operation_id = "title-auto-unlock:" + str(user_id) + ":" + ",".join(sorted(title_ids))
    result = TitleTransactionService(get_paths().player_db).unlock_batch(
        operation_id, user_id, expected, title_ids
    )
    return unlockable if result.succeeded else []


def get_user_unlocked_titles(user_id: str) -> List[str]:
    """获取用户已解锁的称号ID列表"""
    from ..xiuxian_utils.xiuxian2_handle import player_data_manager

    unlocked_str = player_data_manager.get_field_data(str(user_id), "title", "unlocked")
    if not unlocked_str:
        return []
    if isinstance(unlocked_str, str):
        try:
            return json.loads(unlocked_str)
        except Exception:
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
    global _TITLE_CACHE, _TITLE_CONDITION_KEY_CACHE
    _TITLE_CACHE.clear()
    _TITLE_CONDITION_KEY_CACHE.clear()
    load_title_data()
    logger.info("称号数据缓存已刷新")
