import json
from datetime import datetime

from ..xiuxian_config import convert_rank


def _is_none_like(value):
    """
    兼容历史脏数据：
    None / "" / "None" / "null" / "NULL" 都视为无值。
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in ["", "none", "null"]:
        return True
    return False


def safe_int(value, default=0):
    try:
        if _is_none_like(value):
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


def _normalize_id_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if not _is_none_like(v)]
    if isinstance(value, str):
        if _is_none_like(value):
            return []
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v) for v in parsed if not _is_none_like(v)]
        except (json.JSONDecodeError, TypeError):
            return [v.strip() for v in value.split(",") if v.strip()]
    return []


def _normalize_dict(value):
    if value is None or _is_none_like(value):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _normalize_history(value):
    if value is None or _is_none_like(value):
        return []
    if isinstance(value, list):
        return [record for record in value if isinstance(record, dict)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [record for record in parsed if isinstance(record, dict)]
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _parse_datetime(value):
    if _is_none_like(value):
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _format_seconds(seconds):
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}天")
    if hours:
        parts.append(f"{hours}小时")
    if minutes or not parts:
        parts.append(f"{minutes}分钟")
    return "".join(parts)


def _rank_value(level_name):
    rank, _ = convert_rank(level_name)
    return rank


def is_wujie_or_above(level_name):
    rank = _rank_value(level_name)
    wujie_rank = _rank_value("无界境初期")
    return rank is not None and wujie_rank is not None and rank <= wujie_rank


def get_mentor_required_gap(mentor_level):
    return 3 if is_wujie_or_above(mentor_level) else 6


def get_realm_gap(mentor_level, apprentice_level):
    mentor_rank = _rank_value(mentor_level)
    apprentice_rank = _rank_value(apprentice_level)
    if mentor_rank is None or apprentice_rank is None:
        return 0
    return apprentice_rank - mentor_rank


def _config_rate(value, default):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


__all__ = [
    "_is_none_like",
    "safe_int",
    "_normalize_id_list",
    "_normalize_dict",
    "_normalize_history",
    "_parse_datetime",
    "_format_seconds",
    "_rank_value",
    "is_wujie_or_above",
    "get_mentor_required_gap",
    "get_realm_gap",
    "_config_rate",
]
