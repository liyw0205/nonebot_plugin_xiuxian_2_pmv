from .xiuxian2_handle import XiuxianDateManage


_sql_message_instance = None


def _sql_message():
    global _sql_message_instance
    if _sql_message_instance is None:
        _sql_message_instance = XiuxianDateManage()
    return _sql_message_instance


def get_user_sect_fairyland_level(user_info: dict, sql_message=None) -> int:
    sect_id = (user_info or {}).get("sect_id")
    if not sect_id:
        return 0
    manager = sql_message or _sql_message()
    sect_info = manager.get_sect_info(sect_id)
    if not sect_info:
        return 0
    try:
        return int(sect_info.get("sect_fairyland", 0) or 0)
    except Exception:
        return 0
