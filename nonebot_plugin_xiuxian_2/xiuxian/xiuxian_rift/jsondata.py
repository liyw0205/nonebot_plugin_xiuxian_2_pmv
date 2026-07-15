try:
    import ujson as json
except ImportError:
    import json
import os
from pathlib import Path
from nonebot.log import logger
from ...paths import get_paths
from ..xiuxian_utils.json_store import save_json_file
from .transaction_service import RiftEntryService

SKILLPATH = get_paths().data / "功法" / "功法概率设置.json"
PLAYERSDATA = get_paths().players
_rift_entry_reader = RiftEntryService(get_paths().game_db)


def read_f():
    with open(SKILLPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def read_rift_data(user_id):
    user_id = str(user_id)
    database_state = _rift_entry_reader.read_entry(user_id, active_only=True)
    if database_state is not None:
        return database_state
    FILEPATH = PLAYERSDATA / user_id / "riftinfo.json"
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def save_rift_data(user_id, data):
    user_id = str(user_id)
    if not os.path.exists(PLAYERSDATA / user_id):
        logger.opt(colors=True).info("<red>目录不存在，创建目录</red>")
        os.makedirs(PLAYERSDATA / user_id)
    FILEPATH = PLAYERSDATA / user_id / "riftinfo.json"
    save_json_file(FILEPATH, data)
    return True
