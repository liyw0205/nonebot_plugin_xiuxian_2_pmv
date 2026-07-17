import json
import os
from datetime import datetime
from contextlib import closing
from nonebot.log import logger

from ...paths import get_paths

from ..xiuxian_utils.data_source import JsonDate
from ..xiuxian_utils.json_store import load_json_file, save_json_file
from ..xiuxian_utils import db_backend

WORKDATA = get_paths().work
PLAYERSDATA = get_paths().players

class reward(JsonDate):
    def __init__(self):
        super().__init__()
        self.Reward_ansa_jsonpath = WORKDATA / "暗杀.json"
        self.Reward_levelprice_jsonpath = WORKDATA / "等级奖励稿.json"
        self.Reward_yaocai_jsonpath = WORKDATA / "灵材.json"
        self.Reward_zuoyao_jsonpath = WORKDATA / "镇妖.json"

    def reward_ansa_data(self):
        """获取暗杀名单信息"""
        with open(self.Reward_ansa_jsonpath, 'r', encoding='utf-8') as e:
            file_data = e.read()
            data = json.loads(file_data)
            return data

    def reward_levelprice_data(self):
        """获取等级奖励信息"""
        with open(self.Reward_levelprice_jsonpath, 'r', encoding='utf-8') as e:
            file_data = e.read()
            data = json.loads(file_data)
            return data

    def reward_yaocai_data(self):
        """获取药材信息"""
        with open(self.Reward_yaocai_jsonpath, 'r', encoding='utf-8') as e:
            file_data = e.read()
            data = json.loads(file_data)
            return data

    def reward_zuoyao_data(self):
        """获取捉妖信息"""
        with open(self.Reward_zuoyao_jsonpath, 'r', encoding='utf-8') as e:
            file_data = e.read()
            data = json.loads(file_data)
            return data

def savef(user_id, data, sync_snapshot=True):
    """保存兼容 JSON 投影，并同步数据库悬赏快照。"""
    user_id = str(user_id)
    if not os.path.exists(PLAYERSDATA / user_id):
        os.makedirs(PLAYERSDATA / user_id)
    
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
    # 保留 task_order，保证「悬赏编号」与接取一致（避免 sort_keys 打乱 dict 顺序）
    task_order = data.get("task_order")
    if not task_order:
        task_order = list((data.get("tasks") or {}).keys())
    save_data = {
        "tasks": data["tasks"],
        "task_order": list(task_order),
        "status": data.get("status", 1),  # 默认1-未接取
        "refresh_time": data.get("refresh_time", datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')),
        "user_level": data.get("user_level")
    }
    save_json_file(FILEPATH, save_data)
    if not sync_snapshot:
        return
    with closing(db_backend.connect(get_paths().game_db)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
            "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
            "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
            (user_id, json.dumps(save_data, ensure_ascii=True, sort_keys=True), save_data["refresh_time"]),
        )
        conn.commit()

def readf(user_id):
    """优先读取数据库权威快照，并兼容迁移旧 JSON。"""
    user_id = str(user_id)
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
    with closing(db_backend.connect(get_paths().game_db)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
            "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        row = conn.execute(
            "SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", (user_id,)
        ).fetchone()
        if row is not None:
            return json.loads(str(row[0]))
        if not os.path.exists(FILEPATH):
            return None
        data = load_json_file(FILEPATH, {}, dict)
        if not data:
            return None
        conn.execute(
            "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s)",
            (user_id, json.dumps(data, ensure_ascii=True, sort_keys=True), str(data.get("refresh_time", ""))),
        )
        conn.commit()
        return data

def delete_work_file(user_id, delete_snapshot=True):
    """删除数据库悬赏快照及兼容 JSON。"""
    user_id = str(user_id)
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
    if delete_snapshot:
        with closing(db_backend.connect(get_paths().game_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
                "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
            )
            conn.execute("DELETE FROM work_offer_snapshots WHERE user_id=%s", (user_id,))
            conn.commit()
    if os.path.exists(FILEPATH):
        try:
            os.remove(FILEPATH)
            return True
        except Exception as e:
            logger.error(f"删除悬赏令文件失败: {e}")
            return False
    return False

def has_unaccepted_work(user_id, check_expired=True, expire_minutes=30):
    """
    检查用户是否有未接取的悬赏令
    :param user_id: 用户ID
    :param check_expired: 是否检查过期状态
    :param expire_minutes: 过期时间(分钟)
    :return: (has_work, work_data) 是否有未接取悬赏令和悬赏数据
    """
    work_data = readf(user_id)
    if not work_data:
        return False, None
    
    # 检查状态是否为未接取(1)
    if work_data.get("status") != 1:
        return False, work_data
    
    # 如果需要检查过期状态
    if check_expired:
        try:
            refresh_time = datetime.strptime(work_data["refresh_time"], "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                refresh_time = datetime.strptime(work_data["refresh_time"], "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                logger.error(f"解析悬赏令时间失败: {e}, 时间: {work_data['refresh_time']}")
                return False, work_data
        
        time_diff = datetime.now() - refresh_time
        if time_diff.total_seconds() > expire_minutes * 60:
            # 自动标记为过期
            work_data["status"] = 0
            savef(user_id, work_data)
            return False, work_data
    
    return True, work_data
