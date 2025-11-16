import tomllib
import json
import os
from pathlib import Path
from datetime import datetime
from ..xiuxian_utils.data_source import JsonData

WORKDATA = Path() / "data" / "xiuxian" / "work"
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

class reward(JsonData):
    def __init__(self):
        super().__init__()
        self.files = {
            "ansa": WORKDATA / "暗杀.toml",
            "levelprice": WORKDATA / "等级奖励稿.toml",
            "yaocai": WORKDATA / "灵材.toml",
            "zuoyao": WORKDATA / "镇妖.toml"
        }

    def _load(self, key):
        with open(self.files[key].with_suffix('.toml'), 'rb') as f:
            return tomllib.load(f)

    def reward_ansa_data(self):
        return self._load("ansa")

    def reward_levelprice_data(self):
        return self._load("levelprice")

    def reward_yaocai_data(self):
        return self._load("yaocai")

    def reward_zuoyao_data(self):
        return self._load("zuoyao")

def savef(user_id, data):
    """保存悬赏令信息到JSON"""
    user_id = str(user_id)
    if not os.path.exists(PLAYERSDATA / user_id):
        os.makedirs(PLAYERSDATA / user_id)
    
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
    save_data = {
        "tasks": data["tasks"],
        "status": data.get("status", 1),  # 默认1-未接取
        "refresh_time": data.get("refresh_time", datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')),
        "user_level": data.get("user_level")
    }
    with open(FILEPATH, "w", encoding="UTF-8") as f:
        json.dump(save_data, f, ensure_ascii=False, indent=4)

def readf(user_id):
    """从JSON加载悬赏令信息"""
    user_id = str(user_id)
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
    if not os.path.exists(FILEPATH):
        return None
    
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = json.load(f)
    return data

def delete_work_file(user_id):
    """删除悬赏令信息文件"""
    user_id = str(user_id)
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
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
