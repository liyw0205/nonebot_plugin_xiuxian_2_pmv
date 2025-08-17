import json
import os
from pathlib import Path
from datetime import datetime
from ..xiuxian_utils.data_source import JsonDate

WORKDATA = Path() / "data" / "xiuxian" / "work"
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

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

def savef(user_id, data):
    """保存悬赏令信息到JSON"""
    user_id = str(user_id)
    if not os.path.exists(PLAYERSDATA / user_id):
        os.makedirs(PLAYERSDATA / user_id)
    
    FILEPATH = PLAYERSDATA / user_id / "workinfo.json"
    save_data = {
        "tasks": data["tasks"],
        "status": data.get("status", 1),  # 默认1-未接取
        "refresh_time": data.get("refresh_time", datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
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
