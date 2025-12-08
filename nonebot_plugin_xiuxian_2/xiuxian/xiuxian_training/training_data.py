import json
import random
import os
from pathlib import Path
from datetime import datetime, timedelta
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .training_events import training_events
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to

PLAYERSDATA = Path() / "data" / "xiuxian" / "players"
TRAINING_RANK_PATH = Path(__file__).parent / "training_rank.json"
TRAINING_CONFIG_PATH = Path(__file__).parent / "training_config.json"
sql_message = XiuxianDateManage()
items = Items()

DEFAULT_CONFIG = {
    "商店商品": {
        "1999": {
            "name": "渡厄丹",
            "cost": 1000,
            "weekly_limit": 10
        },
        "20004": {
            "name": "蕴灵石",
            "cost": 10000,
            "weekly_limit": 10
        },
        "20003": {
            "name": "神圣石",
            "cost": 50000,
            "weekly_limit": 3
        },
        "20002": {
            "name": "化道石",
            "cost": 200000,
            "weekly_limit": 1
        },
        "20005": {
            "name": "祈愿石",
            "cost": 2000,
            "weekly_limit": 10
        },
        "15357": {
            "name": "八九玄功",
            "cost": 100000,
            "weekly_limit": 1
        },
        "9935": {
            "name": "暗渊灭世功",
            "cost": 100000,
            "weekly_limit": 1
        },
        "9940": {
            "name": "化功大法",
            "cost": 100000,
            "weekly_limit": 1
        },
        "10405": {
            "name": "醉仙",
            "cost": 50000,
            "weekly_limit": 1
        },
        "20011": {
            "name": "易名符",
            "cost": 10000,
            "weekly_limit": 1
        },
        "20006": {
            "name": "福缘石",
            "cost": 5000,
            "weekly_limit": 1
        }
    }
}

class TrainingData:
    def __init__(self):
        self.config = self.get_training_config()
    
    def get_training_config(self):
        """加载历练配置"""
        try:
            if not TRAINING_CONFIG_PATH.exists():
                with open(TRAINING_CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
                return DEFAULT_CONFIG
            
            with open(TRAINING_CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            # 确保所有配置项都存在
            for key in DEFAULT_CONFIG:
                if key not in config:
                    config[key] = DEFAULT_CONFIG[key]
            
            return config
        except Exception as e:
            print(f"加载历练配置失败: {e}")
            return DEFAULT_CONFIG
    
    def get_user_training_info(self, user_id):
        """获取用户历练信息"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "training_info.json"
        
        default_data = {
            "progress": 0,        # 当前进度(0-12)
            "last_time": None,    # 上次历练时间
            "points": 0,          # 成就点
            "completed": 0,       # 累计完成次数
            "max_progress": 0,    # 历史最高进度
            "last_event": ""      # 最后经历的事件
        }
        
        if not file_path.exists():
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(default_data, f, ensure_ascii=False, indent=4)
            return default_data
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 确保所有字段都存在
        for key in default_data:
            if key not in data:
                data[key] = default_data[key]
        
        # 转换时间字符串为datetime对象
        if data["last_time"] and isinstance(data["last_time"], str):
            data["last_time"] = datetime.strptime(data["last_time"], "%Y-%m-%d %H:%M:%S")
        
        return data
    
    def save_user_training_info(self, user_id, data):
        """保存用户历练信息"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "training_info.json"
        
        # 转换datetime对象为字符串
        save_data = data.copy()
        if isinstance(save_data["last_time"], datetime):
            save_data["last_time"] = save_data["last_time"].strftime("%Y-%m-%d %H:%M:%S")
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(save_data, f, ensure_ascii=False, indent=4)
    
    def make_choice(self, user_id):
        """进行历练选择"""
        training_info = self.get_user_training_info(user_id)
        user_info = sql_message.get_user_info_with_id(user_id)
        now = datetime.now()
        
        # 记录本次历练时间
        training_info["last_time"] = now
        
        # weights = {  # 等价于原版
        #     "progress_plus_1": 33,
        #     "progress_plus_2": 20,
        #     "nothing": 27,
        #     "progress_minus_1": 13,
        #     "progress_minus_2": 7
        # }
        weights = {
            "progress_plus_1": 35,
            "progress_plus_2": 30,
            "nothing": 20,
            "progress_minus_1": 10,
            "progress_minus_2": 5,
        }
        # 随机选择事件
        event_type = random.choices(list(weights.keys()), weights=list(weights.values()))[0]
        
        # 调用事件处理器，传入用户信息
        event_result = training_events.handle_event(user_id, user_info, event_type)
        
        # 更新进度 - 默认+1
        base_progress = 1
        
        if "plus_1" in event_type:  # 小奖励: +1 (总+2)
            progress_change = base_progress + 1
        elif "plus_2" in event_type:  # 大奖励: +1 (总+2)
            progress_change = base_progress + 1
        elif "minus_1" in event_type:  # 小惩罚: -1 (总0)
            progress_change = base_progress - 1
        elif "minus_2" in event_type:  # 大惩罚: -2 (总-1)
            progress_change = base_progress - 2
        else:  # nothing: 0 (总+1)
            progress_change = base_progress
        
        training_info["progress"] = max(0, training_info["progress"] + progress_change)
        
        # 处理事件结果
        if isinstance(event_result, dict):
            # 更新成就点
            if event_result.get("type") == "points":
                training_info["points"] += event_result["amount"]
            
            # 记录最后事件
            training_info["last_event"] = event_result.get("message", "")
        else:
            training_info["last_event"] = str(event_result)
        
        # 检查是否完成一个进程
        if training_info["progress"] >= 12:
            training_info["progress"] = 0
            training_info["completed"] += 1
            training_info["max_progress"] = max(training_info["max_progress"], 12)
            
            # 完成奖励
            exp_reward = int(user_info["exp"] * 0.01)  # 1%修为
            stone_reward = random.randint(5000000, 10000000)  # 500万-1000万灵石
            points_reward = 1000  # 1000成就点
            
            sql_message.update_exp(user_id, exp_reward)
            sql_message.update_ls(user_id, stone_reward, 1)
            training_info["points"] += points_reward
            
            # 添加随机物品奖励
            user_rank = convert_rank(user_info["level"])[0]
            min_rank = max(user_rank - 16, 16)
            item_rank = random.randint(min_rank, min_rank + 20)
            item_types = ["功法", "神通", "药材"]
            item_type = random.choice(item_types)
            item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
            
            if item_id_list:
                item_id = random.choice(item_id_list)
                item_info = items.get_data_by_item_id(item_id)
                sql_message.send_back(user_id, item_id, item_info["name"], item_info["type"], 1)
                item_reward_msg = f"\n随机物品：{item_info['level']}:{item_info['name']}"
            else:
                item_reward_msg = ""
            
            training_info["last_event"] += (
                f"\n恭喜道友完成一个历练进程！获得：\n"
                f"修为+{number_to(exp_reward)}\n"
                f"灵石+{number_to(stone_reward)}\n"
                f"成就点+{points_reward}{item_reward_msg}"
            )
        
        # 更新最高进度
        training_info["max_progress"] = max(training_info["max_progress"], training_info["progress"])
        
        self.save_user_training_info(user_id, training_info)
        
        # 更新排行榜
        self.update_training_rank(user_id, user_info["user_name"], training_info["completed"], training_info["max_progress"])
        
        return True, training_info["last_event"]
    
    def update_training_rank(self, user_id, user_name, completed, max_progress):
        """更新历练排行榜"""
        rank_data = self._load_rank_data()
    
        # 更新或添加用户记录
        rank_data[str(user_id)] = {
            "name": user_name,
            "completed": completed,
            "max_progress": max_progress,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
        # 保存排行榜
        with open(TRAINING_RANK_PATH, "w", encoding="utf-8") as f:
            json.dump(rank_data, f, ensure_ascii=False, indent=4)

    def get_training_rank(self, limit=50):
        """获取历练排行榜"""
        rank_data = self._load_rank_data()
    
        # 按完成次数降序排序
        sorted_rank = sorted(
            rank_data.items(),
            key=lambda x: x[1]["completed"],
            reverse=True
        )[:limit]
    
        return sorted_rank
    
    def _load_rank_data(self):
        """加载排行榜数据"""
        if not TRAINING_RANK_PATH.exists():
            return {}
        
        with open(TRAINING_RANK_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except:
                return {}
    
    def get_weekly_purchases(self, user_id, item_id):
        """获取用户本周已购买某商品的数量"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "training_purchases.json"
        
        if not file_path.exists():
            # 初始化文件并设置重置日期
            self._init_purchase_file(user_id)
            return 0
        
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                # 检查是否需要重置
                if "_last_reset" in data:
                    last_reset = datetime.strptime(data["_last_reset"], "%Y-%m-%d")
                    current_week = datetime.now().isocalendar()[1]
                    last_week = last_reset.isocalendar()[1]
                    current_year = datetime.now().year
                    last_year = last_reset.year
                    
                    if current_week != last_week or current_year != last_year:
                        # 重置购买记录
                        self._init_purchase_file(user_id)
                        return 0
                else:
                    # 没有重置日期，初始化
                    self._init_purchase_file(user_id)
                    return 0
                    
                return data.get(str(item_id), 0)
            except:
                # 文件损坏，重新初始化
                self._init_purchase_file(user_id)
                return 0

    def _init_purchase_file(self, user_id):
        """初始化购买记录文件"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "training_purchases.json"
        
        data = {
            "_last_reset": datetime.now().strftime("%Y-%m-%d")
        }
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def update_weekly_purchase(self, user_id, item_id, quantity):
        """更新用户本周购买某商品的数量"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "training_purchases.json"
        
        data = {}
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except:
                    pass
        
        # 确保有重置日期
        if "_last_reset" not in data:
            data["_last_reset"] = datetime.now().strftime("%Y-%m-%d")
        
        current = data.get(str(item_id), 0)
        data[str(item_id)] = current + quantity
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

training_data = TrainingData()
