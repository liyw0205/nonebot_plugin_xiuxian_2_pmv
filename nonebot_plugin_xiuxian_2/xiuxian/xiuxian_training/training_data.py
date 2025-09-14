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
        "1": {
            "id": 1999,
            "cost": 1000,
            "weekly_limit": 10
        },
        "2": {
            "id": 4003,
            "cost": 5000,
            "weekly_limit": 1
        },
        "3": {
            "id": 4002,
            "cost": 25000,
            "weekly_limit": 1
        },
        "4": {
            "id": 4001,
            "cost": 100000,
            "weekly_limit": 1
        },
        "5": {
            "id": 2500,
            "cost": 5000,
            "weekly_limit": 1
        },
        "6": {
            "id": 2501,
            "cost": 10000,
            "weekly_limit": 1
        },
        "7": {
            "id": 2502,
            "cost": 20000,
            "weekly_limit": 1
        },
        "8": {
            "id": 2503,
            "cost": 40000,
            "weekly_limit": 1
        },
        "9": {
            "id": 2504,
            "cost": 80000,
            "weekly_limit": 1
        },
        "10": {
            "id": 2505,
            "cost": 100000,
            "weekly_limit": 1
        },
        "11": {
            "id": 7085,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "12": {
            "id": 8931,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "13": {
            "id": 9937,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "14": {
            "id": 10402,
            "cost": 700000,
            "weekly_limit": 1
        },
        "15": {
            "id": 10403,
            "cost": 1000000,
            "weekly_limit": 1
        },
        "16": {
            "id": 10411,
            "cost": 1200000,
            "weekly_limit": 1
        },
        "17": {
            "id": 20004,
            "cost": 10000,
            "weekly_limit": 10
        },
        "18": {
            "id": 20003,
            "cost": 50000,
            "weekly_limit": 3
        },
        "19": {
            "id": 20002,
            "cost": 200000,
            "weekly_limit": 1
        },
        "20": {
            "id": 20005,
            "cost": 1000,
            "weekly_limit": 10
        },
        "21": {
            "id": 15357,
            "cost": 100000,
            "weekly_limit": 1
        },
        "22": {
            "id": 9935,
            "cost": 100000,
            "weekly_limit": 1
        },
        "23": {
            "id": 9940,
            "cost": 100000,
            "weekly_limit": 1
        },
        "24": {
            "id": 10405,
            "cost": 50000,
            "weekly_limit": 1
        },
        "25": {
            "id": 10410,
            "cost": 1000000,
            "weekly_limit": 1
        },
        "26": {
            "id": 10412,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "27": {
            "id": 8933,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "28": {
            "id": 8934,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "29": {
            "id": 8935,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "30": {
            "id": 8936,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "31": {
            "id": 20011,
            "cost": 10000,
            "weekly_limit": 1
        },
        "32": {
            "id": 20006,
            "cost": 5000,
            "weekly_limit": 3
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
    
    def make_choice(self, user_id, choice_type):
        """进行历练选择"""
        training_info = self.get_user_training_info(user_id)
        user_info = sql_message.get_user_info_with_id(user_id)
        now = datetime.now()
        
        # 记录本次历练时间
        training_info["last_time"] = now
        
        # 根据选择类型确定事件权重
        if choice_type == 1:  # 前进
            weights = {
                "progress_plus_1": 30,
                "progress_plus_2": 30,
                "nothing": 20,
                "progress_minus_1": 10,
                "progress_minus_2": 10
            }
        elif choice_type == 2:  # 后退
            weights = {
                "progress_plus_1": 40,
                "progress_plus_2": 0,
                "nothing": 40,
                "progress_minus_1": 20,
                "progress_minus_2": 0
            }
        else:  # 休息
            weights = {
                "progress_plus_1": 30,
                "progress_plus_2": 30,
                "nothing": 20,
                "progress_minus_1": 10,
                "progress_minus_2": 10
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
            return 0
        
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data.get(str(item_id), 0)
            except:
                return 0
    
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
        
        current = data.get(str(item_id), 0)
        data[str(item_id)] = current + quantity
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    def reset_weekly_limits(self):
        """重置每周购买限制(由定时任务调用)"""
        # 每周一0点重置
        for file in PLAYERSDATA.glob("*/training_purchases.json"):
            try:
                file.unlink()
            except:
                pass

training_data = TrainingData()
