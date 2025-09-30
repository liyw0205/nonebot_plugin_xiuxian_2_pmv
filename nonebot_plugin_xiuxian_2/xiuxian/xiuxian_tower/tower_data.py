import json
import os
from pathlib import Path
from datetime import datetime
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage

PLAYERSDATA = Path() / "data" / "xiuxian" / "players"
TOWER_RANK_PATH = Path(__file__).parent / "tower_rank.json"
TOWER_CONFIG_PATH = Path(__file__).parent / "tower_config.json"
sql_message = XiuxianDateManage()

DEFAULT_CONFIG = {
    "体力消耗": {
        "单层爬塔": 5,
        "连续爬塔": 20
    },
    "积分奖励": {
        "每层基础": 100,
        "每10层额外": 500
    },
    "灵石奖励": {
        "每层基础": 1000000,
        "每10层额外": 5000000
    },
    "修为奖励": {
        "每10层": 0.001
    },
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
    },
    "重置时间": {
        "day_of_week": "mon",  # 每周一
        "hour": 0,
        "minute": 0
    }
}

class TowerData:
    def __init__(self):
        self.config = self.get_tower_config()
    
    def get_tower_config(self):
        """加载通天塔配置"""
        try:
            if not TOWER_CONFIG_PATH.exists():
                with open(TOWER_CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
                return DEFAULT_CONFIG
            
            with open(TOWER_CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            # 确保所有配置项都存在
            for key in DEFAULT_CONFIG:
                if key not in config:
                    config[key] = DEFAULT_CONFIG[key]
            
            return config
        except Exception as e:
            print(f"加载通天塔配置失败: {e}")
            return DEFAULT_CONFIG
    
    def _check_reset(self, last_reset_str):
        """检查是否需要重置(每周一)"""
        if not last_reset_str:
            return True
            
        last_reset = datetime.strptime(last_reset_str, "%Y-%m-%d %H:%M:%S")
        now = datetime.now()
        
        # 检查是否是新的周(周一0点后)
        return (now.isocalendar()[1] > last_reset.isocalendar()[1] or  # 周数不同
                now.year > last_reset.year)  # 或跨年

    def reset_all_floors(self):
        """重置所有用户的通天塔层数"""
        reset_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for user_file in PLAYERSDATA.glob("*/tower_info.json"):
            with open(user_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # 保留历史最高层数，只重置当前层数
            data["current_floor"] = 0
            data["last_reset"] = reset_time  # 使用统一的重置时间
            
            with open(user_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    def get_user_tower_info(self, user_id):
        """获取用户通天塔信息"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "tower_info.json"
        
        default_data = {
            "current_floor": 0,  # 当前层数
            "max_floor": 0,      # 历史最高层数
            "score": 0,          # 总积分
            "last_reset": None   # 上次重置时间
        }
        
        if not file_path.exists():
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(default_data, f, ensure_ascii=False, indent=4)
            return default_data
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 检查是否需要重置(每周一)
        if self._check_reset(data.get("last_reset")):
            data["current_floor"] = 0
            data["last_reset"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_user_tower_info(user_id, data)
        
        # 确保所有字段都存在
        for key in default_data:
            if key not in data:
                data[key] = default_data[key]
        
        return data
    
    def save_user_tower_info(self, user_id, data):
        """保存用户通天塔信息"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "tower_info.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    def update_tower_rank(self, user_id, user_name, floor):
        """更新通天塔排行榜"""
        rank_data = self._load_rank_data()
        
        # 更新或添加用户记录
        rank_data[str(user_id)] = {
            "name": user_name,
            "floor": floor,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 保存排行榜
        with open(TOWER_RANK_PATH, "w", encoding="utf-8") as f:
            json.dump(rank_data, f, ensure_ascii=False, indent=4)
    
    def get_tower_rank(self, limit=50):
        """获取通天塔排行榜"""
        rank_data = self._load_rank_data()
        
        # 按层数降序排序
        sorted_rank = sorted(
            rank_data.items(),
            key=lambda x: x[1]["floor"],
            reverse=True
        )[:limit]
        
        return sorted_rank
    
    def _load_rank_data(self):
        """加载排行榜数据"""
        if not TOWER_RANK_PATH.exists():
            return {}
        
        with open(TOWER_RANK_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except:
                return {}
    
    def get_weekly_purchases(self, user_id, item_id):
        """获取用户本周已购买某商品的数量"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "tower_purchases.json"
        
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
        file_path = PLAYERSDATA / user_id / "tower_purchases.json"
        
        data = {
            "_last_reset": datetime.now().strftime("%Y-%m-%d")
        }
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def update_weekly_purchase(self, user_id, item_id, quantity):
        """更新用户本周购买某商品的数量"""
        user_id = str(user_id)
        file_path = PLAYERSDATA / user_id / "tower_purchases.json"
        
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

tower_data = TowerData()
