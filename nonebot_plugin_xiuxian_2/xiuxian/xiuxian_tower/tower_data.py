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
            "desc": "渡厄丹,使下一次突破丢失的修为减少为0!",
            "weekly_limit": 10
        },
        "2": {
            "id": 4003,
            "cost": 5000,
            "desc": "陨铁炉,以陨铁炼制的丹炉,耐高温,具有基础的炼丹功能",
            "weekly_limit": 1
        },
        "3": {
            "id": 4002,
            "cost": 25000,
            "desc": "雕花紫铜炉,表面刻有精美雕花的紫铜丹炉,一看便出自大师之手,可以使产出的丹药增加1枚",
            "weekly_limit": 1
        },
        "4": {
            "id": 4001,
            "cost": 100000,
            "desc": "寒铁铸心炉,由万年寒铁打造的顶尖丹炉,可以使产出的丹药增加2枚",
            "weekly_limit": 1
        },
        "5": {
            "id": 2500,
            "cost": 5000,
            "desc": "一级聚灵旗,提升洞天福地中的灵气汇集速度,加速修炼速度和灵田中药材生长速度",
            "weekly_limit": 1
        },
        "6": {
            "id": 2501,
            "cost": 10000,
            "desc": "二级聚灵旗,提升洞天福地中的灵气汇集速度,加速修炼速度和灵田中药材生长速度",
            "weekly_limit": 1
        },
        "7": {
            "id": 2502,
            "cost": 20000,
            "desc": "三级聚灵旗,提升洞天福地中的灵气汇集速度,加速修炼速度和灵田中药材生长速度",
            "weekly_limit": 1
        },
        "8": {
            "id": 2503,
            "cost": 40000,
            "desc": "四级聚灵旗,提升洞天福地中的灵气汇集速度,加速修炼速度和灵田中药材生长速度",
            "weekly_limit": 1
        },
        "9": {
            "id": 2504,
            "cost": 80000,
            "desc": "仙级聚灵旗,大幅提升洞天福地中的灵气汇集速度,加速修炼速度和灵田中药材生长速度",
            "weekly_limit": 1
        },
        "10": {
            "id": 2505,
            "cost": 100000,
            "desc": "无上聚灵旗,极大提升洞天福地中的灵气汇集速度",
            "weekly_limit": 1
        },
        "11": {
            "id": 7085,
            "cost": 2000000,
            "desc": "冲天槊，无上仙器，不属于这个位面的武器，似乎还有种种能力未被发掘...提升100%攻击力！提升50%会心率！提升20%减伤率！提升50%会心伤害！",
            "weekly_limit": 1
        },
        "12": {
            "id": 8931,
            "cost": 2000000,
            "desc": "苍寰变，无上神通，不属于这个位面的神通，连续攻击两次，造成6.5倍！7倍伤害！消耗气血0%、真元70%，发动概率100%，发动后休息一回合 ",
            "weekly_limit": 1
        },
        "13": {
            "id": 9937,
            "cost": 2000000,
            "desc": "一气化三清，无上仙法，比上面几个还厉害，可惜太长写不下 ",
            "weekly_limit": 1
        },
        "14": {
            "id": 10402,
            "cost": 700000,
            "desc": "真神威录，天阶下品辅修功法，增加70%会心率！",
            "weekly_limit": 1
        },
        "15": {
            "id": 10403,
            "cost": 1000000,
            "desc": "太乙剑诀，天阶下品辅修功法，增加100%会心伤害！",
            "weekly_limit": 1
        },
        "16": {
            "id": 10411,
            "cost": 1200000,
            "desc": "真龙九变，天阶上品辅修功法，增加攻击力60%！",
            "weekly_limit": 1
        },
        "17": {
            "id": 20004,
            "cost": 10000,
            "desc": "蕴灵石，蕴藏充足灵性，似有灵韵波动，神秘而独特。",
            "weekly_limit": 10
        },
        "18": {
            "id": 20003,
            "cost": 50000,
            "desc": "神圣石，神圣灵魂实体，散发庄严气息，蕴含神圣奥秘。",
            "weekly_limit": 3
        },
        "19": {
            "id": 20002,
            "cost": 200000,
            "desc": "化道石，无上强者化道遗留，承载其修行感悟，神秘非凡。",
            "weekly_limit": 1
        },
        "20": {
            "id": 20005,
            "cost": 1000,
            "desc": "祈愿石，蕴含神秘祈愿之力！它似命运水晶球，能感知你内心渴望。依靠它，能带来好运奇迹，助祈愿成真！",
            "weekly_limit": 10
        },
        "21": {
            "id": 15357,
            "cost": 100000,
            "desc": "八九玄功：无上仙法!",
            "weekly_limit": 1
        },
        "22": {
            "id": 9935,
            "cost": 100000,
            "desc": "暗渊灭世功：无上仙法!",
            "weekly_limit": 1
        },
        "23": {
            "id": 9940,
            "cost": 100000,
            "desc": "化功大法：无上仙法!",
            "weekly_limit": 1
        },
        "24": {
            "id": 10405,
            "cost": 50000,
            "desc": "醉仙：获得10%穿甲!",
            "weekly_limit": 1
        },
        "25": {
            "id": 10410,
            "cost": 1000000,
            "desc": "劫破：获得20%穿甲!",
            "weekly_limit": 1
        },
        "26": {
            "id": 10412,
            "cost": 2000000,
            "desc": "无极·靖天：获得35%穿甲!",
            "weekly_limit": 1
        },
        "27": {
            "id": 8933,
            "cost": 2000000,
            "desc": "冥河鬼镰·千慄慄葬世：冥河之水化作鬼镰，千慄慄葬世，收割万物生机！鬼镰划破天际，冥气弥漫，敌人灵魂被生生撕裂，坠入无尽深渊！此乃鬼噬之终章，天地同寂！",
            "weekly_limit": 1
        },
        "28": {
            "id": 8934,
            "cost": 2000000,
            "desc": "血影碎空·胧剑劫：血影碎空，胧剑出鞘，剑气化血雾，弥漫战场！敌人陷入血胧之中，剑气纵横，劫难降临，万物皆灭！",
            "weekly_limit": 1
        },
        "29": {
            "id": 8935,
            "cost": 2000000,
            "desc": "剑御九天·万剑归墟：剑御九天，万剑归墟，剑气化九天，剑网笼罩天地！敌人被剑气撕裂，万物归于虚无，此乃剑道极致！",
            "weekly_limit": 1
        },
        "30": {
            "id": 8936,
            "cost": 2000000,
            "desc": "华光·万影噬空：刹诀！华光·万影噬空！华光化万影，噬空夺命！敌人被无尽的光影吞噬，灵魂化为灰烬，天地皆为空影，唯我独尊！",
            "weekly_limit": 1
        },
        "31": {
            "id": 20011,
            "cost": 10000,
            "desc": "易名符：用于修改道号的珍贵符箓",
            "weekly_limit": 1
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
        file_path = PLAYERSDATA / user_id / "tower_purchases.json"
        
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
        for file in PLAYERSDATA.glob("*/tower_purchases.json"):
            try:
                file.unlink()
            except:
                pass

tower_data = TowerData()
