import random
import asyncio
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.utils import number_to, check_user, check_user_type, send_msg_handler
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.item_json import Items
from .tower_data import tower_data

sql_message = XiuxianDateManage()
items = Items()

# BOSS配置数据
TOWER_BOSS_CONFIG = {
    "Boss名字": [
        "九寒", "精卫", "少姜", "陵光", "莫女", "术方", "卫起", 
        "血枫", "以向", "砂鲛鲛鲛鲛", "鲲鹏", "天龙", "莉莉丝", 
        "霍德尔", "历飞雨", "神风王", "衣以候", "金凰儿", 
        "元磁道人", "外道贩卖鬼", "散发着威压的尸体"
    ],
    "Boss倍率": {
        "气血": 10,  # 气血是修为的10倍
        "真元": 2,    # 真元是修为的2倍
        "攻击": 0.3   # 攻击是修为的0.3倍
    }
}

JINGJIEEXP = {
    "感气境": [954, 1923, 2917],
    "练气境": [5832, 7634, 9712],
    "筑基境": [29145, 58217, 87324],
    "结丹境": [140112, 155712, 171072],
    "金丹境": [276288, 342528, 404992],
    "元神境": [809664, 872448, 935232],
    "化神境": [1868160, 1992960, 2117760],
    "炼神境": [4235520, 4485120, 4734720],
    "返虚境": [9467520, 12003840, 14564160],
    "大乘境": [30130800, 34987200, 39843600],
    "虚道境": [59343600, 69093600, 78843600],
    "斩我境": [117741600, 137341600, 156941600],
    "遁一境": [313483680, 438691440, 563929200],
    "至尊境": [1128355200, 1578996000, 2030659200],
    "微光境": [4061980800, 5685177600, 7308374400],
    "星芒境": [14619744000, 20457600000, 26295456000],
    "月华境": [52570860000, 73636980000, 94703100000],
    "耀日境": [189330000000, 362156000000, 535112000000],
    "祭道境": [2647500000000, 4742000000000, 13200300000000]
}

class TowerBattle:
    def __init__(self):
        self.config = tower_data.config
    
    def generate_tower_boss(self, floor):
        """根据层数生成通天塔BOSS"""
        if floor <= 0:
            floor = 1
        
        # 每10层一个境界循环
        base_floor = (floor - 1) % 10 + 1
        jj_index = (floor - 1) // 10
        
        # 获取境界列表
        jj_list = list(JINGJIEEXP.keys())

        jj = jj_list[min(jj_index, len(jj_list) - 1)]
        
        # 根据小层数决定境界阶段(前期/中期/圆满)
        if base_floor <= 3:
            stage = "前期"
            exp = JINGJIEEXP[jj][0]
        elif base_floor <= 6:
            stage = "中期"
            exp = JINGJIEEXP[jj][1]
        else:
            stage = "圆满"
            exp = JINGJIEEXP[jj][2]

        if jj_index >= len(jj_list):
            # 超过最高境界后，每层属性增加10%
            jj_index = len(jj_list) - 1
            scale = 1.1 ** (floor - len(jj_list) * 10)
        else:
            scale = 1.0
        
        # 计算BOSS属性
        boss_info = {
            "name": f"{random.choice(TOWER_BOSS_CONFIG['Boss名字'])}",
            "jj": jj,
            "气血": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["气血"] * scale),
            "总血量": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["气血"] * scale),
            "真元": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["真元"] * scale),
            "攻击": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["攻击"] * scale),
            "floor": floor,
            "stone": 1000000
        }
        
        return boss_info
    
    async def challenge_floor(self, bot, event, user_id, floor=None, continuous=False):
        """挑战通天塔"""
        isUser, user_info, msg = check_user(event)
        if not isUser:
            return False, msg
        
        # 检查用户状态
        is_type, msg = check_user_type(user_id, 0)
        if not is_type:
            return False, msg
        
        # 获取用户当前层数
        tower_info = tower_data.get_user_tower_info(user_id)
        current_floor = tower_info["current_floor"]
        
        # 如果是首次挑战或指定层数
        if floor is None:
            floor = current_floor + 1
        else:
            if floor != current_floor + 1:
                return False, f"只能挑战下一层({current_floor + 1})！"
        
        # 生成BOSS
        boss_info = self.generate_tower_boss(floor)
        
        # 准备玩家数据
        player = self._prepare_player_data(user_info)
        
        # 执行战斗
        if continuous:
            # 连续爬塔模式
            return await self._continuous_challenge(bot, event, user_info, player, floor)
        else:
            # 单层挑战模式
            return await self._single_challenge(bot, event, user_info, player, boss_info)
    
    async def _single_challenge(self, bot, event, user_info, player, boss_info):
        """单层挑战"""
        user_id = user_info["user_id"]
        result, victor, bossinfo_new, _ = await Boss_fight(player, boss_info, type_in=1, bot_id=bot.self_id)        
        await send_msg_handler(bot, event, result)
        if victor == "群友赢了":
            # 挑战成功
            reward_msg = self._give_floor_reward(user_id, user_info, boss_info["floor"])
            msg = (
                f"恭喜道友击败{boss_info['name']}，成功通关通天塔第{boss_info['floor']}层！\n"
                f"{reward_msg}"
            )
            
            # 更新层数
            tower_info = tower_data.get_user_tower_info(user_id)
            tower_info["current_floor"] = boss_info["floor"]
            tower_info["max_floor"] = max(tower_info["max_floor"], boss_info["floor"])
            tower_data.save_user_tower_info(user_id, tower_info)
            
            # 更新排行榜
            tower_data.update_tower_rank(
                user_id, 
                user_info["user_name"], 
                boss_info["floor"]
            )
            
            return True, msg
        else:
            # 挑战失败
            msg = f"道友不敌{boss_info['name']}，止步通天塔第{boss_info['floor'] - 1}层！"
            return False, msg
    
    async def _continuous_challenge(self, bot, event, user_info, player, start_floor):
        """连续挑战10层"""
        user_id = user_info["user_id"]
        max_floor = min(start_floor + 9, start_floor + 100)  # 最多连续挑战100层
        success_floors = []
        failed_floor = None
        reward_msg = ""
        total_score = 0
    
        for floor in range(start_floor, max_floor + 1):
            boss_info = self.generate_tower_boss(floor)
            result, victor, bossinfo_new, _ = await Boss_fight(player, boss_info, type_in=1, bot_id=bot.self_id)
            if victor == "群友赢了":
                success_floors.append(floor)
                # 给予基础奖励
                score = self.config["积分奖励"]["每层基础"]
                total_score += score
            
                # 每10层额外奖励
                if floor % 10 == 0:
                    score += self.config["积分奖励"]["每10层额外"]
                    total_score += self.config["积分奖励"]["每10层额外"]
                    reward_msg = self._give_special_reward(user_id, user_info, floor)
                    reward_msg = f"\n{reward_msg}"
                
                # 更新层数
                tower_info = tower_data.get_user_tower_info(user_id)
                tower_info["current_floor"] = floor
                tower_info["max_floor"] = max(tower_info["max_floor"], floor)
                tower_info["score"] += score  # 添加积分
                tower_data.save_user_tower_info(user_id, tower_info)
                
                # 更新排行榜
                tower_data.update_tower_rank(
                    user_id, 
                    user_info["user_name"], 
                    floor
                )
            else:
                failed_floor = floor
                break
        await send_msg_handler(bot, event, result)
        if failed_floor:
            msg = f"连续挑战失败，止步第{failed_floor - 1}层！共获得积分：{total_score}点"
            return False, msg
        else:
            msg = f"连续挑战完成，成功通关第{max_floor}层！共获得积分：{total_score}点{reward_msg}"
            return True, msg
    
    def _prepare_player_data(self, user_info):
        """准备玩家战斗数据"""
        player = {
            "user_id": user_info["user_id"],
            "道号": user_info["user_name"],
            "气血": user_info["hp"],
            "攻击": user_info["atk"],
            "真元": user_info["mp"],
            "exp": user_info["exp"],
            "会心": 0  # 简化处理，实际应根据装备计算
        }
        return player
    
    def _give_floor_reward(self, user_id, user_info, floor):
        """给予层数奖励"""
        # 基础奖励
        score = self.config["积分奖励"]["每层基础"]
        stone = self.config["灵石奖励"]["每层基础"]
        
        # 每10层额外奖励
        if floor % 10 == 0:
            score += self.config["积分奖励"]["每10层额外"]
            stone += self.config["灵石奖励"]["每10层额外"]
        
        # 更新积分
        tower_info = tower_data.get_user_tower_info(user_id)
        tower_info["score"] += score
        tower_data.save_user_tower_info(user_id, tower_info)
        
        # 给予灵石
        sql_message.update_ls(user_id, stone, 1)
        
        msg = f"获得积分：{score}点，灵石：{number_to(stone)}枚"
        
        # 每10层额外奖励
        if floor % 10 == 0:
            item_msg = self._give_random_item(user_id, user_info["level"])
            exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"])
            sql_message.update_exp(user_id, exp_reward)
            
            msg += f"，额外奖励：{item_msg}，修为：{number_to(exp_reward)}点"
        
        return msg
    
    def _give_special_reward(self, user_id, user_info, floor):
        """给予特殊奖励(每10层)"""
        item_msg = self._give_random_item(user_id, user_info["level"])
        exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"])
        sql_message.update_exp(user_id, exp_reward)
        
        return f"通关第{floor}层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点"
    
    def _give_random_item(self, user_id, user_level):
        """给予随机物品奖励"""
        # 获取用户境界对应的物品等级
        user_rank = convert_rank(user_level)[0]
        min_rank = max(user_rank - 16, 16)  # 最低16级物品
        item_rank = random.randint(min_rank, min_rank + 20)
        
        # 随机选择物品类型
        item_types = ["功法", "神通", "药材", "法器", "防具"]
        item_type = random.choice(item_types)
        
        # 获取随机物品
        item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
        if not item_id_list:
            return "无"
        
        item_id = random.choice(item_id_list)
        item_info = items.get_data_by_item_id(item_id)
        
        # 给予物品
        sql_message.send_back(
            user_id, 
            item_id, 
            item_info["name"], 
            item_info["type"], 
            1
        )
        
        return f"{item_info['level']}:{item_info['name']}"

tower_battle = TowerBattle()
