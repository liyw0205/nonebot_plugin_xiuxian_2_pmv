import random
import asyncio
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, UserBuffDate, leave_harm_time
from ..xiuxian_utils.data_source import jsondata
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
        "气血": 50,  # 气血是修为的50倍
        "真元": 10,    # 真元是修为的10倍
        "攻击": 10   # 攻击是修为的10倍
    }
}

jinjie_list = [
    "感气境",
    "练气境",
    "筑基境",
    "结丹境",
    "金丹境",
    "元神境",
    "化神境",
    "炼神境",
    "返虚境",
    "大乘境",
    "虚道境",
    "斩我境",
    "遁一境",
    "至尊境",
    "微光境",
    "星芒境",
    "月华境",
    "耀日境",
    "祭道境"
]

class TowerBattle:
    def __init__(self):
        self.config = tower_data.config
    
    def generate_tower_boss(self, floor):
        """根据层数生成通天塔BOSS"""
        if floor <= 0:
            floor = 1
        
        base_floor = (floor - 1) % 10 + 1
        jj_index = (floor - 1) // 10
        jj_list = jinjie_list
        exp_rate = random.randint(8, 12)

        if jj_index >= len(jj_list) - 1:
            exceed_floor = floor - (len(jj_list) - 1) * 10
            jj = "祭道境"
            base_exp = int(jsondata.level_data()["祭道境中期"]["power"])
            hundred_layers = exceed_floor // 100
            base_scale = 1.0 + (hundred_layers * 0.5)
            floor_scale = 1.0 + (base_scale * 0.1 * exceed_floor)
            base_exp = int(base_exp * floor_scale)
            hundred_layers = exceed_floor // 10
            base_scale = 1.0 + (hundred_layers * 0.1)
            exp = int(base_exp * floor_scale * base_scale)
        else:
            jj = jj_list[min(jj_index, len(jj_list) - 1)]
            if base_floor <= 3:
                stage = "初期"
            elif base_floor <= 6:
                stage = "中期"
            else:
                stage = "圆满"
            level = f"{jj}{stage}"
            exp = int(jsondata.level_data()[level]["power"])
            scale = 1.0

        boss_info = {
            "name": f"{random.choice(TOWER_BOSS_CONFIG['Boss名字'])}",
            "jj": jj,
            "气血": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["气血"]),
            "总血量": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["气血"]),
            "真元": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["真元"]),
            "攻击": int(exp * TOWER_BOSS_CONFIG["Boss倍率"]["攻击"]),
            "floor": floor,
            "stone": 1000000
        }
        
        return boss_info
    
    async def challenge_floor(self, bot, event, user_id, floor=None, continuous=False, target_floors=10):
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
        
        # 执行战斗
        if continuous:
            # 连续爬塔模式
            return await self._continuous_challenge(bot, event, user_info, floor, target_floors)
        else:
            # 单层挑战模式
            return await self._single_challenge(bot, event, user_info, boss_info)
    
    async def _single_challenge(self, bot, event, user_info, boss_info):
        """单层挑战"""
        user_id = user_info["user_id"]
        user_buff_data = UserBuffDate(user_info['user_id'])
        sub_buff_data = user_buff_data.get_user_sub_buff_data()
        sub_buff_integral_buff = sub_buff_data.get('integral', 0) if sub_buff_data is not None else 0
        sub_buff_stone_buff = sub_buff_data.get('stone', 0) if sub_buff_data is not None else 0
        result, victor, bossinfo_new = await Boss_fight(user_id, boss_info, bot_id=bot.self_id)        
        await send_msg_handler(bot, event, result)
        if victor == "群友赢了":
            # 挑战成功
            total_score = 0
            total_stone = 0
            reward_msg = ""
            
            # 基础奖励
            base_score = self.config["积分奖励"]["每层基础"]
            base_stone = self.config["灵石奖励"]["每层基础"]
            total_score += base_score
            total_stone += base_stone
            
            # 每10层首通奖励
            tower_info = tower_data.get_user_tower_info(user_id)
            if boss_info["floor"] % 10 == 0 and boss_info["floor"] > tower_info["max_floor"]:
                extra_score = self.config["积分奖励"]["每10层额外"]
                extra_stone = self.config["灵石奖励"]["每10层额外"]
                total_score += extra_score
                total_stone += extra_stone
                
                item_msg = self._give_random_item(user_id, user_info["level"])
                user_rank = convert_rank(user_info['level'])[0]
                exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * min(0.1 * user_rank, 1))
                sql_message.update_exp(user_id, exp_reward)
                
                reward_msg = f"\n通关第{boss_info['floor']}层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点"

            # 每100层可重复奖励(双倍十层奖励)
            if boss_info["floor"] % 100 == 0:
                extra_score = self.config["积分奖励"]["每10层额外"] * 2
                extra_stone = self.config["灵石奖励"]["每10层额外"] * 2
                total_score += extra_score
                total_stone += extra_stone
                
                item_msg = self._give_random_item(user_id, user_info["level"])
                user_rank = convert_rank(user_info['level'])[0]
                exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * 2 * min(0.1 * user_rank, 1))
                sql_message.update_exp(user_id, exp_reward)
                
                reward_msg += f"\n百层奖励：{item_msg}，修为：{number_to(exp_reward)}点"

            # 更新积分
            total_score = int(total_score * (1 + sub_buff_integral_buff))
            total_stone = int(total_stone * (1 + sub_buff_stone_buff))
            tower_info = tower_data.get_user_tower_info(user_id)
            tower_info["score"] += total_score
            tower_info["current_floor"] = boss_info["floor"]
            tower_info["max_floor"] = max(tower_info["max_floor"], boss_info["floor"])
            tower_data.save_user_tower_info(user_id, tower_info)
            
            # 给予灵石
            sql_message.update_ls(user_id, total_stone, 1)
            
            # 更新排行榜
            tower_data.update_tower_rank(
                user_id, 
                user_info["user_name"], 
                boss_info["floor"]
            )
            
            msg = (
                f"恭喜道友击败{boss_info['name']}，成功通关通天塔第{boss_info['floor']}层！\n"
                f"共获得积分：{total_score}点，灵石：{number_to(total_stone)}枚"
                f"{reward_msg}"
            )
            
            return True, msg
        else:
            # 挑战失败
            msg = f"道友不敌{boss_info['name']}，止步通天塔第{boss_info['floor'] - 1}层！"
            return False, msg
    
    async def _continuous_challenge(self, bot, event, user_info, start_floor, target_floors=10):
        """连续挑战指定层数"""
        user_id = user_info["user_id"]
        user_buff_data = UserBuffDate(user_info['user_id'])
        sub_buff_data = user_buff_data.get_user_sub_buff_data()
        sub_buff_integral_buff = sub_buff_data.get('integral', 0) if sub_buff_data is not None else 0
        sub_buff_stone_buff = sub_buff_data.get('stone', 0) if sub_buff_data is not None else 0
        tower_info = tower_data.get_user_tower_info(user_id)
        initial_max_floor = tower_info["max_floor"]  # 保存初始的最大层数
        
        # 计算最大挑战层数，限制为100层
        max_floor = min(start_floor + target_floors - 1, start_floor + 100)
        
        success_floors = []
        failed_floor = None
        reward_msg = ""
        total_score = 0
        total_stone = 0
        last_result = None  # 存储最后一次战斗结果

        for floor in range(start_floor, max_floor + 1):
            boss_info = self.generate_tower_boss(floor)
            result, victor, bossinfo_new = await Boss_fight(user_id, boss_info, bot_id=bot.self_id)
            last_result = result  # 始终保存最后一次战斗结果
            
            if victor == "群友赢了":
                success_floors.append(floor)
                # 给予基础奖励
                score = self.config["积分奖励"]["每层基础"]
                stone = self.config["灵石奖励"]["每层基础"]
                total_score += score
                total_stone += stone
            
                # 每10层额外奖励 - 使用初始的最大层数来判断首通
                if floor % 10 == 0 and floor > initial_max_floor:
                    extra_score = self.config["积分奖励"]["每10层额外"]
                    extra_stone = self.config["灵石奖励"]["每10层额外"]
                    total_score += extra_score
                    total_stone += extra_stone
                    
                    item_msg = self._give_random_item(user_id, user_info["level"])
                    exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"])
                    sql_message.update_exp(user_id, exp_reward)
                    reward_msg += f"\n通关第{floor}层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点"

                # 每100层可重复奖励(双倍十层奖励)
                if floor % 100 == 0:
                    extra_score = self.config["积分奖励"]["每10层额外"] * 2
                    extra_stone = self.config["灵石奖励"]["每10层额外"] * 2
                    total_score += extra_score
                    total_stone += extra_stone
                    
                    item_msg = self._give_random_item(user_id, user_info["level"])
                    exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * 2)
                    sql_message.update_exp(user_id, exp_reward)
                    reward_msg += f"\n百层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点"
            else:
                failed_floor = floor
                break
        
        # 发送最后一次战斗结果
        if last_result:
            await send_msg_handler(bot, event, last_result)
        
        # 如果有成功层数
        if success_floors:
            max_success = max(success_floors)
            # 一次性更新所有数据
            total_score = int(total_score * (1 + sub_buff_integral_buff))
            total_stone = int(total_stone * (1 + sub_buff_stone_buff))
            tower_info["current_floor"] = max_success
            tower_info["max_floor"] = max(tower_info["max_floor"], max_success)
            tower_info["score"] += total_score
            tower_data.save_user_tower_info(user_id, tower_info)
            
            # 更新排行榜
            tower_data.update_tower_rank(
                user_id, 
                user_info["user_name"], 
                max_success
            )
            
            # 给予总灵石奖励
            if total_stone > 0:
                sql_message.update_ls(user_id, total_stone, 1)
        
        if failed_floor:
            msg = f"连续挑战失败，止步第{failed_floor - 1}层！共获得积分：{total_score}点，灵石：{number_to(total_stone)}枚{reward_msg}"
            return False, msg
        else:
            msg = f"连续挑战完成，成功通关第{max_floor}层！共获得积分：{total_score}点，灵石：{number_to(total_stone)}枚{reward_msg}"
            return True, msg
    
    def _give_floor_reward(self, user_id, user_info, floor):
        tower_info = tower_data.get_user_tower_info(user_id)
        user_buff_data = UserBuffDate(user_info['user_id'])
        sub_buff_data = user_buff_data.get_user_sub_buff_data()
        sub_buff_integral_buff = sub_buff_data.get('integral', 0) if sub_buff_data is not None else 0
        sub_buff_stone_buff = sub_buff_data.get('stone', 0) if sub_buff_data is not None else 0
        """给予层数奖励"""
        # 基础奖励
        base_score = self.config["积分奖励"]["每层基础"]
        base_stone = self.config["灵石奖励"]["每层基础"]
        
        total_score = base_score
        total_stone = base_stone
        total_score = int(total_score * (1 + sub_buff_integral_buff))
        total_stone = int(total_stone * (1 + sub_buff_stone_buff))
        
        msg = f"获得积分：{base_score}点，灵石：{number_to(base_stone)}枚"

        # 每10层首通奖励
        if floor % 10 == 0 and floor > tower_info["max_floor"]:
            extra_score = self.config["积分奖励"]["每10层额外"]
            extra_stone = self.config["灵石奖励"]["每10层额外"]
            total_score += extra_score
            total_stone += extra_stone
            total_score = int(total_score * (1 + sub_buff_integral_buff))
            total_stone = int(total_stone * (1 + sub_buff_stone_buff))
            item_msg = self._give_random_item(user_id, user_info["level"])
            exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"])
            sql_message.update_exp(user_id, exp_reward)
        
            msg += f"\n通关第{floor}层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点，额外积分：{extra_score}点"

        # 每100层可重复奖励(双倍十层奖励)
        if floor % 100 == 0:
            extra_score = self.config["积分奖励"]["每10层额外"] * 2
            extra_stone = self.config["灵石奖励"]["每10层额外"] * 2
            total_score += extra_score
            total_stone += extra_stone
            total_score = int(total_score * (1 + sub_buff_integral_buff))
            total_stone = int(total_stone * (1 + sub_buff_stone_buff))
            item_msg = self._give_random_item(user_id, user_info["level"])
            exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * 2)
            sql_message.update_exp(user_id, exp_reward)
        
            msg += f"\n百层奖励：{item_msg}，修为：{number_to(exp_reward)}点，额外积分：{extra_score}点"

        # 更新积分
        tower_info = tower_data.get_user_tower_info(user_id)
        tower_info["score"] += total_score  # 使用总积分
        tower_data.save_user_tower_info(user_id, tower_info)

        # 给予灵石
        sql_message.update_ls(user_id, total_stone, 1)

        return msg
    
    def _give_random_item(self, user_id, user_level):
        """给予随机物品奖励"""
        # 随机选择物品类型
        item_types = ["功法", "神通", "药材", "法器", "防具", "身法", "瞳术"]
        item_type = random.choice(item_types)

        if item_type in ["法器", "防具", "辅修功法", "身法", "瞳术"]:
            base_rank = max(convert_rank(user_level)[0], 16)
        else:
            base_rank = max(convert_rank(user_level)[0] - 22, 10)
        zx_rank = random.randint(base_rank, base_rank + 35)
        zx_rank = min(zx_rank, 55)
        # 获取随机物品
        item_id_list = items.get_random_id_list_by_rank_and_item_type(zx_rank, item_type)
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
