import random
import asyncio
import time
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, UserBuffDate, leave_harm_time
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.utils import (
    number_to, check_user, check_user_type, send_msg_handler,
    update_statistics_value
)
from ..xiuxian_config import convert_rank, base_rank
from ..xiuxian_utils.item_json import Items
from .tower_data import tower_data
from .tower_limit import tower_limit
from .settlement_service import TowerSettlementService
from ...paths import get_paths
from ..xiuxian_config import XiuConfig

sql_message = XiuxianDateManage()
items = Items()
tower_settlement_service = TowerSettlementService(get_paths().game_db, get_paths().player_db)

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
    
    async def _single_challenge(self, bot, event, user_info, boss_info):
        """单层挑战"""
        user_id = user_info["user_id"]
        event_id = getattr(event, "message_id", None)
        operation_id = f"tower-challenge:{event_id}:{user_id}" if event_id else f"tower-challenge:{time.time_ns()}:{user_id}"
        stamina_cost = int(self.config["体力消耗"]["单层爬塔"])
        # 先回放：成功后楼层/体力变化，且不可重开战。
        prior = tower_settlement_service.get_result(operation_id)
        if prior is not None and prior.succeeded:
            if prior.challenge_succeeded:
                msg = (
                    f"恭喜道友击败{boss_info['name']}，成功通关通天塔第{prior.floor or boss_info['floor']}层！\n"
                    f"共获得积分：{prior.score}点，灵石：{number_to(prior.stone)}枚\n"
                    "该挑战请求已经处理，无需重复提交。"
                )
                return True, msg
            msg = (
                f"道友不敌{boss_info['name']}，止步通天塔第{(prior.floor or boss_info['floor']) - 1}层！\n"
                "该挑战请求已经处理，无需重复提交。"
            )
            return False, msg
        # expected_* 必须用原始 DB 状态，避免 buff 放大后的 real_info 导致 concurrency 误冲突。
        raw_user = sql_message.get_user_info_with_id(user_id) or user_info
        expected_player = {key: int(raw_user[key]) for key in ("hp", "mp", "user_stamina")}
        if expected_player["user_stamina"] < stamina_cost:
            return False, "你没有足够的体力，请等待体力恢复后再试！"
        user_buff_data = UserBuffDate(user_info['user_id'])
        sub_buff_data = user_buff_data.get_user_sub_buff_data()
        sub_buff_integral_buff = sub_buff_data.get('integral', 0) if sub_buff_data is not None else 0
        sub_buff_stone_buff = sub_buff_data.get('stone', 0) if sub_buff_data is not None else 0
        tower_info = tower_limit.get_user_tower_info(user_id)
        result, victor, bossinfo_new, status_list = await Boss_fight(user_id, boss_info, type_in=0, bot_id=bot.self_id, return_status=True)
        await send_msg_handler(bot, event, result)
        final_hp, final_mp = self._player_status(status_list, user_id)
        if victor == "群友赢了":
            reward_rng = random.Random(operation_id)
            # 挑战成功
            total_score = 0
            total_stone = 0
            total_exp = 0
            reward_items = []
            reward_msg = ""
            
            # 基础奖励
            base_score = self.config["积分奖励"]["每层基础"]
            base_stone = self.config["灵石奖励"]["每层基础"]
            if boss_info["floor"] <= tower_info["max_floor"]:
                base_score = int(base_score * 0.7)
                base_stone = int(base_stone * 0.7)
            total_score += base_score
            total_stone += base_stone
            
            # 每10层首通奖励
            if boss_info["floor"] % 10 == 0 and boss_info["floor"] > tower_info["max_floor"]:
                extra_score = self.config["积分奖励"]["每10层额外"]
                extra_stone = self.config["灵石奖励"]["每10层额外"]
                total_score += extra_score
                total_stone += extra_stone
                
                item, item_msg = self._select_random_item(user_info["level"], reward_rng)
                user_rank = max(convert_rank(user_info['level'])[0] // 3, 1)
                exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * min(0.1 * user_rank, 1))
                total_exp += exp_reward
                if item:
                    reward_items.append(item)
                
                reward_msg = f"\n通关第{boss_info['floor']}层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点"

            # 每100层可重复奖励(双倍十层奖励)
            if boss_info["floor"] % 100 == 0:
                extra_score = self.config["积分奖励"]["每10层额外"] * 2
                extra_stone = self.config["灵石奖励"]["每10层额外"] * 2
                total_score += extra_score
                total_stone += extra_stone
                
                item, item_msg = self._select_random_item(user_info["level"], reward_rng)
                user_rank = max(convert_rank(user_info['level'])[0] // 3, 1)
                exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * 2 * min(0.1 * user_rank, 1))
                total_exp += exp_reward
                if item:
                    reward_items.append(item)
                
                reward_msg += f"\n百层奖励：{item_msg}，修为：{number_to(exp_reward)}点"

            # 更新积分
            total_score = int(total_score * (1 + sub_buff_integral_buff))
            total_stone = int(total_stone * (1 + sub_buff_stone_buff))
            settlement = tower_settlement_service.settle(
                operation_id, user_id, tower_info, boss_info["floor"], total_score, total_stone,
                total_exp, reward_items, XiuConfig().max_goods_num,
                expected_player=expected_player, final_hp=final_hp, final_mp=final_mp,
                stamina_cost=stamina_cost, challenge_succeeded=True,
            )
            if settlement.status == "duplicate":
                msg = (
                    f"恭喜道友击败{boss_info['name']}，成功通关通天塔第{boss_info['floor']}层！\n"
                    f"共获得积分：{settlement.score}点，灵石：{number_to(settlement.stone)}枚\n"
                    "该挑战请求已经处理，无需重复提交。"
                )
                return True, msg
            if not settlement.succeeded:
                return False, "通天塔奖励结算失败，请稍后重试。"
            update_statistics_value(user_id, "通天塔通关层数")
            update_statistics_value(user_id, "通天塔最高层", value=max(tower_info["max_floor"], boss_info["floor"]))
            
            msg = (
                f"恭喜道友击败{boss_info['name']}，成功通关通天塔第{boss_info['floor']}层！\n"
                f"共获得积分：{total_score}点，灵石：{number_to(total_stone)}枚"
                f"{reward_msg}"
            )
            
            return True, msg
        else:
            settlement = tower_settlement_service.settle(
                operation_id, user_id, tower_info, boss_info["floor"], 0, 0, 0, [], XiuConfig().max_goods_num,
                expected_player=expected_player, final_hp=final_hp, final_mp=final_mp,
                stamina_cost=stamina_cost, challenge_succeeded=False,
            )
            if settlement.status == "duplicate":
                return False, (
                    f"道友不敌{boss_info['name']}，止步通天塔第{boss_info['floor'] - 1}层！\n"
                    "该挑战请求已经处理，无需重复提交。"
                )
            if not settlement.succeeded:
                return False, "通天塔挑战结算失败，请稍后重试。"
            msg = f"道友不敌{boss_info['name']}，止步通天塔第{boss_info['floor'] - 1}层！"
            return False, msg

    @staticmethod
    def _player_status(status_list, user_id):
        for team in status_list:
            for attr in team.values():
                if str(attr.get("user_id")) != str(user_id):
                    continue
                hp_divisor = float(attr.get("hp_multiplier", 1)) or 1
                mp_divisor = float(attr.get("mp_multiplier", 1)) or 1
                return max(1, int(attr.get("hp", 1) / hp_divisor)), max(1, int(attr.get("mp", 1) / mp_divisor))
        raise ValueError("tower battle did not return player status")
    
    async def _continuous_challenge(self, bot, event, user_info, start_floor, target_floors=10):
        """连续挑战指定层数"""
        user_id = user_info["user_id"]
        user_buff_data = UserBuffDate(user_info['user_id'])
        sub_buff_data = user_buff_data.get_user_sub_buff_data()
        sub_buff_integral_buff = sub_buff_data.get('integral', 0) if sub_buff_data is not None else 0
        sub_buff_stone_buff = sub_buff_data.get('stone', 0) if sub_buff_data is not None else 0
        tower_info = tower_limit.get_user_tower_info(user_id)
        initial_max_floor = tower_info["max_floor"]  # 保存初始的最大层数
        event_id = getattr(event, "message_id", None)
        operation_id = f"tower-continuous:{event_id}:{user_id}:{target_floors}" if event_id else f"tower-continuous:{time.time_ns()}:{user_id}:{target_floors}"
        prior = tower_settlement_service.get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = (
                f"连续挑战完成，成功通关第{prior.floor or start_floor}层！共获得积分：{prior.score}点，"
                f"灵石：{number_to(prior.stone)}枚\n该挑战请求已经处理，无需重复提交。"
            )
            return True, msg
        reward_rng = random.Random(operation_id)
        
        # 计算最大挑战层数，限制为100层
        max_floor = min(start_floor + target_floors - 1, start_floor + 100)
        
        success_floors = []
        failed_floor = None
        reward_msg = ""
        total_score = 0
        total_stone = 0
        total_exp = 0
        reward_items = []
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
                if floor <= tower_info["max_floor"]:
                    score = int(score * 0.7)
                    stone = int(stone * 0.7)
                total_score += score
                total_stone += stone
            
                # 每10层额外奖励 - 使用初始的最大层数来判断首通
                if floor % 10 == 0 and floor > initial_max_floor:
                    extra_score = self.config["积分奖励"]["每10层额外"]
                    extra_stone = self.config["灵石奖励"]["每10层额外"]
                    total_score += extra_score
                    total_stone += extra_stone
                    
                    item, item_msg = self._select_random_item(user_info["level"], reward_rng)
                    exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"])
                    total_exp += exp_reward
                    if item:
                        reward_items.append(item)
                    reward_msg += f"\n通关第{floor}层特别奖励：{item_msg}，修为：{number_to(exp_reward)}点"

                # 每100层可重复奖励(双倍十层奖励)
                if floor % 100 == 0:
                    extra_score = self.config["积分奖励"]["每10层额外"] * 2
                    extra_stone = self.config["灵石奖励"]["每10层额外"] * 2
                    total_score += extra_score
                    total_stone += extra_stone
                    
                    item, item_msg = self._select_random_item(user_info["level"], reward_rng)
                    exp_reward = int(user_info["exp"] * self.config["修为奖励"]["每10层"] * 2)
                    total_exp += exp_reward
                    if item:
                        reward_items.append(item)
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
            settlement = tower_settlement_service.settle(
                operation_id, user_id, tower_info, max_success, total_score, total_stone,
                total_exp, reward_items, XiuConfig().max_goods_num,
            )
            if settlement.status == "duplicate":
                msg = (
                    f"连续挑战完成，成功通关第{max_success}层！共获得积分：{settlement.score}点，"
                    f"灵石：{number_to(settlement.stone)}枚\n该挑战请求已经处理，无需重复提交。"
                )
                return True, msg
            if not settlement.succeeded:
                return False, "通天塔奖励结算失败，请稍后重试。"
            update_statistics_value(user_id, "通天塔通关层数", increment=len(success_floors))
            update_statistics_value(user_id, "通天塔最高层", value=max(tower_info["max_floor"], max_success))
        
        if failed_floor:
            msg = f"连续挑战失败，止步第{failed_floor - 1}层！共获得积分：{total_score}点，灵石：{number_to(total_stone)}枚{reward_msg}"
            return False, msg
        else:
            msg = f"连续挑战完成，成功通关第{max_floor}层！共获得积分：{total_score}点，灵石：{number_to(total_stone)}枚{reward_msg}"
            return True, msg

    def _select_random_item(self, user_level, rng):
        """生成本次通天塔奖励物品，不在此处写入背包。"""
        # 随机选择物品类型
        item_types = ["功法", "神通", "药材", "法器", "防具", "身法", "瞳术"]
        item_type = rng.choice(item_types)

        if item_type in ["法器", "防具", "辅修功法", "身法", "瞳术"]:
            zx_rank = base_rank(user_level, 16)
        else:
            zx_rank = base_rank(user_level, 5)
        # 获取随机物品
        item_id_list = items.get_random_id_list_by_rank_and_item_type(zx_rank, item_type)
        if not item_id_list:
            return None, "无"
        
        item_id = rng.choice(item_id_list)
        item_info = items.get_data_by_item_id(item_id)
        
        return {"id": item_id, "name": item_info["name"], "type": item_info["type"], "amount": 1}, f"{item_info['level']}:{item_info['name']}"

tower_battle = TowerBattle()
