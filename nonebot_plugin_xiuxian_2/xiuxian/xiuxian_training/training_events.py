import random
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from ..xiuxian_config import convert_rank

sql_message = XiuxianDateManage()
items = Items()

# 正面事件列表
POSITIVE_EVENTS = [
    "偶遇一位隐世高人，得到指点",
    "发现一处灵气充沛的洞天福地",
    "救助了一位受伤的修士，获得馈赠",
    "在一处古遗迹中找到宝物",
    "与一位道友论道，有所感悟",
    "偶得一本上古功法残卷",
    "发现一株珍稀灵药",
    "帮助凡人解决困难，获得功德",
    "在一处秘境中有所收获",
    "与灵兽结缘，获得帮助"
]

# 负面事件列表
NEGATIVE_EVENTS = [
    "遭遇妖兽袭击，受了轻伤",
    "误入一处险地，耗费精力才脱身",
    "被一位邪修盯上，损失了些财物",
    "修炼时出了点岔子，损耗了些修为",
    "遇到一群劫修，被抢了些灵石",
    "被幻阵所困，耗费心神才脱身",
    "误食毒草，身体不适",
    "被一位大能修士的威压所伤",
    "遭遇天劫余波，受了点伤",
    "被卷入修士争斗，受了波及"
]

NOTHING_EVENTS = [
    "静心修炼，参悟天地大道",
    "游览名山大川，陶冶情操",
    "参加一场修士交流会，增长见闻",
    "在坊市中闲逛，感受人间烟火",
    "闭关数日，巩固修为",
    "与几位道友品茶论道",
    "观赏一场灵兽表演",
    "在藏书阁中阅读古籍",
    "参加凡间的花灯节",
    "帮助宗门处理一些杂务"
]

class TrainingEvents:
    def handle_event(self, user_id, event_type):
        """处理历练事件"""
        user_info = sql_message.get_user_info_with_id(user_id)
        
        if "plus_2" in event_type:  # 大奖励
            event_msg = random.choice(POSITIVE_EVENTS)
            # 默认奖励150万灵石
            sql_message.update_ls(user_id, 1500000, 1)
            # 50%概率触发额外奖励
            if random.random() < 0.5:
                extra_reward = self._get_extra_reward(user_id, user_info)
                return f"道友历练中{event_msg}，获得大机缘！\n获得灵石：1,500,000\n{extra_reward}"
            else:
                return f"道友历练中{event_msg}，获得大机缘！\n获得灵石：1,500,000"
        elif "plus_1" in event_type:  # 小奖励
            event_msg = random.choice(POSITIVE_EVENTS)
            # 默认奖励150万灵石
            sql_message.update_ls(user_id, 1500000, 1)
            return f"道友历练中{event_msg}\n获得灵石：1,500,000"
        elif "minus_2" in event_type:  # 大惩罚
            event_msg = random.choice(NEGATIVE_EVENTS)
            # 默认扣除50万灵石
            sql_message.update_ls(user_id, 500000, 2)
            # 40%概率触发额外惩罚
            if random.random() < 0.4:
                extra_punish = self._get_extra_punish(user_id, user_info)
                return f"道友历练中{event_msg}，遭遇大劫难！\n损失灵石：500,000\n{extra_punish}"
            else:
                return f"道友历练中{event_msg}，遭遇大劫难！\n损失灵石：500,000"
        elif "minus_1" in event_type:  # 小惩罚
            event_msg = random.choice(NEGATIVE_EVENTS)
            # 默认扣除50万灵石
            sql_message.update_ls(user_id, 500000, 2)
            return f"道友历练中{event_msg}\n损失灵石：500,000"
        else:  # nothing
            # 无事发生默认奖励100万灵石
            sql_message.update_ls(user_id, 1000000, 1)
            # 随机选择正面或负面事件描述
            event_msg = random.choice(NOTHING_EVENTS)
            return f"道友历练中{event_msg}\n获得灵石：1,000,000"

    def _get_extra_reward(self, user_id, user_info):
        """获取额外奖励"""
        reward_type = random.choices(
            ["exp", "stone", "item", "points"],
            weights=[10, 50, 20, 20]
        )[0]
        
        if reward_type == "exp":
            exp = int(user_info["exp"] * random.uniform(0.002, 0.005))
            sql_message.update_exp(user_id, exp)
            return f"额外获得修为：{number_to(exp)}"
        elif reward_type == "stone":
            stone = random.randint(2000000, 8000000)
            sql_message.update_ls(user_id, stone, 1)
            return f"额外获得灵石：{number_to(stone)}"
        elif reward_type == "points":
            points = random.randint(200, 500)
            training_data = training_data.get_user_training_info(user_id)
            training_data["points"] += points
            training_data.save_user_training_info(user_id, training_data)
            return f"额外获得成就点：{points}"
        else:  # item
            user_rank = convert_rank(user_info["level"])[0]
            min_rank = max(user_rank - 16, 16)
            item_rank = random.randint(min_rank, min_rank + 20)
            item_types = ["功法", "神通", "药材", "法器", "防具"]
            item_type = random.choice(item_types)
            item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
            if not item_id_list:
                return "无额外物品奖励"
            item_id = random.choice(item_id_list)
            item_info = items.get_data_by_item_id(item_id)
            sql_message.send_back(user_id, item_id, item_info["name"], item_info["type"], 1)
            return f"额外获得物品：{item_info['level']}:{item_info['name']}"

    def _get_extra_punish(self, user_id, user_info):
        """获取额外惩罚"""
        punish_type = random.choices(
            ["exp", "stone", "item", "hp"],
            weights=[10, 20, 20, 50]
        )[0]
        
        if punish_type == "exp":
            exp_loss = int(user_info["exp"] * 0.002)  # 0.2%修为
            sql_message.update_j_exp(user_id, exp_loss)
            return f"额外损失修为：{number_to(exp_loss)}"
        elif punish_type == "stone":
            stone_loss = random.randint(2000000, 5000000)
            sql_message.update_ls(user_id, stone_loss, 2)
            return f"额外损失灵石：{number_to(stone_loss)}"
        elif punish_type == "hp":
            hp_loss = int(user_info["hp"] * 0.15)  # 15%气血
            sql_message.update_user_hp_mp(user_id, user_info["hp"] - hp_loss, user_info["mp"])
            return f"额外损失气血：{number_to(hp_loss)}"
        else:  # item
            back_msg = sql_message.get_back_msg(user_id)
            if not back_msg:
                stone_loss = 5000000
                sql_message.update_ls(user_id, stone_loss, 2)
                return f"背包空空如也，额外损失灵石：{number_to(stone_loss)}"
            else:
                same_name_items = [item for item in back_msg if item["goods_num"] > 0]
                if same_name_items:
                    item = random.choice(same_name_items)
                    sql_message.update_back_j(user_id, item["goods_id"], 1)
                    return f"额外损失物品：{item['goods_name']}"
                else:
                    user_rank = convert_rank(user_info["level"])[0]
                    same_rank_items = [
                        item for item in back_msg 
                        if items.get_data_by_item_id(item["goods_id"])["level"] == user_rank
                    ]
                    if same_rank_items:
                        item = random.choice(same_rank_items)
                        sql_message.update_back_j(user_id, item["goods_id"], 1)
                        return f"额外损失物品：{item['goods_name']}"
                    else:
                        stone_loss = 5000000
                        sql_message.update_ls(user_id, stone_loss, 2)
                        return f"没有合适物品扣除，额外损失灵石：{number_to(stone_loss)}"

training_events = TrainingEvents()
