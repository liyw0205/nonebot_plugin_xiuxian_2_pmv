import random
import json
import os
from pathlib import Path
from datetime import datetime
from nonebot import on_command, on_regex
from nonebot.params import CommandArg, RegexGroup
from nonebot.log import logger
from nonebot.adapters.onebot.v11 import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent
)
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, check_user_type, 
    get_msg_pic, log_message, handle_send, 
    number_to, send_msg_handler, update_statistics_value
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank

sql_message = XiuxianDateManage()
items = Items()

# 定义命令
illusion_heart = on_command("幻境寻心", aliases={"心境试炼"}, priority=5, block=True)
illusion_reset = on_command("重置幻境", permission=SUPERUSER, priority=5, block=True)
illusion_clear = on_command("清空幻境", permission=SUPERUSER, priority=5, block=True)

async def reset_illusion_data():
    IllusionData.reset_player_data_only()
    logger.opt(colors=True).info("<green>幻境寻心玩家数据已重置</green>")

# 幻境问题和选项配置
DEFAULT_QUESTIONS = {
    "questions": [
        {
            "question": "你在修炼时遇到瓶颈，你会：",
            "options": [
                "闭关苦修，不突破不出关",
                "外出游历，寻找机缘",
                "请教前辈，寻求指点"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "面对强大的敌人，你会：",
            "options": [
                "正面迎战，绝不退缩",
                "智取为上，寻找弱点",
                "暂时退避，提升实力后再战"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "修炼最重要的是：",
            "options": [
                "坚定的道心",
                "强大的功法",
                "丰富的资源"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你如何看待因果：",
            "options": [
                "种因得果，必须谨慎",
                "随心而行，不问因果",
                "因果循环，自有定数"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你追求的是：",
            "options": [
                "无上大道",
                "逍遥自在",
                "守护重要之人"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "发现秘境时，你会：",
            "options": [
                "立即探索，机缘稍纵即逝",
                "做好准备再进入",
                "邀请同伴一同前往"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "对于仇敌，你的态度是：",
            "options": [
                "斩草除根，不留后患",
                "小惩大诫，点到为止",
                "冤冤相报何时了，化解恩怨"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "修炼遇到心魔，你会：",
            "options": [
                "直面心魔，战胜它",
                "寻求静心之法化解",
                "暂时停止修炼调整心态"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你更倾向于：",
            "options": [
                "独自修炼",
                "与志同道合者一起",
                "建立自己的势力"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "面对天劫，你的准备是：",
            "options": [
                "依靠自身实力硬抗",
                "准备大量防御法宝",
                "寻找特殊地点渡劫"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你更相信：",
            "options": [
                "人定胜天",
                "天命难违",
                "天人合一"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "对于宗门，你的看法是：",
            "options": [
                "必须忠诚于宗门",
                "只是修炼的跳板",
                "可有可无的存在"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你更看重：",
            "options": [
                "实力境界",
                "实战经验",
                "人脉关系"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "修炼资源不足时，你会：",
            "options": [
                "抢夺他人资源",
                "自己寻找或创造",
                "交易或合作获取"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你更倾向于修炼：",
            "options": [
                "攻击型功法",
                "防御型功法",
                "辅助型功法"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "对于凡人，你的态度是：",
            "options": [
                "视如蝼蝼蚁",
                "平等相待",
                "庇护一方"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你更愿意：",
            "options": [
                "追求长生",
                "追求力量",
                "追求逍遥"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "面对诱惑，你会：",
            "options": [
                "坚守本心不为所动",
                "权衡利弊后决定",
                "先拿到手再说"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "你更相信：",
            "options": [
                "正道光明",
                "魔道速成",
                "亦正亦邪"
            ],
            "counts": [0, 0, 0]
        },
        {
            "question": "修仙之路，你认为最重要的是：",
            "options": [
                "天赋资质",
                "勤奋努力",
                "机缘气运"
            ],
            "counts": [0, 0, 0]
        }
    ]
}

class IllusionData:
    DATA_PATH = Path(__file__).parent / "illusion"
    QUESTIONS_FILE = Path(__file__).parent / "illusion_questions.json"
    DAILY_RESET_HOUR = 8  # 每天8点重置
    
    @classmethod
    def get_or_create_user_illusion_info(cls, user_id):
        """获取或创建用户幻境信息"""
        user_id = str(user_id)
        file_path = cls.DATA_PATH / f"{user_id}.json"
        
        questions = cls.get_questions()["questions"]  # 获取问题列表
        question_count = len(questions)  # 获取问题总数
        
        default_data = {
            "last_participate": None,  # 上次参与时间
            "today_choice": None,      # 今日选择
            "question_index": random.randint(0, question_count - 1) if question_count > 0 else None  # 随机分配问题索引
        }
        
        if not file_path.exists():
            os.makedirs(cls.DATA_PATH, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(default_data, f, ensure_ascii=False, indent=4)
            return default_data
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 检查是否需要重置(每天8点)
        if cls._check_reset(data.get("last_participate")):
            data["today_choice"] = None
            data["question_index"] = random.randint(0, question_count - 1) if question_count > 0 else None  # 重置时重新分配问题
            data["last_participate"] = None
            cls.save_user_illusion_info(user_id, data)
        
        # 确保所有字段都存在
        for key in default_data:
            if key not in data:
                data[key] = default_data[key]
        
        # 如果问题索引不存在或无效，分配一个
        if data["question_index"] is None or data["question_index"] >= question_count:
            data["question_index"] = random.randint(0, question_count - 1) if question_count > 0 else None
            cls.save_user_illusion_info(user_id, data)
        
        return data
    
    @classmethod
    def save_user_illusion_info(cls, user_id, data):
        """保存用户幻境信息"""
        user_id = str(user_id)
        file_path = cls.DATA_PATH / f"{user_id}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    @classmethod
    def get_questions(cls):
        """获取问题数据"""
        if not cls.QUESTIONS_FILE.exists():
            # 如果文件不存在，创建默认问题文件
            os.makedirs(cls.QUESTIONS_FILE.parent, exist_ok=True)
            with open(cls.QUESTIONS_FILE, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_QUESTIONS, f, ensure_ascii=False, indent=4)
            return DEFAULT_QUESTIONS
        
        with open(cls.QUESTIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # 确保数据结构正确
            if "questions" not in data or not isinstance(data["questions"], list):
                # 如果结构不正确，恢复默认设置
                with open(cls.QUESTIONS_FILE, "w", encoding="utf-8") as f:
                    json.dump(DEFAULT_QUESTIONS, f, ensure_ascii=False, indent=4)
                return DEFAULT_QUESTIONS
            
            # 确保每个问题都有counts字段
            for question in data["questions"]:
                if "counts" not in question or len(question["counts"]) != len(question["options"]):
                    question["counts"] = [0] * len(question["options"])
            
            return data
    
    @classmethod
    def save_questions(cls, questions):
        """保存问题数据"""
        with open(cls.QUESTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(questions, f, ensure_ascii=False, indent=4)
    
    @classmethod
    def update_question_stats(cls, question_index, choice_index):
        """更新问题统计数据"""
        questions = cls.get_questions()
        if 0 <= question_index < len(questions["questions"]):
            question = questions["questions"][question_index]
            if 0 <= choice_index < len(question["counts"]):
                question["counts"][choice_index] += 1
                cls.save_questions(questions)
    
    @classmethod
    def _check_reset(cls, last_participate_str):
        """检查是否需要重置(每天8点)"""
        if not last_participate_str:
            return False
            
        try:
            last_participate = datetime.strptime(last_participate_str, "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            
            # 检查是否是新的天数且过了8点
            return (now.day > last_participate.day and now.hour >= cls.DAILY_RESET_HOUR) or \
                   (now.day == last_participate.day and now.hour >= cls.DAILY_RESET_HOUR and last_participate.hour < cls.DAILY_RESET_HOUR)
        except:
            return False
    
    @classmethod
    def reset_player_data_only(cls):
        """仅重置玩家数据（每日定时任务调用）"""
        for file in cls.DATA_PATH.glob("*.json"):
            try:
                # 直接删除玩家数据文件，下次访问时会自动创建
                file.unlink()
            except:
                continue
    
    @classmethod
    def reset_all_data(cls):
        """重置所有数据（玩家数据和问题统计数据）"""
        # 重置玩家数据
        cls.reset_player_data_only()
        
        # 重置问题统计数据
        questions = cls.get_questions()
        for question in questions["questions"]:
            question["counts"] = [0] * len(question["options"])
        cls.save_questions(questions)

@illusion_heart.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """幻境寻心"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await illusion_heart.finish()
    
    user_id = user_info["user_id"]
    illusion_info = IllusionData.get_or_create_user_illusion_info(user_id)
    questions = IllusionData.get_questions()["questions"]
    
    # 检查问题索引是否有效
    if illusion_info["question_index"] is None or illusion_info["question_index"] >= len(questions):
        msg = "幻境寻心功能暂时无法使用，请联系管理员检查问题配置"
        await handle_send(bot, event, msg)
        await illusion_heart.finish()
    
    # 获取用户输入
    user_input = args.extract_plain_text().strip()
    
    # 检查是否已经参与过今日的幻境
    if illusion_info["today_choice"] is not None:
        question_data = questions[illusion_info["question_index"]]
        question = question_data["question"]
        choice = illusion_info["today_choice"]
        msg = (
            f"\n═══  幻境寻心  ════\n"
            f"今日问题：{question}\n"
            f"{choice}\n"
            f"════════════\n"
            f"每日8点重置，请明日再来！"
        )
        await handle_send(bot, event, msg)
        await illusion_heart.finish()
    
    # 获取当前问题数据
    question_data = questions[illusion_info["question_index"]]
    question = question_data["question"]
    options = question_data["options"]
    
    # 如果没有输入参数，显示问题和选项
    if not user_input:
        msg = ["\n═══  幻境寻心  ════"]
        msg.append(f"今日问题：{question}")
        msg.append("请选择：")
        for i, option in enumerate(options, 1):
            msg.append(f"{i}. {option}")
        msg.append("════════════")
        msg.append("输入【幻境寻心+数字】进行选择")
        
        await send_msg_handler(bot, event, "幻境寻心", bot.self_id, msg)
        await illusion_heart.finish()
    
    # 检查输入是否有效
    try:
        choice_num = int(user_input)
        if choice_num < 1 or choice_num > len(options):
            raise ValueError
    except ValueError:
        msg = f"请输入有效的选择数字(1-{len(options)})！"
        await handle_send(bot, event, msg)
        await illusion_heart.finish()
    
    # 记录用户选择
    selected_option = options[choice_num - 1]  # 获取不带数字的选项文本
    illusion_info["today_choice"] = selected_option
    illusion_info["last_participate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    IllusionData.save_user_illusion_info(user_id, illusion_info)
    
    # 更新问题统计数据
    IllusionData.update_question_stats(illusion_info["question_index"], choice_num - 1)
    
    # 获取当前问题的统计数据
    question_data = IllusionData.get_questions()["questions"][illusion_info["question_index"]]
    counts = question_data["counts"]
    total_choices = sum(counts)
    
    # 计算当前选择的排名
    sorted_counts = sorted([(i+1, count) for i, count in enumerate(counts)], key=lambda x: -x[1])
    rank_dict = {x[0]: i+1 for i, x in enumerate(sorted_counts)}
    user_rank = rank_dict[choice_num]
    choice_count = counts[choice_num - 1]
    
    # 计算当前选择的占比
    percentage = choice_count / total_choices * 100 if total_choices > 0 else 100
    
    # 根据占比给予奖励
    reward_msg = ""
    if percentage < 30:  # 少数派
        user_rank = convert_rank(user_info['level'])[0]
        exp_reward = int(user_info["exp"] * 0.01 * min(0.1 * user_rank, 1))
        sql_message.update_exp(user_id, exp_reward)
        reward_msg = f"你的选择是第{user_rank}受欢迎的(第{choice_count}位道友)，获得修为：{number_to(exp_reward)}点"
    elif 30 <= percentage <= 70:  # 中数派
        item_msg = _give_random_item(user_id, user_info["level"])
        reward_msg = f"你的选择是第{user_rank}受欢迎的(第{choice_count}位道友)，获得：{item_msg}"
    else:  # 多数派
        stone_reward = random.randint(1000000, 10000000)
        sql_message.update_ls(user_id, stone_reward, 1)
        reward_msg = f"你的选择是第{user_rank}受欢迎的(第{choice_count}位道友)，获得灵石：{number_to(stone_reward)}枚"
    
    msg = (
        f"\n═══  幻境寻心  ════\n"
        f"今日问题：{question}\n"
        f"你的选择：{selected_option}\n"
        f"════════════\n"
        f"{reward_msg}\n"
        f"════════════\n"
        f"每日8点重置，请明日再来！"
    )
    update_statistics_value(user_id, "寻心次数")
    await handle_send(bot, event, msg)
    await illusion_heart.finish()

@illusion_reset.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置幻境数据(管理员) - 重置玩家数据和问题统计数据"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    IllusionData.reset_all_data()
    
    msg = "所有用户的幻境寻心数据和问题统计数据已重置！"
    await handle_send(bot, event, msg)
    await illusion_reset.finish()

@illusion_clear.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空幻境数据(管理员) - 仅清空玩家数据"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    IllusionData.reset_player_data_only()
    
    msg = "所有用户的幻境寻心数据已清空！"
    await handle_send(bot, event, msg)
    await illusion_clear.finish()

def _give_random_item(user_id, user_level):
    """给予随机物品奖励"""    
    # 随机选择物品类型
    item_types = ["功法", "神通", "药材", "法器", "防具", "身法", "瞳术"]
    item_type = random.choice(item_types)
    if item_type in ["法器", "防具", "辅修功法", "身法", "瞳术"]:
        base_rank = max(convert_rank(user_level)[0], 16)
    else:
        base_rank = max(convert_rank(user_level)[0] - 22, 10)
    zx_rank = random.randint(base_rank, min(base_rank + 35, 54))
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
