from nonebot import on_command
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.utils import handle_send, check_user
import random
import time
from datetime import datetime
import json
import os
from pathlib import Path

# 创建数据存储目录
DATA_PATH = Path(__file__).parent / "morning_night_data"
os.makedirs(DATA_PATH, exist_ok=True)

# 加载或初始化计数数据
def load_count_data():
    count_file = DATA_PATH / "count_data.json"
    if count_file.exists():
        with open(count_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return {
            "morning_count": 0,
            "night_count": 0,
            "morning_users": {},  # 存储用户ID和日期
            "night_users": {}     # 存储用户ID和日期
        }

def save_count_data(data):
    count_file = DATA_PATH / "count_data.json"
    with open(count_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

async def reset_data_by_time():
    """根据当前时间重置早安或晚安数据"""
    now = datetime.now()
    hour = now.hour
    
    data = load_count_data()
    
    # 午夜0点重置早安数据
    if hour == 0:
        data["morning_count"] = 0
        data["morning_users"] = {}
        save_count_data(data)
        return "morning"
    # 中午12点重置晚安数据
    elif hour == 12:
        data["night_count"] = 0
        data["night_users"] = {}
        save_count_data(data)
        return "night"
    else:
        return None

def has_user_triggered(user_id, is_morning=True):
    """检查用户是否已经触发过"""
    data = load_count_data()
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    if is_morning:
        user_data = data.get("morning_users", {})
    else:
        user_data = data.get("night_users", {})
    
    # 检查用户是否在今天已经触发过
    user_date = user_data.get(str(user_id))
    return user_date == current_date

def mark_user_triggered(user_id, is_morning=True):
    """标记用户已触发"""
    data = load_count_data()
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    if is_morning:
        data["morning_users"][str(user_id)] = current_date
        data["morning_count"] += 1
    else:
        data["night_users"][str(user_id)] = current_date
        data["night_count"] += 1
    
    save_count_data(data)

def get_current_count(is_morning=True):
    """获取当前计数"""
    data = load_count_data()
    return data["morning_count"] if is_morning else data["night_count"]

# 运势类型和对应的星数
FORTUNE_TYPES = {
    "大凶": 1,
    "凶": 2,
    "小凶": 3,
    "小吉": 4,
    "吉": 5,
    "大吉": 6
}

# 运势描述库
FORTUNE_DESCRIPTIONS = {
    "大凶": [
        "今日诸事不宜，宜闭关修炼，静待时机",
        "运势低迷，恐有劫难，谨慎行事",
        "乌云遮日，需防小人暗算，守成为上",
        "道心不稳，易生心魔，宜静心养性",
        "天降灾厄，宜诵经祈福，化解凶煞",
        "黑云压顶，魔气缠身，今日不宜外出历练",
        "心魔作祟，易走火入魔，切记固守本心",
        "劫数将至，宜寻道友护法，共渡难关",
        "灵气紊乱，修炼难有寸进，不如休养生息",
        "煞星临门，诸事不顺，静待转机方为上策"
    ],
    "凶": [
        "运势不佳，行事多阻，需加倍努力",
        "前路多艰，宜稳扎稳打，不可冒进",
        "恐有破财之灾，投资需谨慎",
        "人际关系紧张，谨言慎行为上",
        "修炼易遇瓶颈，需耐心突破",
        "灵气稀薄，修炼事倍功半，需持之以恒",
        "易遇心魔干扰，当以清心咒护体",
        "外出易遇劫匪，贵重法宝需妥善保管",
        "丹炉易炸，炼丹需格外小心",
        "易与同门产生口角，退一步海阔天空"
    ],
    "小凶": [
        "稍有阻碍，但努力可克服",
        "运势平平，需提防意外变故",
        "小有波折，无伤大雅",
        "宜守不宜攻，稳中求进",
        "需注意健康，适当休息",
        "修炼略感吃力，但坚持必有收获",
        "外出遇小雨，记得带避水符",
        "小人在背后议论，清者自清不必理会",
        "灵兽略显烦躁，需多加安抚",
        "法宝略有损耗，记得及时温养"
    ],
    "小吉": [
        "运势尚可，小有机缘",
        "平稳之中暗藏机遇",
        "勤勉修炼，必有所得",
        "人际关系和谐，易得助力",
        "稍有收获，宜知足常乐",
        "偶得灵光一闪，修炼略有心得",
        "路边捡到低阶灵石，小有收获",
        "同门相助，修炼难题迎刃而解",
        "丹成中品，虽非上乘但已足用",
        "偶遇灵兽示好，或为祥瑞之兆"
    ],
    "吉": [
        "运势亨通，诸事顺遂",
        "机缘巧合，易得贵人相助",
        "修炼事半功倍，进步神速",
        "财源广进，投资有利",
        "心想事成，美满如意",
        "灵气充沛，修炼如有神助",
        "偶得前辈指点，茅塞顿开",
        "寻得灵草仙材，炼丹成功率大增",
        "法宝通灵，威力更胜往昔",
        "悟得新法术，实力大进"
    ],
    "大吉": [
        "紫气东来，祥瑞之兆",
        "天降洪福，万事如意",
        "修炼突飞猛进，境界提升",
        "奇遇连连，仙缘深厚",
        "福星高照，逢凶化吉",
        "天降异象，得天道眷顾",
        "偶获上古传承，道途一片光明",
        "炼制出九转金丹，修为暴涨",
        "得仙器认主，实力突飞猛进",
        "顿悟天道至理，境界突破在即"
    ]
}

# 加载运势数据
def load_fortune_data():
    fortune_file = DATA_PATH / "fortune_data.json"
    if fortune_file.exists():
        with open(fortune_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return {}

# 保存运势数据
def save_fortune_data(data):
    fortune_file = DATA_PATH / "fortune_data.json"
    with open(fortune_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# 生成运势星号显示
def generate_fortune_stars(fortune_type):
    stars_count = FORTUNE_TYPES[fortune_type]
    # 大凶显示红色星，大吉显示金色星，其他显示黄色星
    
    stars = "★" * stars_count
    return f"{stars}"

# 获取用户今日运势
def get_user_fortune(user_id):
    data = load_fortune_data()
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 检查用户是否有今日的运势记录
    user_key = str(user_id)
    if user_key in data and data[user_key]["date"] == current_date:
        return data[user_key]["fortune"]
    
    # 生成新的运势（加权概率，吉兆更容易出现）
    fortune_options = list(FORTUNE_TYPES.keys())
    weights = [0.05, 0.1, 0.15, 0.25, 0.3, 0.15]  # 概率权重
    fortune_type = random.choices(fortune_options, weights=weights, k=1)[0]
    description = random.choice(FORTUNE_DESCRIPTIONS[fortune_type])
    
    # 保存用户运势
    data[user_key] = {
        "date": current_date,
        "fortune": {
            "type": fortune_type,
            "description": description,
            "stars": generate_fortune_stars(fortune_type)
        }
    }
    save_fortune_data(data)
    
    return data[user_key]["fortune"]

good_morning = on_command("早安", aliases={"早上好", "早啊", "早", "晨安", "道友早"}, priority=30, block=True)
good_night = on_command("晚安", aliases={"晚安啦", "睡觉", "睡了", "安寝", "道友晚安"}, priority=30, block=True)
cute_command = on_command("可爱", aliases={"好可爱", "萌萌哒", "可爱捏", "卡哇伊", "萌"}, priority=30, block=True)
hello_command = on_command("你好", aliases={"你好呀", "嗨", "hello", "hi", "哈喽", "道友你好"}, priority=30, block=True)
thanks_command = on_command("谢谢", aliases={"多谢", "感谢", "thx", "谢啦", "感恩"}, priority=30, block=True)
bye_command = on_command("再见", aliases={"拜拜", "再会", "bye", "goodbye", "告辞", "后会有期"}, priority=30, block=True)
how_are_you = on_command("你好吗", aliases={"最近怎么样", "怎么样", "howareyou", "近来可好", "别来无恙"}, priority=30, block=True)
weather_command = on_command("天气", aliases={"今天天气", "天气预报", "天象", "气象"}, priority=30, block=True)
time_command = on_command("时间", aliases={"现在几点", "几点钟", "当前时间", "时辰", "什么时辰了"}, priority=30, block=True)
eat_command = on_command("吃饭", aliases={"吃什么", "饿了", "干饭", "用膳", "进食"}, priority=30, block=True)
study_command = on_command("学习", aliases={"修炼", "用功", "study", "修行", "练功"}, priority=30, block=True)
work_command = on_command("工作", aliases={"上班", "打工", "work", "劳作", "当值"}, priority=30, block=True)
rest_command = on_command("休息", aliases={"歇会", "放松", "relax", "小憩", "休憩"}, priority=30, block=True)
joke_command = on_command("讲个笑话", aliases={"笑话", "来点乐子", "joke", "搞笑", "逗我笑"}, priority=30, block=True)
encourage_command = on_command("加油", aliases={"鼓励", "fighting", "冲鸭", "努力", "加把劲"}, priority=30, block=True)
# 新增命令
crazy_thursday = on_command("疯狂星期四", aliases={"周四", "肯德基", "KFC", "礼拜四"}, priority=30, block=True)
funny_story = on_command("讲个段子", aliases={"段子", "来段搞笑的", "趣事", "幽默"}, priority=30, block=True)
love_sentence = on_command("土味情话", aliases={"情话", "土味", "表白", "说情话"}, priority=30, block=True)
fortune_command = on_command("今日运势", aliases={"运势", "占卜", "算命", "卜卦", "求签"}, priority=30, block=True)

# 根据时间获取不同的问候语
def get_morning_message_by_time(count):
    now = datetime.now()
    hour = now.hour
    
    if 5 <= hour < 8:
        time_msg = "清晨"
        messages = [
            f"道友{time_msg}好！你是今日第{count}个早起修炼的道友，晨光初露，正是吸纳紫气的好时机！",
            f"{time_msg}安好！第{count}位勤勉的道友，愿晨辉助你道途精进！",
            f"道友{time_msg}吉祥！你是今日第{count}个起身的道友，朝阳伴你修行路~",
            f"{time_msg}好！第{count}位道友已就位，一起迎接这灵气充沛的早晨吧！",
            f"道友{time_msg}安！第{count}个开始今日修炼，晨露未干，道心已明！",
            f"{time_msg}曦微露，第{count}位道友已开始今日修行，勤勉可嘉！",
            f"道友{time_msg}好！第{count}个吸纳朝阳紫气，今日必有所获！",
            f"{time_msg}时分，第{count}位道友踏上修行路，愿仙途顺利！",
            f"道友{time_msg}安！第{count}个问早，晨钟暮鼓正是修行时！",
            f"{time_msg}好！第{count}位道友，一日之计在于晨，修炼正当时！"
        ]
    elif 8 <= hour < 12:
        time_msg = "上午"
        messages = [
            f"道友{time_msg}好！你是今日第{count}个问安的道友，日上三竿，修炼正当时！",
            f"{time_msg}安好！第{count}位道友，阳光正好，适合外出历练呢~",
            f"道友{time_msg}吉祥！第{count}个开始今日修行，愿道友道法精进！",
            f"{time_msg}好！第{count}位道友，修炼之余别忘了用些灵食补充体力~",
            f"道友{time_msg}安！第{count}个问早，今日也要努力修炼哦！",
            f"{time_msg}时分，第{count}位道友开始修行，愿有所成！",
            f"道友{time_msg}好！第{count}个问安，修炼如逆水行舟，不进则退！",
            f"{time_msg}安！第{count}位道友，勤修不辍，方得大道！",
            f"道友{time_msg}吉祥！第{count}个开始修炼，愿得天道眷顾！",
            f"{time_msg}好！第{count}位道友，修炼之路贵在坚持！"
        ]
    else:
        # 非早晨时间段的早安回复
        time_msg = get_time_period(hour)
        messages = [
            f"道友现在已是{time_msg}时分，才来说早安吗？不过还是第{count}个问早的呢~",
            f"{time_msg}安好！虽然是第{count}个说早安的，但修炼不分早晚，加油！",
            f"道友，现在都是{time_msg}了才早安？不过你是第{count}个，也算有心了~",
            f"{time_msg}时分道早安？第{count}位特别的道友，愿你修行顺利！",
            f"道友{time_msg}好！虽然是第{count}个说早安的，但心意收到啦~",
            f"{time_msg}才道早安？第{count}位道友真是与众不同呢！",
            f"道友{time_msg}安！第{count}个问早，虽晚但仍显诚意！",
            f"{time_msg}时分，第{count}位道友问早，修炼之心可嘉！",
            f"道友{time_msg}好！第{count}个说早安，有心不怕晚！",
            f"{time_msg}安！第{count}位道友，早安虽迟但到！"
        ]
    
    return random.choice(messages)

def get_night_message_by_time(count):
    now = datetime.now()
    hour = now.hour
    
    if 18 <= hour < 22:
        time_msg = "傍晚"
        messages = [
            f"道友{time_msg}安！你是今日第{count}个道晚安的道友，暮色渐浓，好好休息~",
            f"{time_msg}安好！第{count}位道友，晚霞相伴，愿你好梦！",
            f"道友{time_msg}吉祥！第{count}个准备休息，养足精神明日再战修仙路~",
            f"{time_msg}好！第{count}位道友，今日修行辛苦了，早些安歇吧~",
            f"道友{time_msg}安！第{count}个道晚安，月华初上，正好入定冥想~",
            f"{time_msg}时分，第{count}位道友准备休息，明日再续仙缘！",
            f"道友{time_msg}安！第{count}个道晚安，暮鼓晨钟修行路！",
            f"{time_msg}好！第{count}位道友，休息是为了更好的修炼！",
            f"道友{time_msg}吉祥！第{count}个安寝，愿好梦相伴！",
            f"{time_msg}安！第{count}位道友，今日修行圆满，好好休息！"
        ]
    elif 22 <= hour < 24:
        time_msg = "夜晚"
        messages = [
            f"道友{time_msg}安！你是今日第{count}个道晚安的道友，夜深了，好好休息~",
            f"{time_msg}安好！第{count}位道友，星辰为伴，愿你好梦连连！",
            f"道友{time_msg}吉祥！第{count}个准备入睡，明日继续追寻大道！",
            f"{time_msg}好！第{count}位道友，修炼重要，休息更重要呢~",
            f"道友{time_msg}安！第{count}个道晚安，愿月光助你安眠~",
            f"{time_msg}深沉，第{count}位道友安歇，养精蓄锐待明日！",
            f"道友{time_msg}安！第{count}个道晚安，星河为被好入眠！",
            f"{time_msg}好！第{count}位道友，子时将至，宜安寝养神！",
            f"道友{time_msg}吉祥！第{count}个休息，愿梦境如仙境！",
            f"{time_msg}安！第{count}位道友，今日修行辛苦，好好休息！"
        ]
    elif 0 <= hour < 5:
        time_msg = "深夜"
        messages = [
            f"道友{time_msg}安！你是今日第{count}个道晚安的道友，这么晚才休息吗？",
            f"{time_msg}安好！第{count}位夜修的道友，注意劳逸结合哦~",
            f"道友{time_msg}吉祥！第{count}个深夜道晚安，修炼虽好，也要保重身体！",
            f"{time_msg}好！第{count}位道友，子时已过，该休息啦~",
            f"道友{time_msg}安！第{count}个道晚安，愿你能有个好梦~",
            f"{time_msg}时分，第{count}位道友才休息，修炼虽重要但勿忘养生！",
            f"道友{time_msg}安！第{count}个道晚安，夜深人静好修行但也需休息！",
            f"{time_msg}好！第{count}位道友，熬夜伤身，宜适当休息！",
            f"道友{time_msg}吉祥！第{count}个安寝，虽晚但仍需保证睡眠！",
            f"{time_msg}安！第{count}位道友，深夜修炼虽好但勿过度！"
        ]
    else:
        # 非夜晚时间段的晚安回复
        time_msg = get_time_period(hour)
        messages = [
            f"道友现在还是{time_msg}呢，就要说晚安了吗？不过你是第{count}个~",
            f"{time_msg}安好！虽然是第{count}个说晚安的，但休息也很重要呢~",
            f"道友，现在才是{time_msg}就要晚安？第{count}位特别的道友~",
            f"{time_msg}时分道晚安？第{count}位道友，愿你有个好休息~",
            f"道友{time_msg}好！虽然是第{count}个说晚安的，但心意收到啦~",
            f"{time_msg}道晚安？第{count}位道友真是与众不同呢！",
            f"道友{time_msg}安！第{count}个说晚安，休息之心可嘉！",
            f"{time_msg}时分，第{count}位道友道晚安，虽早但仍显诚意！",
            f"道友{time_msg}好！第{count}个说晚安，有心不怕早！",
            f"{time_msg}安！第{count}位道友，晚安虽早但到！"
        ]
    
    return random.choice(messages)

def get_time_period(hour):
    if 5 <= hour < 8:
        return "清晨"
    elif 8 <= hour < 12:
        return "上午"
    elif 12 <= hour < 14:
        return "中午"
    elif 14 <= hour < 18:
        return "下午"
    elif 18 <= hour < 22:
        return "傍晚"
    elif 22 <= hour < 24:
        return "夜晚"
    else:
        return "深夜"

# 所有消息数组
CUTE_MESSAGES = [
    "道友谬赞了~ 你也很可爱呢！",
    "嘿嘿，被夸可爱了，有点不好意思呢~",
    "在修仙界，可爱也是一种道心境界呢！",
    "谢谢道友夸奖，愿你今日道心澄明~",
    "(*/ω＼*) 道友突然这么说，让人家有点害羞呢",
    "可爱？道友过奖了，不过是修炼时的一点灵气外泄罢了~",
    "道友这么说，我的法器都要害羞得发光了呢！",
    "在修仙界，可爱可是很重要的天赋哦！",
    "谢谢夸奖~ 不过比起可爱，我更希望道友夸我修为高深呢",
    "道友嘴真甜，是不是偷吃了蜜糖灵果？",
    "可爱捏~ 道友也很萌萌哒呢！",
    "被夸可爱了，今天修炼都要更有动力了！",
    "道友这么说，我的本命法宝都要开心得旋转了~",
    "在下一介修士，何德何能受此夸奖~",
    "道友过誉了，不过是皮相而已，修行重在内心~",
    "谢谢~ 不过修仙之人当以修为论高低，可爱只是附加呢",
    "道友这么会说话，一定很受灵兽欢迎吧！",
    "被夸可爱了，感觉灵气都运转得更顺畅了呢~",
    "道友真是慧眼识珠呢！",
    "嘿嘿，偷偷告诉你，我每天都有用灵气保养哦~"
]

HELLO_MESSAGES = [
    "道友你好！今日可要一起探讨修仙心得？",
    "你好呀~ 看来又有一位道友踏上修仙之路了！",
    "嗨！道友今日气色不错，想必修为有所精进吧~",
    "Hello！修仙之路漫漫，有道友相伴真好！",
    "道友安好！愿你我都能早日得道成仙~",
    "道友你好！今日灵气充沛，正是修炼好时机！",
    "哈喽~ 又见面了道友，最近修炼可还顺利？",
    "道友安！今日可有所悟？",
    "你好呀！修仙路上有你相伴，真是不寂寞呢~",
    "道友你好！观你气色，今日必有好事发生！",
    "嗨~ 今日也要一起努力修炼哦！",
    "道友安好！愿仙途顺利，早日飞升~",
    "你好！不知道友今日准备修炼何种功法？",
    "道友你好！看你印堂发亮，定是修为有所突破！",
    "哈喽！修仙之路虽艰，但有志者事竟成！",
    "道友安！今日天气晴朗，适合外出历练呢~",
    "你好呀！愿道友今日修炼事半功倍！",
    "道友你好！观星象得知，今日宜修炼水系法术~",
    "嗨~ 又见面了！你的修为似乎又精进了呢！",
    "道友安好！今日也要保持道心澄明哦~"
]

THANKS_MESSAGES = [
    "道友客气了，举手之劳何足挂齿~",
    "不用谢！修仙之路互相扶持是应该的！",
    "能帮到道友是我的荣幸！",
    "嘿嘿，被道友感谢了，有点开心呢~",
    "区区小事，不足言谢！道友太见外了~",
    "道友何必言谢，同是修仙路上人~",
    "能助道友一臂之力，是我的缘分！",
    "不用客气~ 他日我若需帮助，还望道友相助！",
    "道友太客气了，这点小事不足挂齿~",
    "举手之劳，何足道谢~",
    "能帮到道友，我也很开心呢！",
    "道友不必多礼，修仙之人本应互相帮助~",
    "区区小事，道友如此客气反倒让我不好意思了~",
    "能得道友一句感谢，比获得灵丹妙药还开心！",
    "道友言重了，这只是分内之事~",
    "不用谢~ 希望他日能在修仙路上再次相助！",
    "道友的感谢我收下了，愿你好运常伴~",
    "能帮助道友，说明你我缘分不浅呢！",
    "道友太客气了，这点小事何足挂齿~",
    "不用谢！愿这份善意能在修仙路上传递下去~"
]

BYE_MESSAGES = [
    "道友再见，期待下次相会！",
    "再会了，愿道友一路平安~",
    "拜拜！记得常回来看看哦！",
    "青山不改，绿水长流，咱们后会有期！",
    "告辞了，道友保重！",
    "道友慢走，愿仙途顺利！",
    "再会了，期待他日重逢时道友已得大道！",
    "拜拜~ 记得勤加修炼哦！",
    "道友保重，修行路上多珍重！",
    "后会有期！愿下次相见时你我皆有所成！",
    "告辞了，道友记得按时修炼~",
    "再会！愿天道眷顾与你~",
    "道友慢走，有空常来论道~",
    "拜拜！愿你一路奇遇连连！",
    "再见了，期待下次论道之约！",
    "道友保重，修行路上切记固守本心！",
    "告辞了，愿你好运常伴~",
    "再会！记得我等的修炼之约哦！",
    "道友慢走，愿你早日得道成仙！",
    "拜拜~ 修炼上有问题随时来找我！"
]

HOW_ARE_YOU_MESSAGES = [
    "多谢道友关心，我一切安好！你呢？",
    "最近在潜心修炼，感觉修为又精进了不少~",
    "托道友的福，一切顺利！不知道友近来如何？",
    "正在参悟天道，略有心得~",
    "还好还好，就是有点想念道友了呢~",
    "近日修炼小有所成，心情甚好！道友如何？",
    "一切如常，每日修炼不辍~ 道友近来可好？",
    "最近偶得奇遇，修为有所突破！道友呢？",
    "多谢关心！正在闭关冲击新境界~",
    "还好，就是修炼上遇到些瓶颈... 道友可有心得？",
    "托道友洪福，近日诸事顺遂！",
    "正在游历历练，收获颇多！道友别来无恙？",
    "近日炼丹成功率高了不少，心情大好！",
    "一切安好，只是思念与道友论道的时光~",
    "最近在研习新法术，略有小成！道友如何？",
    "多谢道友挂念！近日道心澄明，修炼顺利~",
    "还好，只是有些挂念道友~",
    "近日得高人指点，茅塞顿开！道友近来可好？",
    "托道友的福，修炼一路顺畅！",
    "正在参悟剑道，感觉快要突破了！道友呢？"
]

WEATHER_MESSAGES = [
    "今日天气晴朗，灵气充沛，正是外出历练的好时机！",
    "观天象得知，今日有雨，道友出门记得带伞~",
    "微风和煦，阳光正好，适合在洞府外打坐修炼！",
    "天降祥瑞，紫气东来，今日必有好事发生！",
    "天气变幻莫测，犹如修仙之路，道友且行且珍惜~",
    "今日乌云密布，恐有雷雨，道友修炼时小心雷电~",
    "风和日丽，灵气盎然，正是修炼大好时机！",
    "观星象得知，今日宜修炼火系法术~",
    "细雨绵绵，水灵气充沛，适合修炼水系功法！",
    "今日天高云淡，适合御剑飞行，游览名山大川~",
    "狂风大作，风灵气活跃，修炼风系法术事半功倍！",
    "大雪纷飞，冰灵气充沛，修炼冰系功法正当时！",
    "雷电交加，雷灵气旺盛，修炼雷法大有裨益！",
    "雾霭朦胧，神秘莫测，今日宜参悟天机~",
    "彩虹横空，祥瑞之兆，今日必有奇遇！",
    "月明星稀，夜晚灵气纯净，适合夜修~",
    "烈日当空，阳灵气充沛，修炼阳性功法最佳！",
    "阴雨连绵，阴灵气活跃，修炼阴系功法正当时！",
    "霞光万道，紫气东来，今日修炼必有所成！",
    "天气变幻无常，恰似修仙之路，道友当随机应变~"
]

def get_time_message():
    now = datetime.now()
    hour = now.hour
    if 5 <= hour < 8:
        time_msg = "清晨"
        additional = "晨光初露，正是修炼好时机！"
    elif 8 <= hour < 12:
        time_msg = "上午"
        additional = "日上三竿，修炼正当时！"
    elif 12 <= hour < 14:
        time_msg = "中午"
        additional = "午时阳气最盛，宜稍作休息~"
    elif 14 <= hour < 18:
        time_msg = "下午"
        additional = "午后时光，适合研读功法秘籍~"
    elif 18 <= hour < 22:
        time_msg = "晚上"
        additional = "夜幕降临，宜打坐冥想~"
    else:
        time_msg = "深夜"
        additional = "子时夜深，宜安寝养神~"
    
    return f"现在时间是：{now.strftime('%Y年%m月%d日 %H:%M:%S')}，{time_msg}时分。{additional}"

EAT_MESSAGES = [
    "道友饿了吗？要不要尝尝我刚炼制的辟谷丹？",
    "修仙之人当以灵气为食，不过偶尔享受人间美食也不错呢~",
    "推荐道友试试灵食堂的灵米饭，对修炼大有裨益！",
    "我这儿有些灵果，道友要不要尝尝？",
    "吃饭时间到！吃饱了才有力气修炼嘛~",
    "刚采了些灵菇，道友可要一起来尝尝鲜？",
    "修炼之余也要记得用膳，身体是修仙的本钱！",
    "推荐道友试试百花酿，既能充饥又能增长灵气~",
    "我新研制了一种灵食，道友可愿做第一个品尝的人？",
    "修仙之人虽可辟谷，但美食也是一种享受呢~",
    "道友喜欢什么口味的灵食？甜的还是咸的？",
    "刚出炉的灵丹...啊不对，是灵食，道友要尝尝吗？",
    "用膳时间到！今日特供：灵鸡炖仙菇~",
    "道友可知，饮食也是一种修行？",
    "推荐道友试试清心斋的素斋，清淡养生~",
    "修炼消耗大，要及时补充营养哦！",
    "我刚从秘境采了些仙果，道友可要分享？",
    "用膳时间，休息一下再继续修炼吧~",
    "道友喜欢喝茶吗？我这儿有上好的灵茶~",
    "吃饭不积极，修炼有问题！道友快来用膳~"
]

STUDY_MESSAGES = [
    "道友勤学苦练，必能早日得道！",
    "修炼之路漫漫，持之以恒方见真章~",
    "看来道友又要闭关修炼了，祝你有所突破！",
    "学习修仙知识很重要，但也要注意劳逸结合哦~",
    "有什么修炼上的难题，我们可以一起探讨！",
    "道友如此勤勉，他日必成大器！",
    "修炼如逆水行舟，不进则退，共勉之！",
    "看来道友又要精进了，真是令人期待！",
    "修行路上有道友这样的同道，真是幸事！",
    "勤修不辍，方得大道！道友加油！",
    "修炼遇到瓶颈时，不妨换个思路~",
    "道友的勤奋令人敬佩！",
    "修行重在感悟，有时放松一下反而更有收获~",
    "看来道友又要突破新境界了，恭喜！",
    "修炼之路虽艰，但有志者事竟成！",
    "道友如此用功，天道必不负你！",
    "修行贵在坚持，日积月累必有所成~",
    "看来道友道心坚定，令人钦佩！",
    "修炼之余也不要忘了巩固基础哦~",
    "道友的进取精神值得学习！"
]

WORK_MESSAGES = [
    "道友辛苦了，修炼之余也要适当休息~",
    "工作虽重要，但别忘了每日的修炼功课哦！",
    "愿道友工作顺利，早日攒够修炼资源！",
    "打工修仙两不误，道友真是勤奋啊！",
    "工作累了就来修炼一会儿，转换下心情~",
    "道友如此勤劳，天道必会眷顾！",
    "工作也是修行的一种方式呢~",
    "愿道友工作顺心，有余力修炼~",
    "打工赚灵石，修炼涨修为，两全其美！",
    "工作虽忙，也不要荒废修行哦~",
    "道友真是能者多劳啊！",
    "工作之余记得打坐调息，恢复精力~",
    "愿道友早日实现财务自由，专心修仙！",
    "工作也是炼心的一种方式呢~",
    "道友如此勤奋，必能早日得道！",
    "工作辛苦啦！记得犒劳一下自己~",
    "愿道友工作顺利，有多余时间修炼~",
    "打工赚来的灵石要好好利用在修炼上哦！",
    "工作也是积累功德的过程呢~",
    "道友平衡工作与修炼的能力令人钦佩！"
]

REST_MESSAGES = [
    "道友确实该休息一下了，劳逸结合最重要~",
    "休息是为了走更长的修仙路，好好放松吧！",
    "需要我给道友泡杯灵茶吗？有助于放松心神~",
    "打坐冥想也是很好的休息方式呢！",
    "好好休息，养精蓄锐后再继续修炼！",
    "道友辛苦了，是该好好休息一下~",
    "休息时间到！让身心都放松一下吧~",
    "适当的休息能让修炼事半功倍哦！",
    "道友累了吧？休息一下再继续~",
    "劳逸结合，方能长久~",
    "休息也是修行的一部分呢！",
    "让疲惫的身心得到恢复，明日再战！",
    "道友脸色略显疲惫，确实该休息了~",
    "休息时间，让灵气自然运转调息~",
    "好好休息，待精力充沛再继续修炼！",
    "道友懂得休息，说明修行已有心得~",
    "休息是为了更好的突破！",
    "让心神放松，道心会更加澄明~",
    "适当的休息能让修炼更有效率！",
    "道友休息时，灵气也在自动运转温养呢~"
]

JOKE_MESSAGES = [
    "为什么修仙者不用手机？因为他们的传音术比5G还快！",
    "有个修士去集市买飞剑，老板问：要什么款的？修士说：御剑飞行不卡顿的！",
    "两个修士比试，一个说：我能让山河变色！另一个说：我能让你话费欠费！",
    "为什么炼丹师总是很冷静？因为他们习惯慢火细炖~",
    "有个弟子问师尊：修仙最重要的是什么？师尊答：最重要的是先充个VIP！",
    "现代修仙界最流行的app是什么？『修仙宝』和『灵气银行』！",
    "为什么修仙者考试从不作弊？因为他们会用天眼通！",
    "有个修士网购飞剑，给了差评：'说好的御剑飞行，结果卡顿掉帧！'",
    "修仙界最尴尬的事：御剑飞行时没信号，导航失灵迷路了...",
    "为什么修仙者喜欢穿古装？因为现代服装没有属性加成！",
    "有个弟子抱怨：'师尊，为什么我修炼这么久还是练气期？'师尊：'你充钱了吗？'",
    "现代炼丹师最头疼的事：丹方被注册专利，不能随意炼制了...",
    "为什么修仙者不用支付宝？因为他们交易都用灵石扫码！",
    "最让修仙者崩溃的瞬间：正在渡劫时，手机没电了...",
    "有个修士去医院：'医生，我修炼走火入魔了。'医生：'重启试试？'",
    "为什么修仙者不用闹钟？因为他们的生物钟比原子钟还准！",
    "现代修仙者最常用的社交软件：『道友圈』和『仙信』~",
    "有个弟子问：'师尊，为什么我总是遇到心魔？'师尊：'因为你没装杀毒软件！'",
    "最让炼丹师尴尬的事：炼出的丹药被误认为是巧克力豆...",
    "为什么修仙者不用健身房？因为他们修炼比健身效果更好！"
]

ENCOURAGE_MESSAGES = [
    "道友加油！你一定能突破瓶颈的！",
    "坚持就是胜利，修仙之路贵在持之以恒！",
    "加油！我相信道友的天赋和努力！",
    "前路虽艰，但道友定能披荆斩棘，直达仙途！",
    "一起加油！我们互相督促，共同进步！",
    "道友莫灰心，天道酬勤！",
    "加油！困难只是暂时的，成功就在前方！",
    "相信你自己，你比想象中更强大！",
    "道友坚持住，黎明前的黑暗最是难熬！",
    "加油！每一次努力都在为飞升积累能量！",
    "道友勿弃，持之以恒必有所成！",
    "加油！你的努力天道都看在眼里！",
    "道友挺住，突破就在眼前！",
    "加油！修炼路上没有白费的努力！",
    "道友坚持！风雨过后必见彩虹！",
    "加油！你的潜力远未完全发掘！",
    "道友勿躁，静心修炼必有突破！",
    "加油！每一次失败都是成功的垫脚石！",
    "道友坚持！天道不会辜负有心人！",
    "加油！你离成功只差最后一步！"
]

CRAZY_THURSDAY_MESSAGES = [
    "道友，今天是疯狂星期四！V我50灵石，待我飞升后带你一起享仙福！",
    "修仙之人也要享受凡间美味！今天是疯狂星期四，谁请我吃肯德基？",
    "闻到香味了吗？不是灵丹妙药，是疯狂星期四的炸鸡香气！",
    "修炼累了？不如来份疯狂星期四补充体力，继续冲击金丹期！",
    "今日宜吃鸡，忌辟谷！疯狂星期四，道友一起吗？",
    "道友，疯狂星期四到了！V我50，他日我成仙了带你一起飞！",
    "修仙之人也要食人间烟火！疯狂星期四，求赞助~",
    "闻到那股香气了吗？是疯狂星期四在召唤！",
    "修炼消耗大，需要疯狂星期四补充能量！",
    "道友，今天疯狂星期四，请我吃炸鸡，传授你独家心法！",
    "疯狂星期四，修仙者也要放松一下嘛~",
    "道友，肯德基疯狂星期四，错过要等七天！",
    "修炼重要，但疯狂星期四也很重要！",
    "道友，今日宜破戒，忌辟谷！疯狂星期四走起~",
    "闻到炸鸡香，修炼都没心思了...疯狂星期四啊！",
    "道友，赞助疯狂星期四，传授你快速结丹秘诀！",
    "修仙之路漫漫，不如先享受疯狂星期四~",
    "道友，疯狂星期四，求投喂！他日必报答~",
    "修炼累了？疯狂星期四治愈一切！",
    "道友，今天疯狂星期四，你懂的~"
]

FUNNY_STORY_MESSAGES = [
    "有个修士去应聘，面试官问：你有什么特长？修士答：我会御剑飞行。面试官：我们这是办公室工作...",
    "为什么修仙者考试总是满分？因为他们会作弊...用天眼通！",
    "两个炼丹师比赛，一个炼出了九转金丹，一个炼出了珍珠奶茶，你猜谁赢了？",
    "修仙界最流行的游戏是什么？『今天你渡劫了吗』和『灵石消消乐』！",
    "有个弟子问师尊：为什么我们要打坐？师尊答：因为WIFI信号不好，站着接收不到天道讯号！",
    "现代修仙者最头疼的事：5G信号干扰修炼，灵气被电磁波污染...",
    "有个修士网购飞剑，结果收到货发现是玩具剑，差评：'这怎么御剑飞行？'",
    "为什么修仙者不用空调？因为他们会调节自身温度！",
    "最让修仙者尴尬的瞬间：御剑飞行时被飞机超过...",
    "有个弟子抱怨：'师尊，为什么我修炼这么久还是不会飞？'师尊：'你买的是体验版功法！'",
    "现代炼丹师的新烦恼：丹炉要符合环保标准，不能排放太多烟气...",
    "为什么修仙者喜欢深山老林？因为那里信号差，没人打扰修炼！",
    "有个修士尝试用无人机送快递，结果飞剑把无人机打下来了...",
    "最让修仙者崩溃的事：正在重要闭关时，快递来了...",
    "为什么修仙者不用信用卡？因为他们信用度用灵石衡量！",
    "有个弟子问：'师尊，为什么我修炼总没进步？'师尊：'你该换5G功法了！'",
    "现代修仙界新职业：灵气网络工程师，专门维护修炼信号~",
    "有个修士尝试直播修炼，结果因为内容过于无聊被封号...",
    "为什么修仙者不用健身房？因为他们自己就是移动的健身房！",
    "最让御剑飞行者头疼的事：空中交通堵塞，还要等红绿灯..."
]

LOVE_SENTENCE_MESSAGES = [
    "道友，你知道我和灵石有什么区别吗？灵石在天边，而你在我心里~",
    "我不是在修炼，我是在修炼怎么不喜欢你，可惜走火入魔了...",
    "你是我的本命法宝，丢了你就等于丢了半条命~",
    "如果我有一千年的修为，我会用九百九十九年来想你，剩下一年来见你~",
    "你就像那九转金丹，让我欲罢不能，神魂颠倒~",
    "道友，我练成了心眼神通，但只能看见一个人...就是你！",
    "我不是在打坐，我是在坐想你~",
    "你就像那灵气，让我离不开你~",
    "道友，我愿用千年修行换你一世相伴~",
    "你比飞剑还快，瞬间就击中了我的心~",
    "我不是在炼丹，我是在炼一颗爱你的心~",
    "你就像那天道，让我琢磨不透却又深深着迷~",
    "道友，我愿为你放弃飞升，只求相伴一生~",
    "你比心魔还难摆脱，但我心甘情愿~",
    "我不是在修炼，我是在修一颗爱你的心~",
    "你就像那秘境，让我想一探究竟~",
    "道友，我练成了情丝绕，但只绕你一人~",
    "你比天道还难参透，但我愿用一生来领悟~",
    "我不是在游历，我是在游向你心里~",
    "你就像那本命灯，照亮了我的修仙路~"
]

@good_morning.handle()
async def handle_good_morning(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理早安命令"""
    _, user_info, _ = check_user(event)
    user_id = user_info["user_id"]
    
    # 检查用户是否已经触发过
    if has_user_triggered(user_id, is_morning=True):
        await handle_send(bot, event, "道友，你今天已经道过早安了哦~")
        return
    
    # 标记用户已触发并更新计数
    mark_user_triggered(user_id, is_morning=True)
    
    # 根据时间获取不同的早安消息
    current_count = get_current_count(is_morning=True)
    message = get_morning_message_by_time(current_count)
    await handle_send(bot, event, message)

@good_night.handle()
async def handle_good_night(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理晚安命令"""
    _, user_info, _ = check_user(event)
    user_id = user_info["user_id"]
    
    # 检查用户是否已经触发过
    if has_user_triggered(user_id, is_morning=False):
        await handle_send(bot, event, "道友，你今天已经道过晚安了哦~")
        return
    
    # 标记用户已触发并更新计数
    mark_user_triggered(user_id, is_morning=False)
    
    # 根据时间获取不同的晚安消息
    current_count = get_current_count(is_morning=False)
    message = get_night_message_by_time(current_count)
    await handle_send(bot, event, message)

@cute_command.handle()
async def handle_cute(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理可爱命令"""
    message = random.choice(CUTE_MESSAGES)
    await handle_send(bot, event, message)

@hello_command.handle()
async def handle_hello(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理你好命令"""
    message = random.choice(HELLO_MESSAGES)
    await handle_send(bot, event, message)

@thanks_command.handle()
async def handle_thanks(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理谢谢命令"""
    message = random.choice(THANKS_MESSAGES)
    await handle_send(bot, event, message)

@bye_command.handle()
async def handle_bye(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理再见命令"""
    message = random.choice(BYE_MESSAGES)
    await handle_send(bot, event, message)

@how_are_you.handle()
async def handle_how_are_you(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理你好吗命令"""
    message = random.choice(HOW_ARE_YOU_MESSAGES)
    await handle_send(bot, event, message)

@weather_command.handle()
async def handle_weather(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理天气命令"""
    message = random.choice(WEATHER_MESSAGES)
    await handle_send(bot, event, message)

@time_command.handle()
async def handle_time(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理时间命令"""
    message = get_time_message()
    await handle_send(bot, event, message)

@eat_command.handle()
async def handle_eat(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理吃饭命令"""
    message = random.choice(EAT_MESSAGES)
    await handle_send(bot, event, message)

@study_command.handle()
async def handle_study(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理学习命令"""
    message = random.choice(STUDY_MESSAGES)
    await handle_send(bot, event, message)

@work_command.handle()
async def handle_work(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理工作命令"""
    message = random.choice(WORK_MESSAGES)
    await handle_send(bot, event, message)

@rest_command.handle()
async def handle_rest(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理休息命令"""
    message = random.choice(REST_MESSAGES)
    await handle_send(bot, event, message)

@joke_command.handle()
async def handle_joke(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理笑话命令"""
    message = random.choice(JOKE_MESSAGES)
    await handle_send(bot, event, message)

@encourage_command.handle()
async def handle_encourage(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理加油命令"""
    message = random.choice(ENCOURAGE_MESSAGES)
    await handle_send(bot, event, message)

@crazy_thursday.handle()
async def handle_crazy_thursday(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理疯狂星期四命令"""
    message = random.choice(CRAZY_THURSDAY_MESSAGES)
    await handle_send(bot, event, message)

@funny_story.handle()
async def handle_funny_story(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理讲个段子命令"""
    message = random.choice(FUNNY_STORY_MESSAGES)
    await handle_send(bot, event, message)

@love_sentence.handle()
async def handle_love_sentence(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理土味情话命令"""
    message = random.choice(LOVE_SENTENCE_MESSAGES)
    await handle_send(bot, event, message)

@fortune_command.handle()
async def handle_fortune_command(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理今日运势命令"""
    _, user_info, _ = check_user(event)
    user_id = user_info["user_id"]
    
    # 获取用户运势
    fortune_data = get_user_fortune(user_id)
    
    # 格式化运势消息
    fortune_message = (
        f"✨ {user_info['user_name']} 的今日运势 ✨\n"
        f"运势：{fortune_data['type']} {fortune_data['stars']}\n"
        f"签文：{fortune_data['description']}\n"
        f"愿道友今日修行顺利，福缘深厚！"
    )
    
    await handle_send(bot, event, fortune_message)
