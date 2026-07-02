from copy import deepcopy
from datetime import datetime

from .core import *  # noqa: F401,F403
from ..xiuxian_utils.activity_helpers import as_bool as _as_bool
from ..xiuxian_utils.activity_helpers import default_stage_features as _default_stage_features
from ..xiuxian_activity.service import (
    ACTIVITY_EVENT_CHOICES,
    ACTIVITY_EVENT_LABELS,
    CONFIG_PATH as ACTIVITY_CONFIG_PATH,
    DEFAULT_COLLECT_DROP_EVENTS,
    DEFAULT_ACTIVITY_PASS,
    DEFAULT_ACTIVITY_STAGES,
    DEFAULT_POINT_EVENT_RULES,
    STAGE_FEATURES,
    STAGE_TYPE_LABELS,
    adjust_activity_points,
    adjust_activity_pass_exp,
    adjust_collect_word,
    activity_runtime_state,
    activity_state,
    get_activity_data_overview,
    load_config as load_activity_config,
    parse_reward,
    reset_activity_data,
    save_config as save_activity_config,
)


TASK_TEMPLATES = {
    "daily_sign": {
        "key": "daily_sign",
        "name": "今日问道",
        "description": "完成修仙签到 1 次",
        "target": 1,
        "events": ["sign_in"],
    },
    "daily_out_closing": {
        "key": "daily_out_closing",
        "name": "闭关归元",
        "description": "完成出关 1 次",
        "target": 1,
        "events": ["out_closing"],
    },
    "daily_work": {
        "key": "daily_work",
        "name": "悬赏历练",
        "description": "结算悬赏令 1 次",
        "target": 1,
        "events": ["work"],
    },
    "daily_boss": {
        "key": "daily_boss",
        "name": "斩妖除魔",
        "description": "讨伐世界BOSS 1 次",
        "target": 1,
        "events": ["boss"],
    },
    "daily_sect_task_complete": {
        "key": "daily_sect_task_complete",
        "name": "同门小务",
        "description": "完成宗门任务 1 次",
        "target": 1,
        "events": ["sect_task_complete"],
    },
    "daily_pet_travel_claim": {
        "key": "daily_pet_travel_claim",
        "name": "灵宠归来",
        "description": "领取宠物游历 1 次",
        "target": 1,
        "events": ["pet_travel_claim"],
    },
    "daily_dongfu_harvest": {
        "key": "daily_dongfu_harvest",
        "name": "洞府经营",
        "description": "完成洞府收获 1 次",
        "target": 1,
        "events": ["dongfu_harvest"],
    },
    "daily_map_mission_complete": {
        "key": "daily_map_mission_complete",
        "name": "寻踪问路",
        "description": "完成地图委托 1 次",
        "target": 1,
        "events": ["map_mission_complete"],
    },
    "weekly_sign": {
        "key": "weekly_sign",
        "name": "七日勤修",
        "description": "本周完成修仙签到 6 次",
        "target": 6,
        "events": ["sign_in"],
    },
    "weekly_out_closing": {
        "key": "weekly_out_closing",
        "name": "道心不辍",
        "description": "本周累计修炼 7200 分钟",
        "target": 7200,
        "events": ["cultivation_time"],
    },
    "weekly_work": {
        "key": "weekly_work",
        "name": "悬赏达人",
        "description": "本周结算悬赏令 25 次",
        "target": 25,
        "events": ["work"],
    },
    "weekly_boss": {
        "key": "weekly_boss",
        "name": "伏魔周行",
        "description": "本周讨伐世界BOSS 150 次",
        "target": 150,
        "events": ["boss"],
    },
    "weekly_sect_task_complete": {
        "key": "weekly_sect_task_complete",
        "name": "宗门勤务",
        "description": "本周完成宗门任务 15 次",
        "target": 15,
        "events": ["sect_task_complete"],
    },
    "weekly_map_mission_complete": {
        "key": "weekly_map_mission_complete",
        "name": "山河踏遍",
        "description": "本周完成地图委托 20 次",
        "target": 20,
        "events": ["map_mission_complete"],
    },
    "weekly_elixir_or_dongfu": {
        "key": "weekly_elixir_or_dongfu",
        "name": "炼丹不辍",
        "description": "本周炼丹累计 20 次",
        "target": 20,
        "events": ["mix_elixir_complete"],
    },
    "weekly_dungeon_clear": {
        "key": "weekly_dungeon_clear",
        "name": "组队试炼",
        "description": "本周完成副本 10 次",
        "target": 10,
        "events": ["dungeon_clear"],
    },
}


def _task_row(key: str, reward: str, *, target: int | None = None, name: str | None = None) -> dict:
    task = deepcopy(TASK_TEMPLATES[key])
    if target is not None:
        task["target"] = target
    if name:
        task["name"] = name
    task["reward"] = reward
    return task


ACTIVITY_TEMPLATE_DEFINITIONS = {
    "festival_sign": {
        "name": "通用节日签到",
        "description": "适合多数短期节日活动，含基础日常与周常目标。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "festival_sign",
            "enabled": True,
            "festival_name": "XX节日",
            "name": "XX节日签到活动",
            "description": "节日期间每日签到领取奖励，完成活动日常与周常目标可获得额外补给。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "首日签到", "reward": "灵石x50000"},
                {"day": 2, "name": "次日签到", "reward": "灵石x80000"},
                {"day": 3, "name": "三日签到", "reward": "灵石x100000"},
                {"day": 4, "name": "四日签到", "reward": "灵石x120000"},
                {"day": 5, "name": "五日签到", "reward": "灵石x150000"},
                {"day": 6, "name": "六日签到", "reward": "灵石x180000"},
                {"day": 7, "name": "七日签到", "reward": "灵石x200000,渡厄丹x1"},
            ],
            "milestone_rewards": [
                {"days": 3, "name": "累计三日", "reward": "灵石x150000"},
                {"days": 7, "name": "累计七日", "reward": "灵石x500000,渡厄丹x1"},
            ],
            "daily_tasks": [
                _task_row("daily_sign", "灵石x50000"),
                _task_row("daily_work", "灵石x60000"),
                _task_row("daily_dongfu_harvest", "灵石x60000"),
            ],
            "weekly_tasks": [
                _task_row("weekly_sign", "灵石x200000"),
                _task_row("weekly_work", "灵石x300000"),
            ],
            "extra_rules": [],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "spring_festival": {
        "name": "春节签到",
        "description": "适合春节、元宵等新春周期，偏签到、宗门和洞府经营。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "spring_festival",
            "enabled": True,
            "festival_name": "春节",
            "name": "春节签到活动",
            "description": "新春期间每日签到领取年礼，完成迎春节日任务可获得额外红包奖励。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "迎春礼", "reward": "灵石x100000"},
                {"day": 2, "name": "纳福礼", "reward": "灵石x120000"},
                {"day": 3, "name": "开运礼", "reward": "灵石x150000"},
                {"day": 4, "name": "贺岁礼", "reward": "灵石x180000"},
                {"day": 5, "name": "团圆礼", "reward": "灵石x200000"},
                {"day": 6, "name": "鸿运礼", "reward": "灵石x250000"},
                {"day": 7, "name": "新春大礼", "reward": "灵石x300000,渡厄丹x1"},
            ],
            "milestone_rewards": [
                {"days": 3, "name": "三日红包", "reward": "灵石x200000"},
                {"days": 7, "name": "七日年礼", "reward": "灵石x800000,渡厄丹x2"},
            ],
            "daily_tasks": [
                _task_row("daily_sign", "灵石x80000", name="新春问道"),
                _task_row("daily_sect_task_complete", "灵石x100000", name="同门拜年"),
                _task_row("daily_dongfu_harvest", "灵石x100000", name="洞府纳福"),
                _task_row("daily_pet_travel_claim", "灵石x80000", name="灵宠迎春"),
            ],
            "weekly_tasks": [
                _task_row("weekly_sign", "灵石x500000", name="七日迎春"),
                _task_row("weekly_sect_task_complete", "灵石x600000", name="宗门贺岁"),
                _task_row("weekly_elixir_or_dongfu", "灵石x600000,渡厄丹x1", name="炉火添福"),
            ],
            "extra_rules": ["春节主题奖励可按服务器经济情况调整"],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "anniversary": {
        "name": "周年庆签到",
        "description": "适合服庆、版本周年和长期纪念活动，任务覆盖更完整。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "anniversary",
            "enabled": True,
            "festival_name": "周年庆",
            "name": "周年庆签到活动",
            "description": "周年庆期间每日签到领取庆典补给，完成纪念任务可获得额外奖励。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "庆典补给一", "reward": "灵石x150000"},
                {"day": 2, "name": "庆典补给二", "reward": "灵石x180000"},
                {"day": 3, "name": "庆典补给三", "reward": "灵石x200000"},
                {"day": 4, "name": "庆典补给四", "reward": "灵石x220000"},
                {"day": 5, "name": "庆典补给五", "reward": "灵石x250000"},
                {"day": 6, "name": "庆典补给六", "reward": "灵石x280000"},
                {"day": 7, "name": "周年纪念礼", "reward": "灵石x500000,渡厄丹x1"},
            ],
            "milestone_rewards": [
                {"days": 5, "name": "五日纪念", "reward": "灵石x300000"},
                {"days": 7, "name": "周年礼盒", "reward": "灵石x1000000,渡厄丹x2"},
            ],
            "daily_tasks": [
                _task_row("daily_sign", "灵石x100000", name="庆典问道"),
                _task_row("daily_work", "灵石x120000", name="庆典悬赏"),
                _task_row("daily_boss", "灵石x150000", name="庆典伏魔"),
                _task_row("daily_map_mission_complete", "灵石x120000", name="山河巡礼"),
                _task_row("daily_dongfu_harvest", "灵石x100000", name="洞府补给"),
            ],
            "weekly_tasks": [
                _task_row("weekly_sign", "灵石x800000", name="周年勤修"),
                _task_row("weekly_work", "灵石x1000000", name="悬赏庆典"),
                _task_row("weekly_boss", "灵石x1200000,渡厄丹x1", name="伏魔庆典", target=100),
                _task_row("weekly_map_mission_complete", "灵石x1000000", name="山河庆典"),
            ],
            "extra_rules": ["后续可追加纪念称号、抽奖或兑换入口"],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "rank_warmup": {
        "name": "冲榜预热签到",
        "description": "适合大型活动开始前预热，任务偏悬赏、BOSS 和地图委托。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "rank_warmup",
            "enabled": True,
            "festival_name": "冲榜预热",
            "name": "冲榜预热签到活动",
            "description": "活动预热期每日签到领取基础补给，通过悬赏、伏魔和地图委托提前积累活跃。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "预热补给一", "reward": "灵石x50000"},
                {"day": 2, "name": "预热补给二", "reward": "灵石x60000"},
                {"day": 3, "name": "预热补给三", "reward": "灵石x70000"},
                {"day": 4, "name": "预热补给四", "reward": "灵石x80000"},
                {"day": 5, "name": "预热补给五", "reward": "灵石x100000"},
            ],
            "milestone_rewards": [
                {"days": 3, "name": "预热三日", "reward": "灵石x100000"},
                {"days": 5, "name": "预热五日", "reward": "灵石x200000,渡厄丹x1"},
            ],
            "daily_tasks": [
                _task_row("daily_work", "灵石x50000", name="预热悬赏"),
                _task_row("daily_boss", "灵石x60000", name="预热伏魔"),
                _task_row("daily_map_mission_complete", "灵石x50000", name="预热委托"),
            ],
            "weekly_tasks": [
                _task_row("weekly_work", "灵石x250000", name="悬赏蓄势", target=15),
                _task_row("weekly_boss", "灵石x300000", name="伏魔蓄势", target=80),
                _task_row("weekly_map_mission_complete", "灵石x250000", name="山河蓄势", target=12),
            ],
            "extra_rules": ["正式冲榜规则、积分来源和兑换内容后续补充"],
            "extensions": {"repeat_last_daily_reward": False},
        },
    },
}


GAMEPLAY_TEMPLATE_DEFINITIONS = {
    "duanwu_collect_words": {
        "name": "端午集字",
        "description": "做任务随机获得字牌，集齐端午安康等词组兑换奖励。",
        "config": {
            "key": "duanwu_collect_words",
            "template_key": "duanwu_collect_words",
            "type": "collect_words",
            "enabled": False,
            "name": "端午集字",
            "description": "活动期间完成指定玩法有机会获得字牌，集齐祝福词组可兑换奖励。",
            "start_time": "0",
            "end_time": "无限",
            "drop_events": DEFAULT_COLLECT_DROP_EVENTS,
            "drop_rate": 0.35,
            "daily_drop_limit": 8,
            "rolls_per_record": 1,
            "pity_threshold": 6,
            "letters": [
                {"char": "端", "weight": 30},
                {"char": "午", "weight": 30},
                {"char": "安", "weight": 26},
                {"char": "康", "weight": 22},
                {"char": "平", "weight": 18},
                {"char": "喜", "weight": 16},
                {"char": "乐", "weight": 16},
                {"char": "吉", "weight": 14},
                {"char": "祥", "weight": 14},
                {"char": "高", "weight": 12},
                {"char": "照", "weight": 12},
                {"char": "万", "weight": 10},
                {"char": "事", "weight": 10},
                {"char": "顺", "weight": 8},
                {"char": "遂", "weight": 8},
            ],
            "phrases": [
                {"phrase": "端午安康", "name": "端午安康", "reward": "灵石x200000,渡厄丹x1", "limit": 1},
                {"phrase": "平安喜乐", "name": "平安喜乐", "reward": "灵石x180000", "limit": 2},
                {"phrase": "吉祥高照", "name": "吉祥高照", "reward": "灵石x250000", "limit": 1},
                {"phrase": "万事顺遂", "name": "万事顺遂", "reward": "灵石x300000,渡厄丹x1", "limit": 1},
            ],
        },
    },
    "guoqing_collect_words": {
        "name": "国庆集字",
        "description": "适合国庆、中秋等长周期活动，词组偏祝福和庆典。",
        "config": {
            "key": "guoqing_collect_words",
            "template_key": "guoqing_collect_words",
            "type": "collect_words",
            "enabled": False,
            "name": "国庆集字",
            "description": "活动期间完成指定玩法有机会获得字牌，集齐国庆词组可兑换奖励。",
            "start_time": "0",
            "end_time": "无限",
            "drop_events": DEFAULT_COLLECT_DROP_EVENTS,
            "drop_rate": 0.32,
            "daily_drop_limit": 8,
            "rolls_per_record": 1,
            "pity_threshold": 6,
            "letters": [
                {"char": "祖", "weight": 28},
                {"char": "国", "weight": 28},
                {"char": "万", "weight": 24},
                {"char": "岁", "weight": 20},
                {"char": "山", "weight": 18},
                {"char": "河", "weight": 18},
                {"char": "锦", "weight": 14},
                {"char": "绣", "weight": 14},
                {"char": "泰", "weight": 12},
                {"char": "民", "weight": 12},
                {"char": "安", "weight": 12},
                {"char": "普", "weight": 10},
                {"char": "天", "weight": 10},
                {"char": "同", "weight": 8},
                {"char": "庆", "weight": 8},
            ],
            "phrases": [
                {"phrase": "祖国万岁", "name": "祖国万岁", "reward": "灵石x220000", "limit": 2},
                {"phrase": "山河锦绣", "name": "山河锦绣", "reward": "灵石x260000,渡厄丹x1", "limit": 1},
                {"phrase": "国泰民安", "name": "国泰民安", "reward": "灵石x300000", "limit": 1},
                {"phrase": "普天同庆", "name": "普天同庆", "reward": "灵石x360000,渡厄丹x1", "limit": 1},
            ],
        },
    },
    "blessing_collect_words": {
        "name": "通用祝福集字",
        "description": "用于任意节日或版本活动，字牌、词组、奖励都可调整。",
        "config": {
            "key": "blessing_collect_words",
            "template_key": "blessing_collect_words",
            "type": "collect_words",
            "enabled": False,
            "name": "祝福集字",
            "description": "完成指定玩法随机获得字牌，集齐祝福词组后兑换活动奖励。",
            "start_time": "0",
            "end_time": "无限",
            "drop_events": DEFAULT_COLLECT_DROP_EVENTS,
            "drop_rate": 0.3,
            "daily_drop_limit": 8,
            "rolls_per_record": 1,
            "pity_threshold": 6,
            "letters": [
                {"char": "福", "weight": 24},
                {"char": "运", "weight": 22},
                {"char": "绵", "weight": 18},
                {"char": "长", "weight": 18},
                {"char": "仙", "weight": 16},
                {"char": "途", "weight": 16},
                {"char": "顺", "weight": 14},
                {"char": "遂", "weight": 14},
                {"char": "道", "weight": 12},
                {"char": "心", "weight": 12},
                {"char": "明", "weight": 10},
                {"char": "万", "weight": 10},
                {"char": "象", "weight": 8},
                {"char": "更", "weight": 8},
                {"char": "新", "weight": 8},
            ],
            "phrases": [
                {"phrase": "福运绵长", "name": "福运绵长", "reward": "灵石x180000", "limit": 2},
                {"phrase": "仙途顺遂", "name": "仙途顺遂", "reward": "灵石x220000", "limit": 2},
                {"phrase": "道心长明", "name": "道心长明", "reward": "灵石x300000,渡厄丹x1", "limit": 1},
                {"phrase": "万象更新", "name": "万象更新", "reward": "灵石x350000,渡厄丹x1", "limit": 1},
            ],
        },
    },
    "lucky_bag_collect_words": {
        "name": "福袋祈愿",
        "description": "完成活动来源获得祈愿字牌，集齐福袋祝词兑换节日奖励。",
        "config": {
            "key": "lucky_bag_collect_words",
            "template_key": "lucky_bag_collect_words",
            "type": "collect_words",
            "enabled": False,
            "name": "福袋祈愿",
            "description": "活动期间完成指定玩法有机会获得祈愿字牌，集齐祝词可兑换福袋奖励。",
            "start_time": "0",
            "end_time": "无限",
            "drop_events": DEFAULT_COLLECT_DROP_EVENTS,
            "drop_rate": 0.28,
            "daily_drop_limit": 10,
            "rolls_per_record": 1,
            "pity_threshold": 6,
            "letters": [
                {"char": "福", "weight": 24},
                {"char": "袋", "weight": 20},
                {"char": "临", "weight": 16},
                {"char": "门", "weight": 16},
                {"char": "金", "weight": 14},
                {"char": "玉", "weight": 14},
                {"char": "满", "weight": 12},
                {"char": "堂", "weight": 12},
                {"char": "瑞", "weight": 10},
                {"char": "气", "weight": 10},
                {"char": "盈", "weight": 8},
                {"char": "鸿", "weight": 8},
                {"char": "运", "weight": 8},
                {"char": "当", "weight": 8},
                {"char": "头", "weight": 8},
            ],
            "phrases": [
                {"phrase": "福袋临门", "name": "福袋临门", "reward": "灵石x220000", "limit": 2},
                {"phrase": "金玉满堂", "name": "金玉满堂", "reward": "灵石x300000,渡厄丹x1", "limit": 1},
                {"phrase": "瑞气盈门", "name": "瑞气盈门", "reward": "灵石x260000", "limit": 1},
                {"phrase": "鸿运当头", "name": "鸿运当头", "reward": "灵石x360000,渡厄丹x1", "limit": 1},
            ],
        },
    },
    "treasure_map_collect_words": {
        "name": "秘境寻宝",
        "description": "用地图、悬赏、宗门等事件掉落寻宝线索，集齐线索兑换宝藏。",
        "config": {
            "key": "treasure_map_collect_words",
            "template_key": "treasure_map_collect_words",
            "type": "collect_words",
            "enabled": False,
            "name": "秘境寻宝",
            "description": "完成地图委托、悬赏令、宗门任务等玩法有机会获得寻宝线索，集齐线索兑换宝藏。",
            "start_time": "0",
            "end_time": "无限",
            "drop_events": [
                "work",
                "sect_task_complete",
                "map_mission_complete",
                "dungeon_clear",
                "boss",
                "pet_travel_claim",
            ],
            "drop_rate": 0.3,
            "daily_drop_limit": 9,
            "rolls_per_record": 1,
            "pity_threshold": 6,
            "letters": [
                {"char": "东", "weight": 20},
                {"char": "海", "weight": 18},
                {"char": "寻", "weight": 18},
                {"char": "珠", "weight": 14},
                {"char": "南", "weight": 18},
                {"char": "岭", "weight": 16},
                {"char": "采", "weight": 14},
                {"char": "药", "weight": 14},
                {"char": "西", "weight": 16},
                {"char": "荒", "weight": 14},
                {"char": "探", "weight": 12},
                {"char": "宝", "weight": 12},
                {"char": "北", "weight": 12},
                {"char": "境", "weight": 10},
                {"char": "访", "weight": 10},
                {"char": "仙", "weight": 10},
            ],
            "phrases": [
                {"phrase": "东海寻珠", "name": "东海寻珠", "reward": "灵石x260000", "limit": 2},
                {"phrase": "南岭采药", "name": "南岭采药", "reward": "灵石x260000", "limit": 2},
                {"phrase": "西荒探宝", "name": "西荒探宝", "reward": "灵石x350000,渡厄丹x1", "limit": 1},
                {"phrase": "北境访仙", "name": "北境访仙", "reward": "灵石x420000,渡厄丹x1", "limit": 1},
            ],
        },
    },
    "firework_collect_words": {
        "name": "烟火庆典",
        "description": "节庆活动中收集烟火字牌，兑换庆典补给。",
        "config": {
            "key": "firework_collect_words",
            "template_key": "firework_collect_words",
            "type": "collect_words",
            "enabled": False,
            "name": "烟火庆典",
            "description": "活动期间完成指定玩法有机会获得烟火字牌，集齐庆典词组兑换奖励。",
            "start_time": "0",
            "end_time": "无限",
            "drop_events": DEFAULT_COLLECT_DROP_EVENTS,
            "drop_rate": 0.34,
            "daily_drop_limit": 8,
            "rolls_per_record": 1,
            "pity_threshold": 6,
            "letters": [
                {"char": "烟", "weight": 24},
                {"char": "火", "weight": 24},
                {"char": "满", "weight": 20},
                {"char": "天", "weight": 18},
                {"char": "星", "weight": 16},
                {"char": "河", "weight": 16},
                {"char": "长", "weight": 14},
                {"char": "明", "weight": 14},
                {"char": "灯", "weight": 12},
                {"char": "影", "weight": 12},
                {"char": "成", "weight": 10},
                {"char": "双", "weight": 10},
                {"char": "良", "weight": 8},
                {"char": "宵", "weight": 8},
                {"char": "同", "weight": 8},
                {"char": "庆", "weight": 8},
            ],
            "phrases": [
                {"phrase": "烟火满天", "name": "烟火满天", "reward": "灵石x220000", "limit": 2},
                {"phrase": "星河长明", "name": "星河长明", "reward": "灵石x260000", "limit": 2},
                {"phrase": "灯影成双", "name": "灯影成双", "reward": "灵石x320000,渡厄丹x1", "limit": 1},
                {"phrase": "良宵同庆", "name": "良宵同庆", "reward": "灵石x380000,渡厄丹x1", "limit": 1},
            ],
        },
    },
    "festival_points_shop": {
        "name": "节日积分商店",
        "description": "完成活动事件获得节日积分，用积分兑换限量补给。",
        "config": {
            "key": "festival_points_shop",
            "template_key": "festival_points_shop",
            "type": "event_points",
            "enabled": False,
            "name": "节日积分商店",
            "description": "活动期间完成签到、悬赏、宗门、洞府等玩法获得节日积分，可在活动商店兑换奖励。",
            "start_time": "0",
            "end_time": "无限",
            "point_name": "节日积分",
            "event_rules": DEFAULT_POINT_EVENT_RULES,
            "shop": [
                {"item_key": "stone_pack", "name": "灵石补给", "cost": 80, "reward": "灵石x300000", "limit": 3, "stock_limit": 0},
                {"item_key": "practice_pack", "name": "修炼补给", "cost": 160, "reward": "灵石x500000,渡厄丹x1", "limit": 2, "stock_limit": 0},
                {"item_key": "festival_box", "name": "节日礼盒", "cost": 260, "reward": "灵石x800000,渡厄丹x2", "limit": 1, "stock_limit": 80},
            ],
        },
    },
    "trial_points_shop": {
        "name": "试炼积分商店",
        "description": "偏战斗和副本的积分玩法，适合搭配冲榜或周常活动。",
        "config": {
            "key": "trial_points_shop",
            "template_key": "trial_points_shop",
            "type": "event_points",
            "enabled": False,
            "name": "试炼积分商店",
            "description": "活动期间参与世界BOSS、副本通关、地图委托和悬赏令获得试炼积分，积分可兑换试炼补给。",
            "start_time": "0",
            "end_time": "无限",
            "point_name": "试炼积分",
            "event_rules": [
                {"event": "work", "points": 12, "daily_limit": 80},
                {"event": "boss", "points": 6, "daily_limit": 120},
                {"event": "map_mission_complete", "points": 15, "daily_limit": 90},
                {"event": "dungeon_clear", "points": 20, "daily_limit": 100},
                {"event": "sect_task_complete", "points": 12, "daily_limit": 60},
            ],
            "shop": [
                {"item_key": "trial_stone", "name": "试炼灵石", "cost": 100, "reward": "灵石x400000", "limit": 3, "stock_limit": 0},
                {"item_key": "trial_pill", "name": "试炼丹礼", "cost": 220, "reward": "灵石x600000,渡厄丹x1", "limit": 2, "stock_limit": 0},
                {"item_key": "trial_box", "name": "试炼宝匣", "cost": 360, "reward": "灵石x1000000,渡厄丹x2", "limit": 1, "stock_limit": 60},
            ],
        },
    },
    "harvest_points_shop": {
        "name": "洞府收获商店",
        "description": "偏轻量日常的积分玩法，适合和签到一起开启。",
        "config": {
            "key": "harvest_points_shop",
            "template_key": "harvest_points_shop",
            "type": "event_points",
            "enabled": False,
            "name": "洞府收获商店",
            "description": "活动期间通过签到、洞府收获、宠物游历和炼丹获得丰收积分，积分可兑换经营补给。",
            "start_time": "0",
            "end_time": "无限",
            "point_name": "丰收积分",
            "event_rules": [
                {"event": "sign_in", "points": 30, "daily_limit": 30},
                {"event": "dongfu_harvest", "points": 18, "daily_limit": 72},
                {"event": "pet_travel_claim", "points": 16, "daily_limit": 48},
                {"event": "mix_elixir_complete", "points": 10, "daily_limit": 60},
                {"event": "sect_task_complete", "points": 12, "daily_limit": 48},
            ],
            "shop": [
                {"item_key": "harvest_stone", "name": "丰收灵石", "cost": 90, "reward": "灵石x350000", "limit": 3, "stock_limit": 0},
                {"item_key": "harvest_supply", "name": "洞府补给", "cost": 180, "reward": "灵石x600000,渡厄丹x1", "limit": 2, "stock_limit": 0},
                {"item_key": "harvest_box", "name": "丰收礼盒", "cost": 300, "reward": "灵石x900000,渡厄丹x2", "limit": 1, "stock_limit": 80},
            ],
        },
    },
    "nian_beast_raid": {
        "name": "年兽讨伐（道具）",
        "description": "全服协力击退年兽，可用烟花爆竹等道具造成随机伤害；任务掉落道具。",
        "config": {
            "key": "nian_beast_raid",
            "template_key": "nian_beast_raid",
            "type": "activity_boss",
            "enabled": False,
            "name": "年兽讨伐",
            "description": "活动期间完成任务可获得烟花爆竹，对年兽造成随机伤害，全服共享血量。",
            "start_time": "0",
            "end_time": "无限",
            "boss_name": "年兽",
            "mode": "item_raid",
            "max_hp": 0,
            "atk_ratio": 0.1,
            "hit_hp_cap_ratio": 0.01,
            "daily_fight_limit": 3,
            "drop_events": ["sign_in", "work", "boss"],
            "items": [
                {"id": "firecracker", "name": "爆竹", "damage_min": 50000, "damage_max": 150000, "cost": 1},
                {"id": "firework", "name": "烟花", "damage_min": 120000, "damage_max": 280000, "cost": 1},
                {"id": "firework_box", "name": "烟花礼盒", "damage_min": 300000, "damage_max": 600000, "cost": 1},
            ],
            "rank_rewards": [
                {"rank_min": 1, "rank_max": 1, "name": "魁首", "reward": "灵石x2000000,渡厄丹x3"},
                {"rank_min": 2, "rank_max": 3, "name": "前列", "reward": "灵石x1200000,渡厄丹x2"},
                {"rank_min": 4, "rank_max": 10, "name": "十强", "reward": "灵石x800000,渡厄丹x1"},
            ],
            "server_milestones": [
                {"key": "hp75", "hp_percent": 75, "name": "年兽受挫", "reward": "灵石x200000"},
                {"key": "hp50", "hp_percent": 50, "name": "势如破竹", "reward": "灵石x350000"},
                {"key": "hp25", "hp_percent": 25, "name": "最后一战", "reward": "灵石x500000,渡厄丹x1"},
                {"key": "hp0", "hp_percent": 0, "name": "年兽击退", "reward": "灵石x800000,渡厄丹x2"},
            ],
        },
    },
    "festival_world_boss": {
        "name": "节日全服首领",
        "description": "全服共打，血量按永恒境；每人每日3次，伤害取攻击力十分之一，单次上限1%血量；讨伐世界BOSS可计入。",
        "config": {
            "key": "festival_world_boss",
            "template_key": "festival_world_boss",
            "type": "activity_boss",
            "enabled": False,
            "name": "节日全服首领",
            "description": "全服协力讨伐节日首领，按累计伤害排名领取奖励，并解锁全服进度宝箱。",
            "start_time": "0",
            "end_time": "无限",
            "boss_name": "节日魔尊",
            "mode": "cooperative",
            "max_hp": 0,
            "atk_ratio": 0.1,
            "hit_hp_cap_ratio": 0.01,
            "daily_fight_limit": 3,
            "items": [],
            "rank_rewards": [
                {"rank_min": 1, "rank_max": 1, "name": "伤害第一", "reward": "灵石x3000000,渡厄丹x4"},
                {"rank_min": 2, "rank_max": 5, "name": "前五", "reward": "灵石x1500000,渡厄丹x2"},
                {"rank_min": 6, "rank_max": 20, "name": "前二十", "reward": "灵石x900000,渡厄丹x1"},
            ],
            "server_milestones": [
                {"key": "p90", "hp_percent": 90, "name": "初战告捷", "reward": "灵石x150000"},
                {"key": "p70", "hp_percent": 70, "name": "鏖战正酣", "reward": "灵石x280000"},
                {"key": "p50", "hp_percent": 50, "name": "半壁江山", "reward": "灵石x450000"},
                {"key": "p20", "hp_percent": 20, "name": "穷途末路", "reward": "灵石x700000,渡厄丹x1"},
            ],
        },
    },
    "mid_autumn_moon_boss": {
        "name": "中秋月魔（双模式首领）",
        "category": "boss",
        "type_tag": "activity_boss",
        "description": "全服共打月魔 + 月饼天灯道具随机伤害；非集字，适合中秋档期。",
        "config": {
            "key": "mid_autumn_moon_boss",
            "template_key": "mid_autumn_moon_boss",
            "type": "activity_boss",
            "enabled": False,
            "name": "中秋月魔讨伐",
            "description": "全服协力击退月魔：活动讨伐计入伤害，或消耗月饼、天灯造成随机伤害。",
            "start_time": "0",
            "end_time": "无限",
            "boss_name": "蚀月魔尊",
            "mode": "both",
            "max_hp": 0,
            "atk_ratio": 0.1,
            "hit_hp_cap_ratio": 0.01,
            "daily_fight_limit": 3,
            "drop_events": ["sign_in", "dongfu_harvest", "work", "boss"],
            "items": [
                {"id": "mooncake", "name": "月饼", "damage_min": 80000, "damage_max": 200000, "cost": 1},
                {"id": "sky_lantern", "name": "天灯", "damage_min": 150000, "damage_max": 350000, "cost": 1},
                {"id": "jade_rabbit_charm", "name": "玉兔护符", "damage_min": 400000, "damage_max": 750000, "cost": 1},
            ],
            "rank_rewards": [
                {"rank_min": 1, "rank_max": 1, "name": "逐月魁首", "reward": "灵石x2500000,渡厄丹x3"},
                {"rank_min": 2, "rank_max": 5, "name": "前五", "reward": "灵石x1400000,渡厄丹x2"},
                {"rank_min": 6, "rank_max": 15, "name": "十五强", "reward": "灵石x900000,渡厄丹x1"},
            ],
            "server_milestones": [
                {"key": "moon80", "hp_percent": 80, "name": "月蚀初现", "reward": "灵石x180000"},
                {"key": "moon50", "hp_percent": 50, "name": "桂影摇波", "reward": "灵石x400000"},
                {"key": "moon20", "hp_percent": 20, "name": "玉兔相助", "reward": "灵石x650000,渡厄丹x1"},
                {"key": "moon0", "hp_percent": 0, "name": "月圆人圆", "reward": "灵石x1000000,渡厄丹x2"},
            ],
        },
    },
    "demon_seal_raid": {
        "name": "封魔令讨伐（符箓道具）",
        "category": "boss",
        "type_tag": "activity_boss",
        "description": "伏魔/副本掉落封魔符，对魔将造成区间伤害；战斗向，与集字无关。",
        "config": {
            "key": "demon_seal_raid",
            "template_key": "demon_seal_raid",
            "type": "activity_boss",
            "enabled": False,
            "name": "封魔令",
            "description": "悬赏、伏魔、副本有概率获得封魔符与破魔钉，对异界魔将造成随机伤害。",
            "start_time": "0",
            "end_time": "无限",
            "boss_name": "异界魔将",
            "mode": "item_raid",
            "max_hp": 0,
            "atk_ratio": 0.1,
            "hit_hp_cap_ratio": 0.01,
            "daily_fight_limit": 5,
            "drop_events": ["boss", "dungeon_clear", "map_mission_complete", "work"],
            "items": [
                {"id": "seal_talisman", "name": "封魔符", "damage_min": 60000, "damage_max": 180000, "cost": 1},
                {"id": "demon_break_nail", "name": "破魔钉", "damage_min": 200000, "damage_max": 450000, "cost": 1},
            ],
            "rank_rewards": [
                {"rank_min": 1, "rank_max": 3, "name": "封魔前三", "reward": "灵石x1500000,渡厄丹x2"},
                {"rank_min": 4, "rank_max": 20, "name": "封魔勇士", "reward": "灵石x700000,渡厄丹x1"},
            ],
            "server_milestones": [
                {"key": "seal60", "hp_percent": 60, "name": "魔气渐弱", "reward": "灵石x250000"},
                {"key": "seal30", "hp_percent": 30, "name": "阵眼将破", "reward": "灵石x500000,渡厄丹x1"},
                {"key": "seal0", "hp_percent": 0, "name": "魔将陨落", "reward": "灵石x900000,渡厄丹x2"},
            ],
        },
    },
    "boss_trial_points_shop": {
        "name": "伏魔积分（首领导向）",
        "category": "points",
        "type_tag": "event_points",
        "description": "积分主要来自世界BOSS与副本，商店换战斗补给；不是集字。",
        "config": {
            "key": "boss_trial_points_shop",
            "template_key": "boss_trial_points_shop",
            "type": "event_points",
            "enabled": False,
            "name": "伏魔积分",
            "description": "讨伐世界BOSS、通关副本、完成地图委托获得伏魔积分，兑换限量战斗补给。",
            "start_time": "0",
            "end_time": "无限",
            "point_name": "伏魔积分",
            "event_rules": [
                {"event": "boss", "points": 8, "daily_limit": 160},
                {"event": "dungeon_clear", "points": 25, "daily_limit": 100},
                {"event": "map_mission_complete", "points": 12, "daily_limit": 72},
                {"event": "work", "points": 6, "daily_limit": 60},
            ],
            "shop": [
                {"item_key": "demon_stone", "name": "伏魔灵石", "cost": 120, "reward": "灵石x450000", "limit": 4, "stock_limit": 0},
                {"item_key": "demon_pill", "name": "伏魔丹礼", "cost": 280, "reward": "灵石x700000,渡厄丹x2", "limit": 2, "stock_limit": 80},
                {"item_key": "demon_chest", "name": "封魔宝匣", "cost": 480, "reward": "灵石x1200000,渡厄丹x3", "limit": 1, "stock_limit": 40},
            ],
        },
    },
    "sect_contribution_points": {
        "name": "宗门贡献积分",
        "category": "points",
        "type_tag": "event_points",
        "description": "宗门任务与悬赏为主积分来源，适合宗门主题周。",
        "config": {
            "key": "sect_contribution_points",
            "template_key": "sect_contribution_points",
            "type": "event_points",
            "enabled": False,
            "name": "宗门贡献",
            "description": "完成宗门任务、悬赏与洞府经营获得贡献积分，兑换宗门庆典礼包。",
            "start_time": "0",
            "end_time": "无限",
            "point_name": "贡献积分",
            "event_rules": [
                {"event": "sect_task_complete", "points": 20, "daily_limit": 80},
                {"event": "work", "points": 10, "daily_limit": 50},
                {"event": "dongfu_harvest", "points": 12, "daily_limit": 48},
                {"event": "sign_in", "points": 15, "daily_limit": 15},
            ],
            "shop": [
                {"item_key": "sect_stone", "name": "宗门灵石", "cost": 70, "reward": "灵石x280000", "limit": 5, "stock_limit": 0},
                {"item_key": "sect_gift", "name": "同门礼盒", "cost": 200, "reward": "灵石x550000,渡厄丹x1", "limit": 2, "stock_limit": 100},
            ],
        },
    },
    "alchemy_furnace_points": {
        "name": "丹炉积分（炼丹洞府）",
        "category": "points",
        "type_tag": "event_points",
        "description": "炼丹、洞府、灵宠游历攒积分；经营向，与集字区分明显。",
        "config": {
            "key": "alchemy_furnace_points",
            "template_key": "alchemy_furnace_points",
            "type": "event_points",
            "enabled": False,
            "name": "丹炉积分",
            "description": "炼丹、洞府收获与灵宠游历获得丹炉积分，兑换炼丹与经营物资。",
            "start_time": "0",
            "end_time": "无限",
            "point_name": "丹炉积分",
            "event_rules": [
                {"event": "mix_elixir_complete", "points": 14, "daily_limit": 84},
                {"event": "dongfu_harvest", "points": 16, "daily_limit": 64},
                {"event": "pet_travel_claim", "points": 18, "daily_limit": 54},
                {"event": "sign_in", "points": 20, "daily_limit": 20},
            ],
            "shop": [
                {"item_key": "furnace_herb", "name": "药引礼包", "cost": 100, "reward": "灵石x320000", "limit": 3, "stock_limit": 0},
                {"item_key": "furnace_elixir", "name": "丹成礼", "cost": 240, "reward": "灵石x600000,渡厄丹x2", "limit": 2, "stock_limit": 80},
            ],
        },
    },
}


GAMEPLAY_TEMPLATE_CATEGORY_LABELS = {
    "boss": "首领讨伐",
    "points": "积分商店",
    "collect": "集字兑换",
    "bundle": "组合方案",
}

GAMEPLAY_TYPE_LABELS = {
    "activity_boss": "全服首领",
    "event_points": "积分商店",
    "collect_words": "集字兑换",
    "bundle": "一键组合",
}


def _gameplay_template_category(key: str, value: dict) -> str:
    if value.get("category"):
        return str(value["category"])
    cfg = value.get("config") or {}
    t = cfg.get("type", "collect_words")
    if t == "activity_boss":
        return "boss"
    if t == "event_points":
        return "points"
    return "collect"


def _gameplay_template_type_tag(value: dict) -> str:
    if value.get("type_tag"):
        return str(value["type_tag"])
    return str((value.get("config") or {}).get("type", "collect_words"))


GAMEPLAY_BUNDLE_DEFINITIONS = {
    "mid_autumn_combo": {
        "name": "中秋档期组合",
        "category": "bundle",
        "type_tag": "bundle",
        "description": "一次添加：中秋月魔首领 + 伏魔积分商店。",
        "template_keys": ["mid_autumn_moon_boss", "boss_trial_points_shop"],
    },
    "spring_festival_combo": {
        "name": "春节档期组合",
        "category": "bundle",
        "type_tag": "bundle",
        "description": "一次添加：年兽道具讨伐 + 节日积分商店。",
        "template_keys": ["nian_beast_raid", "festival_points_shop"],
    },
    "demon_week_combo": {
        "name": "伏魔周组合",
        "category": "bundle",
        "type_tag": "bundle",
        "description": "一次添加：节日全服首领 + 封魔令 + 伏魔积分。",
        "template_keys": ["festival_world_boss", "demon_seal_raid", "boss_trial_points_shop"],
    },
}


def _serialize_templates():
    return {
        key: {
            "key": key,
            "name": value["name"],
            "description": value["description"],
            "config": deepcopy(value["config"]),
        }
        for key, value in ACTIVITY_TEMPLATE_DEFINITIONS.items()
    }


def _serialize_gameplay_templates():
    items = {}
    for key, value in GAMEPLAY_TEMPLATE_DEFINITIONS.items():
        category = _gameplay_template_category(key, value)
        type_tag = _gameplay_template_type_tag(value)
        items[key] = {
            "key": key,
            "name": value["name"],
            "description": value["description"],
            "category": category,
            "type_tag": type_tag,
            "type_label": GAMEPLAY_TYPE_LABELS.get(type_tag, type_tag),
            "category_label": GAMEPLAY_TEMPLATE_CATEGORY_LABELS.get(category, category),
            "config": deepcopy(value["config"]),
        }
    for key, value in GAMEPLAY_BUNDLE_DEFINITIONS.items():
        items[key] = {
            "key": key,
            "name": value["name"],
            "description": value["description"],
            "category": "bundle",
            "type_tag": "bundle",
            "type_label": GAMEPLAY_TYPE_LABELS["bundle"],
            "category_label": GAMEPLAY_TEMPLATE_CATEGORY_LABELS["bundle"],
            "template_keys": list(value.get("template_keys") or []),
            "config": None,
        }
    return items


def _serialize_gameplay_template_groups():
    templates = _serialize_gameplay_templates()
    order = ("boss", "points", "collect", "bundle")
    groups = {cat: [] for cat in order}
    for key, meta in templates.items():
        cat = meta.get("category") or "collect"
        if cat not in groups:
            groups[cat] = []
        groups[cat].append({"key": key, **meta})
    for cat in groups:
        groups[cat].sort(key=lambda x: x.get("name") or x.get("key"))
    return [
        {
            "key": cat,
            "label": GAMEPLAY_TEMPLATE_CATEGORY_LABELS.get(cat, cat),
            "templates": groups.get(cat) or [],
        }
        for cat in order
        if groups.get(cat)
    ]


def _serialize_event_choices():
    return list(ACTIVITY_EVENT_CHOICES)


def _clean_text(value, default: str = "") -> str:
    text = str(value if value is not None else "").strip()
    return text or default


def _validate_time_text(value: str, field_name: str):
    text = _clean_text(value)
    if text in ("", "0", "无限"):
        return
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            datetime.strptime(text, fmt)
            return
        except ValueError:
            pass
    raise ValueError(f"{field_name}格式应为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")


def _normalize_time_text(value, field_name: str, default: str, *, allow_special: bool = True) -> str:
    text = _clean_text(value)
    if not text:
        if not allow_special:
            raise ValueError(f"请选择{field_name}")
        text = default
    if allow_special and text in ("0", "无限"):
        return text
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return parsed.strftime("%Y-%m-%d")
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    raise ValueError(f"{field_name}格式应为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")


def _to_positive_int(value, field_name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name}必须是正整数")
    if number <= 0:
        raise ValueError(f"{field_name}必须是正整数")
    return number


def _normalize_reward_rows(rows, number_key: str, number_label: str) -> list[dict]:
    normalized = []
    seen_numbers = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        reward = _clean_text(row.get("reward"))
        name = _clean_text(row.get("name"))
        raw_number = row.get(number_key)
        if raw_number in (None, "") and not reward and not name:
            continue
        number = _to_positive_int(raw_number, number_label)
        if number in seen_numbers:
            raise ValueError(f"{number_label}{number}重复")
        seen_numbers.add(number)
        parse_reward(reward)
        normalized.append({
            number_key: number,
            "name": name or f"{number_label}{number}",
            "reward": reward,
        })
    return sorted(normalized, key=lambda item: item[number_key])


def _normalize_events(value, *, strict: bool = True) -> list[str]:
    if isinstance(value, str):
        source = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        source = value
    else:
        source = []

    events = []
    seen_events = set()
    for item in source:
        if isinstance(item, dict):
            event = _clean_text(item.get("value"))
        else:
            event = _clean_text(item)
        if not event or event in seen_events:
            continue
        if strict and event not in ACTIVITY_EVENT_LABELS:
            raise ValueError(f"不支持的活动事件：{event}")
        if event not in ACTIVITY_EVENT_LABELS:
            continue
        seen_events.add(event)
        events.append(event)
    return events


def _make_task_key(cycle: str, event: str, seen_keys: set[str]) -> str:
    base_event = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in (event or "task"))
    base = f"{cycle}_{base_event or 'task'}"
    key = base
    index = 2
    while key in seen_keys:
        key = f"{base}_{index}"
        index += 1
    return key


def _normalize_task_rows(rows, cycle: str, cycle_label: str) -> list[dict]:
    normalized = []
    seen_keys = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        key = _clean_text(row.get("key"))
        name = _clean_text(row.get("name"))
        description = _clean_text(row.get("description") or row.get("desc"))
        reward = _clean_text(row.get("reward"))
        events = _normalize_events(row.get("events"))
        raw_target = row.get("target")

        if not any((key, name, description, reward, events)):
            continue
        if not events:
            raise ValueError(f"{cycle_label}任务事件不能为空")
        if not key:
            key = _make_task_key(cycle, events[0], seen_keys)
        if key in seen_keys:
            raise ValueError(f"{cycle_label}任务编号 {key} 重复")
        seen_keys.add(key)
        target = _to_positive_int(raw_target, f"{cycle_label}任务目标")
        if reward:
            parse_reward(reward)
        normalized.append({
            "key": key,
            "name": name or key,
            "description": description,
            "target": target,
            "events": events,
            "reward": reward,
        })
    return normalized


def _normalize_extra_rules(value) -> list[str]:
    if isinstance(value, str):
        source = value.splitlines()
    elif isinstance(value, list):
        source = value
    else:
        source = []
    return [_clean_text(item) for item in source if _clean_text(item)]


def _as_reward_rows(value, number_key: str) -> list[dict]:
    rows = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if not isinstance(item, dict):
            continue
        rows.append({
            number_key: item.get(number_key, ""),
            "name": item.get("name", ""),
            "reward": item.get("reward", ""),
        })
    return rows


def _as_task_rows(value) -> list[dict]:
    rows = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if not isinstance(item, dict):
            continue
        events = []
        for event in _normalize_events(item.get("events"), strict=False):
            events.append({
                "value": event,
                "label": ACTIVITY_EVENT_LABELS.get(event, event),
            })
        rows.append({
            "key": item.get("key", ""),
            "name": item.get("name", ""),
            "description": item.get("description", item.get("desc", "")),
            "target": item.get("target", ""),
            "events": events,
            "reward": item.get("reward", ""),
        })
    return rows


def _to_non_negative_int(value, field_name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name}必须是非负整数")
    if number < 0:
        raise ValueError(f"{field_name}必须是非负整数")
    return number


def _normalize_rate(value, field_name: str) -> float:
    try:
        rate = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name}必须是数字")
    if rate > 1:
        rate = rate / 100
    if rate < 0 or rate > 1:
        raise ValueError(f"{field_name}必须在 0 到 1 之间")
    return round(rate, 4)


def _normalize_activity_key(value, fallback: str) -> str:
    text = _clean_text(value, fallback)
    key = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in text)
    return key.strip("_") or fallback


def _normalize_collect_letters(rows, phrases: list[dict]) -> list[dict]:
    weights: dict[str, int] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        char = _clean_text(row.get("char"))
        if not char:
            continue
        if len(char) != 1:
            raise ValueError("字牌只能填写单个字")
        weight = _to_positive_int(row.get("weight"), f"字牌{char}权重")
        weights[char] = weights.get(char, 0) + weight

    for phrase in phrases:
        for char in str(phrase.get("phrase") or ""):
            if char.strip() and char not in weights:
                weights[char] = 10

    return [{"char": char, "weight": weight} for char, weight in weights.items()]


def _normalize_collect_phrases(rows) -> list[dict]:
    phrases = []
    seen = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        phrase = _clean_text(row.get("phrase"))
        name = _clean_text(row.get("name"), phrase)
        reward = _clean_text(row.get("reward"))
        if not any((phrase, name, reward)):
            continue
        if not phrase:
            raise ValueError("兑换词组不能为空")
        if phrase in seen:
            raise ValueError(f"兑换词组 {phrase} 重复")
        seen.add(phrase)
        if reward:
            parse_reward(reward)
        phrases.append({
            "phrase": phrase,
            "name": name or phrase,
            "reward": reward,
            "limit": _to_non_negative_int(row.get("limit", 1), f"{phrase}兑换次数"),
        })
    return phrases


def _as_point_rule_rows(value) -> list[dict]:
    rows = []
    source = value if isinstance(value, list) else []
    for item in source:
        if not isinstance(item, dict):
            continue
        event = _clean_text(item.get("event") or item.get("event_key") or item.get("value"))
        rows.append({
            "event": event,
            "label": ACTIVITY_EVENT_LABELS.get(event, event),
            "points": item.get("points", ""),
            "daily_limit": item.get("daily_limit", 0),
        })
    return rows


def _as_point_shop_rows(value) -> list[dict]:
    rows = []
    source = value if isinstance(value, list) else []
    for item in source:
        if not isinstance(item, dict):
            continue
        rows.append({
            "item_key": item.get("item_key", item.get("key", "")),
            "name": item.get("name", ""),
            "cost": item.get("cost", ""),
            "reward": item.get("reward", ""),
            "limit": item.get("limit", 1),
            "stock_limit": item.get("stock_limit", item.get("total_limit", 0)),
        })
    return rows


def _as_gameplay_rows(value) -> list[dict]:
    if not isinstance(value, list):
        return []
    rows = []
    for item in value:
        if not isinstance(item, dict):
            continue
        events = []
        for event in _normalize_events(item.get("drop_events"), strict=False):
            events.append({
                "value": event,
                "label": ACTIVITY_EVENT_LABELS.get(event, event),
            })
        rows.append({
            "key": item.get("key", ""),
            "template_key": item.get("template_key", item.get("key", "")),
            "type": item.get("type", "collect_words"),
            "enabled": _as_bool(item.get("enabled")),
            "name": item.get("name", ""),
            "description": item.get("description", ""),
            "start_time": item.get("start_time", "0"),
            "end_time": item.get("end_time", "无限"),
            "drop_events": events,
            "drop_rate": item.get("drop_rate", 0.35),
            "daily_drop_limit": item.get("daily_drop_limit", 8),
            "rolls_per_record": item.get("rolls_per_record", 1),
            "pity_threshold": item.get("pity_threshold", 0),
            "letters": item.get("letters") if isinstance(item.get("letters"), list) else [],
            "phrases": item.get("phrases") if isinstance(item.get("phrases"), list) else [],
            "point_name": item.get("point_name", "活动积分"),
            "event_rules": _as_point_rule_rows(item.get("event_rules")),
            "shop": _as_point_shop_rows(item.get("shop")),
            "boss_name": item.get("boss_name", ""),
            "mode": item.get("mode", "cooperative"),
            "max_hp": item.get("max_hp", 0),
            "atk_ratio": item.get("atk_ratio", 0.1),
            "hit_hp_cap_ratio": item.get("hit_hp_cap_ratio", 0.01),
            "daily_fight_limit": item.get("daily_fight_limit", 3),
            "items": item.get("items") if isinstance(item.get("items"), list) else [],
            "rank_rewards": item.get("rank_rewards") if isinstance(item.get("rank_rewards"), list) else [],
            "server_milestones": (
                item.get("server_milestones") if isinstance(item.get("server_milestones"), list) else []
            ),
        })
    return rows


def _normalize_point_event_rules(rows, activity_label: str) -> list[dict]:
    normalized = []
    seen_events = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        event = _clean_text(row.get("event") or row.get("event_key") or row.get("value"))
        points_value = row.get("points")
        daily_limit_value = row.get("daily_limit", 0)
        if not any((event, points_value not in (None, ""), daily_limit_value not in (None, ""))):
            continue
        if not event:
            raise ValueError(f"{activity_label}积分事件不能为空")
        if event not in ACTIVITY_EVENT_LABELS:
            raise ValueError(f"不支持的活动事件：{event}")
        if event in seen_events:
            raise ValueError(f"{activity_label}积分事件 {ACTIVITY_EVENT_LABELS.get(event, event)} 重复")
        seen_events.add(event)
        normalized.append({
            "event": event,
            "points": _to_positive_int(points_value, f"{activity_label}积分数量"),
            "daily_limit": _to_non_negative_int(daily_limit_value, f"{activity_label}每日积分上限"),
        })
    return normalized


def _normalize_point_shop_items(rows, activity_label: str) -> list[dict]:
    normalized = []
    seen_keys = set()
    for index, row in enumerate(rows or [], 1):
        if not isinstance(row, dict):
            continue
        name = _clean_text(row.get("name"))
        reward = _clean_text(row.get("reward"))
        cost_value = row.get("cost")
        limit_value = row.get("limit", 1)
        if not any((name, reward, cost_value not in (None, ""), limit_value not in (None, ""))):
            continue
        if not name:
            raise ValueError(f"{activity_label}商店商品名称不能为空")
        item_key = _normalize_activity_key(row.get("item_key") or row.get("key") or name, f"item_{index}")
        if item_key in seen_keys:
            raise ValueError(f"{activity_label}商店商品编号 {item_key} 重复")
        seen_keys.add(item_key)
        if reward:
            parse_reward(reward)
        else:
            raise ValueError(f"{activity_label}商店商品 {name} 奖励不能为空")
        normalized.append({
            "item_key": item_key,
            "name": name,
            "cost": _to_positive_int(cost_value, f"{activity_label}商店商品 {name} 价格"),
            "reward": reward,
            "limit": _to_non_negative_int(limit_value, f"{activity_label}商店商品 {name} 兑换次数"),
            "stock_limit": _to_non_negative_int(row.get("stock_limit", row.get("total_limit", 0)) or 0, f"{activity_label}商店商品 {name} 全服库存"),
        })
    return normalized


def _normalize_gameplay_activities(value) -> list[dict]:
    if not isinstance(value, list):
        return []
    normalized = []
    seen_keys = set()
    for index, row in enumerate(value, 1):
        if not isinstance(row, dict):
            continue
        if not any((
            _clean_text(row.get("name")),
            _clean_text(row.get("description")),
            row.get("letters"),
            row.get("phrases"),
            row.get("event_rules"),
            row.get("shop"),
            row.get("boss_name"),
            row.get("items"),
        )):
            continue
        activity_type = _clean_text(row.get("type"), "collect_words")
        if activity_type not in {"collect_words", "event_points", "activity_boss"}:
            raise ValueError("当前支持集字、积分与活动首领玩法")
        key = _normalize_activity_key(row.get("key") or row.get("template_key"), f"{activity_type}_{index}")
        if key in seen_keys:
            raise ValueError(f"玩法活动编号 {key} 重复")
        seen_keys.add(key)

        start_time = _normalize_time_text(
            row.get("start_time"),
            f"玩法活动{index}开始时间",
            "0",
            allow_special=_as_bool(row.get("start_special"), True),
        )
        end_time = _normalize_time_text(
            row.get("end_time"),
            f"玩法活动{index}结束时间",
            "无限",
            allow_special=_as_bool(row.get("end_special"), True),
        )
        if activity_type == "activity_boss":
            from ..xiuxian_activity.activity_boss import normalize_activity_boss

            boss_row = dict(row)
            boss_row["start_time"] = start_time
            boss_row["end_time"] = end_time
            boss_row["enabled"] = _as_bool(row.get("enabled"))
            normalized.append(normalize_activity_boss(boss_row, index, key))
            continue
        if activity_type == "event_points":
            activity_label = f"玩法活动{index}"
            event_rules = _normalize_point_event_rules(row.get("event_rules"), activity_label)
            if not event_rules:
                raise ValueError(f"{activity_label}至少配置一个积分事件")
            shop = _normalize_point_shop_items(row.get("shop"), activity_label)
            if not shop:
                raise ValueError(f"{activity_label}至少配置一个商店商品")
            normalized.append({
                "key": key,
                "template_key": _clean_text(row.get("template_key"), key),
                "type": activity_type,
                "enabled": _as_bool(row.get("enabled")),
                "name": _clean_text(row.get("name"), f"积分活动{index}"),
                "description": _clean_text(row.get("description")),
                "start_time": start_time,
                "end_time": end_time,
                "point_name": _clean_text(row.get("point_name"), "活动积分"),
                "event_rules": event_rules,
                "shop": shop,
            })
            continue

        drop_events = _normalize_events(row.get("drop_events"))
        if not drop_events:
            raise ValueError(f"玩法活动{index}至少选择一个掉落事件")
        phrases = _normalize_collect_phrases(row.get("phrases"))
        if not phrases:
            raise ValueError(f"玩法活动{index}至少配置一个兑换词组")
        letters = _normalize_collect_letters(row.get("letters"), phrases)
        if not letters:
            raise ValueError(f"玩法活动{index}至少配置一个字牌")

        normalized.append({
            "key": key,
            "template_key": _clean_text(row.get("template_key"), key),
            "type": activity_type,
            "enabled": _as_bool(row.get("enabled")),
            "name": _clean_text(row.get("name"), f"集字活动{index}"),
            "description": _clean_text(row.get("description")),
            "start_time": start_time,
            "end_time": end_time,
            "drop_events": drop_events,
            "drop_rate": _normalize_rate(row.get("drop_rate"), f"玩法活动{index}掉落概率"),
            "daily_drop_limit": _to_non_negative_int(row.get("daily_drop_limit", 8), f"玩法活动{index}每日上限"),
            "rolls_per_record": _to_positive_int(row.get("rolls_per_record", 1), f"玩法活动{index}单次判定"),
            "pity_threshold": _to_non_negative_int(row.get("pity_threshold", 0) or 0, f"玩法活动{index}保底次数"),
            "letters": letters,
            "phrases": phrases,
        })
    return normalized


def _normalize_stage_features(value, stage_type: str, stage_label: str) -> list[str]:
    if isinstance(value, str):
        source = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        source = value
    else:
        return _default_stage_features(stage_type)

    features = []
    seen = set()
    alias_map = {
        "activity": "task",
        "tasks": "task",
        "activity_pass": "pass",
        "event_points": "points",
        "collect_words": "collect",
        "activity_boss": "boss",
        "reward": "claim",
        "rewards": "claim",
        "buy": "shop",
    }
    for item in source:
        feature = _clean_text(item.get("value") if isinstance(item, dict) else item)
        if not feature:
            continue
        feature = alias_map.get(feature, feature)
        if feature not in STAGE_FEATURES:
            raise ValueError(f"{stage_label}不支持的玩法开关：{feature}")
        if feature in seen:
            continue
        seen.add(feature)
        features.append(feature)
    return features


def _normalize_activity_stages(value) -> list[dict]:
    if not isinstance(value, list):
        value = deepcopy(DEFAULT_ACTIVITY_STAGES)

    normalized = []
    seen_keys = set()
    for index, row in enumerate(value, 1):
        if not isinstance(row, dict):
            continue
        stage_label = f"活动阶段{index}"
        stage_type = _clean_text(row.get("stage_type") or row.get("type"), "open")
        if stage_type not in STAGE_TYPE_LABELS:
            raise ValueError(f"{stage_label}类型无效：{stage_type}")
        key = _normalize_activity_key(row.get("key") or stage_type, f"stage_{index}")
        if key in seen_keys:
            raise ValueError(f"{stage_label}编号 {key} 重复")
        seen_keys.add(key)
        start_time = _normalize_time_text(
            row.get("start_time"),
            f"{stage_label}开始时间",
            "0",
            allow_special=_as_bool(row.get("start_special"), True),
        )
        end_time = _normalize_time_text(
            row.get("end_time"),
            f"{stage_label}结束时间",
            "无限",
            allow_special=_as_bool(row.get("end_special"), True),
        )
        try:
            multiplier = float(row.get("multiplier", 1.0))
        except (TypeError, ValueError):
            raise ValueError(f"{stage_label}倍率必须是数字")
        if multiplier < 0:
            raise ValueError(f"{stage_label}倍率必须大于等于 0")
        normalized.append({
            "key": key,
            "name": _clean_text(row.get("name"), STAGE_TYPE_LABELS.get(stage_type, "活动阶段")),
            "stage_type": stage_type,
            "start_time": start_time,
            "end_time": end_time,
            "features": _normalize_stage_features(row.get("features"), stage_type, stage_label),
            "multiplier": round(multiplier, 4),
            "description": _clean_text(row.get("description")),
        })
    return normalized


def _pass_config_int(value, default: int, field_name: str, *, strict: bool) -> int:
    try:
        return _to_non_negative_int(value, field_name)
    except ValueError:
        if strict:
            raise
        return default


def _prepare_activity_pass_config(value, *, strict: bool = False) -> dict:
    raw = value if isinstance(value, dict) else {}
    merged = deepcopy(DEFAULT_ACTIVITY_PASS)
    merged.update(raw)
    merged["enabled"] = _as_bool(merged.get("enabled"), True)
    merged["name"] = _clean_text(merged.get("name"), "节日战令")
    merged["exp_name"] = _clean_text(merged.get("exp_name"), "活跃值")
    merged["level_exp"] = max(1, _pass_config_int(merged.get("level_exp", 100), 100, "战令升级所需活跃", strict=strict))
    merged["max_level"] = max(1, _pass_config_int(merged.get("max_level", 12), 12, "战令最高等级", strict=strict))
    merged["catchup_enabled"] = _as_bool(merged.get("catchup_enabled"), True)
    merged["catchup_start_day"] = max(1, _pass_config_int(merged.get("catchup_start_day", 5), 5, "战令追赶开始天数", strict=strict))
    merged["catchup_level_gap"] = max(1, _pass_config_int(merged.get("catchup_level_gap", 3), 3, "战令追赶等级差", strict=strict))
    try:
        catchup_multiplier = float(merged.get("catchup_multiplier", 1.5))
    except (TypeError, ValueError):
        if strict:
            raise ValueError("战令追赶倍率必须是数字")
        catchup_multiplier = 1.5
    if catchup_multiplier < 1:
        if strict:
            raise ValueError("战令追赶倍率必须大于等于 1")
        catchup_multiplier = 1.5
    merged["catchup_multiplier"] = round(catchup_multiplier, 4)
    return merged


def _prepare_activity_config(config: dict) -> dict:
    if not isinstance(config, dict):
        config = {}

    extensions = dict(config.get("extensions")) if isinstance(config.get("extensions"), dict) else {}
    extensions["repeat_last_daily_reward"] = _as_bool(extensions.get("repeat_last_daily_reward"), True)
    extensions["activity_info_mode"] = _clean_text(extensions.get("activity_info_mode"), "brief")
    extensions["sign_reply_mode"] = _clean_text(extensions.get("sign_reply_mode"), "minimal")
    extensions["activity_pass"] = _prepare_activity_pass_config(extensions.get("activity_pass"))
    if not isinstance(extensions.get("stages"), list):
        extensions["stages"] = deepcopy(DEFAULT_ACTIVITY_STAGES)
    prepared = {
        "template_type": _clean_text(config.get("template_type"), "festival_sign"),
        "template_key": _clean_text(config.get("template_key"), config.get("template_type") or "festival_sign"),
        "enabled": _as_bool(config.get("enabled")),
        "festival_name": _clean_text(config.get("festival_name"), "XX节日"),
        "name": _clean_text(config.get("name"), "XX节日签到活动"),
        "description": _clean_text(config.get("description")),
        "start_time": _clean_text(config.get("start_time"), "0"),
        "end_time": _clean_text(config.get("end_time"), "无限"),
        "sign_command": _clean_text(config.get("sign_command"), "活动签到"),
        "daily_rewards": _as_reward_rows(config.get("daily_rewards"), "day"),
        "milestone_rewards": _as_reward_rows(config.get("milestone_rewards"), "days"),
        "daily_tasks": _as_task_rows(config.get("daily_tasks")),
        "weekly_tasks": _as_task_rows(config.get("weekly_tasks")),
        "extra_rules": _normalize_extra_rules(config.get("extra_rules")),
        "gameplay_activities": _as_gameplay_rows(config.get("gameplay_activities")),
        "extensions": extensions,
    }
    return prepared


def _normalize_activity_config(data: dict) -> dict:
    if not isinstance(data, dict):
        raise ValueError("活动配置数据无效")

    extensions = data.get("extensions") if isinstance(data.get("extensions"), dict) else {}
    extensions = dict(extensions)
    extensions["repeat_last_daily_reward"] = _as_bool(extensions.get("repeat_last_daily_reward"), True)
    extensions["activity_info_mode"] = _clean_text(extensions.get("activity_info_mode"), "brief")
    extensions["sign_reply_mode"] = _clean_text(extensions.get("sign_reply_mode"), "minimal")
    extensions["activity_pass"] = _prepare_activity_pass_config(extensions.get("activity_pass"), strict=True)
    if not isinstance(extensions.get("stages"), list):
        extensions["stages"] = deepcopy(DEFAULT_ACTIVITY_STAGES)
    extensions["stages"] = _normalize_activity_stages(extensions.get("stages"))

    start_time = _normalize_time_text(
        data.get("start_time"),
        "开始时间",
        "0",
        allow_special=_as_bool(data.get("start_special"), True),
    )
    end_time = _normalize_time_text(
        data.get("end_time"),
        "结束时间",
        "无限",
        allow_special=_as_bool(data.get("end_special"), True),
    )
    _validate_time_text(start_time, "开始时间")
    _validate_time_text(end_time, "结束时间")

    daily_rewards = _normalize_reward_rows(data.get("daily_rewards"), "day", "第几天")
    milestone_rewards = _normalize_reward_rows(data.get("milestone_rewards"), "days", "累计天数")
    daily_tasks = _normalize_task_rows(data.get("daily_tasks"), "daily", "每日活动")
    weekly_tasks = _normalize_task_rows(data.get("weekly_tasks"), "weekly", "周常活动")
    gameplay_activities = _normalize_gameplay_activities(data.get("gameplay_activities"))
    if not daily_rewards:
        raise ValueError("至少需要配置一条每日签到奖励")

    template_key = _clean_text(data.get("template_key"), "custom")
    template_type = _clean_text(data.get("template_type"), "festival_sign")

    return {
        "template_type": template_type,
        "template_key": template_key,
        "enabled": _as_bool(data.get("enabled")),
        "festival_name": _clean_text(data.get("festival_name"), "XX节日"),
        "name": _clean_text(data.get("name"), "XX节日签到活动"),
        "description": _clean_text(data.get("description")),
        "start_time": start_time,
        "end_time": end_time,
        "sign_command": _clean_text(data.get("sign_command"), "活动签到"),
        "daily_rewards": daily_rewards,
        "milestone_rewards": milestone_rewards,
        "daily_tasks": daily_tasks,
        "weekly_tasks": weekly_tasks,
        "extra_rules": _normalize_extra_rules(data.get("extra_rules")),
        "gameplay_activities": gameplay_activities,
        "extensions": extensions,
    }


@app.route("/activity")
def activity_management():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    config = _prepare_activity_config(load_activity_config())
    ok, reason = activity_state(config)
    runtime = activity_runtime_state(config)
    return render_template(
        "activity.html",
        activity_config=config,
        activity_templates=_serialize_templates(),
        gameplay_templates=_serialize_gameplay_templates(),
        gameplay_template_groups=_serialize_gameplay_template_groups(),
        activity_event_choices=_serialize_event_choices(),
        activity_state={"ok": ok, "text": "进行中" if ok else reason, "runtime": runtime},
        activity_config_path=str(ACTIVITY_CONFIG_PATH),
    )


@app.route("/api/activity/config", methods=["GET", "POST"])
def api_activity_config():
    if "admin_id" not in session:
        return api_error("未登录")

    if request.method == "GET":
        config = _prepare_activity_config(load_activity_config())
        ok, reason = activity_state(config)
        runtime = activity_runtime_state(config)
        return api_success(
            config=config,
            templates=_serialize_templates(),
            gameplay_templates=_serialize_gameplay_templates(),
            gameplay_template_groups=_serialize_gameplay_template_groups(),
            event_choices=_serialize_event_choices(),
            state={"ok": ok, "text": "进行中" if ok else reason, "runtime": runtime},
            config_path=str(ACTIVITY_CONFIG_PATH),
        )

    try:
        payload = request.get_json() or {}
        config = _normalize_activity_config(payload.get("config", payload))
        save_activity_config(config)
        ok, reason = activity_state(config)
        runtime = activity_runtime_state(config)
        return api_success(
            message="活动配置已保存",
            config=config,
            state={"ok": ok, "text": "进行中" if ok else reason, "runtime": runtime},
        )
    except Exception as e:
        return api_error(str(e))


@app.route("/api/activity/template/<template_key>")
def api_activity_template(template_key):
    if "admin_id" not in session:
        return api_error("未登录")

    template = ACTIVITY_TEMPLATE_DEFINITIONS.get(str(template_key))
    if not template:
        return api_error("活动模板不存在")
    return api_success(template=_serialize_templates()[template_key])


@app.route("/api/activity/gameplay-template/<template_key>")
def api_activity_gameplay_template(template_key):
    if "admin_id" not in session:
        return api_error("未登录")

    template = GAMEPLAY_TEMPLATE_DEFINITIONS.get(str(template_key))
    if not template:
        bundle = GAMEPLAY_BUNDLE_DEFINITIONS.get(str(template_key))
        if bundle:
            return api_success(template=_serialize_gameplay_templates()[template_key])
        return api_error("玩法模板不存在")
    return api_success(template=_serialize_gameplay_templates()[template_key])


@app.route("/api/activity/data")
def api_activity_data():
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        data = get_activity_data_overview(
            activity_key=request.args.get("activity_key"),
            user_id=request.args.get("user_id"),
            limit=request.args.get("limit", 10),
        )
        return api_success(data=data)
    except Exception as e:
        return api_error(str(e))


@app.route("/api/activity/data/reset", methods=["POST"])
def api_activity_data_reset():
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        payload = request.get_json() or {}
        message = reset_activity_data(payload.get("scope"), payload.get("activity_key"))
        return api_success(message=message)
    except Exception as e:
        return api_error(str(e))


@app.route("/api/activity/data/adjust", methods=["POST"])
def api_activity_data_adjust():
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        payload = request.get_json() or {}
        adjust_type = _clean_text(payload.get("type"))
        if adjust_type == "points":
            result = adjust_activity_points(
                payload.get("activity_key"),
                payload.get("user_id"),
                payload.get("amount"),
            )
        elif adjust_type == "word":
            result = adjust_collect_word(
                payload.get("activity_key"),
                payload.get("user_id"),
                payload.get("word_char"),
                payload.get("amount"),
            )
        elif adjust_type == "pass_exp":
            result = adjust_activity_pass_exp(
                payload.get("user_id"),
                payload.get("amount"),
            )
        else:
            raise ValueError("调整类型无效")
        return api_success(message="活动数据已调整", result=result)
    except Exception as e:
        return api_error(str(e))
