from nonebot.params import CommandArg

from .command import *
from ..xiuxian_utils.utils import (
    parse_page_arg,
    paginate_text_blocks,
    build_pagination_buttons,
    send_help_message,
)
from .mod.today_wife import *
from .mod.today_superpower import *
from .mod.answer_book import *
from .mod.click_music import *
from .mod.random_voice import *
from .mod.daily_bing import *
from .mod.moyu_calendar import *
from .mod.cat_picture import *
from .mod.tiangou_diary import *
from .mod.hitokoto import *
from .mod.history_today import *
from .mod.kfc_copywriting import *
from .mod.random_duanzi import *
from .mod.steam_plus_one import *
from .mod.brainteasers import *
from .mod.ruozhiba_qa import *
from .mod.hot_rank_image import *
from .mod.daily_60s_image import *
from .mod.world_60s import *
from .mod.music import *
from .mod.random_hakimi import *
from .mod.half_ten import *
from .mod.gomoku import *
from .mod.minesweeper import *
from .mod.guess_number import *
from .mod.guess_number_puzzle import *
from .mod.random_girl_video import *
from .mod.anime_reaction import *
from .mod.pokemon_box import *
from .mod.random_anime_box import *
from .mod.media_parse_link import *
from .mod.bangumi_calendar import *
from .mod.newapi_commands import *
from .mod.newapi_scheduler import *  # noqa: F401 — 注册 12:30 自动签到
from .mod.alist_webdav import *


fun_menu_cmd = on_command("娱乐帮助", aliases={"娱乐菜单", "娱乐功能"}, priority=5, block=True)


__FUN_HELP__ = """**娱乐帮助**

**日常趣味**
- 今日老婆
- 今日超能力
- 答案之书
- 舔狗日记
- 随机一言
- 历史上的今天
- 肯德基文案
- 搞笑段子
- 弱智吧问答
- 脑筋急转弯

**图片与资讯**
- 每日Bing图
- 摸鱼日报
- 随机猫猫 / 猫猫说 / 猫猫帮助
- 随机二次元 / 随机猫娘 / 抱抱 / 贴贴 / 摸摸
- 番剧（Bangumi）：今日番剧 / 番剧周表（别名：每日番剧、番剧日历、每周番剧、番剧总表）
- 番剧盲盒 / 随机番剧
- 宝可梦盲盒 / 宝可梦图鉴
- 热榜图片
- 60S读世界
- 每日60S图片

**音视频**
- 随机点歌
- 随机语音
- 点歌帮助
- 随机小姐姐
- 哈基米

**其他**
- Steam喜加一
- NewAPI：newapi帮助 / newapi绑定 / newapi查看 / newapi签到 / newapi签到历史 / newapi自动签到 / newapi信息 / newapi删除
- WebDAV：webdav帮助 / webdav查看 / webdav列表 / webdav信息 / webdav链接 / webdav文件；绑定和删除仅管理员可用
- 链接解析（视频解析 / 解析链接）
- 发含分享链接的消息可自动解析

**小游戏**
- 小游戏帮助
- 五子棋帮助 / 扫雷帮助 / 十点半帮助 / 猜数字帮助 / 猜数谜帮助
""".strip()


__GAME_HELP__ = """**小游戏帮助**

**五子棋（双人）**
- 开始五子棋 [房间号]
- 加入五子棋 <房间号>
- 落子 A1
- 认输 / 棋局信息 / 退出五子棋

**单人五子棋（AI）**
- 开始单人五子棋 [房间号]

**扫雷**
- 开始扫雷 [初级|中级|高级|自定义 宽 高 雷数]
- 翻开 A1 / 标记 B2 / 扫雷信息 / 结束扫雷

**十点半**
- 开始十点半 [房间号]
- 加入十点半 <房间号>
- 结算十点半（仅房主）
- 退出十点半 / 十点半信息

**猜数字**
- 开始猜数字
- 猜 50
- 猜数字信息
- 结束猜数字

**猜数谜**
- 开始猜数谜 [简单|普通|困难]
- 猜数谜 2223
- 猜数谜 状态
- 猜数谜 答案 / 猜数谜 结束
""".strip()


@fun_menu_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def fun_menu_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    page = parse_page_arg(args.extract_plain_text())
    msg, page, total_pages = paginate_text_blocks(__FUN_HELP__, page, per_page=3)
    msg = f"{msg}\n\n翻页：娱乐帮助 页码；小游戏详见「小游戏帮助」。"
    button_kwargs = build_pagination_buttons(
        "娱乐帮助",
        page,
        total_pages,
        extras=[
            ("舔狗日记", "舔狗日记"),
            ("答案之书", "答案之书"),
            ("摸鱼日报", "摸鱼日报"),
            ("猫猫", "随机猫猫"),
            ("二次元", "二次元帮助"),
            ("宝可梦", "宝可梦盲盒"),
            ("番剧盲盒", "番剧盲盒"),
            ("今日番剧", "今日番剧"),
            ("WebDAV", "webdav帮助"),
            ("链接解析", "链接解析"),
            ("NewAPI", "newapi帮助"),
            ("小游戏", "小游戏帮助"),
            ("点歌", "点歌帮助"),
        ],
    )
    await send_help_message(
        bot,
        event,
        msg,
        **button_kwargs,
    )
    await fun_menu_cmd.finish()


game_menu_cmd = on_command("小游戏帮助", aliases={"游戏帮助", "小游戏菜单", "游戏菜单"}, priority=5, block=True)


@game_menu_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def game_menu_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await send_help_message(
        bot,
        event,
        __GAME_HELP__,
        k1="五子棋帮助",
        v1="五子棋帮助",
        k2="扫雷帮助",
        v2="扫雷帮助",
        k3="猜数字帮助",
        v3="猜数字帮助",
        k4="猜数谜帮助",
        v4="猜数谜帮助",
    )
    await game_menu_cmd.finish()
