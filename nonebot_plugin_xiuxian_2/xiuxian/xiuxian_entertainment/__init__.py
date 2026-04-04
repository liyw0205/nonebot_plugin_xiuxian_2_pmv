from .command import *
from .mod.today_wife import *
from .mod.today_superpower import *
from .mod.click_music import *
from .mod.random_voice import *
from .mod.daily_bing import *
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


fun_menu_cmd = on_command("娱乐帮助", aliases={"娱乐菜单", "娱乐功能"}, priority=5, block=True)


def _build_fun_help_page(page: int):
    # ===================== 第1页 =====================
    if page == 1:
        title = "🎮 娱乐帮助 第1页"
        text_msg = (
            "=== 娱乐帮助 第1页 ===\n"
            "1、今日老婆\n"
            "2、今日超能力\n"
            "3、随机点歌\n"
            "4、随机语音（含怼人）\n"
            "5、每日Bing图\n"
            "6、舔狗日记\n"
            "7、随机一言\n"
            "8、历史上的今天\n\n"
            "发送【娱乐帮助 2】查看第2页。"
        )
        shell_text = (
            "娱乐功能 第1页\r"
            "1. 今日老婆\r"
            "2. 今日超能力\r"
            "3. 随机点歌\r"
            "4. 随机语音（含怼人）\r"
            "5. 每日Bing图\r"
            "6. 舔狗日记\r"
            "7. 随机一言\r"
            "8. 历史上的今天\r\r"
            "发送 娱乐帮助 2 查看下一页"
        )
        t1 = {
            "key": "t1",
            "values": [
                generate_command("今日老婆", command="今日老婆", status="start", msg2=" | "),
                generate_command("今日超能力", command="今日超能力", status="start", msg2="\r"),
                generate_command("随机点歌", command="随机点歌", status="start", msg2=" | "),
                generate_command("随机语音", command="随机语音", status="end", msg2="\r[娱乐功能"),
            ]
        }
        t2 = {
            "key": "t2",
            "values": [
                generate_command("每日Bing图", command="每日Bing图", status="start", msg2=" | "),
                generate_command("舔狗日记", command="舔狗日记", status="start", msg2="\r"),
                generate_command("随机一言", command="随机一言", status="start", msg2=" | "),
                generate_command("历史上的今天", command="历史上的今天", status="start", msg2="\r"),
                generate_command("下一页", command="娱乐帮助 2", status="end", msg2="\r[直接发送对应指令即可使用"),
            ]
        }
        md_msg = (
            "## 🎮 娱乐帮助 第1页\r"
            "> [今日老婆](mqqapi://aio/inlinecmd?command=今日老婆&enter=false&reply=false) | "
            "[今日超能力](mqqapi://aio/inlinecmd?command=今日超能力&enter=false&reply=false)\r"
            "[随机点歌](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false) | "
            "[随机语音](mqqapi://aio/inlinecmd?command=随机语音&enter=false&reply=false)\r"
            "[每日Bing图](mqqapi://aio/inlinecmd?command=每日Bing图&enter=false&reply=false) | "
            "[舔狗日记](mqqapi://aio/inlinecmd?command=舔狗日记&enter=false&reply=false)\r"
            "[随机一言](mqqapi://aio/inlinecmd?command=随机一言&enter=false&reply=false) | "
            "[历史上的今天](mqqapi://aio/inlinecmd?command=历史上的今天&enter=false&reply=false)\r\r"
            "[下一页](mqqapi://aio/inlinecmd?command=娱乐帮助%202&enter=false&reply=false)"
        )
        fallback_buttons = {
            "k1": "今日老婆", "v1": "今日老婆",
            "k2": "随机语音", "v2": "随机语音",
            "k3": "下一页", "v3": "娱乐帮助 2",
        }
        return title, text_msg, shell_text, t1, t2, md_msg, fallback_buttons

    # ===================== 第2页 =====================
    elif page == 2:
        title = "🎮 娱乐帮助 第2页"
        text_msg = (
            "=== 娱乐帮助 第2页 ===\n"
            "1、肯德基文案\n"
            "2、搞笑段子\n"
            "3、Steam喜加一\n"
            "4、脑筋急转弯\n"
            "5、弱智吧问答\n"
            "6、热榜图片\n"
            "7、60S读世界\n"
            "8、每日60S图片\n\n"
            "发送【娱乐帮助 3】查看第3页。"
        )
        shell_text = (
            "娱乐功能 第2页\r"
            "1. 肯德基文案\r"
            "2. 搞笑段子\r"
            "3. Steam喜加一\r"
            "4. 脑筋急转弯\r"
            "5. 弱智吧问答\r"
            "6. 热榜图片\r"
            "7. 60S读世界\r"
            "8. 每日60S图片\r\r"
            "发送 娱乐帮助 3 查看下一页"
        )
        t1 = {
            "key": "t1",
            "values": [
                generate_command("肯德基文案", command="肯德基文案", status="start", msg2=" | "),
                generate_command("搞笑段子", command="搞笑段子", status="start", msg2="\r"),
                generate_command("Steam喜加一", command="Steam喜加一", status="start", msg2=" | "),
                generate_command("脑筋急转弯", command="脑筋急转弯", status="end", msg2="\r[娱乐功能"),
            ]
        }
        t2 = {
            "key": "t2",
            "values": [
                generate_command("弱智吧问答", command="弱智吧问答", status="start", msg2=" | "),
                generate_command("热榜图片", command="热榜图片", status="start", msg2="\r"),
                generate_command("60S读世界", command="60S读世界", status="start", msg2=" | "),
                generate_command("每日60S图片", command="每日60S图片", status="start", msg2="\r"),
                generate_command("上一页", command="娱乐帮助 1", status="start", msg2=" | "),
                generate_command("下一页", command="娱乐帮助 3", status="end", msg2="\r[直接发送对应指令即可使用"),
            ]
        }
        md_msg = (
            "## 🎮 娱乐帮助 第2页\r"
            "> [肯德基文案](mqqapi://aio/inlinecmd?command=肯德基文案&enter=false&reply=false) | "
            "[搞笑段子](mqqapi://aio/inlinecmd?command=搞笑段子&enter=false&reply=false)\r"
            "[Steam喜加一](mqqapi://aio/inlinecmd?command=Steam喜加一&enter=false&reply=false) | "
            "[脑筋急转弯](mqqapi://aio/inlinecmd?command=脑筋急转弯&enter=false&reply=false)\r"
            "[弱智吧问答](mqqapi://aio/inlinecmd?command=弱智吧问答&enter=false&reply=false) | "
            "[热榜图片](mqqapi://aio/inlinecmd?command=热榜图片&enter=false&reply=false)\r"
            "[60S读世界](mqqapi://aio/inlinecmd?command=60S读世界&enter=false&reply=false) | "
            "[每日60S图片](mqqapi://aio/inlinecmd?command=每日60S图片&enter=false&reply=false)\r\r"
            "[上一页](mqqapi://aio/inlinecmd?command=娱乐帮助%201&enter=false&reply=false) | "
            "[下一页](mqqapi://aio/inlinecmd?command=娱乐帮助%203&enter=false&reply=false)"
        )
        fallback_buttons = {
            "k1": "上一页", "v1": "娱乐帮助 1",
            "k2": "下一页", "v2": "娱乐帮助 3",
            "k3": "60S读世界", "v3": "60S读世界",
        }
        return title, text_msg, shell_text, t1, t2, md_msg, fallback_buttons

    # ===================== 第3页 =====================
    else:
        title = "🎮 娱乐帮助 第3页"
        text_msg = (
            "=== 娱乐帮助 第3页 ===\n"
            "1、小游戏帮助\n"
            "2、点歌帮助\n"
            "3、哈基米\n\n"
            "小游戏新增：\n"
            "4、猜数字\n"
            "   - 开始猜数字\n"
            "   - 猜 50\n"
            "   - 猜数字信息\n"
            "   - 结束猜数字\n\n"
            "发送【娱乐帮助 1】返回第1页。"
        )
        shell_text = (
            "娱乐功能 第3页\r"
            "1. 小游戏帮助\r"
            "2. 点歌帮助\r"
            "3. 哈基米\r"
            "4. 猜数字\r"
            "   开始猜数字 / 猜 50 / 猜数字信息 / 结束猜数字\r\r"
            "发送 娱乐帮助 1 返回第一页"
        )
        t1 = {
            "key": "t1",
            "values": [
                generate_command("小游戏帮助", command="小游戏帮助", status="start", msg2=" | "),
                generate_command("点歌帮助", command="点歌帮助", status="start", msg2="\r"),
                generate_command("哈基米", command="哈基米", status="start", msg2=" | "),
                generate_command("猜数字帮助", command="猜数字帮助", status="end", msg2="\r[娱乐功能"),
            ]
        }
        t2 = {
            "key": "t2",
            "values": [
                generate_command("开始猜数字", command="开始猜数字", status="start", msg2=" | "),
                generate_command("猜 50", command="猜 50", status="start", msg2=" | "),
                generate_command("猜数字信息", command="猜数字信息", status="start", msg2="\r"),
                generate_command("上一页", command="娱乐帮助 2", status="start", msg2=" | "),
                generate_command("首页", command="娱乐帮助 1", status="end", msg2="\r[直接发送对应指令即可使用"),
            ]
        }
        md_msg = (
            "## 🎮 娱乐帮助 第3页\r"
            "> [小游戏帮助](mqqapi://aio/inlinecmd?command=小游戏帮助&enter=false&reply=false) | "
            "[点歌帮助](mqqapi://aio/inlinecmd?command=点歌帮助&enter=false&reply=false)\r"
            "[哈基米](mqqapi://aio/inlinecmd?command=哈基米&enter=false&reply=false) | "
            "[猜数字帮助](mqqapi://aio/inlinecmd?command=猜数字帮助&enter=false&reply=false)\r\r"
            "[开始猜数字](mqqapi://aio/inlinecmd?command=开始猜数字&enter=false&reply=false) | "
            "[猜 50](mqqapi://aio/inlinecmd?command=猜%2050&enter=false&reply=false) | "
            "[猜数字信息](mqqapi://aio/inlinecmd?command=猜数字信息&enter=false&reply=false)\r\r"
            "[上一页](mqqapi://aio/inlinecmd?command=娱乐帮助%202&enter=false&reply=false) | "
            "[首页](mqqapi://aio/inlinecmd?command=娱乐帮助%201&enter=false&reply=false)"
        )
        fallback_buttons = {
            "k1": "开始猜数字", "v1": "开始猜数字",
            "k2": "小游戏帮助", "v2": "小游戏帮助",
            "k3": "首页", "v3": "娱乐帮助 1",
        }
        return title, text_msg, shell_text, t1, t2, md_msg, fallback_buttons


async def send_fun_help_page(bot: Bot, event, config: XiuConfig, page: int):
    title, text_msg, shell_text, t1, t2, native_md_msg, fallback_buttons = _build_fun_help_page(page)

    if config.markdown_status:
        # ===== 模板MD =====
        if config.markdown_id:
            try:
                s1 = {"key": "s1", "values": [f"python\r{shell_text}"]}
                md_msg = MessageSegment.markdown_template(
                    bot,
                    config.markdown_id,
                    [t1, t2, s1]
                )
                await bot.send(event=event, message=md_msg)
                return
            except Exception as e:
                logger.warning(f"娱乐帮助 第{page}页 模板MD发送失败：{e}")

        # ===== 原生MD =====
        if not is_channel_event(event):
            try:
                await bot.send(event=event, message=MessageSegment.markdown(bot, native_md_msg))
                return
            except Exception as e:
                logger.warning(f"娱乐帮助 第{page}页 原生MD发送失败：{e}")

    # ===== 普通文本回退 =====
    await handle_send(
        bot, event, text_msg,
        md_type="娱乐",
        k1=fallback_buttons["k1"], v1=fallback_buttons["v1"],
        k2=fallback_buttons["k2"], v2=fallback_buttons["v2"],
        k3=fallback_buttons["k3"], v3=fallback_buttons["v3"],
    )


@fun_menu_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    config = XiuConfig()

    raw_msg = str(event.get_message()).strip()
    # 支持：娱乐帮助 / 娱乐帮助 2 / 娱乐菜单 3 / 娱乐功能 1
    parts = raw_msg.split()
    page = 1

    if len(parts) >= 2:
        try:
            page = int(parts[1])
        except Exception:
            await handle_send(
                bot, event,
                "页码格式错误，示例：娱乐帮助 2",
                md_type="娱乐",
                k1="第1页", v1="娱乐帮助 1",
                k2="第2页", v2="娱乐帮助 2",
                k3="第3页", v3="娱乐帮助 3",
            )
            return

    if page < 1:
        page = 1
    if page > 3:
        page = 3

    await send_fun_help_page(bot, event, config, page=page)


game_menu_cmd = on_command("小游戏帮助", aliases={"游戏帮助", "小游戏菜单", "游戏菜单"}, priority=5, block=True)


async def send_game_help(bot: Bot, event, config: XiuConfig):
    text_msg = (
        "🎮 小游戏帮助\n"
        "1、五子棋（双人）\n"
        "   - 开始五子棋 [房间号]\n"
        "   - 加入五子棋 <房间号>\n"
        "   - 落子 A1\n"
        "   - 认输 / 棋局信息 / 退出五子棋\n\n"
        "2、单人五子棋（AI）\n"
        "   - 开始单人五子棋 [房间号]\n\n"
        "3、扫雷\n"
        "   - 开始扫雷 [初级|中级|高级|自定义 宽 高 雷数]\n"
        "   - 翻开 A1 / 标记 B2 / 扫雷信息 / 结束扫雷\n\n"
        "4、十点半\n"
        "   - 开始十点半 [房间号]\n"
        "   - 加入十点半 <房间号>\n"
        "   - 结算十点半（仅房主）\n"
        "   - 退出十点半 / 十点半信息\n\n"
        "5、猜数字\n"
        "   - 开始猜数字\n"
        "   - 猜 50\n"
        "   - 猜数字信息\n"
        "   - 结束猜数字\n"
    )

    # 模板MD
    if config.markdown_status and config.markdown_id:
        try:
            t1 = {
                "key": "t1",
                "values": [
                    generate_command("五子棋帮助", command="五子棋帮助", status="start", msg2=" | "),
                    generate_command("扫雷帮助", command="扫雷帮助", status="start", msg2=" | "),
                    generate_command("十点半帮助", command="十点半帮助", status="start", msg2=" | "),
                    generate_command("猜数字帮助", command="猜数字帮助", status="end", msg2="\r[小游戏"),
                ]
            }
            t2 = {
                "key": "t2",
                "values": [
                    generate_command("开始五子棋", command="开始五子棋", status="start", msg2=" | "),
                    generate_command("开始单人五子棋", command="开始单人五子棋", status="start", msg2="\r"),
                    generate_command("开始扫雷", command="开始扫雷", status="start", msg2=" | "),
                    generate_command("开始十点半", command="开始十点半", status="start", msg2=" | "),
                    generate_command("开始猜数字", command="开始猜数字", status="end", msg2="\r[发送命令即可开始"),
                ]
            }
            s1 = {
                "key": "s1",
                "values": [(
                    "python\r"
                    "小游戏菜单\r"
                    "1. 五子棋（双人/单人AI）\r"
                    "2. 扫雷\r"
                    "3. 十点半\r"
                    "4. 猜数字\r"
                    "发送：五子棋帮助 / 扫雷帮助 / 十点半帮助 / 猜数字帮助 查看详细规则"
                )]
            }
            md_msg = MessageSegment.markdown_template(
                bot,
                config.markdown_id,
                [t1, t2, s1]
            )
            await bot.send(event=event, message=md_msg)
            return
        except Exception as e:
            logger.warning(f"小游戏帮助 模板MD发送失败：{e}")

    # 原生MD
    if config.markdown_status and not is_channel_event(event):
        try:
            md_msg = (
                "## 🎮 小游戏帮助\r"
                "> [五子棋帮助](mqqapi://aio/inlinecmd?command=五子棋帮助&enter=false&reply=false) | "
                "[扫雷帮助](mqqapi://aio/inlinecmd?command=扫雷帮助&enter=false&reply=false) | "
                "[十点半帮助](mqqapi://aio/inlinecmd?command=十点半帮助&enter=false&reply=false) | "
                "[猜数字帮助](mqqapi://aio/inlinecmd?command=猜数字帮助&enter=false&reply=false)\r\r"
                "[开始五子棋](mqqapi://aio/inlinecmd?command=开始五子棋&enter=false&reply=false) | "
                "[开始单人五子棋](mqqapi://aio/inlinecmd?command=开始单人五子棋&enter=false&reply=false)\r"
                "[开始扫雷](mqqapi://aio/inlinecmd?command=开始扫雷&enter=false&reply=false) | "
                "[开始十点半](mqqapi://aio/inlinecmd?command=开始十点半&enter=false&reply=false) | "
                "[开始猜数字](mqqapi://aio/inlinecmd?command=开始猜数字&enter=false&reply=false)"
            )
            await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
            return
        except Exception as e:
            logger.warning(f"小游戏帮助 原生MD发送失败：{e}")

    # 文本回退
    await handle_send(
        bot, event, text_msg,
        md_type="娱乐",
        k1="五子棋帮助", v1="五子棋帮助",
        k2="扫雷帮助", v2="扫雷帮助",
        k3="猜数字帮助", v3="猜数字帮助",
    )


@game_menu_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    config = XiuConfig()
    await send_game_help(bot, event, config)