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

fun_menu_cmd = on_command("娱乐菜单", aliases={"娱乐帮助", "娱乐功能"}, priority=5, block=True)
fun_menu_cmd2 = on_command("娱乐帮助2", aliases={"娱乐菜单2", "娱乐功能2"}, priority=5, block=True)


def _build_fun_help_page(page: int):
    if page == 1:
        title = "🎮 娱乐帮助 第1页"
        text_msg = (
            "=== 娱乐帮助 第1页 ===\n"
            "1、今日老婆\n"
            "2、今日超能力\n"
            "3、随机点歌\n"
            "4、随机语音\n"
            "5、每日Bing图\n"
            "6、舔狗日记\n"
            "7、随机一言\n"
            "8、历史上的今天\n\n"
            "发送【娱乐帮助2】查看第2页。"
        )
        shell_text = (
            "娱乐功能 第1页\r"
            "1. 今日老婆\r"
            "2. 今日超能力\r"
            "3. 随机点歌\r"
            "4. 随机语音\r"
            "5. 每日Bing图\r"
            "6. 舔狗日记\r"
            "7. 随机一言\r"
            "8. 历史上的今天\r\r"
            "发送 娱乐帮助2 查看下一页"
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
                generate_command("下一页", command="娱乐帮助2", status="end", msg2="\r[直接发送对应指令即可使用"),
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
            "[下一页](mqqapi://aio/inlinecmd?command=娱乐帮助2&enter=false&reply=false)"
        )
        fallback_buttons = {
            "k1": "今日老婆", "v1": "今日老婆",
            "k2": "随机语音", "v2": "随机语音",
            "k3": "下一页", "v3": "娱乐帮助2",
        }
        return title, text_msg, shell_text, t1, t2, md_msg, fallback_buttons

    else:
        title = "🎮 娱乐帮助 第2页"
        text_msg = (
            "=== 娱乐帮助 第2页 ===\n"
            "1、肯德基文案\n"
            "2、搞笑段子\n"
            "3、Steam喜加一\n"
            "4、脑筋急转弯\n"
            "5、弱智吧问答\n"
            "6、热榜60S\n"
            "7、热榜图片\n"
            "8、60S读世界\n\n"
            "补充功能：每日60S图片\n"
            "发送【娱乐帮助】查看第1页。"
        )
        shell_text = (
            "娱乐功能 第2页\r"
            "1. 肯德基文案\r"
            "2. 搞笑段子\r"
            "3. Steam喜加一\r"
            "4. 脑筋急转弯\r"
            "5. 弱智吧问答\r"
            "6. 热榜60S\r"
            "7. 热榜图片\r"
            "8. 60S读世界\r\r"
            "补充功能：每日60S图片\r"
            "发送 娱乐帮助 查看上一页"
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
                generate_command("热榜60S", command="热榜60S", status="start", msg2="\r"),
                generate_command("热榜图片", command="热榜图片", status="start", msg2=" | "),
                generate_command("60S读世界", command="60S读世界", status="start", msg2="\r"),
                generate_command("上一页", command="娱乐帮助", status="end", msg2="\r[直接发送对应指令即可使用"),
            ]
        }
        md_msg = (
            "## 🎮 娱乐帮助 第2页\r"
            "> [肯德基文案](mqqapi://aio/inlinecmd?command=肯德基文案&enter=false&reply=false) | "
            "[搞笑段子](mqqapi://aio/inlinecmd?command=搞笑段子&enter=false&reply=false)\r"
            "[Steam喜加一](mqqapi://aio/inlinecmd?command=Steam喜加一&enter=false&reply=false) | "
            "[脑筋急转弯](mqqapi://aio/inlinecmd?command=脑筋急转弯&enter=false&reply=false)\r"
            "[弱智吧问答](mqqapi://aio/inlinecmd?command=弱智吧问答&enter=false&reply=false) | "
            "[热榜60S](mqqapi://aio/inlinecmd?command=热榜60S&enter=false&reply=false)\r"
            "[热榜图片](mqqapi://aio/inlinecmd?command=热榜图片&enter=false&reply=false) | "
            "[60S读世界](mqqapi://aio/inlinecmd?command=60S读世界&enter=false&reply=false)\r\r"
            "[上一页](mqqapi://aio/inlinecmd?command=娱乐帮助&enter=false&reply=false) | "
            "[每日60S图片](mqqapi://aio/inlinecmd?command=每日60S图片&enter=false&reply=false)"
        )
        fallback_buttons = {
            "k1": "上一页", "v1": "娱乐帮助",
            "k2": "热榜60S", "v2": "热榜60S",
            "k3": "60S读世界", "v3": "60S读世界",
        }
        return title, text_msg, shell_text, t1, t2, md_msg, fallback_buttons


async def send_fun_help_page(bot: Bot, event, config: XiuConfig, page: int):
    title, text_msg, shell_text, t1, t2, native_md_msg, fallback_buttons = _build_fun_help_page(page)

    if config.markdown_status:
        # ===== 模板MD =====
        if config.markdown_id:
            try:
                s1 = {
                    "key": "s1",
                    "values": [f"python\r{shell_text}"]
                }

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

    # ===== 普通文本 =====
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
    await send_fun_help_page(bot, event, config, page=1)


@fun_menu_cmd2.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    config = XiuConfig()
    await send_fun_help_page(bot, event, config, page=2)