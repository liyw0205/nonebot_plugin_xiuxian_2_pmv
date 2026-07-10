from ..command import *
from .music_utils import (
    load_music_config,
    set_music_config,
    detect_platform_from_cmd,
    search_music,
    set_music_session,
    get_music_session,
    clear_music_session,
    build_song_list_page_text,
    send_song_rich,
    get_platform_display_name,
)

music_search_cmd = on_command(
    "点歌",
    aliases={
        "QQ点歌", "网易点歌", "网易云点歌", "酷狗点歌", "酷我点歌",
        "百度点歌", "一听点歌", "咪咕点歌", "荔枝点歌", "蜻蜓点歌",
        "喜马点歌", "5sing原创", "5sing翻唱", "全民K歌"
    },
    priority=5,
    block=True
)

music_select_cmd = on_command(
    "选歌",
    aliases={"播放歌曲", "点歌选择"},
    priority=5,
    block=True
)

music_config_cmd = on_command(
    "点歌配置",
    aliases={"音乐配置"},
    priority=5,
    permission=SUPERUSER,
    block=True
)

music_help_cmd = on_command(
    "点歌帮助",
    aliases={"音乐帮助", "点歌说明"},
    priority=5,
    block=True
)

music_page_cmd = on_command(
    "点歌翻页",
    aliases={"点歌下一页", "点歌上一页"},
    priority=5,
    block=True
)


@music_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def music_help_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = (
        "**点歌帮助**\n\n"
        "**基础用法**\n"
        "- `点歌 歌名`\n"
        "- `选歌 序号`\n"
        "- `点歌下一页` / `点歌上一页` / `点歌翻页 第3页`\n\n"
        "**平台点歌**\n"
        "- `网易点歌 晴天`\n"
        "- `QQ点歌 夜曲`\n\n"
        "> 搜索到多首歌时，先返回列表，再用 `选歌 1` 播放。"
    )
    await send_help_message(
        bot, event,
        msg,
        k1="点歌示例", v1="点歌 稻香",
        k2="下一页", v2="点歌下一页",
        k3="娱乐帮助", v3="娱乐帮助"
    )
    await music_help_cmd.finish()


@music_search_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def music_search_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    raw_msg = str(event.get_message()).strip()
    if not raw_msg:
        await music_search_cmd.finish()

    parts = raw_msg.split(maxsplit=1)
    cmd = parts[0].strip()

    if len(parts) < 2 or not parts[1].strip():
        await handle_send(
            bot, event,
            "【点歌】\n"
            "用法：点歌 歌名\n"
            "例如：点歌 稻香",
            md_type="娱乐",
            k1="示例1", v1=quote("点歌 稻香", safe=""),
            k2="示例2", v2=quote("网易点歌 晴天", safe=""),
            k3="帮助", v3=quote("点歌帮助", safe="")
        )
        await music_search_cmd.finish()

    keyword = parts[1].strip()
    cfg = load_music_config()
    platform = detect_platform_from_cmd(cmd, fallback=cfg["default_platform"])
    platform_name = get_platform_display_name(platform)

    try:
        songs = await run_blocking_io(
            search_music,
            keyword,
            platform,
            cfg["song_limit"],
            timeout=30,
        )
    except Exception as e:
        logger.warning(f"点歌搜索失败: {e}")
        await handle_send(
            bot, event,
            f"点歌搜索失败：{e}",
            md_type="娱乐",
            k1="重试", v1=quote(f"{cmd} {keyword}", safe=""),
            k2="随机语音", v2=quote("随机语音", safe=""),
            k3="点歌帮助", v3=quote("点歌帮助", safe="")
        )
        await music_search_cmd.finish()

    if not songs:
        await handle_send(
            bot, event,
            f"搜索【{keyword}】无结果",
            md_type="娱乐",
            k1="重试", v1=quote(f"{cmd} {keyword}", safe=""),
            k2="换个关键词", v2=quote("点歌 周杰伦", safe=""),
            k3="帮助", v3=quote("点歌帮助", safe="")
        )
        await music_search_cmd.finish()

    # 只有一首时直接发送（模板MD/原生MD/普通图文 + 音频）
    if len(songs) == 1:
        ok, tip = await send_song_rich(bot, event, songs[0])
        if not ok:
            await handle_send(
                bot, event,
                tip,
                md_type="娱乐",
                k1="再试一次", v1=quote(f"{cmd} {keyword}", safe=""),
                k2="换首歌", v2=quote("点歌 晴天", safe=""),
                k3="帮助", v3=quote("点歌帮助", safe="")
            )
        await music_search_cmd.finish()

    user_id = str(event.get_user_id())
    page_size = int(cfg.get("page_size", 5))
    set_music_session(
        user_id=user_id,
        songs=songs,
        platform=platform,
        timeout_sec=cfg["select_timeout"],
        page_size=page_size
    )

    text_msg, _ = build_song_list_page_text(
        platform_name, songs, page=1, page_size=page_size, markdown=True
    )
    fallback_msg, _ = build_song_list_page_text(
        platform_name, songs, page=1, page_size=page_size, markdown=False
    )

    await handle_send(
        bot, event,
        text_msg,
        native_markdown=True,
        fallback_msg=fallback_msg,
        keyboard_rows=[
            [("选歌1", "选歌 1"), ("下一页", "点歌下一页"), ("点歌帮助", "点歌帮助")]
        ],
    )
    await music_search_cmd.finish()


@music_page_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def music_page_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    import re

    raw_msg = str(event.get_message()).strip()
    user_id = str(event.get_user_id())
    session_data = get_music_session(user_id)

    if not session_data:
        await handle_send(
            bot, event,
            "当前没有可翻页的点歌结果，请先点歌。",
            md_type="娱乐",
            k1="去点歌", v1=quote("点歌 稻香", safe=""),
            k2="点歌帮助", v2=quote("点歌帮助", safe=""),
            k3="娱乐帮助", v3=quote("娱乐帮助", safe="")
        )
        await music_page_cmd.finish()

    songs = session_data["songs"]
    platform = session_data["platform"]
    page = int(session_data.get("page", 1))
    page_size = int(session_data.get("page_size", 5))

    total_pages = max(1, (len(songs) + page_size - 1) // page_size)

    if "下一页" in raw_msg:
        page += 1
    elif "上一页" in raw_msg:
        page -= 1
    else:
        m = re.search(r"第\s*(\d+)\s*页", raw_msg)
        if m:
            page = int(m.group(1))

    page = max(1, min(page, total_pages))
    session_data["page"] = page

    platform_name = get_platform_display_name(platform)
    text_msg, _ = build_song_list_page_text(
        platform_name, songs, page, page_size, markdown=True
    )
    fallback_msg, _ = build_song_list_page_text(
        platform_name, songs, page, page_size, markdown=False
    )

    await handle_send(
        bot, event,
        text_msg,
        native_markdown=True,
        fallback_msg=fallback_msg,
        keyboard_rows=[
            [("上一页", "点歌上一页"), ("下一页", "点歌下一页"), ("点歌帮助", "点歌帮助")]
        ],
    )
    await music_page_cmd.finish()


@music_select_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def music_select_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    raw_msg = str(event.get_message()).strip()
    parts = raw_msg.split(maxsplit=1)

    if len(parts) < 2 or not parts[1].strip():
        await handle_send(
            bot, event,
            "【选歌】\n"
            "用法：选歌 序号\n"
            "例如：选歌 1",
            md_type="娱乐",
            k1="示例", v1=quote("选歌 1", safe=""),
            k2="重新点歌", v2=quote("点歌 稻香", safe=""),
            k3="帮助", v3=quote("点歌帮助", safe="")
        )
        await music_select_cmd.finish()

    user_id = str(event.get_user_id())
    session_data = get_music_session(user_id)
    if not session_data:
        await handle_send(
            bot, event,
            "当前没有待选择的歌曲列表或已超时，请重新点歌。",
            md_type="娱乐",
            k1="重新点歌", v1=quote("点歌 稻香", safe=""),
            k2="网易点歌", v2=quote("网易点歌 晴天", safe=""),
            k3="帮助", v3=quote("点歌帮助", safe="")
        )
        await music_select_cmd.finish()

    arg = parts[1].strip()
    if not arg.isdigit():
        await handle_send(
            bot, event,
            "【选歌】\n"
            "格式错误，请发送：选歌 序号\n"
            "例如：选歌 1",
            md_type="娱乐",
            k1="示例", v1=quote("选歌 1", safe=""),
            k2="点歌帮助", v2=quote("点歌帮助", safe=""),
            k3="重新点歌", v3=quote("点歌 稻香", safe="")
        )
        await music_select_cmd.finish()

    index = int(arg)
    songs = session_data["songs"]
    if index < 1 or index > len(songs):
        await handle_send(
            bot, event,
            f"序号超出范围，请输入 1~{len(songs)}",
            md_type="娱乐",
            k1="选歌1", v1=quote("选歌 1", safe=""),
            k2="选歌2", v2=quote("选歌 2", safe=""),
            k3="点歌帮助", v3=quote("点歌帮助", safe="")
        )
        await music_select_cmd.finish()

    selected_song = songs[index - 1]
    clear_music_session(user_id)

    ok, tip = await send_song_rich(bot, event, selected_song)
    if not ok:
        await handle_send(
            bot, event,
            tip,
            md_type="娱乐",
            k1="重新点歌", v1=quote(f"点歌 {selected_song.get('name', '')}", safe=""),
            k2="随机语音", v2=quote("随机语音", safe=""),
            k3="帮助", v3=quote("点歌帮助", safe="")
        )

    await music_select_cmd.finish()


@music_config_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def music_config_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    raw_msg = str(event.get_message()).strip()
    parts = raw_msg.split()

    if len(parts) == 1 or (len(parts) >= 2 and parts[1] == "查看"):
        cfg = load_music_config()
        msg = (
            "【点歌配置】\n"
            f"default_platform: {cfg['default_platform']}\n"
            f"song_limit: {cfg['song_limit']}\n"
            f"select_timeout: {cfg['select_timeout']}\n"
            f"page_size: {cfg.get('page_size', 5)}\n"
            f"api_base: {cfg['api_base']}"
        )
        await handle_send(
            bot, event,
            msg,
            md_type="娱乐",
            k1="设置数量", v1=quote("点歌配置 设置 song_limit 10", safe=""),
            k2="设置平台", v2=quote("点歌配置 设置 default_platform netease", safe=""),
            k3="设置分页", v3=quote("点歌配置 设置 page_size 5", safe="")
        )
        await music_config_cmd.finish()

    if len(parts) >= 4 and parts[1] == "设置":
        key = parts[2]
        value = " ".join(parts[3:])
        ok, tip = set_music_config(key, value)
        await handle_send(
            bot, event,
            tip,
            md_type="娱乐",
            k1="查看配置", v1=quote("点歌配置 查看", safe=""),
            k2="测试点歌", v2=quote("点歌 稻香", safe=""),
            k3="帮助", v3=quote("点歌帮助", safe="")
        )
        await music_config_cmd.finish()

    await handle_send(
        bot, event,
        "【点歌配置用法】\n"
        "1. 点歌配置 查看\n"
        "2. 点歌配置 设置 song_limit 10\n"
        "3. 点歌配置 设置 default_platform netease\n"
        "4. 点歌配置 设置 page_size 5",
        md_type="娱乐",
        k1="查看", v1=quote("点歌配置 查看", safe=""),
        k2="设置示例", v2=quote("点歌配置 设置 song_limit 10", safe=""),
        k3="帮助", v3=quote("点歌帮助", safe="")
    )
    await music_config_cmd.finish()
