from ..command import *

steam_plus_one_cmd = on_command("Steam喜加一", aliases={"喜加一"}, priority=5, block=True)


def _safe_list_data(result: dict) -> list:
    data = result.get("data", [])
    return data if isinstance(data, list) else []


@steam_plus_one_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def steam_plus_one_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """Steam喜加一"""
    config = XiuConfig()
    api_url = "https://api.pearktrue.cn/api/steamplusone/"

    try:
        result = get_json_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取Steam喜加一失败：{e}",
            md_type="娱乐",
            k1="重试", v1="Steam喜加一",
            k2="每日Bing图", v2="每日Bing图",
            k3="帮助", v3="娱乐帮助"
        )
        await steam_plus_one_cmd.finish()

    code = result.get("code")
    msg = result.get("msg", "接口异常")
    time_text = result.get("time", "")
    count = result.get("count", 0)
    data = _safe_list_data(result)

    if str(code) not in {"200", "0"}:
        await handle_send(
            bot, event,
            f"获取Steam喜加一失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="Steam喜加一",
            k2="每日Bing图", v2="每日Bing图",
            k3="帮助", v3="娱乐帮助"
        )
        await steam_plus_one_cmd.finish()

    if not data:
        await handle_send(
            bot, event,
            "获取Steam喜加一失败：接口未返回游戏数据",
            md_type="娱乐",
            k1="重试", v1="Steam喜加一",
            k2="每日Bing图", v2="每日Bing图",
            k3="帮助", v3="娱乐帮助"
        )
        await steam_plus_one_cmd.finish()

    # ===== 普通文本内容 =====
    text_lines = [
        f"🎮 Steam喜加一",
        f"时间：{time_text}",
        f"数量：{count}",
        ""
    ]

    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            continue
        name = item.get("name", "未知游戏")
        game_type = item.get("type", "未知类型")
        starttime = item.get("starttime", "未知")
        endtime = item.get("endtime", "未知")
        perpetual = item.get("perpetual", "未知")
        source = item.get("source", "未知来源")
        url = item.get("url", "")

        game_msg = (
            f"【{idx}】{name}\n"
            f"类型：{game_type}\n"
            f"开始：{starttime}\n"
            f"结束：{endtime}\n"
            f"永久：{perpetual}\n"
            f"来源：{source}"
        )
        if url:
            game_msg += f"\n链接：{url}"
        text_lines.append(game_msg)
        text_lines.append("")

    text_msg = "\n".join(text_lines).strip()

    # ===== 模板MD =====
    if config.markdown_status and config.markdown_id:
        try:
            # 标题部分
            title_value = f"🎮 Steam喜加一\r时间：{time_text}\r数量：{count}"

            # 内容部分放到 s1
            shell_lines = []
            for idx, item in enumerate(data, start=1):
                if not isinstance(item, dict):
                    continue
                name = item.get("name", "未知游戏")
                game_type = item.get("type", "未知类型")
                starttime = item.get("starttime", "未知")
                endtime = item.get("endtime", "未知")
                perpetual = item.get("perpetual", "未知")
                source = item.get("source", "未知来源")
                url = item.get("url", "")

                shell_lines.append(f"【{idx}】{name}")
                shell_lines.append(f"类型：{game_type}")
                shell_lines.append(f"开始：{starttime}")
                shell_lines.append(f"结束：{endtime}")
                shell_lines.append(f"永久：{perpetual}")
                shell_lines.append(f"来源：{source}")
                if url:
                    shell_lines.append(f"链接：{url}")
                shell_lines.append("")

            shell_text = "\r".join(shell_lines).strip() or "暂无内容"

            # t2 放按钮/链接
            t2_values = []
            t2_values.append(
                generate_command("刷新", command="Steam喜加一", status="start", msg2=" | ")
            )

            valid_items = [x for x in data if isinstance(x, dict)]
            max_link_btn = 8  # 留一个给“刷新”，总量别太多
            for idx, item in enumerate(valid_items[:max_link_btn], start=1):
                name = item.get("name", f"游戏{idx}")
                url = item.get("url", "")
                if not url:
                    continue
                cmd_url = quote(url, safe="")
                is_last = idx == min(len(valid_items), max_link_btn)
                status = "end" if is_last else "start"
                msg2 = "\r" if is_last else " | "
                t2_values.append(
                    generate_command(f"打开{name}", command=cmd_url, status=status, msg2=msg2)
                )
            
            t2_values.append(f"\r数量：[{count}")
            param = [
                {"key": "t1", "values": [title_value]},
                {"key": "t2", "values": t2_values},
                {"key": "s1", "values": [f"python\r{shell_text}"]},
            ]

            md_msg = MessageSegment.markdown_template(
                bot,
                config.markdown_id,
                param
            )
            await bot.send(event=event, message=md_msg)
        except Exception as e:
            logger.warning(f"Steam喜加一 模板MD发送失败：{e}")
        await steam_plus_one_cmd.finish()

    # ===== 原生MD =====
    if config.markdown_status and not is_channel_event(event):
        try:
            md_lines = [
                "## 🎮 Steam喜加一",
                f"时间：{time_text}",
                f"数量：{count}",
                ""
            ]

            for idx, item in enumerate(data, start=1):
                if not isinstance(item, dict):
                    continue
                name = item.get("name", "未知游戏")
                game_type = item.get("type", "未知类型")
                starttime = item.get("starttime", "未知")
                endtime = item.get("endtime", "未知")
                perpetual = item.get("perpetual", "未知")
                source = item.get("source", "未知来源")
                url = item.get("url", "")

                md_lines.append(f"### {idx}. {name}")
                md_lines.append(f"类型：{game_type}")
                md_lines.append(f"开始：{starttime}")
                md_lines.append(f"结束：{endtime}")
                md_lines.append(f"永久：{perpetual}")
                md_lines.append(f"来源：{source}")
                if url:
                    cmd_url = quote(url, safe="")
                    md_lines.append(
                        f"[打开链接](mqqapi://aio/inlinecmd?command={cmd_url}&enter=false&reply=false)"
                    )
                md_lines.append("")

            md_lines.append(
                "[刷新](mqqapi://aio/inlinecmd?command=Steam喜加一&enter=false&reply=false) | "
                "[娱乐帮助](mqqapi://aio/inlinecmd?command=娱乐帮助&enter=false&reply=false)"
            )

            md_msg = "\r".join(md_lines)
            await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
            await steam_plus_one_cmd.finish()
        except Exception as e:
            logger.warning(f"Steam喜加一 原生MD发送失败：{e}")

    # ===== 普通文本 =====
    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="刷新", v1="Steam喜加一",
        k2="Bing图", v2="每日Bing图",
        k3="帮助", v3="娱乐帮助"
    )
    await steam_plus_one_cmd.finish()