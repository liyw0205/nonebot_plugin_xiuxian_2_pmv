from nonebot.params import CommandArg

from ..command import *


ANSWER_BOOK_API = "https://60s.viki.moe/v2/answer"

answer_book_cmd = on_command(
    "答案之书",
    aliases={"答案书", "问答案书", "问答案之书"},
    priority=5,
    block=True,
)


@answer_book_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def answer_book_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    question = args.extract_plain_text().strip()

    try:
        result = await get_json_api(ANSWER_BOOK_API, timeout=15)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"翻阅答案之书失败：{e}",
            md_type="娱乐",
            k1="再问一次",
            v1="答案之书",
            k2="随机一言",
            v2="随机一言",
            k3="帮助",
            v3="娱乐帮助",
        )
        await answer_book_cmd.finish()

    data = result.get("data", {})
    if not isinstance(data, dict):
        data = {}

    answer = str(data.get("answer") or result.get("answer") or "").strip()
    answer_en = str(data.get("answer_en") or "").strip()
    if not answer:
        answer = str(result.get("message") or "书页合上了，什么也没留下。").strip()

    lines = ["【答案之书】"]
    if question:
        lines.append(f"问题：{question}")
    lines.append(f"答案：{answer}")
    if answer_en:
        lines.append(f"原句：{answer_en}")

    await handle_send(
        bot,
        event,
        "\n".join(lines),
        md_type="娱乐",
        k1="再问一次",
        v1=f"答案之书 {question}" if question else "答案之书",
        k2="今日超能力",
        v2="今日超能力",
        k3="娱乐帮助",
        v3="娱乐帮助",
    )
    await answer_book_cmd.finish()
