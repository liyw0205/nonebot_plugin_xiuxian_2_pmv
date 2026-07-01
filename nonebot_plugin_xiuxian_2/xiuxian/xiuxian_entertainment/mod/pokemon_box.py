import random
import re

from nonebot.params import CommandArg

from ..command import *


POKEAPI_BASE = "https://pokeapi.co/api/v2"
POKEMON_MAX_ID = 1025

POKEMON_CN_ALIASES = {
    "妙蛙种子": "1",
    "小火龙": "4",
    "杰尼龟": "7",
    "皮卡丘": "25",
    "可达鸭": "54",
    "耿鬼": "94",
    "大岩蛇": "95",
    "卡比兽": "143",
    "快龙": "149",
    "超梦": "150",
    "梦幻": "151",
    "伊布": "133",
    "路卡利欧": "448",
    "烈空坐": "384",
    "固拉多": "383",
    "盖欧卡": "382",
}

POKEMON_CN_BY_ID = {int(value): key for key, value in POKEMON_CN_ALIASES.items() if value.isdigit()}

TYPE_CN = {
    "normal": "一般",
    "fire": "火",
    "water": "水",
    "electric": "电",
    "grass": "草",
    "ice": "冰",
    "fighting": "格斗",
    "poison": "毒",
    "ground": "地面",
    "flying": "飞行",
    "psychic": "超能力",
    "bug": "虫",
    "rock": "岩石",
    "ghost": "幽灵",
    "dragon": "龙",
    "dark": "恶",
    "steel": "钢",
    "fairy": "妖精",
}


def _normalize_pokemon_query(text: str) -> str:
    query = (text or "").strip()
    if not query:
        return str(random.randint(1, POKEMON_MAX_ID))
    if query in POKEMON_CN_ALIASES:
        return POKEMON_CN_ALIASES[query]
    if query.isdigit():
        return query
    return re.sub(r"\s+", "-", query.lower())


def _pokemon_image(data: dict) -> str:
    sprites = data.get("sprites", {})
    other = sprites.get("other", {}) if isinstance(sprites, dict) else {}
    official = other.get("official-artwork", {}) if isinstance(other, dict) else {}
    home = other.get("home", {}) if isinstance(other, dict) else {}
    return (
        official.get("front_default")
        or home.get("front_default")
        or sprites.get("front_default")
        or ""
    )


def _pokemon_types(data: dict) -> str:
    result = []
    for item in data.get("types", []) or []:
        type_data = item.get("type", {}) if isinstance(item, dict) else {}
        name = type_data.get("name")
        if name:
            result.append(TYPE_CN.get(name, name))
    return " / ".join(result) or "未知"


def _pokemon_abilities(data: dict) -> str:
    result = []
    for item in data.get("abilities", []) or []:
        ability = item.get("ability", {}) if isinstance(item, dict) else {}
        name = ability.get("name")
        if name:
            result.append(name)
    return " / ".join(result[:3]) or "未知"


def _pokemon_stats(data: dict) -> str:
    stats = {}
    for item in data.get("stats", []) or []:
        stat = item.get("stat", {}) if isinstance(item, dict) else {}
        name = stat.get("name")
        value = item.get("base_stat")
        if name and value is not None:
            stats[name] = value
    parts = [
        f"HP {stats.get('hp', '?')}",
        f"攻 {stats.get('attack', '?')}",
        f"防 {stats.get('defense', '?')}",
        f"速 {stats.get('speed', '?')}",
    ]
    return " / ".join(parts)


async def _fetch_pokemon(query: str) -> tuple[dict, dict]:
    pokemon = await get_json_api(f"{POKEAPI_BASE}/pokemon/{query}", timeout=15)
    species_url = (pokemon.get("species") or {}).get("url")
    species = {}
    if species_url:
        try:
            species = await get_json_api(species_url, timeout=15)
        except Exception:
            species = {}
    return pokemon, species


def _pokemon_flavor(species: dict) -> str:
    entries = species.get("flavor_text_entries", []) if isinstance(species, dict) else []
    for item in entries:
        if not isinstance(item, dict):
            continue
        lang = (item.get("language") or {}).get("name")
        if lang == "en":
            text = str(item.get("flavor_text") or "").replace("\n", " ").replace("\f", " ").strip()
            return re.sub(r"\s+", " ", text)[:180]
    return ""


def _pokemon_text(pokemon: dict, species: dict, blind: bool) -> str:
    pid = int(pokemon.get("id") or 0)
    name = str(pokemon.get("name") or "unknown")
    cn_name = POKEMON_CN_BY_ID.get(pid)
    display = f"{cn_name} ({name})" if cn_name else name
    prefix = "【宝可梦盲盒】" if blind else "【宝可梦图鉴】"

    lines = [
        f"{prefix} #{pid}",
        f"名称：{display}",
        f"属性：{_pokemon_types(pokemon)}",
        f"身高：{(pokemon.get('height') or 0) / 10:g} m",
        f"体重：{(pokemon.get('weight') or 0) / 10:g} kg",
        f"能力：{_pokemon_abilities(pokemon)}",
        f"基础值：{_pokemon_stats(pokemon)}",
    ]
    flavor = _pokemon_flavor(species)
    if flavor:
        lines.append(f"图鉴：{flavor}")
    return "\n".join(lines)


async def _send_pokemon(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, query: str, blind: bool):
    pokemon, species = await _fetch_pokemon(query)
    text = _pokemon_text(pokemon, species, blind)
    image_url = _pokemon_image(pokemon)
    if image_url:
        await handle_pic_msg_send(bot, event, image_url, text)
    else:
        await handle_send(bot, event, text, md_type="娱乐", k1="再抽", v1="宝可梦盲盒", k2="帮助", v2="宝可梦帮助")


pokemon_box_cmd = on_command("宝可梦盲盒", aliases={"随机宝可梦", "今日宝可梦"}, priority=5, block=True)
pokemon_query_cmd = on_command("宝可梦图鉴", aliases={"宝可梦", "精灵图鉴", "查宝可梦"}, priority=5, block=True)
pokemon_help_cmd = on_command("宝可梦帮助", aliases={"宝可梦盲盒帮助"}, priority=5, block=True)


@pokemon_box_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def pokemon_box_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    try:
        await _send_pokemon(bot, event, str(random.randint(1, POKEMON_MAX_ID)), True)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"打开宝可梦盲盒失败：{e}",
            md_type="娱乐",
            k1="再抽一次",
            v1="宝可梦盲盒",
            k2="查皮卡丘",
            v2="宝可梦图鉴 皮卡丘",
            k3="帮助",
            v3="宝可梦帮助",
        )
    await pokemon_box_cmd.finish()


@pokemon_query_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def pokemon_query_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    raw = args.extract_plain_text().strip()
    if raw in {"帮助", "help", "?"}:
        await _send_pokemon_help(bot, event)
        await pokemon_query_cmd.finish()
    query = _normalize_pokemon_query(raw)
    try:
        await _send_pokemon(bot, event, query, False)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"查询宝可梦失败：{e}",
            md_type="娱乐",
            k1="盲盒",
            v1="宝可梦盲盒",
            k2="皮卡丘",
            v2="宝可梦图鉴 皮卡丘",
            k3="帮助",
            v3="宝可梦帮助",
        )
    await pokemon_query_cmd.finish()


async def _send_pokemon_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await send_help_message(
        bot,
        event,
        "【宝可梦盲盒】\n"
        "用法：\n"
        "- 宝可梦盲盒\n"
        "- 宝可梦图鉴 皮卡丘\n"
        "- 宝可梦图鉴 25\n"
        "- 宝可梦图鉴 pikachu\n\n"
        "中文名只内置了少量常见宝可梦；其他可用英文名或编号。",
        k1="盲盒",
        v1="宝可梦盲盒",
        k2="皮卡丘",
        v2="宝可梦图鉴 皮卡丘",
        k3="伊布",
        v3="宝可梦图鉴 伊布",
        k4="娱乐帮助",
        v4="娱乐帮助",
    )


@pokemon_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def pokemon_help_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _send_pokemon_help(bot, event)
    await pokemon_help_cmd.finish()
