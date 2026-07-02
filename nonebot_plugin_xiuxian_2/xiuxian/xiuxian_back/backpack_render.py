from typing import Any
from urllib.parse import quote

from ..xiuxian_config import convert_rank, added_ranks as _added_ranks


_ADDED_RANKS = _added_ranks()


def _build_backpack_md_with_sections(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    show_use_btn: bool = True,
    next_cmd: str = ""
) -> str:
    """
    构建带分类和交互按钮的背包 Markdown 消息
    sections: [
      ("分类标题", [{"name":"xxx","count":1,"bind":0, "goods_type": "装备/技能/特殊道具..."}, ...]),
      ...
    ]
    """
    lines = [f"【{title}】", ""]

    for sec_title, rows in sections:
        if not rows:
            continue
        lines.append(f"【{sec_title}】")
        lines.append("")
        for row in rows:
            name = row["name"]
            count = row.get("count", 0)
            bind = row.get("bind", 0)
            g_type = row.get("goods_type", "") # 获取物品大类

            # 查看效果按钮
            view_cmd = quote(f"查看效果 {name}")
            name_md = f"[{name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"

            equipped_flag = " ※已装备※" if row.get("is_equipped") else ""
            line = f"> - {name_md} 数量:{count} 绑定:{bind}{equipped_flag}"

            # 动态生成使用指令
            if show_use_btn:
                if g_type == "特殊道具":
                    use_cmd_str = f"道具使用 {name}"
                else:
                    use_cmd_str = f"使用 {name}"

                use_cmd_encoded = quote(use_cmd_str)
                use_md = f" [使用](mqqapi://aio/inlinecmd?command={use_cmd_encoded}&enter=false&reply=false)"
                line += use_md

            lines.append(line)
            lines.append("\r")

    lines.append("")
    lines.append(f"第 {current_page}/{total_pages} 页")

    # 下一页按钮
    if current_page < total_pages and next_cmd:
        next_cmd_q = quote(next_cmd)
        lines.append(f"[下一页](mqqapi://aio/inlinecmd?command={next_cmd_q}&enter=false&reply=false)")

    return "\r".join(lines)


def _build_backpack_fallback_with_sections(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    show_use_btn: bool = True,
    next_cmd: str = ""
) -> str:
    lines = [f"【{title}】", ""]

    for sec_title, rows in sections:
        if not rows:
            continue
        lines.append(f"【{sec_title}】")
        for row in rows:
            name = row["name"]
            count = row.get("count", 0)
            bind = row.get("bind", 0)
            equipped_flag = " ※已装备※" if row.get("is_equipped") else ""
            line = f"- {name} 数量:{count} 绑定:{bind}{equipped_flag}"
            if show_use_btn:
                use_cmd = f"道具使用 {name}" if row.get("goods_type") == "特殊道具" else f"使用 {name}"
                line += f"\n  使用：{use_cmd}"
            lines.append(line)
        lines.append("")

    lines.append(f"第 {current_page}/{total_pages} 页")
    if current_page < total_pages and next_cmd:
        lines.append(f"下一页：{next_cmd}")
    return "\n".join(lines).strip()


def _paginate_sections(
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    per_page: int = 15
):
    """
    按“物品条目数”分页，保留分类结构
    """
    flat = []
    for sec_title, rows in sections:
        for r in rows:
            flat.append((sec_title, r))

    if not flat:
        return [], 1, 1

    total_pages = (len(flat) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start = (current_page - 1) * per_page
    end = start + per_page
    page_flat = flat[start:end]

    grouped = {}
    order = []
    for sec_title, row in page_flat:
        if sec_title not in grouped:
            grouped[sec_title] = []
            order.append(sec_title)
        grouped[sec_title].append(row)

    page_sections = [(k, grouped[k]) for k in order]
    return page_sections, current_page, total_pages


def get_skill_type(skill_type: int) -> str:
    """根据神通类型编码返回描述"""
    if skill_type == 1:
        skill_desc = "伤害"
    elif skill_type == 2:
        skill_desc = "增强"
    elif skill_type == 3:
        skill_desc = "持续伤害"
    elif skill_type == 4:
        skill_desc = "封印"
    elif skill_type == 5:
        skill_desc = "随机伤害"
    elif skill_type == 6:
        skill_desc = "叠加伤害"
    elif skill_type == 7:
        skill_desc = "变化神通"
    else:
        skill_desc = "未知"
    return skill_desc


def format_basic_info(item_name1: str, item1_info: dict, item_name2: str, item2_info: dict, item_type: str) -> str:
    """格式化物品基础信息，用于对比"""
    rank_name_list = convert_rank("江湖好手")[1] # 获取境界列表

    # 计算物品1的所需境界
    item1_rank_raw = item1_info.get('rank', 1)
    if int(item1_rank_raw) == -5: # 特殊品阶处理
        item1_rank = 23
    else:
        item1_rank = int(item1_rank_raw) + _ADDED_RANKS
    item1_required_rank_name = rank_name_list[min(item1_rank, len(rank_name_list) - 1)] # 确保索引不越界

    # 计算物品2的所需境界
    item2_rank_raw = item2_info.get('rank', 1)
    if int(item2_rank_raw) == -5: # 特殊品阶处理
        item2_rank = 23
    else:
        item2_rank = int(item2_rank_raw) + _ADDED_RANKS
    item2_required_rank_name = rank_name_list[min(item2_rank, len(rank_name_list) - 1)] # 确保索引不越界

    # 根据物品类型构建基础信息字符串
    if item_type == '功法':
        basic_info = [
            f"📜 【功法信息】",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{item1_info.get('item_type', '未知')}",
            f"• 境界：{item1_required_rank_name}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{item2_info.get('item_type', '未知')}",
            f"• 境界：{item2_required_rank_name}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f""
        ]

    elif item_type in ['法器', '防具']:
        basic_info = [
            f"⚔️ 【{item_type}信息】",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 境界：{item1_required_rank_name}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 境界：{item2_required_rank_name}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f""
        ]

    elif item_type == '神通':
        skill_type1 = item1_info.get('skill_type', 0)
        skill_desc1 = get_skill_type(skill_type1)
        skill_type2 = item2_info.get('skill_type', 0)
        skill_desc2 = get_skill_type(skill_type2)

        basic_info = [
            f"✨ 【神通信息】",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{skill_desc1}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{skill_desc2}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f""
        ]
    else: # 其他物品类型暂时只显示通用信息
        basic_info = [
            f"【物品信息】",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{item1_info.get('type', '未知')}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{item2_info.get('type', '未知')}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f""
        ]

    return "\n".join(basic_info)


def format_number(value: Any, multiply_hundred: bool = True) -> str:
    """格式化数值为百分比或浮点数/整数"""
    if isinstance(value, (int, float)):
        if multiply_hundred:
            percentage = value * 100
            if isinstance(percentage, int) or percentage.is_integer():
                return f"{int(percentage)}%"
            # 如果是浮点数，保留一位小数
            return f"{percentage:.1f}%"
        else: # 不乘以100，直接格式化
            if isinstance(value, int) or value.is_integer():
                return f"{int(value)}"
            return f"{value:.1f}"
    return str(value)


def format_difference(diff: Any, multiply_hundred: bool = True) -> str:
    """格式化差异值，并添加符号"""
    if isinstance(diff, (int, float)):
        if multiply_hundred:
            percentage_diff = diff * 100
            if isinstance(percentage_diff, int) or percentage_diff.is_integer():
                return f"{abs(int(percentage_diff))}%"
            return f"{abs(percentage_diff):.1f}%"
        else:
            if isinstance(diff, int) or diff.is_integer():
                return f"{abs(int(diff))}"
            return f"{abs(diff):.1f}"
    return str(diff)


def compare_main(item_name1: str, item1_info: dict, item_name2: str, item2_info: dict) -> str:
    """对比两个主功法的属性"""
    comparison = [
        f"\n🎯 【{item_name1} ↔ {item_name2}】"
    ]
    skill_params = {
        'hpbuff': '气血',
        'mpbuff': '真元',
        'atkbuff': '攻击',
        'ratebuff': '修炼速度',
        'crit_buff': '会心',
        'def_buff': '减伤',
        'dan_exp': '炼丹经验',
        'dan_buff': '丹药数量',
        'reap_buff': '药材数量',
        'exp_buff': '经验保护',
        'critatk': '会心伤害',
        'two_buff': '双修次数',
        'number': '突破概率',
        'clo_exp': '闭关经验',
        'clo_rs': '闭关生命回复',
    }

    # 不乘以100的参数列表
    no_multiply_params = {'two_buff', 'number', 'dan_exp', 'dan_buff', 'reap_buff'}

    has_comparison = False
    for param, description in skill_params.items():
        value1 = item1_info.get(param, 0)
        value2 = item2_info.get(param, 0)

        if value1 == 0 and value2 == 0: # 如果两个物品该属性都为0，则跳过
            continue

        has_comparison = True
        multiply_hundred = param not in no_multiply_params # 判断是否需要乘以100显示百分比

        formatted_value1 = format_number(value1, multiply_hundred)
        formatted_value2 = format_number(value2, multiply_hundred)

        diff = value2 - value1
        formatted_diff = format_difference(diff, multiply_hundred) # 格式化差异值

        # 根据差异大小添加趋势符号
        if diff > 0:
            comp_symbol = f"(+{formatted_diff}) 📈"
        elif diff < 0:
            comp_symbol = f"(-{formatted_diff}) 📉"
        else:
            comp_symbol = "(相同)"

        comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

    if not has_comparison:
        comparison.append("• 两个物品在可对比的属性上均无特殊效果")
    return "\n".join(comparison)


def compare_equipment(item_name1: str, item1_info: dict, item_name2: str, item2_info: dict) -> str:
    """对比两个装备的属性"""
    comparison = [
        f"\n⚔️ 【{item_name1} ↔ {item_name2}】"
    ]
    equipment_params = {
        'atk_buff': '攻击',
        'crit_buff': '会心',
        'def_buff': '减伤',
        'mp_buff': '真元降耗',
        'critatk': '会心伤害',
        'crit_damage_reduction': '减会伤',
    }

    has_comparison = False
    for param, description in equipment_params.items():
        value1 = item1_info.get(param, 0)
        value2 = item2_info.get(param, 0)

        if value1 == 0 and value2 == 0:
            continue

        has_comparison = True
        formatted_value1 = format_number(value1)
        formatted_value2 = format_number(value2)
        diff = value2 - value1
        formatted_diff = format_difference(diff)

        if diff > 0:
            comp_symbol = f"(+{formatted_diff}) 📈"
        elif diff < 0:
            comp_symbol = f"(-{formatted_diff}) 📉"
        else:
            comp_symbol = "(相同)"

        comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

    if not has_comparison:
        comparison.append("• 两个装备在可对比的属性上均无特殊加成")
    return "\n".join(comparison)


def compare_skill_types(item_name1: str, skill1: dict, item_name2: str, skill2: dict) -> str:
    """对比两个神通的属性"""
    comparison = []
    skill_type1 = skill1.get('skill_type', 0)
    skill_type2 = skill2.get('skill_type', 0)
    skill_desc1 = get_skill_type(skill_type1)
    skill_desc2 = get_skill_type(skill_type2)

    if skill_type1 == skill_type2: # 只有同类型神通才能进行细致对比
        if skill_type1 == 1:  # 伤害类神通
            comparison.append(f"🔥【{item_name1} ↔ {item_name2}】")

            # 处理伤害值，支持列表（多段伤害）
            atkvalue1 = skill1.get('atkvalue', [0])
            atkvalue2 = skill2.get('atkvalue', [0])

            # 计算总伤害（如果atkvalue是列表，求和）
            total_atk1 = sum(atkvalue1) if isinstance(atkvalue1, list) else atkvalue1
            total_atk2 = sum(atkvalue2) if isinstance(atkvalue2, list) else atkvalue2

            formatted_total_atk1 = format_number(total_atk1)
            formatted_total_atk2 = format_number(total_atk2)
            diff_atk = total_atk2 - total_atk1
            formatted_diff_atk = format_difference(diff_atk)

            if diff_atk > 0:
                comp_symbol_atk = f"(+{formatted_diff_atk}) 📈"
            elif diff_atk < 0:
                comp_symbol_atk = f"(-{formatted_diff_atk}) 📉"
            else:
                comp_symbol_atk = "(相同)"

            comparison.append(f"• 总直接伤害: {formatted_total_atk1} ↔ {formatted_total_atk2} {comp_symbol_atk}")

            # 其他参数
            skill_params = {
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'turncost': ('冷却回合', False),
                'rate': ('触发概率', False),
            }

            has_comparison = False
            for param, (description, multiply_hundred) in skill_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)

                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"

                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")

        elif skill_type1 == 2:  # 增强类神通
            comparison.append(f"💪【{item_name1} ↔ {item_name2}】")
            enhance_params = {
                'atkvalue': ('攻击力提升', True),
                'def_buff': ('减伤提升', True),
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in enhance_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)

                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"

                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊加成")

        elif skill_type1 == 3:  # 持续伤害类神通
            comparison.append(f"🔄【{item_name1} ↔ {item_name2}】")
            continuous_params = {
                'atkvalue': ('伤害倍率', True), # 修正为atkvalue表示伤害倍率
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in continuous_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)

                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"

                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")

        elif skill_type1 == 6:  # 叠加伤害类神通 (之前是stack，现在是叠加伤害)
            comparison.append(f"📈【{item_name1} ↔ {item_name2}】")
            stack_params = {
                'buffvalue': ('每回合伤害倍率', True), # buffvalue表示每回合伤害倍率
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in stack_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)

                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"

                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")

        elif skill_type1 == 5:  # 随机伤害类神通
            comparison.append(f"🌊【{item_name1} ↔ {item_name2}】")
            wave_params = {
                'atkvalue': ('最小伤害倍率', True),
                'atkvalue2': ('最大伤害倍率', True),
                'turncost': ('冷却回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in wave_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)

                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"

                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")

        elif skill_type1 == 4:  # 封印类神通
            comparison.append(f"🔒【{item_name1} ↔ {item_name2}】")
            seal_params = {
                'success': ('命中成功率', False), # success表示命中率
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in seal_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)

                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"

                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")

            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")

        elif skill_type1 == 7: # 变化神通，效果特殊，暂不进行数值对比
            comparison.append(f"🎭【{item_name1} ↔ {item_name2}】")
            comparison.append(f"• 变化神通效果特殊，暂无法进行数值对比，请查看其详细描述。")
        else:
            comparison.append("🤔 【未知类型神通】")
            comparison.append(f"• 该神通类型 ({skill_desc1}) 暂不支持对比！")
    else: # 神通类型不一致
        comparison.append("⚠️ 【类型不匹配】")
        comparison.append(f"• {item_name1}类型: {skill_desc1}，{item_name2}类型: {skill_desc2}")
        comparison.append("• 不同类型的神通无法进行对比！")
    return "\n".join(comparison)
