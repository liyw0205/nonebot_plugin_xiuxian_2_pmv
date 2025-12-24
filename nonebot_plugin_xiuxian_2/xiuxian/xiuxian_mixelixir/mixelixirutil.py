from ..xiuxian_utils.item_json import Items
from random import shuffle, sample
from collections import Counter
from typing import Dict, List, Tuple, Set

mix_config = Items().get_data_by_item_type(['合成丹药'])
mix_configs = {}
for k, v in mix_config.items():
    mix_configs[k] = v['elixir_config']

yonhudenji = 0
Llandudno_info = {
    "max_num": 10,
    "rank": 20
}

# 预计算配方类型组合的映射，避免重复计算
_formula_type_cache = {}
for elixir_id, formula in mix_configs.items():
    type_tuple = tuple(sorted(formula.keys()))
    if type_tuple not in _formula_type_cache:
        _formula_type_cache[type_tuple] = []
    _formula_type_cache[type_tuple].append((elixir_id, formula))

async def check_mix(elixir_config: Dict[str, int]) -> Tuple[bool, int]:
    """
    检查药材组合是否能合成丹药，返回最优解
    """
    input_types = tuple(sorted(elixir_config.keys()))
    
    # 快速检查类型匹配
    if input_types not in _formula_type_cache:
        return False, 0
    
    candidate_ids = []
    
    for elixir_id, formula in _formula_type_cache[input_types]:
        # 检查每种类型的数值是否满足要求
        meets_requirements = True
        for type_key, required_value in formula.items():
            if elixir_config[type_key] < required_value:
                meets_requirements = False
                break
        
        if meets_requirements:
            # 计算总功率作为优先级指标
            total_power = sum(formula.values())
            candidate_ids.append((elixir_id, total_power))
    
    if not candidate_ids:
        return False, 0
    
    # 选择总功率最高的配方
    best_id = max(candidate_ids, key=lambda x: x[1])[0]
    return True, best_id

async def get_mix_elixir_msg(yaocai: Dict) -> List[Dict]:
    """
    生成最多100个有效配方，然后随机选择10个返回
    """
    all_recipes = []  # 收集所有有效配方
    seen_ids: Set[int] = set()
    
    # 随机打乱药材顺序，增加随机性
    yaocai_list = list(yaocai.values())
    shuffle(yaocai_list)
    
    # 按照品阶优先级分类药材
    # 主药只使用高阶（六品-九品），不使用1-5品低阶
    zhuyao_high_grade = [y for y in yaocai_list if y.get('主药') and any(rank in y['level'] for rank in ['六品', '七品', '八品', '九品'])]
    
    # 药引优先低品阶（一品-五品）
    yaoyin_low_grade = [y for y in yaocai_list if y.get('药引') and any(rank in y['level'] for rank in ['一品', '二品', '三品', '四品', '五品'])]
    yaoyin_high_grade = [y for y in yaocai_list if y.get('药引') and any(rank in y['level'] for rank in ['六品', '七品', '八品', '九品'])]
    
    # 辅药优先中高阶（六品-九品）
    fuyao_high_grade = [y for y in yaocai_list if y.get('辅药') and any(rank in y['level'] for rank in ['六品', '七品', '八品', '九品'])]
    fuyao_low_grade = [y for y in yaocai_list if y.get('辅药') and any(rank in y['level'] for rank in ['一品', '二品', '三品', '四品', '五品'])]
    
    # 合并优先级列表（高优先级在前）
    yaoyin_candidates = yaoyin_low_grade + yaoyin_high_grade  # 药引：低品阶优先
    zhuyao_candidates = zhuyao_high_grade  # 主药：只使用高阶，不使用低阶
    fuyao_candidates = fuyao_high_grade + fuyao_low_grade     # 辅药：中高阶优先
    
    # 定义数量组合的顺序（优先主药增加，然后辅药，最后药引）
    quantity_combinations = [
        # 主药递增，辅药和药引最小化
        (1, 1, 1),  # 1主药1药引1辅药
        (2, 1, 1),  # 2主药1药引1辅药
        (3, 1, 1),  # 3主药1药引1辅药
        (4, 1, 1),  # 4主药1药引1辅药
        (5, 1, 1),  # 5主药1药引1辅药
        
        # 主药继续递增，辅药增加到2
        (1, 1, 2),  # 1主药1药引2辅药
        (2, 1, 2),  # 2主药1药引2辅药
        (3, 1, 2),  # 3主药1药引2辅药
        (4, 1, 2),  # 4主药1药引2辅药
        (5, 1, 2),  # 5主药1药引2辅药
        
        (1, 1, 3),  # 1主药1药引3辅药
        (2, 1, 3),  # 2主药1药引3辅药
        (3, 1, 3),  # 3主药1药引3辅药
        (4, 1, 3),  # 4主药1药引3辅药
        (5, 1, 3),  # 5主药1药引3辅药
        
        (1, 1, 4),  # 1主药1药引4辅药
        (2, 1, 4),  # 2主药1药引4辅药
        (3, 1, 4),  # 3主药1药引4辅药
        (4, 1, 4),  # 4主药1药引4辅药
        (5, 1, 4),  # 5主药1药引4辅药
        
        (1, 1, 5),  # 1主药1药引5辅药
        (2, 1, 5),  # 2主药1药引5辅药
        (3, 1, 5),  # 3主药1药引5辅药
        (4, 1, 5),  # 4主药1药引5辅药
        (5, 1, 5),  # 5主药1药引5辅药
    ]
    
    # 限制循环次数，避免性能问题
    max_candidates = min(30, len(zhuyao_candidates), len(yaoyin_candidates), len(fuyao_candidates))
    
    # 按照品阶优先级进行匹配，收集最多100个配方
    for quantity_combo in quantity_combinations:
        if len(all_recipes) >= 100:  # 达到最大收集数量
            break
            
        zhuyao_num, yaoyin_num, fuyao_num = quantity_combo
        
        # 遍历主药（只使用高阶）
        for zhuyao in zhuyao_candidates[:max_candidates]:
            if zhuyao['num'] < zhuyao_num:
                continue
                
            # 遍历药引（低品阶优先）
            for yaoyin in yaoyin_candidates[:max_candidates]:
                if yaoyin['name'] == zhuyao['name']:
                    continue
                if yaoyin['num'] < yaoyin_num:
                    continue
                    
                # 提前检查调和可能性
                if await _check_tiaohe_early(zhuyao, yaoyin, zhuyao_num, yaoyin_num):
                    continue
                
                # 在固定数量下，遍历辅药
                for fuyao in fuyao_candidates[:max_candidates]:
                    if fuyao['name'] in [zhuyao['name'], yaoyin['name']]:
                        continue
                    if fuyao['num'] < fuyao_num:
                        continue
                    
                    # 检查药材数量是否足够
                    required_counts = {
                        zhuyao['name']: zhuyao_num,
                        yaoyin['name']: yaoyin_num, 
                        fuyao['name']: fuyao_num
                    }
                    
                    if not await _check_yaocai_sufficient(yaocai, required_counts):
                        continue
                    
                    # 检查调和
                    if await tiaohe(zhuyao, zhuyao_num, yaoyin, yaoyin_num):
                        continue
                        
                    # 构建配方配置
                    elixir_config = {
                        str(zhuyao['主药']['type']): zhuyao['主药']['power'] * zhuyao_num,
                        str(fuyao['辅药']['type']): fuyao['辅药']['power'] * fuyao_num
                    }
                    
                    is_mix, elixir_id = await check_mix(elixir_config)
                    if is_mix and elixir_id not in seen_ids:
                        recipe = await _create_recipe(
                            elixir_id, zhuyao, zhuyao_num, yaoyin, yaoyin_num, fuyao, fuyao_num
                        )
                        all_recipes.append(recipe)
                        seen_ids.add(elixir_id)
                        
                        if len(all_recipes) >= 100:  # 达到最大收集数量
                            break
                
                if len(all_recipes) >= 100:
                    break
            if len(all_recipes) >= 100:
                break
    
    # 从收集的所有配方中随机选择最多10个返回
    if len(all_recipes) > 10:
        # 随机选择10个配方
        final_recipes = sample(all_recipes, 10)
    elif len(all_recipes) > 0:
        # 如果配方数量不足10个，返回所有找到的配方
        final_recipes = all_recipes
    else:
        # 没有找到任何配方
        final_recipes = []
    
    return final_recipes

async def get_elixir_recipe_msg(elixir_id, elixir, back, top_n=20):
    """获取丹药配方"""
    # ---------- 1. 提取丹药配置 ----------
    config_items = list(elixir["elixir_config"].items())
    zhuyao_type, zhuyao_need = int(config_items[0][0]), config_items[0][1]
    fuyao_type, fuyao_need = int(config_items[1][0]), config_items[1][1]

    # ---------- 2. 候选池 & 药引分桶 ----------
    zhuyao_list, fuyao_list = [], []
    yaoyin_by_hac = {1: [], -1: [], 0: []}

    for item in back.values():
        if "主药" in item and item["主药"]["type"] == zhuyao_type:
            zhuyao_list.append(item)
        if "辅药" in item and item["辅药"]["type"] == fuyao_type:
            fuyao_list.append(item)
        if "药引" in item:
            hac = item["药引"]["h_a_c"]
            key = (hac > 0) - (hac < 0)
            yaoyin_by_hac[key].append(item)

    # ---------- 3. 计算数量----------
    z_list = [
        {
            "name": item["name"],
            "level": item["level"],
            "num": needed,
            "h_a_c": item["主药"]["h_a_c"],
            "hac": (item["主药"]["h_a_c"] > 0) - (item["主药"]["h_a_c"] < 0)
        }
        for item in zhuyao_list
        if (needed := math.ceil(zhuyao_need / item["主药"]["power"])) <= item.get("num", 0)
    ]

    f_list = [
        {
            "name": item["name"],
            "level": item["level"],
            "num": needed,
            "hac": (item["药引"]["h_a_c"] > 0) - (item["药引"]["h_a_c"] < 0)
        }
        for item in fuyao_list
        if (needed := math.ceil(fuyao_need / item["辅药"]["power"])) <= item.get("num", 0)
    ]

    y_list = {
        1: [
            {
                "name": item["name"],
                "level": item["level"],
                "h_a_c": item["药引"]["h_a_c"],
                "num": math.ceil(zhuyao_need / 2 / abs(item["药引"]["h_a_c"])),
                "hac": 1
            }
            for item in yaoyin_by_hac.get(1, [])
            if item.get("num", 0) >= math.ceil(zhuyao_need / 2 / abs(item["药引"]["h_a_c"]))
        ],
        -1: [
            {
                "name": item["name"],
                "level": item["level"],
                "h_a_c": item["药引"]["h_a_c"],
                "num": math.ceil(zhuyao_need / 2 / abs(item["药引"]["h_a_c"])),
                "hac": -1
            }
            for item in yaoyin_by_hac.get(-1, [])
            if item.get("num", 0) >= math.ceil(zhuyao_need / 2 / abs(item["药引"]["h_a_c"]))
        ],
        0: [
            {
                "name": item["name"],
                "level": item["level"],
                "h_a_c": item["药引"]["h_a_c"],
                "num": 1,
                "hac": 0
            }
            for item in yaoyin_by_hac.get(0, [])
            if item.get("num", 0) >= 1
        ]
    }

    results = await _generate_sorted_recipes_optimized(elixir_id, z_list, y_list, f_list, max_per_type=5)
    top_formulas = remove_duplicate_formulas(results, top_n)

    return top_formulas


# ---------------- 辅助函数 ----------------
def remove_duplicate_formulas(formulas, top_n=20):
    """去除重复配方，只保留总消耗最少的"""
    best = {}
    for f in formulas:
        key = (f['主药'], f['药引'], f['辅药'])
        cost = f['主药_num'] + f['药引_num'] + f['辅药_num']
        if key not in best or cost < best[key][0]:
            best[key] = (cost, f)

    sorted_items = sorted(best.values(), key=lambda x: x[0])
    top_formulas = [f for _, f in sorted_items[:top_n]]

    return top_formulas


def get_level_number(level_str):
    """将品级字符串转换为数字，一品最低，九品最高"""
    level_map = {
        '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
        '六': 6, '七': 7, '八': 8, '九': 9
    }
    # 从字符串中提取中文数字字符
    for char in level_str:
        if char in level_map:
            return level_map[char]
    return 10  # 如果无法识别，返回一个较大的数


def sort_medicine_list(medicine_list):
    """排序药材列表：先按数量从小到大，再按品级从低到高"""
    return sorted(
        medicine_list,
        key=lambda x: (x['num'], get_level_number(x['level']))
    )


def sort_zhuyao_list(zhuyao_list):
    """排序主药列表：先按数量从小到大，再按品级从低到高，最后中性药优先"""
    return sorted(
        zhuyao_list,
        key=lambda x: (
            x['num'],  # 数量少的优先
            get_level_number(x['level']),  # 品级低的优先
            1 if x['hac'] == 0 else 2  # 中性药(hac=0)优先，非中性在后
        )
    )


async def _generate_sorted_recipes_optimized(elixir_id, z_list, y_list, f_list, max_per_type=5):
    """
    每个药材列表只取前几个进行组合，提高效率
    参数:
    - top_n: 返回前N个配方
    - max_per_type: 每个药材列表最多取多少个进行组合
    """

    # 对输入列表进行排序并截断
    sorted_z_list = sort_zhuyao_list(z_list)[:max_per_type]
    sorted_f_list = sort_medicine_list(f_list)[:max_per_type]

    # 对y_list的每个子列表排序并截断
    sorted_y_list = {}
    for hac_key, medicine_list in y_list.items():
        sorted_y_list[hac_key] = sort_medicine_list(medicine_list)[:max_per_type]

    recipes = []

    # 遍历所有主药
    for zhuyao in sorted_z_list:
        zhuyao_name = zhuyao['name']
        zhuyao_hac = zhuyao['hac']

        # 根据主药的hac确定药引列表
        if zhuyao_hac == 0:
            yaoyin_candidates = sorted_y_list.get(0, [])
        elif zhuyao_hac == 1:
            yaoyin_candidates = sorted_y_list.get(-1, [])
        else:  # zhuyao_hac == -1
            yaoyin_candidates = sorted_y_list.get(1, [])

        # 遍历所有可能的药引
        for yaoyin in yaoyin_candidates:
            yaoyin_name = yaoyin['name']

            # 检查主药和药引是否重复
            if zhuyao_name == yaoyin_name:
                continue

            # 遍历所有可能的辅药
            for fuyao in sorted_f_list:
                fuyao_name = fuyao['name']

                # 检查是否与主药或药引重复
                if fuyao_name == zhuyao_name or fuyao_name == yaoyin_name:
                    continue

                # 提前检查调和可能性
                zhuyao_hac_power = zhuyao['h_a_c'] * zhuyao['num']
                yaoyin_hac_power = yaoyin['h_a_c'] * yaoyin['num']
                if abs(zhuyao_hac_power + yaoyin_hac_power) > yonhudenji:
                    continue

                recipe = await _create_recipe(
                    elixir_id, zhuyao, zhuyao['num'], yaoyin, yaoyin['num'], fuyao, fuyao['num']
                )

                recipes.append(recipe)

                if len(recipes) > 100:
                    return recipes

    # 返回前N个配方
    return recipes

async def _check_tiaohe_early(zhuyao: Dict, yaoyin: Dict, zhuyao_num: int, yaoyin_num: int) -> bool:
    """
    提前检查调和可能性，避免深入循环
    """
    # 检查指定数量下是否能调和
    zhuyao_power = zhuyao['主药']['h_a_c'] * zhuyao_num
    yaoyin_power = yaoyin['药引']['h_a_c'] * yaoyin_num
    return await absolute(zhuyao_power + yaoyin_power) > yonhudenji

async def _check_yaocai_sufficient(yaocai: Dict, required_counts: Dict[str, int]) -> bool:
    """
    检查药材数量是否足够
    """
    for name, required in required_counts.items():
        for yao in yaocai.values():
            if yao['name'] == name and yao['num'] < required:
                return False
    return True

async def _create_recipe(elixir_id: int, zhuyao: Dict, zhuyao_num: int, 
                         yaoyin: Dict, yaoyin_num: int, fuyao: Dict, fuyao_num: int) -> Dict:
    """
    创建配方信息字典
    """
    goods_info = Items().get_data_by_item_id(elixir_id)
    
    return {
        'id': elixir_id,
        '配方简写': f"主药{zhuyao['name']}{zhuyao_num}药引{yaoyin['name']}{yaoyin_num}辅药{fuyao['name']}{fuyao_num}",
        '主药': zhuyao['name'],
        '主药_num': zhuyao_num,
        '主药_level': zhuyao['level'],
        '药引': yaoyin['name'],
        '药引_num': yaoyin_num,
        '药引_level': yaoyin['level'],
        '辅药': fuyao['name'],
        '辅药_num': fuyao_num,
        '辅药_level': fuyao['level']
    }

async def absolute(x: float) -> float:
    """绝对值函数"""
    return abs(x)

async def tiaohe(zhuyao_info: Dict, zhuyao_num: int, yaoyin_info: Dict, yaoyin_num: int) -> bool:
    """
    检查冷热调和
    """
    zhuyao_power = zhuyao_info['主药']['h_a_c'] * zhuyao_num
    yaoyin_power = yaoyin_info['药引']['h_a_c'] * yaoyin_num
    
    return await absolute(zhuyao_power + yaoyin_power) > yonhudenji

async def make_dict(old_dict: Dict) -> Dict:
    """
    随机选择最多25种药材
    """
    keys = list(old_dict.keys())
    shuffle(keys)
    
    if len(keys) > 25:
        keys = keys[:25]
    
    return {key: old_dict[key] for key in keys}
