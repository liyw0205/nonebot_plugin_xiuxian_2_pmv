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
