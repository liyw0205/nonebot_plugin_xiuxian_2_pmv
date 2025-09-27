from ..xiuxian_utils.item_json import Items
from random import shuffle
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
    生成最多5个有效配方，保持随机性
    """
    recipes = []
    seen_ids: Set[int] = set()
    
    # 随机打乱药材顺序，增加随机性
    yaocai_list = list(yaocai.values())
    shuffle(yaocai_list)
    
    # 预处理药材类型分类
    zhuyao_candidates = [y for y in yaocai_list if y.get('主药')]
    yaoyin_candidates = [y for y in yaocai_list if y.get('药引')]
    fuyao_candidates = [y for y in yaocai_list if y.get('辅药')]
    
    # 进一步打乱各类药材的顺序
    shuffle(zhuyao_candidates)
    shuffle(yaoyin_candidates)
    shuffle(fuyao_candidates)
    
    # 限制循环次数，避免性能问题
    max_candidates = min(8, len(zhuyao_candidates), len(yaoyin_candidates), len(fuyao_candidates))
    
    # 收集所有可能的配方
    all_possible_recipes = []
    
    for zhuyao in zhuyao_candidates[:max_candidates]:
        zhuyao_max = min(zhuyao['num'], 5)
        
        for yaoyin in yaoyin_candidates[:max_candidates]:
            if yaoyin['name'] == zhuyao['name']:
                continue
                
            yaoyin_max = min(yaoyin['num'], 5)
            
            # 提前检查调和可能性
            if await _check_tiaohe_early(zhuyao, yaoyin, zhuyao_max, yaoyin_max):
                continue
                
            for fuyao in fuyao_candidates[:max_candidates]:
                if fuyao['name'] in [zhuyao['name'], yaoyin['name']]:
                    continue
                    
                fuyao_max = min(fuyao['num'], 5)
                
                # 尝试不同的数量组合（随机顺序）
                quantities = []
                for i in range(1, zhuyao_max + 1):
                    for o in range(1, yaoyin_max + 1):
                        for p in range(1, fuyao_max + 1):
                            if i + o + p <= Llandudno_info["max_num"]:
                                quantities.append((i, o, p))
                
                # 打乱数量组合的顺序
                shuffle(quantities)
                
                for i, o, p in quantities[:10]:  # 每种组合最多尝试10种数量
                    # 检查调和
                    if await tiaohe(zhuyao, i, yaoyin, o):
                        continue
                        
                    # 检查药材数量是否足够
                    required_counts = {
                        zhuyao['name']: i,
                        yaoyin['name']: o, 
                        fuyao['name']: p
                    }
                    
                    if not await _check_yaocai_sufficient(yaocai, required_counts):
                        continue
                        
                    # 构建配方配置
                    elixir_config = {
                        str(zhuyao['主药']['type']): zhuyao['主药']['power'] * i,
                        str(fuyao['辅药']['type']): fuyao['辅药']['power'] * p
                    }
                    
                    is_mix, elixir_id = await check_mix(elixir_config)
                    if is_mix:
                        recipe = await _create_recipe(
                            elixir_id, zhuyao, i, yaoyin, o, fuyao, p
                        )
                        # 避免重复配方
                        if elixir_id not in seen_ids:
                            all_possible_recipes.append(recipe)
                            seen_ids.add(elixir_id)
    
    # 随机选择最多10个配方
    shuffle(all_possible_recipes)
    return all_possible_recipes[:10]

async def _check_tiaohe_early(zhuyao: Dict, yaoyin: Dict, zhuyao_max: int, yaoyin_max: int) -> bool:
    """
    提前检查调和可能性，避免深入循环
    """
    # 检查最大数量下是否能调和
    max_zhuyao_power = zhuyao['主药']['h_a_c']['type'] * zhuyao['主药']['h_a_c']['power'] * zhuyao_max
    max_yaoyin_power = yaoyin['药引']['h_a_c']['type'] * yaoyin['药引']['h_a_c']['power'] * yaoyin_max
    return await absolute(max_zhuyao_power + max_yaoyin_power) > yonhudenji

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
    zhuyao_power = zhuyao_info['主药']['h_a_c']['type'] * zhuyao_info['主药']['h_a_c']['power'] * zhuyao_num
    yaoyin_power = yaoyin_info['药引']['h_a_c']['type'] * yaoyin_info['药引']['h_a_c']['power'] * yaoyin_num
    
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
