import random
from .xiuxian2_handle import XiuxianDateManage, OtherSet, UserBuffDate, XIUXIAN_IMPART_BUFF
from ..xiuxian_config import convert_rank
from .utils import number_to, number_to2
from .item_json import Items
items = Items()
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
from nonebot.log import logger

class BossBuff:
    def __init__(self):
        self.boss_zs = 0
        self.boss_hx = 0
        self.boss_bs = 0
        self.boss_xx = 0
        self.boss_jg = 0
        self.boss_jh = 0
        self.boss_jb = 0
        self.boss_xl = 0
        self.boss_cj = 0
        self.boss_js = 0


class UserRandomBuff:
    def __init__(self):
        self.random_break = 0
        self.random_xx = 0
        self.random_hx = 0
        self.random_def = 0

class UserBattleBuffDate:  # 辅修功法14
    def __init__(self, user_id):
        """用户战斗Buff数据"""
        self.user_id = user_id
        # 攻击buff
        self.atk_buff = 0
        # 攻击buff
        self.atk_buff_time = -1

        # 暴击率buff
        self.crit_buff = 0
        # 暴击率buff
        self.crit_buff_time = -1

        # 暴击伤害buff
        self.crit_dmg_buff = 0
        # 暴击伤害buff
        self.crit_dmg__buff_time = -1

        # 回血buff
        self.health_restore_buff = 0
        self.health_restore_buff_time = -1
        # 回蓝buff
        self.mana_restore_buff = 0
        self.mana_restore_buff_time = -1

        # 吸血buff
        self.health_stolen_buff = 0
        self.health_stolen_buff_time = -1
        # 吸蓝buff
        self.mana_stolen_buff = 0
        self.mana_stolen_buff_time = -1
        # 反伤buff
        self.thorns_buff = 0
        self.thorns_buff_time = -1

        # 破甲buff
        self.armor_break_buff = 0
        self.armor_break_buff_time = -1

empty_boss_buff = BossBuff()
empty_ussr_random_buff = UserRandomBuff()

def Player_fight(user1, user2, type_in, bot_id):
    """玩家对决"""
    engine = BattleEngine(bot_id)
    
    # 初始化战斗参与者
    combatant1 = engine.init_combatant(user1)
    combatant2 = engine.init_combatant(user2)
    random_buff1 = get_player_random_buff(combatant1)
    random_buff2 = get_player_random_buff(combatant2)
    combatant1['random_buff'] = random_buff1
    combatant2['random_buff'] = random_buff2
    # 在战斗循环开始前处理辅修功法效果
    user1_battle_buff_date, user2_battle_buff_date, msg = start_sub_buff_handle(
        combatant1['sub_open'], 
        combatant1['sub_buff_data'], 
        combatant1['battle_buff'],
        combatant2['sub_open'], 
        combatant2['sub_buff_data'], 
        combatant2['battle_buff']
    )
    
    if msg:
        formatted_msg = f"{combatant1['player']['道号']}" + msg
        engine.add_message(combatant1, formatted_msg)
    add_special_buffs(engine, combatant1, bot_id)
    add_special_buffs(engine, combatant2, bot_id)
    max_turns = 20
    turn_count = 1
    winner = None
    
    # 战斗循环
    while turn_count <= max_turns and not winner:
        # 玩家1回合
        result = engine.execute_turn(combatant1, combatant2, "player")
        if result == "attacker_win":
            winner = combatant1
            break
            
        # 检查玩家2是否死亡
        if combatant2['player']['气血'] <= 0:
            winner = combatant1
            break
            
        # 玩家2回合
        result = engine.execute_turn(combatant2, combatant1, "player")
        if result == "attacker_win":
            winner = combatant2
            break
            
        # 检查玩家1是否死亡
        if combatant1['player']['气血'] <= 0:
            winner = combatant2
            break
            
        # 检查双方都无法行动的情况
        if not combatant1['turn_skip'] and not combatant2['turn_skip']:
            engine.add_system_message("双方都动弹不得！")
            combatant1['turn_skip'] = True
            combatant2['turn_skip'] = True
            
        turn_count += 1
        
    # 平局处理
    if not winner:
        engine.add_system_message("你们打到了天昏地暗，被大能叫停！")
        suc = "没有人"
    else:
        suc = winner['player']['道号']
    
    # 战斗结束处理
    if type_in == 2:  # 实际战斗，更新气血真元
        update_player_stats(combatant1, combatant2, winner, type_in)
    
    return engine.play_list, suc

async def Boss_fight(user1, boss: dict, type_in=2, bot_id=0):
    """BOSS战斗"""
    engine = BattleEngine(bot_id)
    
    # 初始化玩家
    player_combatant = engine.init_combatant(user1)
    
    # 检查是否为稻草人
    is_scarecrow = boss.get('is_scarecrow', False) or boss['name'] == "稻草人"
    
    # 初始化BOSS，如果是稻草人则使用特殊逻辑
    if is_scarecrow:
        boss_combatant = init_scarecrow_combatant(boss)
    else:
        boss_combatant = init_boss_combatant(boss)
    
    # 获取玩家随机buff
    random_buff = get_player_random_buff(player_combatant)
    player_combatant['random_buff'] = random_buff
    
    # 在战斗循环开始前处理辅修功法效果
    user1_battle_buff_date, user2_battle_buff_date, msg = start_sub_buff_handle(
        player_combatant['sub_open'], 
        player_combatant['sub_buff_data'], 
        player_combatant['battle_buff'],
        False, 
        {}, 
        {}
    )
    
    if msg:
        formatted_msg = f"{player_combatant['player']['道号']}" + msg
        engine.add_message(player_combatant, formatted_msg)
    
    # 如果不是稻草人，添加BOSS特殊buff消息
    if not is_scarecrow:
        add_boss_special_buffs(engine, boss_combatant, player_combatant, bot_id)
    else:
        # 稻草人特殊消息
        engine.add_system_message("这是一个训练用的稻草人，不会反击，尽情练习吧！")
    add_special_buffs(engine, player_combatant, bot_id, si_boss=True, boss_combatant=boss_combatant)
    max_turns = 20
    turn_count = 1
    winner = None
    boss_init_hp = boss_combatant['player']['气血']
    
    # 战斗循环
    while turn_count <= max_turns and not winner:
        # 玩家回合
        result = engine.execute_turn(player_combatant, boss_combatant, "boss")
        if result == "attacker_win":
            winner = player_combatant
            break
            
        # 检查BOSS是否死亡
        if boss_combatant['player']['气血'] <= 0:
            winner = player_combatant
            break
            
        # 如果不是稻草人，BOSS才行动
        if not is_scarecrow:
            result = execute_boss_turn(engine, boss_combatant, player_combatant, boss_init_hp)
            if result == "attacker_win":
                winner = boss_combatant
                break
        else:
            # 稻草人回合，只显示信息不攻击
            boss_name = boss_combatant['player']['name']
            turn_msg = f"☆------{boss_name}的回合------☆"
            engine.add_boss_message(boss_combatant['player'], turn_msg, boss_init_hp)
            engine.add_boss_message(boss_combatant['player'], "稻草人静静地站着，没有任何反应...", boss_init_hp)
            
        # 检查玩家是否死亡
        if player_combatant['player']['气血'] <= 0:
            winner = boss_combatant
            break
            
        turn_count += 1
        
    # 平局处理
    if not winner:
        if not is_scarecrow:
            engine.add_system_message("你们打到了天昏地暗，被大能叫停！")
            suc = "Boss赢了"
        else:
            engine.add_system_message("训练时间结束！")
            suc = "没有人"
    else:
        if winner == player_combatant:
            suc = "群友赢了"
        else:
            suc = "Boss赢了"
    
    # 战斗结束处理
    if type_in == 2:  # 实际战斗，更新玩家状态
        update_boss_fight_stats(player_combatant, winner, type_in)
    
    return engine.play_list, suc, boss_combatant['player']

def check_hit(attacker_hit, defender_dodge):
    """
    判断攻击是否命中
    attacker_hit: 攻击方命中率
    defender_dodge: 防御方闪避率
    return: True命中, False闪避
    """
    actual_hit_rate = max(0, min(100, attacker_hit - defender_dodge))
    return random.randint(0, 100) <= actual_hit_rate

def calculate_damage(attacker, defender, base_damage):
    """
    统一的伤害计算函数
    battle_type: "player"玩家对决, "boss_attack"BOSS攻击玩家, "player_attack_boss"玩家攻击BOSS
    """
    # 获取基础属性
    attacker_break = attacker.get('random_buff', empty_ussr_random_buff).random_break  # 攻击方穿甲 
    defender_def = defender.get('random_buff', empty_ussr_random_buff).random_def  # 防御方减伤
    
    # 获取辅修功法穿甲
    sub_break = 0
    if attacker.get('sub_open', False) and attacker.get('sub_buff_data', {}).get('buff_type') == '14':
        sub_break = attacker['sub_buff_data'].get('break', 0)
    
    if 'boss_cj' in defender:
        battle_type = "player_attack_boss"
    else:
        battle_type = "player"     

    # 根据战斗类型选择不同的计算方式
    if battle_type == "player":
        # 玩家对决：伤害 * (对方减伤 - 对方随机减伤buff + 辅修穿甲 + 自己随机穿甲)
        defense_factor = defender['current_js'] - defender_def + sub_break + attacker_break
    elif battle_type == "player_attack_boss":
        # 玩家攻击BOSS：伤害 * (对方减伤 + 辅修穿甲 + 自己随机穿甲)
        defense_factor = defender['current_js'] + sub_break + attacker_break
    
    # 限制减伤系数在合理范围内
    defense_factor = max(min(defense_factor, 1.0), 0.05)
    
    actual_damage = int(base_damage * defense_factor)
    return actual_damage

ST1 = {
    "攻击": {
        "type_rate": 50,
    },
    "会心": {
        "type_rate": 50,
    },
    "暴伤": {
        "type_rate": 50,
    },
    "禁血": {
        "type_rate": 50,
    }
}

ST2 = {
    "降攻": {
        "type_rate": 50,
    },
    "降会": {
        "type_rate": 50,
    },
    "降暴": {
        "type_rate": 50,
    },
    "禁蓝": {
        "type_rate": 50,
    }
}

def generate_hp_bar(current_hp, max_hp):
    """生成血量条显示
    ⬛️代表有血量，⬜️代表已损失血量
    每10%血量显示一个方块
    """
    if max_hp <= 0:
        return "⬜️⬜️⬜️⬜️⬜️⬜️⬜️⬜️⬜️⬜️ 0%"
    
    # 计算当前血量百分比
    hp_percentage = max(0, min(100, (current_hp / max_hp) * 100))
    percentage_int = int(hp_percentage)
    
    # 计算应该显示多少个⬛️（每10%一个）
    filled_blocks = int(percentage_int // 10)
    filled_blocks = max(0, min(10, filled_blocks))  # 限制在0-10之间
    
    # 生成血量条字符串
    hp_bar = "⬛️" * filled_blocks + "⬜️" * (10 - filled_blocks)
    return f"{hp_bar} {percentage_int}%"

def get_msg_dict(player, player_init_hp, msg):
    player['气血'] = int(round(player['气血']))
    return {
        "type": "node", 
        "data": {
            "name": f"{player['道号']}，当前血量：{int(player['气血'])} / {int(player_init_hp)}",
            "uin": int(player['user_id']), "content": msg
                }
            }


def get_boss_dict(boss, boss_init_hp, msg, bot_id):
    boss['气血'] = int(round(boss['气血']))
    return {
        "type": "node",
        "data": {
            "name": f"{boss['name']}当前血量：{int(boss['气血'])} / {int(boss_init_hp)}", 
            "uin": int(bot_id),
            "content": msg
                }
            }


def get_user_def_buff(user_id):
    user_armor_data = UserBuffDate(user_id).get_user_armor_buff_data()  # 防具减伤
    user_weapon_data = UserBuffDate(user_id).get_user_weapon_data()  # 武器减伤
    user_main_data = UserBuffDate(user_id).get_user_main_buff_data()  # 功法减伤
    if user_weapon_data is not None:
        weapon_def = user_weapon_data['def_buff']  # 武器减伤
    else:
        weapon_def = 0
    if user_main_data is not None:
        main_def = user_main_data['def_buff']  # 功法减伤
    else:
        main_def = 0
    if user_armor_data is not None:
        def_buff = user_armor_data['def_buff']  # 防具减伤
    else:
        def_buff = 0
    return round(1 - (def_buff + weapon_def + main_def), 2)  # 初始减伤率


def get_turnatk(player, buff=0, user_battle_buff_date={},
                boss_buff: BossBuff = empty_boss_buff,
                random_buff: UserRandomBuff = empty_ussr_random_buff):  # 辅修功法14
    sub_atk = 0
    sub_crit = 0
    sub_dmg = 0
    zwsh = 0
    try:
        user_id = player['user_id']
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        user_buff_data = UserBuffDate(user_id)
        weapon_critatk_data = UserBuffDate(user_id).get_user_weapon_data()  # 武器会心伤害
        weapon_zw = UserBuffDate(user_id).get_user_weapon_data()
        main_zw = user_buff_data.get_user_main_buff_data()
        # 专武伤害，其实叫伴生武器更好。。。
        zwsh = 0.5 if main_zw["ew"] != 0 and weapon_zw["zw"] != 0 and main_zw["ew"] == weapon_zw["zw"] else 0
        main_critatk_data = user_buff_data.get_user_main_buff_data()  # 功法会心伤害
        player_sub_open = False  # 辅修功法14
        user_sub_buff_date = {}
        if user_buff_data.get_user_sub_buff_data() != None:
            user_sub_buff_date = UserBuffDate(user_id).get_user_sub_buff_data()
            player_sub_open = True
        buff_value = int(user_sub_buff_date['buff'])
        buff_type = user_sub_buff_date['buff_type']
        if buff_type == '1':
            sub_atk = buff_value / 100
        else:
            sub_atk = 0
        if buff_type == '2':
            sub_crit = buff_value / 100
        else:
            sub_crit = 0
        if buff_type == '3':
            sub_dmg = buff_value / 100
        else:
            sub_dmg = 0
    except:
        impart_data = None
        weapon_critatk_data = None
        main_critatk_data = None
    impart_know_per = impart_data['impart_know_per'] if impart_data is not None else 0
    impart_burst_per = impart_data['impart_burst_per'] if impart_data is not None else 0
    weapon_critatk = weapon_critatk_data['critatk'] if weapon_critatk_data is not None else 0  # 武器会心伤害
    main_critatk = main_critatk_data['critatk'] if main_critatk_data is not None else 0  # 功法会心伤害
    isCrit = False
    turnatk = int(round(random.uniform(0.95, 1.05), 2)
                  * (player['攻击'] * (buff + sub_atk + 1) * (1 - boss_buff.boss_jg)) * (1 + zwsh))  # 攻击波动,buff是攻击buff
    if random.randint(0, 100) <= player['会心'] + (
            impart_know_per + sub_crit - boss_buff.boss_jh + random_buff.random_hx) * 100:  # 会心判断
        turnatk = int(turnatk * (
                    1.5 + impart_burst_per + weapon_critatk + main_critatk + sub_dmg - boss_buff.boss_jb))  # boss战、切磋、秘境战斗会心伤害公式（不包含抢劫）
        isCrit = True
    turnatk = int(round(turnatk))
    return isCrit, turnatk


def get_turnatk_boss(player, buff=0, user_battle_buff_date={},
                     boss_buff: BossBuff = empty_boss_buff):  # boss伤害计算公式
    isCrit = False
    turnatk = int(round(random.uniform(0.95, 1.05), 2)
                  * (player['攻击'] * (buff + 1)))  # 攻击波动,buff是攻击buff
    if random.randint(0, 100) <= 0.3 + boss_buff.boss_hx * 100:  # 会心判断
        turnatk = int(turnatk * (1.5 + boss_buff.boss_bs))  # boss战、切磋、秘境战斗会心伤害公式（不包含抢劫）
        isCrit = True
    turnatk = int(round(turnatk))
    return isCrit, turnatk

def isEnableUserSikll(player, hpcost, mpcost, turncost, skillrate):  # 是否满足技能释放条件
    skill = False
    if turncost < 0:  # 判断是否进入休息状态
        return skill

    if player['气血'] > hpcost and player['真元'] >= mpcost:  # 判断血量、真元是否满足
        if random.randint(0, 100) <= skillrate:  # 随机概率释放技能
            skill = True
    return skill


def get_skill_hp_mp_data(player, secbuffdata):
    """获取技能消耗气血、真元、技能类型、技能释放概率"""
    user_id = player['user_id']
    weapon_data = UserBuffDate(user_id).get_user_weapon_data()
    if weapon_data is not None and "mp_buff" in weapon_data:
        weapon_mp = max(weapon_data["mp_buff"], 0)
    else:
        weapon_mp = 0

    hpcost = int(secbuffdata['hpcost'] * player['气血']) if secbuffdata['hpcost'] != 0 else 0
    mpcost = int(secbuffdata['mpcost'] * player['exp'] * (1 - weapon_mp)) if secbuffdata['mpcost'] != 0 else 0
    return hpcost, mpcost, secbuffdata['skill_type'], secbuffdata['rate']


def calculate_skill_cost(player, hpcost, mpcost):
    player['气血'] = player['气血'] - hpcost  # 气血消耗
    player['真元'] = player['真元'] - mpcost  # 真元消耗

    return player

def get_skill_sh_data(player, secbuffdata):
    skillmsg = ''
    if secbuffdata['skill_type'] == 1:  # 连续攻击类型
        turncost = -secbuffdata['turncost']
        isCrit, turnatk = get_turnatk(player)
        atkvalue = secbuffdata['atkvalue']  # 列表
        skillsh = 0
        atkmsg = ''
        for value in atkvalue:
            atkmsg += f"{number_to2(value * turnatk)}伤害、"
            skillsh += int(value * turnatk)

        if turncost == 0:
            turnmsg = '!'
        else:
            turnmsg = f"，休息{secbuffdata['turncost']}回合！"

        # 构建消耗信息，如果消耗为0则不显示
        cost_msgs = []
        if secbuffdata['hpcost'] != 0:
            cost_msgs.append(f"气血{number_to2(secbuffdata['hpcost'] * player['气血'])}点")
        if secbuffdata['mpcost'] != 0:
            cost_msgs.append(f"真元{number_to2(secbuffdata['mpcost'] * player['exp'])}点")
        
        cost_msg = "、".join(cost_msgs)
        cost_prefix = f"消耗{cost_msg}，" if cost_msgs else ""

        if isCrit:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}并且发生了会心一击，造成{atkmsg[:-1]}{turnmsg}"
        else:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}造成{atkmsg[:-1]}{turnmsg}"

        return skillmsg, skillsh, turncost

    elif secbuffdata['skill_type'] == 2:  # 持续伤害类型
        turncost = secbuffdata['turncost']
        isCrit, turnatk = get_turnatk(player)
        skillsh = int(secbuffdata['atkvalue'] * player['攻击'])  # 改动
        atkmsg = ''
        
        # 构建消耗信息，如果消耗为0则不显示
        cost_msgs = []
        if secbuffdata['hpcost'] != 0:
            cost_msgs.append(f"气血{number_to2(secbuffdata['hpcost'] * player['气血'])}点")
        if secbuffdata['mpcost'] != 0:
            cost_msgs.append(f"真元{number_to2(secbuffdata['mpcost'] * player['exp'])}点")
        
        cost_msg = "、".join(cost_msgs)
        cost_prefix = f"消耗{cost_msg}，" if cost_msgs else ""

        if isCrit:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}并且发生了会心一击，造成{number_to2(skillsh)}点伤害，持续{turncost}回合！"
        else:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}造成{number_to2(skillsh)}点伤害，持续{turncost}回合！"

        return skillmsg, skillsh, turncost

    elif secbuffdata['skill_type'] == 3:  # 持续buff类型
        turncost = secbuffdata['turncost']
        skillsh = secbuffdata['buffvalue']
        atkmsg = ''
        
        # 构建消耗信息，如果消耗为0则不显示
        cost_msgs = []
        if secbuffdata['hpcost'] != 0:
            cost_msgs.append(f"气血{number_to2(secbuffdata['hpcost'] * player['气血'])}点")
        if secbuffdata['mpcost'] != 0:
            cost_msgs.append(f"真元{number_to2(secbuffdata['mpcost'] * player['exp'])}点")
        
        cost_msg = "、".join(cost_msgs)
        cost_prefix = f"消耗{cost_msg}，" if cost_msgs else ""

        if secbuffdata['bufftype'] == 1:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}攻击力增加{skillsh}倍，持续{turncost}回合！"
        elif secbuffdata['bufftype'] == 2:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}获得{skillsh * 100}%的减伤，持续{turncost}回合！"

        return skillmsg, skillsh, turncost

    elif secbuffdata['skill_type'] == 4:  # 封印类技能
        turncost = secbuffdata['turncost']
        
        # 构建消耗信息，如果消耗为0则不显示
        cost_msgs = []
        if secbuffdata['hpcost'] != 0:
            cost_msgs.append(f"气血{number_to2(secbuffdata['hpcost'] * player['气血'])}点")
        if secbuffdata['mpcost'] != 0:
            cost_msgs.append(f"真元{number_to2(secbuffdata['mpcost'] * player['exp'])}点")
        
        cost_msg = "、".join(cost_msgs)
        cost_prefix = f"消耗{cost_msg}，" if cost_msgs else ""

        if random.randint(0, 100) <= secbuffdata['success']:  # 命中
            skillsh = True
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}使对手动弹不得,{secbuffdata['desc']}持续{turncost}回合！"
        else:  # 未命中
            skillsh = False
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}但是被对手躲避！"

        return skillmsg, skillsh, turncost
        
    elif secbuffdata['skill_type'] == 5:  # 随机伤害类型技能
        turncost = -secbuffdata['turncost']
        isCrit, turnatk = get_turnatk(player)
        atkvalue = secbuffdata['atkvalue']  # 最低伤害
        atkvalue2 = secbuffdata['atkvalue2']  # 最高伤害
        value = random.uniform(atkvalue, atkvalue2)
        atkmsg = f"{number_to2(value * turnatk)}伤害、"
        skillsh = int(value * turnatk)

        if turncost == 0:
            turnmsg = '!'
        else:
            turnmsg = f"，休息{secbuffdata['turncost']}回合！"

        # 构建消耗信息，如果消耗为0则不显示
        cost_msgs = []
        if secbuffdata['hpcost'] != 0:
            cost_msgs.append(f"气血{number_to2(secbuffdata['hpcost'] * player['气血'])}点")
        if secbuffdata['mpcost'] != 0:
            cost_msgs.append(f"真元{number_to2(secbuffdata['mpcost'] * player['exp'])}点")
        
        cost_msg = "、".join(cost_msgs)
        cost_prefix = f"消耗{cost_msg}，" if cost_msgs else ""

        if isCrit:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}并且发生了会心一击，造成{atkmsg[:-1]}{turnmsg}"
        else:
            skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}造成{atkmsg[:-1]}{turnmsg}"

        return skillmsg, skillsh, turncost

    elif secbuffdata['skill_type'] == 6:  # 叠加类型技能
        turncost = secbuffdata['turncost']
        skillsh = secbuffdata['buffvalue']
        
        # 构建消耗信息，如果消耗为0则不显示
        cost_msgs = []
        if secbuffdata['hpcost'] != 0:
            cost_msgs.append(f"气血{number_to2(secbuffdata['hpcost'] * player['气血'])}点")
        if secbuffdata['mpcost'] != 0:
            cost_msgs.append(f"真元{number_to2(secbuffdata['mpcost'] * player['exp'])}点")
        
        cost_msg = "、".join(cost_msgs)
        cost_prefix = f"消耗{cost_msg}，" if cost_msgs else ""

        skillmsg = f"{player['道号']}发动技能：{secbuffdata['name']}，{cost_prefix}{secbuffdata['desc']}攻击力叠加{skillsh}倍，持续{turncost}回合！"

        return skillmsg, skillsh, turncost
        
        
# 处理开局的辅修功法效果
def apply_buff(user_battle_buff, subbuffdata, player_sub_open, is_opponent=False):
    if not player_sub_open:
        return ""
    buff_type_to_attr = {
        '1': ('atk_buff', "攻击力"),
        '2': ('crit_buff', "暴击率"),
        '3': ('crit_dmg_buff', "暴击伤害"),
        '4': ('health_restore_buff', "气血回复"),
        '5': ('mana_restore_buff', "真元回复"),
        '6': ('health_stolen_buff', "气血吸取"),
        '7': ('mana_stolen_buff', "真元吸取"),
        '8': ('thorns_buff', "中毒"),
        '9': ('hm_stolen_buff', "气血真元吸取"),
        '10': ('jx_buff', "重伤效果"),
        '11': ('fan_buff', "抵消效果"),
        '12': ('stone_buff', "聚宝效果"),
        '13': ('break_buff', "斗战效果"),
        '14': ('break_buff', "穿甲效果"),
    }

    attr, desc = buff_type_to_attr[subbuffdata['buff_type']]
    break_buff_desc = int(round(subbuffdata['break'] * 100))
    setattr(user_battle_buff, attr, subbuffdata['buff'])
    if int(subbuffdata['buff_type']) >= 0 and int(subbuffdata['buff_type']) <= 9:
        sub_msg = f"提升{subbuffdata['buff']}%{desc}"
    elif int(subbuffdata['buff_type']) == 14:
        sub_msg = f"提升{break_buff_desc}%{desc}"
    else:
        sub_msg = f"获得了{desc}！！"
    prefix = f"\n对手" if is_opponent else ""
    return f"{prefix}使用{subbuffdata['name']}, {sub_msg}"

def start_sub_buff_handle(player1_sub_open, subbuffdata1, user1_battle_buff_date,
                          player2_sub_open, subbuffdata2, user2_battle_buff_date):
    msg1 = apply_buff(user1_battle_buff_date, subbuffdata1, player1_sub_open) if player1_sub_open else ""
    msg2 = apply_buff(user2_battle_buff_date, subbuffdata2, player2_sub_open, is_opponent=True) if player2_sub_open else ""

    return user1_battle_buff_date, user2_battle_buff_date, msg1 + msg2


# 处理攻击后辅修功法效果
def after_atk_sub_buff_handle(player1_sub_open, player1, user1_main_buff_data, subbuffdata1, damage1, player2,
                             boss_buff: BossBuff = empty_boss_buff,
                             random_buff: UserRandomBuff = empty_ussr_random_buff):
    """处理攻击后的辅修功法效果（优化版）"""
    msg = None
    health_stolen_msg = None
    mana_stolen_msg = None
    other_msg = None

    if not player1_sub_open:
        return player1, player2, msg

    # 获取玩家属性
    user_id = player1['user_id']
    user_info = sql_message.get_user_info_with_id(user_id)
    
    # 计算最大气血和真元
    max_hp = int(player1['exp'] / 2)
    max_mp = int(player1['exp'])
    if user1_main_buff_data:
        max_hp = int(player1['exp'] / 2) * (1 + user1_main_buff_data.get('hpbuff', 0))
        max_mp = player1['exp'] * (1 + user1_main_buff_data.get('mpbuff', 0))
    
    buff_value = int(subbuffdata1['buff'])
    buff_type = subbuffdata1['buff_type']
    
    # 获取对方辅修功法信息
    player2_sub_buff_data = UserBuffDate(player2['user_id']).get_user_sub_buff_data() if player2.get('user_id') else None
    player2_sub_buff_jin = player2_sub_buff_data.get('jin', 0) if player2_sub_buff_data else 0

    # 处理不同类型的辅修效果
    if buff_type == '4':  # 回血
        restore_health = max_hp * buff_value / 100
        if player2_sub_buff_jin > 0:
            restore_health = 0
        if restore_health > 0:
            player1['气血'] = min(player1['气血'] + int(restore_health), max_hp)
            other_msg = f"回复气血:{number_to2(int(restore_health))}"
        
    elif buff_type == '5':  # 回蓝
        restore_mana = max_mp * buff_value / 100
        if player2_sub_buff_jin > 0:
            restore_mana = 0
        if restore_mana > 0:
            player1['真元'] = min(player1['真元'] + int(restore_mana), max_mp)
            other_msg = f"回复真元:{number_to2(int(restore_mana))}"
        
    elif buff_type == '6':  # 吸血
        if damage1 > 0:  # 只有命中才吸血
            health_stolen = (damage1 * ((buff_value / 100) + random_buff.random_xx)) * (1 - boss_buff.boss_xx)
            if player2_sub_buff_jin > 0:
                health_stolen = 0
            health_stolen = max(health_stolen, 0)
            player1['气血'] = min(player1['气血'] + int(health_stolen), max_hp)
            if health_stolen > 0:
                health_stolen_msg = f"吸取气血:{number_to2(int(health_stolen))}"
                
    elif buff_type == '7':  # 吸蓝
        if damage1 > 0:  # 只有命中才吸蓝
            mana_stolen = (damage1 * buff_value / 100) * (1 - boss_buff.boss_xl)
            if player2_sub_buff_jin > 0:
                mana_stolen = 0
            mana_stolen = max(mana_stolen, 0)
            player1['真元'] = min(player1['真元'] + int(mana_stolen), max_mp)
            if mana_stolen > 0:
                mana_stolen_msg = f"吸取真元:{number_to2(int(mana_stolen))}"
                
    elif buff_type == '8':  # 中毒
        poison_damage = player2['气血'] / 100 * buff_value
        player2['气血'] = max(player2['气血'] - int(poison_damage), 0)
        if poison_damage > 0:
            other_msg = f"对手中毒消耗血量:{number_to2(int(poison_damage))}"
            
    elif buff_type == '9':  # 双吸
        if damage1 > 0:  # 只有命中才有效
            health_stolen = (damage1 * ((buff_value / 100) + random_buff.random_xx)) * (1 - boss_buff.boss_xx)
            mana_stolen = (damage1 * int(subbuffdata1['buff2']) / 100) * (1 - boss_buff.boss_xl)
            
            if player2_sub_buff_jin > 0:
                health_stolen = 0
                mana_stolen = 0
            
            health_stolen = max(health_stolen, 0)
            mana_stolen = max(mana_stolen, 0)
            
            player1['气血'] = min(player1['气血'] + int(health_stolen), max_hp)
            player1['真元'] = min(player1['真元'] + int(mana_stolen), max_mp)
            
            if health_stolen > 0:
                health_stolen_msg = f"吸取气血:{number_to2(int(health_stolen))}"
            if mana_stolen > 0:
                mana_stolen_msg = f"吸取真元:{number_to2(int(mana_stolen))}"
    
    # 组合消息
    if health_stolen_msg and mana_stolen_msg:
        msg = f"{health_stolen_msg}, {mana_stolen_msg}"
    elif health_stolen_msg:
        msg = health_stolen_msg
    elif mana_stolen_msg:
        msg = mana_stolen_msg
    elif other_msg:
        msg = other_msg
    
    return player1, player2, msg

class BattleEngine:
    def __init__(self, bot_id):
        self.bot_id = bot_id
        self.play_list = []
        
    def init_combatant(self, user_id, is_boss=False):
        """初始化战斗参与者数据"""
        if is_boss:
            player = sql_message.get_player_data(user_id, boss=True)
        else:
            player = sql_message.get_player_data(user_id)
            
        buff_data = UserBuffDate(player['user_id'])
        main_buff_data = buff_data.get_user_main_buff_data()
        
        # 获取各种buff数据
        hp_buff = main_buff_data['hpbuff'] if main_buff_data else 0
        mp_buff = main_buff_data['mpbuff'] if main_buff_data else 0
        
        # 获取传承数据
        try:
            impart_data = xiuxian_impart.get_user_impart_info_with_id(player['user_id'])
            impart_hp = impart_data['impart_hp_per'] if impart_data else 0
            impart_mp = impart_data['impart_mp_per'] if impart_data else 0
        except:
            impart_hp, impart_mp = 0, 0
            
        # 获取修炼数据
        user_info = sql_message.get_user_info_with_id(player['user_id'])
        hppractice = user_info['hppractice'] * 0.08 if user_info['hppractice'] else 0
        mppractice = user_info['mppractice'] * 0.05 if user_info['mppractice'] else 0
        
        # 计算最终buff
        total_hp_buff = hp_buff + impart_hp + hppractice
        total_mp_buff = mp_buff + impart_mp + mppractice
        
        # 获取身法和瞳术数据
        effect1_data = buff_data.get_user_effect1_buff_data()  # 身法
        effect2_data = buff_data.get_user_effect2_buff_data()  # 瞳术
        
        hit = 100  # 基础命中
        dodge = 0   # 基础闪避
        
        if effect2_data and effect2_data['buff_type'] == '2':
            hit_buff = random.uniform(float(effect2_data['buff2']), float(effect2_data['buff']))
            hit += int(hit_buff)
            self.add_system_message(f"{user_info['user_name']}{effect2_data['desc']}！增加{int(hit_buff)}%命中！")
            
        if effect1_data and effect1_data['buff_type'] == '1':
            dodge_buff = random.uniform(float(effect1_data['buff2']), float(effect1_data['buff']))
            dodge += int(dodge_buff)
            self.add_system_message(f"{user_info['user_name']}{effect1_data['desc']}！获得{int(dodge_buff)}%闪避！")
            
        # 获取技能数据
        skill_data = None
        skill_open = False
        if buff_data.get_user_sec_buff_data() is not None:
            skill_data = buff_data.get_user_sec_buff_data()
            skill_open = True
            if skill_data['skill_type'] == 7:  # 随机技能
                goods_id = random.choice(skill_data['skill_content'])
                skill_data = items.get_data_by_item_id(goods_id)
                
        # 获取辅修功法数据
        sub_buff_data = {}
        sub_open = False
        if buff_data.get_user_sub_buff_data() is not None:
            sub_buff_data = buff_data.get_user_sub_buff_data()
            sub_open = True
            
        return {
            'player': player,
            'buff_data': buff_data,
            'main_buff_data': main_buff_data,
            'hp_buff': total_hp_buff,
            'mp_buff': total_mp_buff,
            'hit': hit,
            'dodge': dodge,
            'skill_data': skill_data,
            'skill_open': skill_open,
            'sub_buff_data': sub_buff_data,
            'sub_open': sub_open,
            'turn_cost': 0,
            'turn_skip': True,
            'buff_turn': True,
            'battle_buff': UserBattleBuffDate(player['user_id']),
            'init_hp': player['气血'],
            'def_js': max(get_user_def_buff(player['user_id']), 0.05),
            'current_js': max(get_user_def_buff(player['user_id']), 0.05),
            'skill_sh': 0
        }

    def execute_turn(self, attacker, defender, turn_type="player"):
        """执行单个回合的战斗逻辑"""
        turn_msg = f"☆------{attacker['player']['道号']}的回合------☆"
        self.add_message(attacker, turn_msg)
        
        # 处理辅修功法效果
        self.process_sub_buffs(attacker, defender)
        
        if not attacker['turn_skip']:
            skip_msg = f"☆------{attacker['player']['道号']}动弹不得！------☆"
            self.add_message(attacker, skip_msg)
            if attacker['turn_cost'] > 0:
                attacker['turn_cost'] -= 1
            if attacker['turn_cost'] == 0 and attacker['buff_turn']:
                attacker['turn_skip'] = True
            return None
                
        if attacker['skill_open']:
            result = self.execute_skill_attack(attacker, defender, turn_type)
        else:
            result = self.execute_normal_attack(attacker, defender, turn_type)
        
        # 检查战斗是否结束
        battle_result = self.check_battle_end(attacker, defender)
        if battle_result:
            return battle_result
            
        # 处理回合结束的状态
        if attacker['turn_cost'] < 0:
            attacker['turn_skip'] = False
            attacker['turn_cost'] += 1
            
        return None

    def execute_skill_attack(self, attacker, defender, turn_type):
        """执行技能攻击"""
        player = attacker['player']
        skill_data = attacker['skill_data']
        
        hp_cost, mp_cost, skill_type, skill_rate = get_skill_hp_mp_data(player, skill_data)
        
        if attacker['turn_cost'] == 0:  # 首次释放技能
            attacker['current_js'] = attacker['def_js']  # 恢复减伤
            attacker['atk_buff'] = 0  # 恢复攻击
            
            if isEnableUserSikll(player, hp_cost, mp_cost, attacker['turn_cost'], skill_rate):
                skill_msg, skill_sh, turn_cost = get_skill_sh_data(player, skill_data)
                attacker['turn_cost'] = turn_cost
                attacker['skill_sh'] = skill_sh
                
                # 根据技能类型处理不同的攻击逻辑
                success = self.handle_skill_type(attacker, defender, skill_type, skill_msg, skill_sh, 
                                               hp_cost, mp_cost, turn_type)
                if not success:  # 技能释放失败或未命中，使用普通攻击
                    self.execute_normal_attack_base(attacker, defender, turn_type)
            else:  # 不满足技能条件，使用普通攻击
                self.execute_normal_attack_base(attacker, defender, turn_type)
        else:  # 持续性技能后续回合
            self.handle_persistent_skill(attacker, defender, skill_type, turn_type)

    def execute_normal_attack(self, attacker, defender, turn_type):
        """执行普通攻击"""
        self.execute_normal_attack_base(attacker, defender, turn_type)

    def execute_normal_attack_base(self, attacker, defender, turn_type):
        """普通攻击基础逻辑"""
        # 根据战斗类型选择不同的伤害计算函数
        atk_buff = attacker.get('atk_buff', 0)
        if turn_type == "boss":
            boss_buff = defender.get('boss_buff', empty_boss_buff)
            random_buff = attacker.get('random_buff', empty_ussr_random_buff)
            is_crit, damage = get_turnatk(attacker['player'], atk_buff, attacker['battle_buff'], boss_buff, random_buff)
        else:
            is_crit, damage = get_turnatk(attacker['player'], atk_buff, attacker['battle_buff'])

        if '道号' in defender['player']:
            defender_name = defender['player']['道号']
        else:
            defender_name = defender['player']['name']
        
        attacker_name = attacker['player']['道号']
        actual_damage = 0
        if check_hit(attacker['hit'], defender['dodge']):
            if is_crit:
                msg = "{}发起会心一击，造成了{}伤害"
            else:
                msg = "{}发起攻击，造成了{}伤害"
                
            actual_damage = int(calculate_damage(attacker, defender, damage))
            defender['player']['气血'] -= actual_damage
            
            attack_msg = msg.format(attacker_name, number_to2(actual_damage))
            hp_bar = generate_hp_bar(defender['player']['气血'], defender['init_hp'])
            hp_msg = f"{defender_name}剩余血量{number_to2(defender['player']['气血'])}\n{hp_bar}"
            
            self.add_message(attacker, attack_msg)
            self.process_after_attack_buffs(attacker, defender, actual_damage)
            self.add_message(attacker, hp_msg)
        else:
            miss_msg = f"{attacker_name}的攻击被{defender_name}闪避了！"
            self.add_message(attacker, miss_msg)
            self.process_after_attack_buffs(attacker, defender, actual_damage)

    def handle_skill_type(self, attacker, defender, skill_type, skill_msg, skill_sh, hp_cost, mp_cost, turn_type):
        """处理不同类型的技能"""
        if skill_type in [1, 2, 5]:  # 直接伤害、持续伤害、随机伤害技能
            return self.handle_damage_skill(attacker, defender, skill_type, skill_msg, skill_sh, 
                                          hp_cost, mp_cost, turn_type)
        elif skill_type == 3:  # buff类技能
            return self.handle_buff_skill(attacker, defender, skill_msg, skill_sh, hp_cost, mp_cost, turn_type)
        elif skill_type == 4:  # 封印类技能
            return self.handle_seal_skill(attacker, defender, skill_msg, skill_sh, hp_cost, mp_cost)
        elif skill_type == 6:  # 叠加类技能
            return self.handle_stack_skill(attacker, defender, skill_msg, skill_sh, hp_cost, mp_cost, turn_type)
        
        return False

    def handle_damage_skill(self, attacker, defender, skill_type, skill_msg, skill_sh, hp_cost, mp_cost, turn_type):
        """处理伤害类技能"""
        if '道号' in defender['player']:
            defender_name = defender['player']['道号']
        else:
            defender_name = defender['player']['name']
        actual_damage = 0
        if not check_hit(attacker['hit'], defender['dodge']):
            miss_msg = f"{attacker['player']['道号']}的技能被{defender_name}闪避了！"
            self.add_message(attacker, miss_msg)
            attacker['player'] = calculate_skill_cost(attacker['player'], hp_cost, mp_cost)
            self.process_after_attack_buffs(attacker, defender, actual_damage)
            if skill_type == 2:  # 持续性技能未命中不进入持续状态
                attacker['turn_cost'] = 0
            return False
            
        self.add_message(attacker, skill_msg)
        attacker['player'] = calculate_skill_cost(attacker['player'], hp_cost, mp_cost)
        
        # 计算实际伤害
        if skill_type == 2:  # 持续性伤害有额外系数
            attacker['turn_cost'] -= 1  # 立即消耗一回合
            actual_damage = int(skill_sh * min(0.2 + defender['current_js'], 1.0))
        else:
            actual_damage = int(calculate_damage(attacker, defender, skill_sh))
            
        defender['player']['气血'] -= actual_damage
        hp_bar = generate_hp_bar(defender['player']['气血'], defender['init_hp'])
        hp_msg = f"{defender_name}剩余血量{number_to2(defender['player']['气血'])}\n{hp_bar}"
        self.process_after_attack_buffs(attacker, defender, actual_damage)
        self.add_message(attacker, hp_msg)
        return True

    def handle_buff_skill(self, attacker, defender, skill_msg, skill_sh, hp_cost, mp_cost, turn_type):
        """处理buff类技能"""
        if '道号' in defender['player']:
            defender_name = defender['player']['道号']
        else:
            defender_name = defender['player']['name']
    
        self.add_message(attacker, skill_msg)
        attacker['player'] = calculate_skill_cost(attacker['player'], hp_cost, mp_cost)
        attacker['turn_cost'] -= 1  # 立即消耗一回合
        # 根据buff类型设置效果
        buff_type = attacker['skill_data']['bufftype']
        if buff_type == 1:  # 攻击类buff
            # 存储攻击buff到战斗状态
            attacker['atk_buff'] = skill_sh
            attacker['atk_buff_turns'] = attacker['skill_data']['turncost']
        elif buff_type == 2:  # 减伤buff
            attacker['current_js'] = max(attacker['def_js'] - skill_sh, 0.05)
            attacker['js_buff_turns'] = attacker['skill_data']['turncost']
    
        # 执行普通攻击（应用buff效果）
        self.execute_normal_attack_base(attacker, defender, turn_type)
        
        return True

    def handle_seal_skill(self, attacker, defender, skill_msg, skill_sh, hp_cost, mp_cost):
        """处理封印类技能"""
        if '道号' in defender['player']:
            defender_name = defender['player']['道号']
        else:
            defender_name = defender['player']['name']
        if skill_sh:  # 技能命中
            self.add_message(attacker, skill_msg)
            defender['turn_skip'] = False
            defender['buff_turn'] = False
        else:
            miss_msg = f"{attacker['player']['道号']}的封印技能被{defender_name}闪避了！"
            self.add_message(attacker, miss_msg)
            
        attacker['player'] = calculate_skill_cost(attacker['player'], hp_cost, mp_cost)
        return True

    def handle_stack_skill(self, attacker, defender, skill_msg, skill_sh, hp_cost, mp_cost, turn_type):
        """处理叠加类技能"""
        if '道号' in defender['player']:
            defender_name = defender['player']['道号']
        else:
            defender_name = defender['player']['name']
        if not check_hit(attacker['hit'], defender['dodge']):
            miss_msg = f"{attacker['player']['道号']}的技能被{defender_name}闪避了！"
            self.add_message(attacker, miss_msg)
            attacker['player'] = calculate_skill_cost(attacker['player'], hp_cost, mp_cost)
            attacker['turn_cost'] = 0  # 未命中不进入叠加状态
            return False
            
        self.add_message(attacker, skill_msg)
        attacker['player'] = calculate_skill_cost(attacker['player'], hp_cost, mp_cost)
        
        # 叠加类技能的特殊攻击计算
        base_damage = int(round(random.uniform(0.95, 1.05), 2) * attacker['player']['攻击'] * 1.5)
        
        # 根据剩余回合数计算叠加伤害
        current_stack = attacker['skill_data']['turncost'] - attacker['turn_cost']
        stack_multiplier = max(attacker['skill_sh'] * current_stack, 1.0)
        actual_damage = int(calculate_damage(attacker, defender, int(base_damage + (base_damage * stack_multiplier))))
        defender['player']['气血'] -= actual_damage
        
        msg = "{}发起攻击，造成了{}伤害"
        hp_bar = generate_hp_bar(defender['player']['气血'], defender['init_hp'])
        attack_msg = msg.format(attacker['player']['道号'], number_to2(actual_damage))
        hp_msg = f"{defender_name}剩余血量{number_to2(defender['player']['气血'])}\n{hp_bar}"
        
        self.add_message(attacker, attack_msg)
        self.add_message(attacker, hp_msg)
        
        return True

    def handle_persistent_skill(self, attacker, defender, skill_type, turn_type):
        """处理持续性技能的后续回合"""
        if '道号' in defender['player']:
            defender_name = defender['player']['道号']
        else:
            defender_name = defender['player']['name']
        if skill_type == 2:  # 持续性伤害
            attacker['turn_cost'] -= 1
            
            # 持续性伤害部分
            persistent_damage = int(attacker['skill_sh'] * min(0.2 + defender['current_js'], 1.0))
            defender['player']['气血'] -= persistent_damage

            skill_msg = f"{attacker['skill_data']['name']}持续造成{number_to2(attacker['skill_sh'])}伤害，剩余回合：{attacker['turn_cost']}!"
            self.add_message(attacker, skill_msg)
            hp_bar = generate_hp_bar(defender['player']['气血'], defender['init_hp'])
            persistent_hp_msg = f"{defender_name}剩余血量{number_to2(defender['player']['气血'])}\n{hp_bar}"
            self.add_message(attacker, persistent_hp_msg)
            
        elif skill_type == 3:  # buff类持续效果
            attacker['turn_cost'] -= 1
            buff_type = attacker['skill_data']['bufftype']
            
            if buff_type == 1:  # 攻击buff
                self.execute_normal_attack_base(attacker, defender, turn_type)
                self.add_message(attacker, f"{attacker['skill_data']['name']}增伤剩余:{attacker['turn_cost']}回合")
            elif buff_type == 2:  # 减伤buff
                attacker['current_js'] = max(attacker['def_js'] - attacker['skill_sh'], 0.05)
                self.execute_normal_attack_base(attacker, defender, turn_type)
                self.add_message(attacker, f"{attacker['skill_data']['name']}减伤剩余{attacker['turn_cost']}回合")
        elif skill_type == 4:  # 封印持续效果
            attacker['turn_cost'] -= 1            
            self.execute_normal_attack_base(attacker, defender, turn_type)

            skill_msg = f"{attacker['player']['道号']}的封印技能：{attacker['skill_data']['name']}，剩余回合：{attacker['turn_cost']}!"
            self.add_message(attacker, skill_msg)
            # 封印结束判断
            if attacker['turn_cost'] == 0:
                defender['turn_skip'] = True
                defender['buff_turn'] = True
                
        elif skill_type == 6:  # 叠加类持续效果
            attacker['turn_cost'] -= 1
            current_stack = attacker['skill_data']['turncost'] - attacker['turn_cost']
            stack_multiplier = attacker['skill_sh'] * current_stack
            
            # 叠加伤害计算
            base_damage = int(round(random.uniform(0.95, 1.05), 2) * attacker['player']['攻击'] * 1.5)
            actual_damage = int(calculate_damage(attacker, defender, int(base_damage + (base_damage * stack_multiplier))))
            defender['player']['气血'] -= actual_damage
            
            msg = "{}发起攻击，造成了{}伤害"
            hp_bar = generate_hp_bar(defender['player']['气血'], defender['init_hp'])
            attack_msg = msg.format(attacker['player']['道号'], number_to2(actual_damage))
            hp_msg = f"{defender_name}剩余血量{number_to2(defender['player']['气血'])}\n{hp_bar}"
            
            self.add_message(attacker, attack_msg)
            self.add_message(attacker, f"{attacker['skill_data']['name']}叠伤剩余:{attacker['turn_cost']}回合，当前{round(stack_multiplier, 1)}倍")
            self.add_message(attacker, hp_msg)

    def process_sub_buffs(self, attacker, defender):
        """处理辅修功法效果"""
        # 确保defender有必要的属性
        if 'sub_open' not in defender:
            defender['sub_open'] = False
        if 'sub_buff_data' not in defender:
            defender['sub_buff_data'] = {}
        if 'battle_buff' not in defender:
            defender['battle_buff'] = UserBattleBuffDate("temp")
        
        if not attacker['sub_open'] and not defender['sub_open']:
            return
    
    def process_after_attack_buffs(self, attacker, defender, damage_dealt):
        """处理攻击后的辅修功法效果"""
        if not attacker['sub_open']:
            return
            
        player1, player2, msg = after_atk_sub_buff_handle(
            attacker['sub_open'], 
            attacker['player'], 
            attacker['main_buff_data'],
            attacker['sub_buff_data'], 
            damage_dealt, 
            defender['player'],
            defender.get('boss_buff', empty_boss_buff),
            attacker.get('random_buff', empty_ussr_random_buff)
        )
        
        if msg:
            self.add_message(attacker, msg)
            
        # 更新玩家状态
        attacker['player'] = player1
        defender['player'] = player2

    def check_battle_end(self, attacker, defender):
        """检查战斗是否结束"""
        if defender['player']['气血'] <= 0:
            winner_msg = f"{attacker['player']['道号']}胜利"
            self.add_system_message(winner_msg)
            return "attacker_win"
        return None

    def add_message(self, combatant, message):
        """添加战斗消息"""
        msg_dict = get_msg_dict(combatant['player'], combatant['init_hp'], message)
        self.play_list.append(msg_dict)

    def add_system_message(self, message):
        """添加系统消息"""
        msg_dict = {
            "type": "node", 
            "data": {
                "name": "Bot", 
                "uin": int(self.bot_id), 
                "content": message
            }
        }
        self.play_list.append(msg_dict)

    def add_boss_message(self, boss, message, boss_init_hp):
        """添加BOSS消息"""
        msg_dict = get_boss_dict(boss, boss_init_hp, message, self.bot_id)
        self.play_list.append(msg_dict)

def init_scarecrow_combatant(boss):
    """初始化稻草人战斗参与者"""
    # 稻草人特殊属性：无减伤、不攻击、无buff
    scarecrow_combatant = {
        'player': boss,
        'boss_buff': BossBuff(),  # 空的BOSS buff
        'hit': 0,  # 稻草人命中率为0，不会命中
        'dodge': 0,  # 稻草人闪避率为0，容易被命中
        'turn_skip': False,  # 稻草人永远无法行动
        'buff_turn': False,
        'turn_cost': 0,
        'current_js': 1.0,  # 稻草人减伤为1.0（无减伤）
        'def_js': 1.0,  # 基础减伤也为1.0
        'init_hp': boss['气血'],
        'boss_cj': 0,  # 无穿甲
        'sub_open': False,
        'sub_buff_data': {},
        'battle_buff': None,
        'main_buff_data': None,
        'skill_open': False,
        'skill_data': None,
        'hp_buff': 0,
        'mp_buff': 0,
        'is_scarecrow': True  # 标记为稻草人
    }
    
    return scarecrow_combatant

def add_boss_special_buffs(engine, boss_combatant, player_combatant, bot_id):
    """添加BOSS的特殊buff消息"""
    boss = boss_combatant['player']  # 现在可以正确访问了
    boss_buff = boss_combatant['boss_buff']

    boss_js = boss_combatant['current_js']
    
    # BOSS减伤消息
    if boss_js <= 0.6 and boss['name'] in BOSSDEF:
        effect_name = BOSSDEF[boss['name']]
        engine.add_system_message(f"{effect_name},获得了{int((1 - boss_js) * 100)}%减伤!")
    
    # BOSS攻击buff消息
    if boss_buff.boss_zs > 0:
        engine.add_system_message(f"{boss['name']}使用了真龙九变,提升了{int(boss_buff.boss_zs * 100)}%攻击力!")
    
    # BOSS会心buff消息
    if boss_buff.boss_hx > 0:
        engine.add_system_message(f"{boss['name']}使用了无瑕七绝剑,提升了{int(boss_buff.boss_hx * 100)}%会心率!")
    
    # BOSS暴伤buff消息
    if boss_buff.boss_bs > 0:
        engine.add_system_message(f"{boss['name']}使用了太乙剑诀,提升了{int(boss_buff.boss_bs * 100)}%会心伤害!")
    
    # BOSS吸血削弱消息
    if boss_buff.boss_xx > 0:
        engine.add_system_message(f"{boss['name']}使用了七煞灭魂聚血杀阵,降低了{player_combatant['player']['道号']}{int((boss_buff.boss_xx) * 100)}%气血吸取!")
    
    # BOSS降攻消息
    if boss_buff.boss_jg > 0:
        engine.add_system_message(f"{boss['name']}使用了子午安息香,降低了{player_combatant['player']['道号']}{int((boss_buff.boss_jg) * 100)}%伤害!")
    
    # BOSS降会消息
    if boss_buff.boss_jh > 0:
        engine.add_system_message(f"{boss['name']}使用了玄冥剑气,降低了{player_combatant['player']['道号']}{int((boss_buff.boss_jh) * 100)}%会心率!")
    
    # BOSS降暴消息
    if boss_buff.boss_jb > 0:
        engine.add_system_message(f"{boss['name']}使用了大德琉璃金刚身,降低了{player_combatant['player']['道号']}{int((boss_buff.boss_jb) * 100)}%会心伤害!")
    
    # BOSS禁蓝消息
    if boss_buff.boss_xl > 0:
        engine.add_system_message(f"{boss['name']}使用了千煌锁灵阵,降低了{player_combatant['player']['道号']}{int((boss_buff.boss_xl) * 100)}%真元吸取!")
    
    # BOSS闪避消息
    boss_dodge = boss_combatant['dodge']
    if boss_dodge > 0:
        engine.add_system_message(f"{boss['name']}使用虚无道则残片,提升了{int(boss_dodge)}%闪避!")
    
    # BOSS穿甲消息
    boss_cj = boss_combatant.get('boss_cj', 0)
    if boss_cj > 0:
        engine.add_system_message(f"{boss['name']}使用了钉头七箭书,提升了{int(boss_cj * 100)}%穿甲！")

def add_special_buffs(engine, player_combatant, bot_id, si_boss=False, boss_combatant=None):
    """添加玩家随机buff消息及BOSS特殊buff处理"""
    random_buff = player_combatant.get('random_buff', empty_ussr_random_buff)
    
    # 处理玩家随机buff消息
    # 玩家穿甲buff消息
    if random_buff.random_break > 0:
        engine.add_system_message(f"{player_combatant['player']['道号']}施展了无上战意,获得了{int((random_buff.random_break) * 100)}%穿甲！")
    
    # 玩家吸血buff消息
    if random_buff.random_xx > 0:
        engine.add_system_message(f"{player_combatant['player']['道号']}施展了无上战意,提升了{int((random_buff.random_xx) * 100)}%吸血效果！")
    
    # 玩家会心buff消息
    if random_buff.random_hx > 0:
        engine.add_system_message(f"{player_combatant['player']['道号']}施展了无上战意,提升了{int((random_buff.random_hx) * 100)}%会心率！")
    
    # 玩家减伤buff消息
    if random_buff.random_def > 0:
        engine.add_system_message(f"{player_combatant['player']['道号']}施展了无上战意,获得了{int((random_buff.random_def) * 100)}%减伤！")
    
    # 玩家随机技能消息
    if player_combatant.get('player1_random_sec', 0) > 0:
        player1_sec_name = player_combatant.get('player1_sec_name', '')
        player1_sec_desc = player_combatant.get('player1_sec_desc', '')
        user1_skill_data = player_combatant.get('skill_data', {})
        engine.add_system_message(f"{player_combatant['player']['道号']}发动了{player1_sec_name},{player1_sec_desc}获得了{user1_skill_data.get('name', '')}！")
    
    # 处理BOSS特殊buff消息
    if si_boss and boss_combatant is not None:
        boss_buff = boss_combatant.get('boss_buff', empty_boss_buff)
        fan_data = player_combatant.get('sub_buff_data')['fan']
        
        if fan_data > 0:
            # 将BOSS的特定负面Buff设置为0
            boss_buff.boss_xl = 0
            boss_buff.boss_jb = 0
            boss_buff.boss_jh = 0
            boss_buff.boss_jg = 0
            boss_buff.boss_xx = 0
            engine.add_system_message(f"{player_combatant['player']['道号']}发动了反咒禁制，无效化了BOSS的负面效果！")

def init_boss_combatant(boss):
    """初始化BOSS战斗参与者"""
    # 创建BOSS战斗参与者对象
    boss_buff = init_boss_buff(boss)
    boss_combatant = {
        'player': boss,  # BOSS数据
        'boss_buff': boss_buff,  # BOSS的特殊buff
        'hit': 100,  # BOSS命中率
        'dodge': 0,  # BOSS闪避率
        'turn_skip': True,  # BOSS是否可以行动
        'buff_turn': True,  # BOSS buff回合标志
        'turn_cost': 0,  # BOSS回合计数
        'current_js': boss_buff.boss_js,  # BOSS当前减伤
        'def_js': boss_buff.boss_js,  # BOSS基础减伤
        'init_hp': boss['气血'],  # BOSS初始血量
        'boss_cj': boss_buff.boss_cj,  # BOSS穿甲
        'sub_open': False,  # BOSS没有辅修功法
        'sub_buff_data': {},  # 空的辅修功法数据
        'battle_buff': None,  # BOSS的战斗buff
        'main_buff_data': None,  # BOSS没有主修功法数据
        'skill_open': False,  # BOSS没有技能
        'skill_data': None,  # BOSS没有技能数据
        'hp_buff': 0,  # BOSS没有气血buff
        'mp_buff': 0  # BOSS没有真元buff
    }
    
    # 计算BOSS闪避率
    boss_js = boss_combatant['current_js']
    boss_combatant['dodge'] = int((1 - boss_js) * 100 * random.uniform(0.1, 0.5))
    
    return boss_combatant

def init_boss_buff(boss):
    """初始化BOSS的特殊buff"""
    boss_buff = BossBuff()
    boss_level = boss["jj"]
    
    # 根据BOSS境界设置不同的buff强度
    if boss_level == "祭道境" or convert_rank((boss_level + '中期'))[0] < convert_rank('祭道境初期')[0]:
        # 最高级BOSS拥有最强buff
        boss_buff.boss_js = 0.05  # boss减伤率
        boss_buff.boss_cj = random.randint(25, 50) / 100
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 1
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.7
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 2
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = 1
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.7
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.7
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 1.5
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = 1

    elif convert_rank('至尊境初期')[0] < convert_rank((boss_level + '中期'))[0] < convert_rank('斩我境圆满')[0]:
        boss_buff.boss_js = random.randint(50, 55) / 100  # boss减伤率
        boss_buff.boss_cj = random.randint(15, 30) / 100
        # 中级BOSS
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 0.3
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.1
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 0.5
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = random.randint(5, 100) / 100
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.3
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.3
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 0.5
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = random.randint(5, 100) / 100
            
    elif convert_rank('微光境初期')[0] < convert_rank((boss_level + '中期'))[0] < convert_rank('遁一境圆满')[0]:
        boss_buff.boss_js = random.randint(40, 45) / 100  # boss减伤率
        boss_buff.boss_cj = random.randint(20, 40) / 100
        # 微光境BOSS
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 0.4
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.2
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 0.7
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = random.randint(10, 100) / 100
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.4
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.4
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 0.7
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = random.randint(10, 100) / 100
            
    elif convert_rank('星芒境初期')[0] < convert_rank((boss_level + '中期'))[0] < convert_rank('至尊境圆满')[0]:
        boss_buff.boss_js = random.randint(30, 35) / 100  # boss减伤率
        boss_buff.boss_cj = random.randint(20, 40) / 100
        # 星芒境BOSS
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 0.6
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.35
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 1.1
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = random.randint(30, 100) / 100
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.5
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.5
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 0.9
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = random.randint(30, 100) / 100
            
    elif convert_rank('月华境初期')[0] < convert_rank((boss_level + '中期'))[0] < convert_rank('微光境圆满')[0]:
        boss_buff.boss_js = random.randint(20, 25) / 100  # boss减伤率
        boss_buff.boss_cj = random.randint(20, 40) / 100
        # 月华境BOSS
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 0.7
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.45
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 1.3
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = random.randint(40, 100) / 100
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.55
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.6
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 1.0
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = random.randint(40, 100) / 100
            
    elif convert_rank('耀日境初期')[0] < convert_rank((boss_level + '中期'))[0] < convert_rank('星芒境圆满')[0]:
        boss_buff.boss_js = random.randint(10, 15) / 100  # boss减伤率
        boss_buff.boss_cj = random.randint(25, 45) / 100
        # 耀日境BOSS
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 0.85
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.5
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 1.5
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = random.randint(50, 100) / 100
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.6
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.65
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 1.1
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = random.randint(50, 100) / 100
            
    elif convert_rank('祭道境初期')[0] < convert_rank((boss_level + '中期'))[0] < convert_rank('月华境圆满')[0]:
        boss_buff.boss_js = 0.1  # boss减伤率
        boss_buff.boss_cj = random.randint(25, 45) / 100
        # 祭道境初级BOSS
        boss_st1 = random.randint(0, 100)
        if 0 <= boss_st1 <= 25:
            boss_buff.boss_zs = 0.9
        elif 26 <= boss_st1 <= 50:
            boss_buff.boss_hx = 0.6
        elif 51 <= boss_st1 <= 75:
            boss_buff.boss_bs = 1.7
        elif 76 <= boss_st1 <= 100:
            boss_buff.boss_xx = random.randint(60, 100) / 100
            
        boss_st2 = random.randint(0, 100)
        if 0 <= boss_st2 <= 25:
            boss_buff.boss_jg = 0.62
        elif 26 <= boss_st2 <= 50:
            boss_buff.boss_jh = 0.67
        elif 51 <= boss_st2 <= 75:
            boss_buff.boss_jb = 1.2
        elif 76 <= boss_st2 <= 100:
            boss_buff.boss_xl = random.randint(60, 100) / 100
            
    else:  # 低级BOSS
        boss_buff.boss_js = 1.0  # boss减伤率
        boss_buff.boss_cj = 0
        boss_buff.boss_zs = 0
        boss_buff.boss_hx = 0
        boss_buff.boss_bs = 0
        boss_buff.boss_xx = 0
        boss_buff.boss_jg = 0
        boss_buff.boss_jh = 0
        boss_buff.boss_jb = 0
        boss_buff.boss_xl = 0
    
    return boss_buff

def get_player_random_buff(player_combatant):
    """获取玩家的随机buff"""
    random_buff = UserRandomBuff()
    main_buff_data = player_combatant['main_buff_data']
    
    if main_buff_data and main_buff_data['random_buff'] == 1:
        user1_main_buff = random.randint(0, 100)
        if 0 <= user1_main_buff <= 25:
            random_buff.random_break = random.randint(15, 40) / 100
        elif 26 <= user1_main_buff <= 50:
            random_buff.random_xx = random.randint(2, 10) / 100
        elif 51 <= user1_main_buff <= 75:
            random_buff.random_hx = random.randint(5, 40) / 100
        elif 76 <= user1_main_buff <= 100:
            random_buff.random_def = random.randint(5, 15) / 100
            
    return random_buff

def execute_boss_turn(engine, boss_combatant, player_combatant, boss_init_hp):
    """执行BOSS的回合"""
    if not boss_combatant['turn_skip']:
        # BOSS被封印，无法行动
        boss_name = boss_combatant['player']['name']
        turn_msg = f"☆------{boss_name}的回合------☆"
        engine.add_boss_message(boss_combatant['player'], turn_msg, boss_init_hp)
        engine.add_boss_message(boss_combatant['player'], f"☆------{boss_name}动弹不得！------☆", boss_init_hp)
        
        if boss_combatant.get('turn_cost', 0) > 0:
            boss_combatant['turn_cost'] -= 1
        if boss_combatant.get('turn_cost', 0) == 0 and boss_combatant.get('buff_turn', True):
            boss_combatant['turn_skip'] = True
        return None
    
    # BOSS正常行动
    boss_name = boss_combatant['player']['name']
    turn_msg = f"☆------{boss_name}的回合------☆"
    engine.add_boss_message(boss_combatant['player'], turn_msg, boss_init_hp)
    
    # BOSS有概率使用特殊技能
    boss_sub = random.randint(0, 100)
    
    if boss_sub <= 6:  # 特殊技能1
        execute_boss_special_skill1(engine, boss_combatant, player_combatant, boss_init_hp)
    elif 6 < boss_sub <= 12:  # 特殊技能2
        execute_boss_special_skill2(engine, boss_combatant, player_combatant, boss_init_hp)
    else:  # 普通攻击
        execute_boss_normal_attack(engine, boss_combatant, player_combatant, boss_init_hp)
    
    # 检查战斗是否结束
    if player_combatant['player']['气血'] <= 0:
        engine.add_system_message(f"{boss_combatant['player']['name']}胜利")
        return "attacker_win"
    
    return None

def execute_boss_normal_attack(engine, boss_combatant, player_combatant, boss_init_hp):
    """BOSS普通攻击"""
    boss = boss_combatant['player']
    player = player_combatant['player']
    boss_buff = boss_combatant['boss_buff']
    random_buff = player_combatant.get('random_buff', empty_ussr_random_buff)
    
    # 计算BOSS攻击
    is_crit, boss_damage = get_turnatk_boss(boss, 0, UserBattleBuffDate("9999999"), boss_buff)
    
    # 检查命中
    if check_hit(boss_combatant['hit'], player_combatant['dodge']):
        # 计算实际伤害（考虑玩家减伤和BOSS穿甲）
        player_js = player_combatant['current_js']
        actual_damage = int(boss_damage * (1 + boss_buff.boss_zs) * max(min(player_js - random_buff.random_def + boss_combatant['boss_cj'], 1.0), 0.05))
        
        if is_crit:
            effect_name = boss['name']
            if boss['name'] in BOSSATK:
                effect_name = BOSSATK[boss['name']]
            msg = f"{effect_name}发起会心一击，造成了{number_to2(actual_damage)}伤害"
        else:
            msg = f"{boss['name']}发起攻击，造成了{number_to2(actual_damage)}伤害"
            
        player['气血'] -= actual_damage
        hp_bar = generate_hp_bar(player['气血'], player_combatant['init_hp'])
        engine.add_boss_message(boss, msg, boss_init_hp)
        engine.add_boss_message(boss, f"{player['道号']}剩余血量{number_to2(player['气血'])}\n{hp_bar}", boss_init_hp)
    else:
        engine.add_boss_message(boss, f"{boss['name']}的攻击被{player['道号']}闪避了！", boss_init_hp)

def execute_boss_special_skill1(engine, boss_combatant, player_combatant, boss_init_hp):
    """BOSS特殊技能1"""
    boss = boss_combatant['player']
    player = player_combatant['player']
    boss_buff = boss_combatant['boss_buff']
    random_buff = player_combatant.get('random_buff', empty_ussr_random_buff)
    
    is_crit, boss_damage = get_turnatk_boss(boss, 0, UserBattleBuffDate("9999999"), boss_buff)
    
    if check_hit(boss_combatant['hit'], player_combatant['dodge']):
        # 特殊技能1：造成5倍伤害并附加30%最大生命值的伤害
        player_js = player_combatant['current_js']
        special_damage = int(boss_damage * (1 + boss_buff.boss_zs) * max(min(player_js - random_buff.random_def + boss_combatant['boss_cj'], 1.0), 0.05) * 5)
        extra_damage = int(player['气血'] * 0.3)
        total_damage = special_damage + extra_damage
        
        player['气血'] -= total_damage
        
        if is_crit:
            msg = f"{boss['name']}：紫玄掌！！紫星河！！！并且发生了会心一击，造成了{number_to2(total_damage)}伤害"
        else:
            msg = f"{boss['name']}：紫玄掌！！紫星河！！！造成了{number_to2(total_damage)}伤害"
        hp_bar = generate_hp_bar(player['气血'], player_combatant['init_hp'])            
        engine.add_boss_message(boss, msg, boss_init_hp)
        engine.add_boss_message(boss, f"{player['道号']}剩余血量{number_to2(player['气血'])}\n{hp_bar}", boss_init_hp)
    else:
        engine.add_boss_message(boss, f"{boss['name']}的技能被{player['道号']}闪避了！", boss_init_hp)

def execute_boss_special_skill2(engine, boss_combatant, player_combatant, boss_init_hp):
    """BOSS特殊技能2"""
    boss = boss_combatant['player']
    player = player_combatant['player']
    boss_buff = boss_combatant['boss_buff']
    random_buff = player_combatant.get('random_buff', empty_ussr_random_buff)
    
    is_crit, boss_damage = get_turnatk_boss(boss, 0, UserBattleBuffDate("9999999"), boss_buff)
    
    if check_hit(boss_combatant['hit'], player_combatant['dodge']):
        player_js = player_combatant['current_js']
        # 特殊技能2：穿透护甲，造成3倍伤害
        special_damage = int(boss_damage * (1 + boss_buff.boss_zs) * max(min(player_js - random_buff.random_def + boss_combatant['boss_cj'] + 0.5, 1.0), 0.05) * 3)
        
        player['气血'] -= special_damage
        
        if is_crit:
            msg = f"{boss['name']}：子龙朱雀！！！穿透了对方的护甲！并且发生了会心一击，造成了{number_to2(special_damage)}伤害"
        else:
            msg = f"{boss['name']}：子龙朱雀！！！穿透了对方的护甲！造成了{number_to2(special_damage)}伤害"
        hp_bar = generate_hp_bar(player['气血'], player_combatant['init_hp'])            
        engine.add_boss_message(boss, msg, boss_init_hp)
        engine.add_boss_message(boss, f"{player['道号']}剩余血量{number_to2(player['气血'])}\n{hp_bar}", boss_init_hp)
    else:
        engine.add_boss_message(boss, f"{boss['name']}的技能被{player['道号']}闪避了！", boss_init_hp)

def update_boss_fight_stats(player_combatant, winner, type_in):
    """更新BOSS战斗后的玩家状态"""
    if type_in != 2:  # 只有实际战斗才更新
        return
        
    player = player_combatant['player']
    hp_buff = player_combatant['hp_buff']
    mp_buff = player_combatant['mp_buff']
    
    if winner == player_combatant:  # 玩家胜利
        if player['气血'] <= 0:
            player['气血'] = 1
        sql_message.update_user_hp_mp(
            player['user_id'],
            int(player['气血'] / (1 + hp_buff)),
            int(player['真元'] / (1 + mp_buff))
        )
    else:  # BOSS胜利
        sql_message.update_user_hp_mp(
            player['user_id'], 
            1, 
            int(player['真元'] / (1 + mp_buff))
        )

BOSSDEF = {
        "衣以候": "衣以侯布下了禁制镜花水月，",
        "金凰儿": "金凰儿使用了神通：金凰天火罩！",
        "九寒": "九寒使用了神通：寒冰八脉！",
        "莫女": "莫女使用了神通：圣灯启语诀！",
        "术方": "术方使用了神通：天罡咒！",
        "卫起": "卫起使用了神通：雷公铸骨！",
        "血枫": "血枫使用了神通：混世魔身！",
        "以向": "以向使用了神通：云床九练！",
        "砂鲛鲛": "不说了！开鳖！",
        "神风王": "不说了！开鳖！",
        "鲲鹏": "鲲鹏使用了神通：逍遥游！",
        "天龙": "天龙使用了神通：真龙九变！",
        "历飞雨": "厉飞雨使用了神通：天煞震狱功！",
        "外道贩卖鬼": "不说了！开鳖！",
        "元磁道人": "元磁道人使用了法宝：元磁神山！",
        "散发着威压的尸体": "尸体周围爆发了出强烈的罡气！",
        "贪欲心魔": "贪欲心魔施展七情六欲大法，勾起修士内心贪念！",
        "嗔怒心魔": "嗔怒心魔催动无明业火，点燃修士心中怒火！",
        "痴妄心魔": "痴妄心魔布下颠倒梦想阵，迷惑修士心智！",
        "傲慢心魔": "傲慢心魔施展唯我独尊功，助长修士骄矜之气！",
        "嫉妒心魔": "嫉妒心魔发动红眼诅咒，激发修士妒火中烧！",
        "恐惧心魔": "恐惧心魔唤起九幽幻象，引发修士内心恐惧！",
        "懒惰心魔": "懒惰心魔布下浑噩迷雾，消磨修士意志！",
        "七情心魔": "七情心魔操控喜怒忧思悲恐惊，扰乱修士心神！",
        "六欲心魔": "六欲心魔激发眼耳鼻舌身意之欲，迷惑修士五感！",
        "天魔幻象": "域外天魔投影幻象，直击修士道心破绽！",
        "心魔劫主": "心魔之主显化本体，万劫之源侵蚀修士神魂！"
}

BOSSATK = {
        "衣以候": "衣以侯布下了禁制镜花水月，",
        "金凰儿": "金凰儿使用了神通：金凰天火罩！",
        "九寒": "九寒使用了神通：寒冰八脉！",
        "莫女": "莫女使用了神通：圣灯启语诀！",
        "术方": "术方使用了神通：天罡咒！",
        "卫起": "卫起使用了神通：雷公铸骨！",
        "血枫": "血枫使用了神通：混世魔身！",
        "以向": "以向使用了神通：云床九练！",
        "砂鲛鲛": "不说了！开鳖！",
        "神风王": "不说了！开鳖！",
        "鲲鹏": "鲲鹏使用了神通：逍遥游！",
        "天龙": "天龙使用了神通：真龙九变！",
        "历飞雨": "厉飞雨使用了神通：天煞震狱功！",
        "外道贩卖鬼": "不说了！开鳖！",
        "元磁道人": "元磁道人使用了法宝：元磁神山！",
        "散发着威压的尸体": "尸体周围爆发了出强烈的罡气！",
        "贪欲心魔": "贪欲心魔施展七情六欲大法，勾起修士内心贪念！",
        "嗔怒心魔": "嗔怒心魔催动无明业火，点燃修士心中怒火！",
        "痴妄心魔": "痴妄心魔布下颠倒梦想阵，迷惑修士心智！",
        "傲慢心魔": "傲慢心魔施展唯我独尊功，助长修士骄矜之气！",
        "嫉妒心魔": "嫉妒心魔发动红眼诅咒，激发修士妒火中烧！",
        "恐惧心魔": "恐惧心魔唤起九幽幻象，引发修士内心恐惧！",
        "懒惰心魔": "懒惰心魔布下浑噩迷雾，消磨修士意志！",
        "七情心魔": "七情心魔操控喜怒忧思悲恐惊，扰乱修士心神！",
        "六欲心魔": "六欲心魔激发眼耳鼻舌身意之欲，迷惑修士五感！",
        "天魔幻象": "域外天魔投影幻象，直击修士道心破绽！",
        "心魔劫主": "心魔之主显化本体，万劫之源侵蚀修士神魂！"
}

def update_player_stats(combatant1, combatant2, winner, type_in):
    """更新玩家状态到数据库"""
    if type_in != 2:  # 只有实际战斗才更新
        return
        
    # 更新胜者状态
    if winner == combatant1:
        if combatant1['player']['气血'] <= 0:
            combatant1['player']['气血'] = 1
        sql_message.update_user_hp_mp(
            combatant1['player']['user_id'],
            int(combatant1['player']['气血'] / (1 + combatant1['hp_buff'])),
            int(combatant1['player']['真元'] / (1 + combatant1['mp_buff']))
        )
        sql_message.update_user_hp_mp(
            combatant2['player']['user_id'], 
            1, 
            int(combatant2['player']['真元'] / (1 + combatant2['mp_buff']))
        )
    else:  # combatant2胜利
        sql_message.update_user_hp_mp(
            combatant1['player']['user_id'], 
            1, 
            int(combatant1['player']['真元'] / (1 + combatant1['mp_buff']))
        )
        if combatant2['player']['气血'] <= 0:
            combatant2['player']['气血'] = 1
        sql_message.update_user_hp_mp(
            combatant2['player']['user_id'],
            int(combatant2['player']['气血'] / (1 + combatant2['hp_buff'])),
            int(combatant2['player']['真元'] / (1 + combatant2['mp_buff']))
        )
