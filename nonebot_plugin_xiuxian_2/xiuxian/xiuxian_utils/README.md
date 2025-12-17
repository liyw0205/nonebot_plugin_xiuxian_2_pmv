PvP（玩家对战玩家）战斗逻辑

类和函数概述

1. 
"Player_fight" 函数
   - 作用：处理玩家之间的战斗逻辑。
   - 参数：
      - 
"user1": 玩家1的数据。
      - 
"user2": 玩家2的数据。
      - 
"type_in": 战斗类型（1为切磋，2为正式战斗）。
      - 
"bot_id": 机器人ID，用于记录或输出。
   - 流程：
      1. 初始化战斗引擎和参与者。
      2. 获取并设置玩家的随机buff。
      3. 处理辅修功法效果。
      4. 进入战斗循环，轮流执行玩家回合。
      5. 判断战斗结果，更新玩家状态。
2. 
"BattleEngine" 类
   - 作用：管理战斗逻辑，包括回合执行、伤害计算、状态更新等。
   - 方法：
      - 
"init_combatant": 初始化战斗参与者。
      - 
"execute_turn": 执行单个回合的战斗逻辑。
      - 
"execute_skill_attack": 执行技能攻击。
      - 
"execute_normal_attack": 执行普通攻击。
      - 
"handle_skill_type": 处理不同类型的技能。
      - 
"check_battle_end": 检查战斗是否结束。
3. 
"apply_buff" 和 
"start_sub_buff_handle" 函数
   - 作用：处理辅修功法的增益效果。
   - 参数：
      - 
"player1_sub_open": 玩家1是否开启辅修功法。
      - 
"subbuffdata1": 玩家1的辅修功法数据。
      - 
"user1_battle_buff_date": 玩家1的战斗buff数据。
      - 
"player2_sub_open": 玩家2是否开启辅修功法。
      - 
"subbuffdata2": 玩家2的辅修功法数据。
      - 
"user2_battle_buff_date": 玩家2的战斗buff数据。
4. 
"after_atk_sub_buff_handle" 函数
   - 作用：处理攻击后的辅修功法效果。
   - 参数：
      - 
"player1_sub_open": 玩家1是否开启辅修功法。
      - 
"player1": 玩家1的数据。
      - 
"player1_main_buff_data": 玩家1的主功法数据。
      - 
"player1_sub_buff_data": 玩家1的辅修功法数据。
      - 
"damage_dealt": 造成的伤害。
      - 
"player2": 玩家2的数据。

战斗流程

1. 初始化阶段
   - 初始化战斗引擎和参与者数据。
   - 获取并设置玩家的随机buff。
   - 处理辅修功法效果。
2. 战斗循环
   - 玩家1回合：
      - 执行技能攻击或普通攻击。
      - 处理辅修功法效果。
      - 检查玩家2是否死亡。
   - 玩家2回合：
      - 执行技能攻击或普通攻击。
      - 处理辅修功法效果。
      - 检查玩家1是否死亡。
3. 战斗结束
   - 根据战斗结果更新玩家状态。
   - 返回战斗日志和胜利者的道号。

PvE（玩家对战BOSS）战斗逻辑

类和函数概述

1. 
"Boss_fight" 函数
   - 作用：处理玩家与BOSS之间的战斗逻辑。
   - 参数：
      - 
"user1": 玩家数据。
      - 
"boss": BOSS的数据。
      - 
"type_in": 战斗类型（默认为2，表示正式战斗）。
      - 
"bot_id": 机器人ID，用于记录或输出。
   - 流程：
      1. 初始化战斗引擎和参与者。
      2. 获取并设置玩家的随机buff。
      3. 处理辅修功法效果。
      4. 进入战斗循环，轮流执行玩家和BOSS的回合。
      5. 判断战斗结果，更新玩家状态。
2. 
"BattleEngine" 类
   - 作用：管理战斗逻辑，包括回合执行、伤害计算、状态更新等。
   - 方法：
      - 
"init_combatant": 初始化战斗参与者。
      - 
"execute_turn": 执行单个回合的战斗逻辑。
      - 
"execute_skill_attack": 执行技能攻击。
      - 
"execute_normal_attack": 执行普通攻击。
      - 
"handle_skill_type": 处理不同类型的技能。
      - 
"check_battle_end": 检查战斗是否结束。
3. 
"init_boss_combatant" 和 
"init_scarecrow_combatant" 函数
   - 作用：初始化BOSS或稻草人的战斗参与者数据。
   - 参数：
      - 
"boss": BOSS或稻草人的数据。
4. 
"execute_boss_turn" 和 
"execute_boss_normal_attack" 函数
   - 作用：处理BOSS的回合，包括普通攻击和特殊技能。
   - 参数：
      - 
"engine": 战斗引擎实例。
      - 
"boss_combatant": BOSS的战斗参与者数据。
      - 
"player_combatant": 玩家的战斗参与者数据。
      - 
"boss_init_hp": BOSS的初始气血值。
5. 
"after_atk_sub_buff_handle" 函数
   - 作用：处理攻击后的辅修功法效果。
   - 参数：
      - 
"player1_sub_open": 玩家是否开启辅修功法。
      - 
"player1": 玩家数据。
      - 
"player1_main_buff_data": 玩家主功法数据。
      - 
"player1_sub_buff_data": 玩家辅修功法数据。
      - 
"damage_dealt": 造成的伤害。
      - 
"player2": BOSS数据。

战斗流程

1. 初始化阶段
   - 初始化战斗引擎和参与者数据。
   - 获取并设置玩家的随机buff。
   - 处理辅修功法效果。
2. 战斗循环
   - 玩家回合：
      - 执行技能攻击或普通攻击。
      - 处理辅修功法效果。
      - 检查BOSS是否死亡。
   - BOSS回合：
      - 执行普通攻击或特殊技能。
      - 处理辅修功法效果。
      - 检查玩家是否死亡。
3. 战斗结束
   - 根据战斗结果更新玩家状态。
   - 返回战斗日志、胜负结果和更新后的BOSS状态。
