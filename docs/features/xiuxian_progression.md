# 修为经济与境界压制（开发者参考）

> 长期参考文档；数值结论来自 2026-10 全项目修为奖励盘点，2026-11 按代码重新核验
> （`b4421bb3` 起）。玩家侧简述见 `docs/gameplay_notes.md`「修为奖励与境界压制」。
> 本表由 `tests/test_progression_source_table.py` 防漂移：表里登记的位置与
> `anchor`/`divide_by_three`/`apply_rank_suppress` 必须与代码一致。

## 一、百分比奖励来源（`percent_exp_reward`，16 处调用）

| # | 玩法 | 位置 | anchor | ÷3 | 压制 | 备注 |
|:--|:--|:--|:--|:--|:--|:--|
| 1 | 突破成功 | `compatibility/base_breakthrough_effects.py:193` | gap | 否 | `False` | `gap×0.01`，单次语义 |
| 2 | 世界BOSS 讨伐 | `features/boss/exp_reward.py:61` | gap | **是** | 默认 | `gap×(exp_buff/10000)`；境界名未知时 fail-closed 返回 0 |
| 3 | 历练（规划） | `features/training/event_planner.py:83` | gap | **是** | 默认 | `gap×(0.003+rand)` |
| 4 | 历练（奖励） | `features/training/event_resolver.py:398` | gap | **是** | 默认 | 同上 |
| 5 | 历练（惩罚） | `features/training/event_resolver.py:542` | gap | **是** | 默认 | 与奖励侧同口径（`b4421bb3` 补齐 ÷3） |
| 6 | 幻境 | `xiuxian/xiuxian_Illusion/__init__.py:244` | gap | **是** | 默认 | `gap×0.01` |
| 7 | 双修 | `xiuxian/xiuxian_buff/partner.py:2537` | gap | 否 | 默认 | 另受「对方修为 10%」上限 |
| 8 | 传功 | `xiuxian/xiuxian_buff/partner.py:2630` | gap | 否 | `False` | 由「对方修为 10%」上限承担压制 |
| 9 | 前尘轮回 | `xiuxian/xiuxian_past_life/past_life_events.py:713` | gap | **是** | 默认 | `gap×exp_rate` |
| 10 | 宗门任务（接取） | `xiuxian/xiuxian_sect/__init__.py:2033` | gap | **是** | 默认 | `gap×give`（配置 give） |
| 11 | 宗门任务（完成） | `xiuxian/xiuxian_sect/__init__.py:2116` | gap | **是** | 默认 | 同上 |
| 12 | 爬塔每 10 层 | `xiuxian/xiuxian_tower/tower_battle.py:203` | gap | **是** | 默认 | `gap×0.001` |
| 13 | 爬塔里程碑 ×2 | `xiuxian/xiuxian_tower/tower_battle.py:224` | gap | **是** | 默认 | 同一路径的里程碑加成 |
| 14 | 爬塔每 10 层（另一路径） | `xiuxian/xiuxian_tower/tower_battle.py:370` | gap | **是** | 默认 | 与 12 同口径 |
| 15 | 爬塔里程碑 ×2（另一路径） | `xiuxian/xiuxian_tower/tower_battle.py:390` | gap | **是** | 默认 | 与 13 同口径 |
| 16 | 魔修入侵 | `xiuxian/xiuxian_world_events/__init__.py:1518` | gap | 否 | 默认 | `gap×0.05×贡献`，另受贡献上限 |

## 二、非百分比 / 间接来源

| # | 玩法 | 位置 | 公式 | 压制 |
|:--|:--|:--|:--|:--|
| 1 | 闭关 | `features/buff/closing_reward.py:100-139` | `minutes×100×灵根×spend×(1+ratebuff)×(1+clo_exp)×(1+洞天)` | cap = 距上境 gap |
| 2 | 灵石修炼 | `xiuxian/xiuxian_buff/__init__.py:641+` | 同上 + 灵石转化 | cap |
| 3 | 修为修炼 | `xiuxian/xiuxian_buff/__init__.py:640-660` | `closing_exp×(灵根×spend×(1+buff)³)×rand(0.9,1.3)` | exp_cap |
| 4 | 悬赏令 | `features/work/reward_application.py:110-126` | **固定值** `task["award"]`；大成 ×rand(1.5,2.5)、失败 ÷2 | 任务配置上限 |
| 5 | 秘境（伤害扣修为） | `features/rift/domain.py:88` | 注入 `percent_exp_reward(..., apply_rank_suppress=False, anchor="gap")`，`gap×0.003~0.005` | 不需压制：这是**扣减**而非发放 |
| 6 | 秘境（结算奖励） | `features/rift/domain.py:234-240` | 注入助手，`anchor="gap"`；`max_exp = level_power×closing_exp_upper_limit×0.1` | 上限即压制 |
| 7 | 每日签到 | `features/sign_in/` | 仅灵石，不发修为 | — |

**境界数据**：`data/xiuxian/境界.json`，77 境，命名为「初期/中期/圆满」三段
（**没有**「后期」）。`power` = 该境升级所需修为基数。77 境 `stage_power_gap` 全部 > 0；
境界名不在表内时返回 0——此时 `percent_exp_reward` 会静默回退到 `current` 锚定，
发放方必须自行 fail-closed（世界BOSS 已按此实现）。

## 三、压制公式与分类原则

`rank_suppress_factor = min(0.1×(rank//3), 1.0)`，`rank` 为境界序号。

- **高频重复玩法**（历练、爬塔、幻境、宗门任务、前尘轮回、世界BOSS）：
  `anchor='gap'` + `divide_by_three=True`，单次奖励占 gap 的比值恒定，
  升级节奏只由「玩法次数 × 体力/次数上限」决定。
- **单次或受上限语义**（突破成功、双修、传功、魔修入侵、秘境结算）：
  `anchor='gap'`，压制由 `apply_rank_suppress=False` 加外部上限承担。
- **固定值**（悬赏令、签到、基础掉落）：不随累计修为增长，防膨胀。
- **绝不使用 `current` 锚定发放修为**：单次奖励会随累计修为一起膨胀，是后期难度
  崩塌源。世界BOSS 曾是此类（`b4421bb3` 前手写 `exp×exp_buff/10000×压制`），现已改回 gap。

历史修复：宗门任务 ÷3（`6321662c`）、历练惩罚 ÷3 与世界BOSS 锚点（`b4421bb3`）。
