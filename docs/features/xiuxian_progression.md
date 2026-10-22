# 修为经济与境界压制（开发者参考）

> 长期参考文档；数值结论来自 2026-10 全项目修为奖励盘点
> （盘点卡已归档，本文件是唯一长期承载）。玩家侧简述见
> `docs/gameplay_notes.md`「修为奖励与境界压制」。

## 修为奖励来源总表

| # | 玩法 | 位置 | 公式 | 组成 | 压制 |
|:--|:--|:--|:--|:--|:--|
| 1 | 闭关 | `features/buff/closing_reward.py:100-139` | `minutes×100×灵根×spend×(1+ratebuff)×(1+clo_exp)×(1+洞天)` | 固定基数×乘法增益 | cap=距上境 gap |
| 2 | 灵石修炼 | `xiuxian_buff/__init__.py:641+` | 同上 + stone_cost 转化 | 同上+资源转化 | cap |
| 3 | 修为修炼 | `xiuxian_buff/__init__.py:640-660` | `closing_exp×(灵根×spend×(1+buff)³)×rand(0.9,1.3)` | 同上 | exp_cap |
| 4 | 爬塔每 10 层 | `xiuxian_tower/tower_battle.py:203-226` | `gap×0.001` | 纯百分比 | ÷3 |
| 5 | 历练事件 | `features/training/event_resolver.py:398,542` | `gap×(0.003+rand(0.001~0.008))` | 纯百分比 | ÷3 |
| 6 | 幻境奖励 | `xiuxian_Illusion/__init__.py:244` | `gap×0.01` | 纯百分比 | ÷3（核验确认） |
| 7 | 宗门任务 | `xiuxian_sect/__init__.py:2018,2101` | `gap×give`（配置 give） | 纯百分比 | ÷3（`6321662c` 已加） |
| 8 | 前尘轮回 | `past_life/past_life_events.py:713` | `gap×exp_rate` | 纯百分比 | ÷3 |
| 9 | 魔修入侵 | `world_events/__init__.py:1514` | `gap×0.05×贡献` | 纯百分比 | 无（受贡献上限） |
| 10 | 突破成功 | `compatibility/base_breakthrough_effects.py:193` | `gap×0.01` | 纯百分比 | 单次语义 |
| 11 | 双修 | `compatibility`（`partner.py`） | `min(gap×1%, 对方exp×10%)` | 纯百分比+对方上限 | min 上限 |
| 12 | 每日签到 | `features/sign_in/` | 仅灵石 | 固定 | — |

**境界数据**：`data/xiuxian/境界.json`，77 境；`power` = 下一境升级所需修为基数，
`spend` ≈ power 递增比。极值：感气 gap≈1e3 → 造化 gap≈4.5e17 → 永恒 gap≈2.4e19。

## 压制公式与分类原则

`rank_suppress_factor = min(0.1×rank_slot, 1.0)`；高频重复玩法
（历练/爬塔/幻境/宗门任务/前尘轮回）一律 `divide_by_three=True`，
anchor='gap'。传承/道侣类压制由「不超过对方修为 10%」上限承担。

分类原则：固定值用于日常任务/基础掉落（防膨胀）；固定值+百分比用于
首通/周常/成就一次性奖励；纯百分比仅用于高频重复玩法，且必须叠加
境界压制。单事件价值占 gap 的比值必须恒定，升级节奏由玩法次数/体力
上限决定——任何「gap 锚定百分比且无压制」的可重复入口都是后期难度
崩塌源（曾定位宗门任务为此类来源，已修复）。
