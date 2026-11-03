# 炼丹灵田与结算

## 用户流程

用户在洞天福地灵田成熟后执行灵田收取；自定义配方炼丹则先扣除药材并记录待领取任务，再领取丹药。灵田成熟窗口、可收取数量与药材抽取已由 `features/mixelixir/harvest_rules.py` 承担，只读状态由 `features/mixelixir/harvest_view.py` 提供；配方解析、随机奖励展示文案与冷却仍由旧命令适配器计算。灵田收取、扣材和跨库领奖励统一通过 `MixelixirApplication`。

## 命令与别名

- `灵田收取`
- `灵田结算`
- `升级收取等级`：消耗炼丹经验提升灵田一次收取的药材数量（最高 3 级）。
- `升级丹药控火`：消耗炼丹经验提升炼丹产出数量（最高 2 级）。

## Web API

- `POST /api/v1/mixelixir/harvest`：提交用户、上次收取时间、收取时间、奖励快照和背包上限。
- `POST /api/v1/mixelixir/settle`：提交材料映射、丹药 ID/名称/数量和背包上限。

两者权限均为 `user`，支持 `Idempotency-Key`，使用统一 JSON envelope 和 CSRF。

## 炼丹升级（升级收取等级 / 升级丹药控火）

两条升级命令都经 `MixelixirApplication.harvest_level_upgrade` /
`MixelixirApplication.fire_control_upgrade` 在 game DB 单事务内 `ATTACH` player DB 完成：
校验玩家与快照、扣 `炼丹经验`、提升对应等级、写幂等 operation 行，任何一步失败整体回滚。

列合同（`player_db.mix_elixir_info`，权威来源为 `xiuxian2_handle.mix_elixir_infoconfigkey`）：

- `升级收取等级` 读写 `收取等级`；`升级丹药控火` 读写 `丹药控火`——**fire-control 等级存在
  `丹药控火`，没有 `控火等级` 列**。缺列时 SQLite 会把未解析标识符当字符串字面量（读取静默
  出错数据），而 `UPDATE ... SET <未知列>` 直接抛 `OperationalError`；因此两个仓储都把列名
  收在类常量里，测试也用真实列集建表。
- 两条命令都消耗 `炼丹经验`，**都不扣灵石**；`expected_stone` 只是调用方观察到的钱包并发快照。

快照语义：`expected_stone` 与库中 `user_xiuxian.stone` 不一致时返回 `state_changed` 且零变化；
传 `None` 表示显式跳过钱包校验（供显式调用方使用）。两条 handler 都传
`expected_stone=user_info['stone']`——曾漏传（收取等级）或硬编码为 `0`
（application 层），导致钱包非 0 的玩家恒返回 `state_changed`、永远无法升级。

失败状态：`experience_insufficient`（经验不足）、`state_changed`（快照过期/缺行/写回未命中）、
`operation_conflict`（同一操作号用于不同请求）；`duplicate` 重放返回首次结果且不重复扣经验。
缺 `mix_elixir_info` 表或缺玩家行时返回 `state_changed`，不建表、不补列。

## 灵田收取（成熟窗口 / 数量 / 抽取）

`灵田收取` / `灵田结算` 原先在命令模块里硬编码成熟窗口 `{"time_cost": 23, "加速基数": 0.05}`、
裸 `strptime` 解析 `mix_elixir_info."收取时间"`（缺行、`0`/`'0'` 或脏值直接抛异常），并逐次
`choice` 抽取药材后去重合并。现在：

- 成熟判定与时间解析由 `features/mixelixir/harvest_rules.py` 提供：
  `maturity_state()` 返回 `ready` / `waiting`（含剩余小时）/ `invalid`，窗口为
  `round(23 × (1 − 0.05 × 药材速度), 2)`；`invalid` 与「从未收取」等价（`0` 与文本 `'0'` 同义），
  交给跨库仓储的快照校验决定是否真正发放，不再在命令里抛错。
- 可收取数量 = `灵田数量 + 收取等级 + 传承 impart_reap_per + 功法 reap_buff`（`harvest_count()`，下限 0）。
- `roll_rewards()` 逐次抽取并按 id 去重保持首次出现顺序；候选为空时返回兜底
  `(3001, "恒心草", 数量)`。`剩余时间` 文案由 `remaining_text()` 生成，格式与历史一致（`X小时Y分钟`）。
- 只读状态由 `features/mixelixir/harvest_view.py` 的 `MixelixirHarvestFieldApplication.snapshot()`
  在单个只读 UoW 内读取真实列集；缺 `user_xiuxian`、缺 `mix_elixir_info` 或缺可选列时 fail-soft
  （`ok=False` 或缺失列按默认值），不建表、不补列。
- 兼容：`收取时间` 是历史文本列，`'0'`、空串、非法时间一律视作首次收取；已存在的
  `mixelixir_harvest_operations` 行继续参与重放，重放语义不变。

## 数据模型与迁移

`mixelixir.001` 写入 `mixelixir_feature_migrations`。`mixelixir.002` 在 game DB 创建/升级两阶段任务、扣材操作和领奖励操作表；`mixelixir.003` 在 player DB 准备炼丹次数统计列。`mixelixir.004` 在 game DB 预建四张炼丹回执表
（`mixelixir_harvest_operations`、`mixelixir_settlement_operations`、
`mixelixir_harvest_level_upgrade_operations`、`mixelixir_fire_control_upgrade_operations`）；
四张表此前在请求事务内 `CREATE TABLE IF NOT EXISTS`，与「请求路径不执行 DDL」的仓库级合同冲突，
迁移语句保持 `IF NOT EXISTS`，已按惰性路径建表的历史库保留全部既有回执行。四条请求路径现在只校验
schema，缺表或缺列一律返回 `schema_missing` 且零变化，领取请求不执行 DDL。

## 事务与失败回滚

应用层先在 `game_db.operation_ledger` 登记；灵田收取由跨库仓储通过 `ATTACH DATABASE` 原子更新药材和收取时间。两阶段炼丹在 game DB 同事务校验并扣除药材、增加每日炼丹次数和保存完整任务快照；领取时在 attached UoW 校验修为快照与库存容量，并原子发放丹药、更新 `mix_elixir_info`、增加 player DB 的炼丹统计、完成任务和写幂等结果。状态冲突、材料不足、背包满或异常都不会留下半笔资产变化。

## 定时任务

既有 `daily_reset_mixelixir_num` 仍在 scheduler 时区的每日零点执行，但重置写入改由
`MixelixirApplication -> MixelixirDailyCountResetSqlRepository` 承担。业务日作为稳定操作号，
同一日期的重放只返回首次数量，不会再次清除当天新产生的次数；清零、共享操作回执和审计在同一
immediate UoW 提交。缺少既有 platform ledger/audit 或 `user_xiuxian.mixelixir_num` 时 fail closed，
不执行请求期 DDL，也不增加业务迁移。

## 配置项

- `mixelixir_enabled` / `XIUXIAN_MIXELIXIR_ENABLED`：默认启用，可关闭新 application 接入。

## 适配器差异

NoneBot 适配器继续负责随机奖励、配方文本和消息文案；Web 适配器只做 DTO、权限和响应序列化。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_mixelixir_application -q`
- `python -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/mixelixir/tests/test_daily_reset_repository.py -q`
- `python -m unittest nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_refine_cost_repository nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_refine_reward_repository -q`
- `python -m unittest tests.test_mixelixir_refine_claim_boundary -q`
- `python -m unittest tests.test_mixelixir_harvest_service tests.test_mixelixir_settlement_service tests.test_source_quality -q`
- `python -m unittest tests.test_mixelixir_harvest_domain tests.test_mixelixir_upgrade_production_path -q`

## 灰度开关、回滚和已知限制

关闭 `mixelixir_enabled` 即回到旧命令写入。旧 `transaction_service` 的四条兼容路径（`harvest` /
`settle` / 两个 `upgrade`）保留为显式回滚入口，它们仍在自身事务内 `CREATE TABLE IF NOT EXISTS`；
默认命令路径与 feature 仓储都已改为只校验启动迁移建的 schema。炼丹升级和配方保存仍由兼容服务维护；旧 `MixelixirRefineRewardService` 保留作兼容对照，不再由默认领取 handler 调用。`升级收取等级` / `升级丹药控火` 的兼容 service（`MixelixirHarvestLevelUpgradeService` / `MixelixirFireControlUpgradeService`）保留为显式回滚路径，其列名常量与 feature 仓储一致；两条默认路径都已由 feature 仓储承担。配方解析（`mixelixirutil`）与炼丹随机奖励展示仍在命令适配器侧。

## Manifest 清单

- `command: 灵田收取`
- `alias: 灵田结算`
- `route: POST /api/v1/mixelixir/harvest`
- `route: POST /api/v1/mixelixir/settle`
- `config: mixelixir_enabled`
- `migration: mixelixir.001`
