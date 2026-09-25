# 炼丹灵田与结算

## 用户流程

用户在洞天福地灵田成熟后执行灵田收取；自定义配方炼丹则先扣除药材并记录待领取任务，再领取丹药。随机奖励、配方解析和数值加成仍由旧命令适配器计算；灵田收取、扣材和跨库领奖励通过 `MixelixirApplication`。

## 命令与别名

- `灵田收取`
- `灵田结算`

## Web API

- `POST /api/v1/mixelixir/harvest`：提交用户、上次收取时间、收取时间、奖励快照和背包上限。
- `POST /api/v1/mixelixir/settle`：提交材料映射、丹药 ID/名称/数量和背包上限。

两者权限均为 `user`，支持 `Idempotency-Key`，使用统一 JSON envelope 和 CSRF。

## 数据模型与迁移

`mixelixir.001` 写入 `mixelixir_feature_migrations`。`mixelixir.002` 在 game DB 创建/升级两阶段任务、扣材操作和领奖励操作表；`mixelixir.003` 在 player DB 准备炼丹次数统计列。领取请求不执行 DDL。

## 事务与失败回滚

应用层先在 `game_db.operation_ledger` 登记；灵田收取由跨库仓储通过 `ATTACH DATABASE` 原子更新药材和收取时间。两阶段炼丹在 game DB 同事务校验并扣除药材、增加每日炼丹次数和保存完整任务快照；领取时在 attached UoW 校验修为快照与库存容量，并原子发放丹药、更新 `mix_elixir_info`、增加 player DB 的炼丹统计、完成任务和写幂等结果。状态冲突、材料不足、背包满或异常都不会留下半笔资产变化。

## 定时任务

无新增任务。

## 配置项

- `mixelixir_enabled` / `XIUXIAN_MIXELIXIR_ENABLED`：默认启用，可关闭新 application 接入。

## 适配器差异

NoneBot 适配器继续负责随机奖励、配方文本和消息文案；Web 适配器只做 DTO、权限和响应序列化。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_mixelixir_application -q`
- `python -m unittest nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_refine_cost_repository nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_refine_reward_repository -q`
- `python -m unittest tests.test_mixelixir_refine_claim_boundary -q`
- `python -m unittest tests.test_mixelixir_harvest_service tests.test_mixelixir_settlement_service tests.test_source_quality -q`

## 灰度开关、回滚和已知限制

关闭 `mixelixir_enabled` 即回到旧命令写入。炼丹升级和配方保存仍由兼容服务维护；旧 `MixelixirRefineRewardService` 保留作兼容对照，不再由默认领取 handler 调用。
