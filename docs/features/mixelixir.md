# 炼丹灵田与结算

## 用户流程

用户在洞天福地灵田成熟后执行灵田收取；自定义配方炼丹则消耗药材并发放丹药。奖励随机、配方解析和数值加成仍由旧命令适配器计算；灵田收取和单事务结算通过 `MixelixirApplication`，旧配方两阶段流程继续走兼容服务。

## 命令与别名

- `灵田收取`
- `灵田结算`

## Web API

- `POST /api/v1/mixelixir/harvest`：提交用户、上次收取时间、收取时间、奖励快照和背包上限。
- `POST /api/v1/mixelixir/settle`：提交材料映射、丹药 ID/名称/数量和背包上限。

两者权限均为 `user`，支持 `Idempotency-Key`，使用统一 JSON envelope 和 CSRF。

## 数据模型与迁移

`mixelixir.001` 写入 `mixelixir_feature_migrations`。旧表 `mixelixir_harvest_operations`、`mixelixir_settlement_operations` 继续作为兼容操作记录。

## 事务与失败回滚

应用层先在 `game_db.operation_ledger` 登记；灵田收取由旧仓储使用 `ATTACH DATABASE` 在 `game_db` 与 `player_db` 原子更新药材和收取时间。炼丹结算在 `game_db` 同事务扣除材料、增加丹药和炼丹次数。状态冲突、材料不足、背包满或异常都不会留下半笔资产变化。

## 定时任务

无新增任务。

## 配置项

- `mixelixir_enabled` / `XIUXIAN_MIXELIXIR_ENABLED`：默认启用，可关闭新 application 接入。

## 适配器差异

NoneBot 适配器继续负责随机奖励、配方文本和消息文案；Web 适配器只做 DTO、权限和响应序列化。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.mixelixir.tests.test_mixelixir_application -q`
- `python -m unittest tests.test_mixelixir_harvest_service tests.test_mixelixir_settlement_service tests.test_source_quality -q`

## 灰度开关、回滚和已知限制

关闭 `mixelixir_enabled` 即回到旧命令写入。炼丹升级、配方保存和两阶段补领奖励仍由兼容服务维护，待后续切片迁移。
