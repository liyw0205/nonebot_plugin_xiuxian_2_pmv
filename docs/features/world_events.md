# 世界事件魔修奖励

## 用户流程

魔修入侵结束或当前境界魔修被击退后，用户按本期贡献领取一次奖励。旧命令继续保留，奖励计算仍在兼容命令适配器完成，资产写入统一由 `DemonClaimApplication` 调度。

## 命令与别名

- `领取魔修奖励`

## Web API

`POST /api/v1/world-events/demon/claim`，权限为 `user`，支持 `Idempotency-Key` 或请求体 `operation_id`。请求体包含 `event_key`、`event_id`、`user_id`、`expected_claimed`、`stone`、`exp`、`items` 和 `max_goods_num`。响应使用统一 API envelope，成功时返回奖励摘要；状态冲突、重复领取、背包满和用户不存在返回可重试/拒绝状态。

## 数据模型与迁移

`world_events.001` 写入 `world_events_feature_migrations`；`world_events.002` 只在 `player_db` 建立/扩展事件状态、讨伐结算和统计 schema；`world_events.003` 只在 `game_db` 建立或扩展 `demon_claim_operations`，迁移旧表时保留记录并为缺失的 `stone`、`exp` 列补默认值。领奖 SQL repository 不在请求中执行 DDL。

## 事务与失败回滚

应用层先记录 `operation_ledger`，feature-owned SQL repository 随后使用 `ATTACH DATABASE` 与 `BEGIN IMMEDIATE` 在 `game_db`、`player_db` 中原子更新领奖标记、灵石、修为、物品和领奖 operation。业务拒绝不改变资产，异常由 repository 事务回滚并由应用层记录 failed ledger。超出 SQLite 64 位整数范围的修为/灵石以安全文本参数绑定，并在 SQL 中按历史规则进行数值累加。

## 定时任务

本切片不新增任务；魔修生命周期、波次刷新仍由兼容调度管理。

## 配置项

- `world_events_enabled` / `XIUXIAN_WORLD_EVENTS_ENABLED`：默认启用，可关闭新组合根接入。

## 适配器差异

命令适配器保留旧奖励计算和用户提示，application 不发送消息；Web blueprint 只负责 DTO、权限、CSRF 和统一响应。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.world_events.tests.test_world_events_application -q`
- `python -m unittest tests.test_demon_claim_service tests.test_source_quality -q`
- 使用隔离数据目录重复提交相同操作号，仓储调用次数应保持为一次。

## 灰度开关、回滚和已知限制

关闭 `world_events_enabled` 可切回旧入口。旧 `DemonClaimService` 与已退出默认路径的 `DemonAttackSettlementService` 分别保留在 `compatibility/legacy_demon_claim.py`、`compatibility/legacy_demon_attack_settlement.py`，并由原 transaction module 身份一致地 re-export；奖励随机池和贡献计算暂未迁移到新 domain，仍由兼容命令提供。event lifecycle、wave refresh、spirit vein 仍有兼容事务路径。live migration/recovery 与完整发布周期证据仍未完成，不能据此关闭整个 world-events 切片。
