# 世界事件魔修奖励

## 用户流程

魔修入侵结束或当前境界魔修被击退后，用户按本期贡献领取一次奖励。旧命令继续保留，奖励计算仍在兼容命令适配器完成，资产写入统一由 `DemonClaimApplication` 调度。

## 命令与别名

- `领取魔修奖励`

## Web API

`POST /api/v1/world-events/demon/claim`，权限为 `user`，支持 `Idempotency-Key` 或请求体 `operation_id`。请求体包含 `event_key`、`event_id`、`user_id`、`expected_claimed`、`stone`、`exp`、`items` 和 `max_goods_num`。响应使用统一 API envelope，成功时返回奖励摘要；状态冲突、重复领取、背包满和用户不存在返回可重试/拒绝状态。

## 数据模型与迁移

`world_events.001` 写入 `world_events_feature_migrations`。应用层的 operation ledger 位于 `game_db`；旧跨库服务仍维护 `demon_claim_operations`，以兼容历史请求。

## 事务与失败回滚

应用层先记录 `operation_ledger`，兼容仓储随后使用 `ATTACH DATABASE` 与 `BEGIN IMMEDIATE` 在 `game_db`、`player_db` 中原子更新领奖标记、灵石、修为和物品。业务拒绝不改变资产，异常由旧事务回滚并由应用层记录 failed ledger。

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

关闭 `world_events_enabled` 可切回旧入口。奖励随机池和贡献计算暂未迁移到新 domain，仍由兼容命令提供；满足完整发布周期、历史迁移和运行命中证据后再删除旧服务。
