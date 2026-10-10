# 游戏事件统计投影

## 用户流程

玩法结算将带有稳定 `event_id` 的事件投递到 outbox。兼容投影调用
`GameEventApplication.record_statistics`，由 `GameEventStatisticsRepository` 在 player DB
写入统计增量及事件回执；同一事件重放返回 `replayed`，不会重复累计。

## 命令与别名

统计投影是内部效果，没有 NoneBot 命令或别名。原有玩法命令仍由各自 feature 声明。

## Web API

无直接 Web API。投影由 outbox dispatcher 驱动；后台管理通过所属玩法查看统计，不直接写入本功能表。

## 数据模型与迁移

`game_events.001` 只路由到 `player_db`，由 `apply_game_event_statistics_player` 创建
`statistics` 的统计列和 `game_event_statistics_events(event_id,event_key)` 回执表。
请求路径只检查既有 schema，缺失时返回 `schema_missing`，不执行动态 DDL。

## 事务与失败回滚

每次 `record_statistics` 在一个 player DB Unit of Work 内写回执和统计值。事件 ID、用户 ID 或增量与已有回执冲突时抛出 `OperationConflictError`；异常回滚本次增量，outbox 保留 pending 供后续重试。

## 定时任务

无独立 job，`jobs.JOBS` 为空。重试由共享 outbox dispatcher 的既有调度策略负责。

## 配置项

无独立配置项。统计字段由迁移固定声明，新增字段必须随版本化迁移和回放测试提交。

## 适配器差异

本功能只依赖 `DatabaseUnitOfWork` 和仓储接口，不导入 NoneBot、Flask 或网络客户端。不同平台事件由兼容层在进入 outbox 前归一化。

## 测试与手工验收

`features/game_events/tests/test_game_event_projections.py` 覆盖迁移数据库归属、统计回执幂等、投影失败重试和部分投影重放。手工验收应使用隔离 player DB，并检查回执与统计值各只增加一次。
