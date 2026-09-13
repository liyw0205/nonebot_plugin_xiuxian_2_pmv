# 悬赏令接取与结算

## 用户流程

用户先刷新悬赏令，再执行接取编号。接取只改变当前悬赏状态，不消耗刷新次数；完成后结算会按固定的工作快照发放修为和额外物品。刷新次数、悬赏快照、用户冷却状态和结算奖励在各自事务中校验。

## 命令与别名

- `悬赏令接取`（旧入口使用 `悬赏令接取 1`）
- `悬赏令结算`
- `悬赏令`

## Web API

`POST /api/v1/work/claim`，权限为 `user`，请求体包含 `operation_id`、`user_id`、`expected_count`、`expected_offer`、`task_index` 和 `started_at`。支持 `Idempotency-Key`；成功响应返回任务名、开始时间和剩余刷新次数。

`POST /api/v1/work/settle`，权限为 `user`，请求体包含工作快照、修为增量、可选物品、等级上限和背包上限。支持 `Idempotency-Key`；成功响应返回实际修为、奖励类型和任务名。

## 数据模型与迁移

`work.001` 写入功能迁移标记。兼容仓储继续维护 `work_claim_operations` 与 `work_active_snapshots`，历史 JSON 仅作为读取投影。

## 事务与失败回滚

应用层在 `game_db.operation_ledger` 记录请求，旧 `WorkClaimService` 和 `WorkSettlementService` 使用 `BEGIN IMMEDIATE` 原子更新 `user_cd`、活动快照、经验、背包和操作表。状态冲突、编号无效、背包满或用户缺失不修改资产，异常完整回滚。

## 定时任务

刷新、结算、过期清理和每日重置仍由兼容调度管理，本切片不新增任务。

## 配置项

- `work_claim_enabled` / `XIUXIAN_WORK_CLAIM_ENABLED`：默认启用，可关闭新应用接入。

## 适配器差异

命令适配器负责解析旧的正则命令和 JSON 投影；Web blueprint 负责 DTO、权限、CSRF 和统一响应；application 不发送消息。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.work.tests.test_work_application -q`
- `python -m unittest tests.test_work_claim_service tests.test_work_settlement_service tests.test_source_quality -q`
- 重复提交相同操作号不应重复改变悬赏状态。

## 灰度开关、回滚和已知限制

关闭 `work_claim_enabled` 可恢复旧命令实现。悬赏刷新和终止仍是兼容动作，待后续垂直切片迁移；删除旧服务前需满足完整发布周期和恢复演练要求。
