# 活动奖励领取

## 用户流程

用户通过 `活动领取` 一次性领取当前可领取的任务、战令和首领奖励。旧活动服务继续负责具体奖励规则，新 application 负责统一 operation ledger、审计和 Web 合约。

## 命令与别名

`活动领取`、`活动一键领取`、`领取活动奖励`。兼容命令在旧活动包中保留一个发布周期。

## Web API

`POST /api/v1/activity/rewards/claim`，权限 `user`，需要 CSRF 和 `Idempotency-Key`；请求字段为 `user_id`。

## 数据模型与迁移

`activity_reward_feature_migrations` 记录 `activity_reward.001`；统一 `operation_ledger` 和 `operation_audit` 位于 `game_db`。活动专用数据库仍由兼容仓储维护。

## 事务与失败回滚

application 对外操作号幂等；旧活动协调器对每个子奖励步骤持久化进度，失败返回可重试状态，不重复已完成步骤。

## 定时任务

无新增任务。

## 配置项

无新增配置；活动开放状态由旧活动配置投影提供。

## 适配器差异

NoneBot/Flask 只负责上下文解析和响应，奖励规则留在 repository adapter 之后。

## 测试与手工验收

覆盖成功、拒绝、重复操作、参数校验和 Web CSRF/权限边界。

## 灰度开关、回滚和已知限制

旧命令可继续工作；关闭新 API 不会删除活动数据库。具体活动奖励 SQL 完整迁移后再移除兼容仓储。

## Manifest 清单
- `alias: 活动领奖`
