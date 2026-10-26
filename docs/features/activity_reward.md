# 活动奖励领取

## 用户流程

用户通过 `活动领取` 一次性领取当前可领取的任务、战令、首领进度、首领排行、阶段和排行奖励。旧活动服务继续负责具体奖励规则，新 application 负责统一 operation ledger、审计和 Web 合约。

固定计划由 `features/activity_reward/claim_all_repository.py` 的 `STEP_LABELS` 声明，共六步：任务、战令、首领进度、首领排行、阶段、排行；旧兼容侧 `xiuxian/xiuxian_activity/transaction_service.py` 的 `ActivityClaimAllService.STEP_LABELS` 必须与该顺序完全一致，否则 `run()` 直接拒绝执行。阶段与排行奖励由活动 feature 的领取应用结算（`ActivityStageClaimApplication` / `ActivityRankClaimApplication`，同一 `game_db`），兼容层只做运行器接线。计划变更会让在飞行中的旧计划得到 `operation_conflict`（领取计划冲突，请重新发送），不自动补偿。

## 命令与别名

`活动领取`、`活动一键领取`、`领取活动奖励`。兼容命令在旧活动包中保留一个发布周期。

## Web API

`POST /api/v1/activity/rewards/claim`，权限 `admin`，需要 CSRF 和 `Idempotency-Key`；请求字段为 `user_id`。该端点只供管理员面板代玩家领取（`adapters/web/static/pages/admin.js` 是唯一调用方）；玩家本人通过 bot 命令 `活动领取` 走同一业务协调器。

## 数据模型与迁移

`activity_reward_feature_migrations` 记录 `activity_reward.001`；统一 `operation_ledger` 和 `operation_audit` 位于 `game_db`。活动专用数据库仍由兼容仓储维护。

## 事务与失败回滚

application 对外操作号幂等；旧活动协调器对每个子奖励步骤持久化进度，失败返回可重试状态，不重复已完成步骤。六个步骤逐步隔离：任一步抛异常即返回 `retryable_failure` 并保留已完成步骤，重试只补跑未完成步骤；全部步骤落空时聚合为暂无可领取奖励并逐项列出原因。阶段/排行步骤在无可领时返回 `(False, 原因)`（如阶段奖励已领取、没有可领取的排行奖励），重复发放由 `activity_stage_claim` / `activity_rank_claim` 主键兜底。每一步还受当前活动阶段 `features` 约束：阶段不含 `claim` 时全部六步一致拒绝（`当前阶段【…】不开放活动领奖`）。

`retryable_failure` 的玩家文案会追加已落发放步骤：

```
活动奖励领取未完成，请重试
首领排行：<原因>
已发放：任务、战令、首领进度（重试不会重复发放）
```

无步骤落发放时该行为 `暂无已发放步骤，重试从第一步开始`，因此“重试不会重复发放”对玩家可见而不只停留在持久化行里。文案只列步骤标签，不回显奖励明细——明细在重试结算后按已落账数据重新给出。旧兼容协调器（`ActivityClaimAllService`）与新仓储（`ActivityClaimAllRepository`）共用同一文案规则。

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
