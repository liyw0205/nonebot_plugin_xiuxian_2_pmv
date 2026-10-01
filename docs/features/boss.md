# 世界BOSS资产结算

## 用户流程

世界BOSS兑换和讨伐结算均通过 application 生成唯一 operation_id。旧 NoneBot 命令通过兼容门面调用新 application，返回字段保持兼容。

## 命令与别名

- `世界BOSS兑换`：积分商店兑换。
- `讨伐世界BOSS`：战斗结果结算。

## Web API

- `POST /api/v1/boss/purchase`，权限 `user`，支持 `Idempotency-Key`。
- `POST /api/v1/boss/settle`，权限 `user`，支持 `Idempotency-Key`。

响应使用统一 `OperationOutcome`。业务拒绝返回 HTTP 409，输入错误返回 HTTP 400。

## 数据模型与迁移

`boss.001` 在 `game_db` 创建 `boss_feature_migrations`，`boss.004` 在 `player_db` 启动时创建世界BOSS生命周期表和限额字段。积分、背包和战斗结算仍兼容既有历史表；手动生成与每日限额重置由 `BossApplication` 的 feature-owned repository 写入，运行期只读校验 schema，缺失时 fail closed，不执行 DDL。

## 事务与失败回滚

资产 application 先写 `operation_ledger`，再执行结算；手动生成使用版本 CAS 与回执幂等，每日限额重置冻结目标并按 chunk 恢复。相同 operation/日期重试只返回首次结果；异常在当前事务回滚并保留可恢复进度。旧 transaction service 仅作为显式兼容/回滚路径。

## 定时任务

世界BOSS刷新和天罚仍由显式兼容生命周期注册，手动生成与每日重置的持久化由 feature-owned repository 承担；未在 feature 导入时重复注册。

## 配置项

`boss_enabled`（`XIUXIAN_BOSS_ENABLED`）控制新资产边界，默认启用。

## 灰度开关、回滚和已知限制

关闭开关即可停止新 manifest/API 并保留旧命令。活动联动奖励和读模型仍由旧适配器提供，完整发布周期后再删除兼容层。

## 适配器差异

application 保持框架无关；命令适配器负责战斗快照解析，Web 适配器负责 DTO、权限和 CSRF。

## 测试与手工验收

覆盖兑换、讨伐结算的成功、拒绝、异常回滚和重放，并在隔离数据目录执行 Web client 验收。
