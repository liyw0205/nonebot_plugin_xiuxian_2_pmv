# 通天塔资产结算

## 用户流程

通天塔的战斗算法、随机 Boss 和奖励计算继续由兼容命令适配器负责；积分兑换、单层挑战和连续挑战的最终资产变更统一经过 `TowerApplication`。

## Web API

- `POST /api/v1/tower/purchase`：积分兑换物品，支持每周限购和背包容量校验。
- `POST /api/v1/tower/settle`：提交塔状态、战斗结果、体力和奖励快照。

两者均要求 `user` 权限、CSRF 和 `Idempotency-Key`，返回统一 JSON envelope。

## 数据与事务

`tower.001` 写入 `tower_feature_migrations`。旧跨库事务服务作为惰性仓储适配器；统一 ledger 写入 `game_db`，实际塔状态仍由 `player_db` 的兼容仓储维护。

## 灰度与回滚

`tower_enabled` / `XIUXIAN_TOWER_ENABLED` 可关闭新 application 接入并恢复旧入口。旧重置任务和读模型暂留兼容层，待完整发布周期后删除。

## 验证

- `python -m unittest nonebot_plugin_xiuxian_2.features.tower.tests.test_tower_application -q`
- `python -m unittest tests.test_tower_purchase_service tests.test_tower_settlement_service -q`

## 命令与别名

保留通天塔兑换、单层挑战和连续挑战命令及旧别名。

## 数据模型与迁移

`tower.001` 写入 `game_db.tower_feature_migrations`，旧塔状态仍由兼容仓储维护。

## 事务与失败回滚

统一 ledger 记录请求和结果；体力、积分、背包容量拒绝均不产生资产变更。

## 定时任务

每周限购重置由兼容调度器执行，使用稳定 ID 和业务周快照。

## 配置项

`tower_enabled` / `XIUXIAN_TOWER_ENABLED` 控制新边界，默认开启。

## 适配器差异

战斗算法和奖励计划由兼容命令适配器提供，application 只协调事务和审计。

## 测试与手工验收

覆盖兑换、单层/连续结算、容量拒绝、失败回滚和幂等重放；执行 Flask client 与恢复冒烟。

## 灰度开关、回滚和已知限制

关闭开关即恢复旧入口；战斗算法、Boss 随机和读模型仍待迁移。

## Manifest 清单
- `command: 爬塔`
- `alias: 挑战通天塔`
