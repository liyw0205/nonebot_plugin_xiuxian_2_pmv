# Training

## 用户流程
The compatibility command remains available while the new application boundary is enabled. The administrator command runs `TrainingApplication.reset_limits` in the existing background chunk worker; the legacy reset service is only a fallback when no player database is supplied.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.training.001` records the historical slice in `game_db`. Event settlement uses `training.001` on `game_db` for the replay projection and `training.002` on `player_db` for the training/statistics schema. Shop exchange adds game-only `training.003` for its replay ledger. Administrator reset adds game-only `training.004` for the frozen target and progress tables. `TrainingEventSqlRepository`, `TrainingPurchaseSqlRepository` and `TrainingResetSqlRepository` perform their state CAS, resources, inventory, statistics, replay and resumable reset writes in attached SQLite transactions; the request paths do not create or alter tables. Existing two-column event rows, legacy purchase payloads and older reset operation columns remain readable after startup migration.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Event settlement records the legacy-compatible replay payload and result in `training_event_operations`; shop exchange records quantity/cost/points/inventory in `training_purchase_operations`. Administrator reset freezes the full user set on its first chunk, then resumes pending targets with the same operation id; completed requests replay and operator conflicts reject. Duplicate payloads replay and conflicting payloads reject; limit, points and capacity failures leave both databases unchanged. A late SQL error rolls back both attached databases, including a failed reset chunk. This is process-level rollback: SQLite WAL crash recovery cannot promise a cross-file atomic commit at every power-loss point, so startup backup/recovery/reconcile remains required. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`training_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application, event, purchase and reset repository tests plus the full architecture gate. Verify applied, duplicate, conflict, frozen-target chunk resume, skipped users, CAS rejection, inventory/resource rejection, late-failure rollback, legacy-row replay and game/player migration routing. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy event settlement, shop exchange and administrator reset remain available only as explicit repository fallbacks when no player database is supplied. The normal composition root uses the feature repositories; the compatibility hit counter determines when the fallback can be removed.

## Manifest 清单
- `command: 历练兑换`
- `command: 历练商店`
- `command: 历练帮助`
- `command: 历练排行榜`
- `command: 历练状态`
- `command: 历练积分排行榜`
- `command: 开始历练`
- `alias: 历练开始`
