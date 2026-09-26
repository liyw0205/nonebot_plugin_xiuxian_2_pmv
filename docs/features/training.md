# Training

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.training.001` records the historical slice in `game_db`. Event settlement uses `training.001` on `game_db` for the replay projection and `training.002` on `player_db` for the training/statistics schema. Shop exchange adds game-only `training.003` for its replay ledger. `TrainingEventSqlRepository` and `TrainingPurchaseSqlRepository` perform their state CAS, resources, inventory, statistics and replay writes in attached SQLite transactions; the request paths do not create or alter tables. Existing two-column event rows and legacy purchase payloads remain readable. Administrator reset remains an explicit legacy compatibility action.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Event settlement records the legacy-compatible replay payload and result in `training_event_operations`; shop exchange records quantity/cost/points/inventory in `training_purchase_operations`. Duplicate payloads replay and conflicting payloads reject; limit, points and capacity failures leave both databases unchanged. A late SQL error rolls back both attached databases. This is process-level rollback: SQLite WAL crash recovery cannot promise a cross-file atomic commit at every power-loss point, so startup backup/recovery/reconcile remains required. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`training_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application, event repository and purchase repository tests plus the full architecture gate. Verify applied, duplicate, conflict, CAS rejection, inventory/resource rejection, late-failure rollback, legacy-row replay and game/player migration routing. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy event settlement and shop exchange remain available only as explicit repository fallbacks when no player database is supplied. Administrator reset remains behind a compatibility adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 历练兑换`
- `command: 历练商店`
- `command: 历练帮助`
- `command: 历练排行榜`
- `command: 历练状态`
- `command: 历练积分排行榜`
- `command: 开始历练`
- `alias: 历练开始`
