# Training

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.training.001` records the historical slice in `game_db`. The event settlement boundary now uses `training.001` on `game_db` for the replay projection and `training.002` on `player_db` for the training/statistics schema. `TrainingEventSqlRepository` performs the state CAS, resources, inventory, statistics and replay write in one attached SQLite transaction; the request path does not create or alter tables. Existing two-column `training_event_operations` rows remain readable. Training shop exchange and administrator reset remain explicit legacy compatibility actions.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Event settlement additionally records the legacy-compatible replay payload and result in `training_event_operations`; duplicate payloads replay and conflicting payloads reject. A late SQL error rolls back both attached databases. This is process-level rollback: SQLite WAL crash recovery cannot promise a cross-file atomic commit at every power-loss point, so startup backup/recovery/reconcile remains required. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`training_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application and event repository tests plus the full architecture gate. Verify applied, duplicate, conflict, CAS rejection, inventory/resource rejection, late-failure rollback, legacy-row replay and game/player migration routing. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy event settlement remains available only as an explicit repository fallback when no player database is supplied. Shop/reset algorithms and their schemas remain behind compatibility adapters for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 历练兑换`
- `command: 历练商店`
- `command: 历练帮助`
- `command: 历练排行榜`
- `command: 历练状态`
- `command: 历练积分排行榜`
- `command: 开始历练`
- `alias: 历练开始`
