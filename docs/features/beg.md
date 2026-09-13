# Novice gifts

## 用户流程
The historical commands remain available while the new application boundary owns both claim transactions.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `beg.001` creates the operation projections in `game_db`; the historical marker is retained for upgraded installations.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`beg_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
The old transaction-service import path remains a thin facade for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 仙途奇缘`
- `command: 仙途奇缘帮助`
- `command: 新手礼包`
