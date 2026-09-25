# Impart cards

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.impart.001` records the feature boundary in `game_db`. The application ledger stays in `game_db`, while `ImpartRepository` is wired to the actual `xiuxian_impart.db`. `impart.002` creates the prayer replay table in `game_db`, while `impart.003` prepares the three prayer statistics columns in `player_db`. The `20005` use handler keeps random card selection at the adapter boundary; `ImpartPrayerSqlRepository` atomically consumes the item, updates cards and bonuses, increments those statistics, and records replay state across the attached game/impart/player databases. Legacy tables remain the compatibility data source.

## 事务与失败回滚
Prayer requests carry an `operation_id`; item consumption, card changes, player statistics, and replay data commit or roll back together. Replays return the first result without incrementing statistics again. The request path does not create or alter schema. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`impart_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 传承信息`
- `command: 传承分解`
- `command: 传承卡图`
- `alias: 传承卡片`
- `command: 传承合成`
- `command: 传承帮助`
- `command: 传承抽卡`
- `command: 传承祈愿`
- `command: 传承背包`
- `command: 加载传承数据`
- `command: 虚神界帮助`
