# Cave dwelling

## 用户流程
The historical command package remains the transport adapter. Mutating actions enter `DongfuApplication` and its feature-owned repositories.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.dongfu.001` remains the original feature marker in `game_db`. `dongfu.002` and `dongfu.003` create the successful- and failed-infiltration operation ledgers. Planting, harvest, fertilizing, acceleration, patrol, expansion, visit rewards, array upgrades, and both infiltration outcomes use feature repositories. No migration was added in the transaction compatibility-isolation slice.

## 事务与失败回滚
Feature repositories retain per-action operation replay and transactional rollback. Historical service classes are isolated in `compatibility/legacy_dongfu_transactions.py`; `xiuxian/xiuxian_dongfu/transaction_service.py` only re-exports their old names. The default command facade no longer imports or constructs those services. Reverting the code can restore the prior facade wiring; schema rollback is not required for this slice.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`dongfu_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Read-only cave-dwelling displays and legacy player-data projections still use compatibility managers and JSON state; they are separate migration work and are not covered by the transaction-service isolation. The legacy classes remain importable through the shim for external callers. Removing `dongfu.002` or `dongfu.003`, if ever required, needs restoration from the pre-migration database backup.

## Manifest 清单
- `command: 我的洞府`
- `command: 拜访道友`
- `command: 洞府催熟`
- `command: 洞府地脉`
- `alias: 地脉查看`
- `command: 洞府巡山`
- `alias: 巡山护府`
- `command: 洞府布阵`
- `command: 洞府帮助`
- `command: 洞府扩建`
- `command: 洞府收获`
- `alias: 收获洞府`
- `command: 洞府施肥`
- `command: 洞府种植`
- `command: 潜入洞府`
- `alias: 随机潜入`
- `alias: 随机潜入洞府`
