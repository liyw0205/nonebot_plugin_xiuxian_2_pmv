# Cave dwelling

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.dongfu.001` remains the original feature marker in `game_db`. `dongfu.002` creates `dongfu_infiltrate_success_operations` for the feature-owned successful-infiltration operation ledger. The `潜入洞府` successful settlement path now uses the feature repository directly; the failed-infiltration settlement remains an independent compatibility path.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`dongfu_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Other cave-dwelling algorithms and schemas remain behind compatibility adapters for one complete release cycle; the compatibility hit counter determines when removal is safe. A code rollback can keep the shared successful-infiltration operation ledger. Removing the `dongfu.002` schema, if ever required, needs restoration from the pre-migration database backup.

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
