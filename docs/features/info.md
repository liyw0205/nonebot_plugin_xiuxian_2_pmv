# Player information

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.info.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source. Shared player profile reads, including ID/道号 lookups used by base commands and identity fields in the info projection, now use `PlayerProfileApplication -> PlayerProfileSqlRepository` in read-only mode; missing database/schema is fail-closed and never creates tables during a request.
Dynamic attribute reads used by the info projection, status display, and default player-fight helper now pass through `PlayerAttributeApplication`. The legacy `get_final_attributes` formula remains an explicit compatibility provider so buff/impart/accessory/tianti projections can be migrated one at a time; the application preserves `ratio`, `include_current`, and provider injection ports.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`info_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application/profile/attribute-read tests and the full architecture gate. Repeat the same operation ID to verify replay; verify a missing profile database remains absent after a read and that the attribute provider receives its ratio/current flags without opening a writer.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 我的ID`
- `alias: myid`
- `command: 我的修仙信息`
- `alias: 修仙信息`
- `alias: 存档`
- `alias: 我的存档`
- `command: 我的修仙信息图片版`
- `alias: 修仙信息图片版`
- `alias: 存档图片版`
- `alias: 我的存档图片版`
- `command: 更新日志`
- `alias: 更新记录`
- `command: 身外化身`

## 活动时间边界
普通入口对 `user_cd.last_check_info_time` 的更新时间和读取统一通过
`PlayerActivityApplication -> PlayerActivitySqlRepository`。仓储只操作已存在的
`user_cd` 行，缺少数据库、表、列或用户时 fail-closed，不在请求路径创建数据库、表或
执行 DDL；时间仍保存为旧 reader 兼容的本地无时区字符串。运行时通过 `Clock` 注入，
测试不会依赖系统时间，也不会保留额外的进程级缓存。

宗门闲置判定继续由 `SectActivitySqlRepository` 负责，Boss 旧结算事务中的活动时间写入
继续和战斗 CAS 共用同一事务；这两处不是本切片的普通活动边界，待各自结算 owner 明确后再迁移。
