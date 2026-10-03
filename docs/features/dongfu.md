# Cave dwelling

## 用户流程
The historical command package remains the transport adapter. Mutating actions enter `DongfuApplication` and its feature-owned repositories.

Named infiltration selects one public target through `DongfuApplication.nearby_target` without loading a nearby-user list. The read-only query uses the actor's `realm/heaven/node_id` in `player_db.map_status` and the earliest profile row per user ID in `game_db.user_xiuxian`. Duplicate names are resolved by the earliest matching map row; the old query did not specify ordering. Self, cave construction, active plots, and daily intrusion limits are still checked after selection, not used to skip to a later namesake. Missing databases or schema return no target without repairs or file creation.

Random infiltration awaits `DongfuApplication.random_target`. A read-only keyset scan holds at most 256 raw metadata rows and one selected public profile; state is read one candidate at a time, not as a page of potentially large plot JSON. Each eligible row enters a size-one reservoir, preserving self exclusion, valid seed plots, daily target limits and earliest-profile selection without adding location or maturity restrictions. The day and seed configuration are frozen once. Missing optional cave columns use legacy normalization defaults; missing required schema or any scan error discards the partial sample. Queries close before every cooperative yield (32 raw rows and page boundaries), including cancellation. No shared candidate cache or request-time DDL is used.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.dongfu.001` remains the original feature marker in `game_db`. `dongfu.002` and `dongfu.003` create the successful- and failed-infiltration operation ledgers. `dongfu.004` creates the planting, harvest, fertilizing, acceleration, patrol, expansion, visit-reward, and array-upgrade operation ledgers in `game_db`; existing receipts are preserved. These requests only validate the startup schema and return `schema_missing` when either database file or the operation schema is absent. They do not create databases or tables at request time.

## 事务与失败回滚
Feature repositories retain per-action operation replay and transactional rollback. Historical service classes are isolated in `compatibility/legacy_dongfu_transactions.py`; `xiuxian/xiuxian_dongfu/transaction_service.py` only re-exports their old names. The default command facade no longer imports or constructs those services. Reverting the code can restore the prior facade wiring; schema rollback is not required for this slice.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`dongfu_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the Dongfu repository/schema tests and the full architecture gate. Verify operation replay after startup migration and verify that requests without `dongfu.004` fail closed without creating tables.

## 灰度开关、回滚和已知限制
Status displays use the feature-owned player projection; named and random infiltration no longer materialize full candidate lists. Random scans use independent short read transactions, not a global snapshot: the initial rowid high-water mark excludes larger inserts, but gap inserts or reused rowids ahead of the cursor may enter; deletion and updates are observed on subsequent reads. Fairness applies to the observed eligible stream (duplicate legacy cave IDs retain their old weighting), and settlement still rechecks current state. Scanning all raw cave metadata bounds work between yields even for sparse eligibility. One transaction/attachment per built candidate is an explicit I/O tradeoff to avoid holding a page of large JSON; missing indexes, single large fields, map nearby full-list reads, and legacy player-data writes remain separate work. The legacy classes remain importable through the shim for external callers. No new migration is required for these reads. Removing `dongfu.002`, `.003`, or `.004`, if ever required, needs restoration from a pre-migration database backup.

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
