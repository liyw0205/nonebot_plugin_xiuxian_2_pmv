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

`洞府收获 [灵田编号]` 先持久化随机奖励方案，再在同一双库事务内保存清田状态、背包数量/绑定数、永久收获回执和效果 outbox。
每步核影响行数与实际投影，事务末再次核验；已知拒写/改写保留具体原因和对象并整笔回滚，冻结方案保留供恢复重试。
成熟判定按解析后的业务时间比较，兼容历史微秒格式。原消息重放先读永久回执，支持旧两字段回执，不受当前灵田已清空或未成熟影响。
业务提交后的统计/任务失败显示补偿中，并保持 outbox pending；同消息恢复只补效果，不重新随机或发物。
`game_events.002` 仅向 player DB 增补“洞府收获”统计列，保留已发布 `.001` 与既有统计。
洞府自身的产出池、施肥、阵法和地脉收益规则不变；传承的灵田加成仍由原有灵田/傀儡消费路径使用，不新增洞府加成或种子奖励。

`洞府施肥 <灵田编号>` 每次消耗一份五色灵壤，优先消耗绑定材料，肥力上限仍为3。
同一双库事务逐步核验库存数量/绑定数、完整洞府状态和永久回执，并在最后写入后复核前序状态；
已知拒写或投影不一致分别保留库存、灵田或回执的具体原因与对象，整笔回滚后可重试原消息。
原消息重放先核永久回执，不受当前满肥或已清田影响，同消息换田仍拒绝；没有新增独立 pending 表。
发送失败可能发生在业务提交之后，恢复消息投递不得再次扣材料。作物展示将 aware 业务时钟换算到存量 UTC 文本口径，
不改时间持久化格式或成熟、产出规则。

`洞府催熟 <灵田编号>` 每次仍消耗一份灵息露，基础缩短60分钟，水域额外20分钟，最多提前到当前时刻。
业务时钟统一为存量 UTC 文本口径，成熟判断按解析后的时间比较，保留已有微秒精度，避免时区混用和同秒误判。
同一双库事务核验库存数量/绑定数、完整洞府状态（含旧首田字段）与永久回执，并在末步复核前序写入；
绑定材料优先消耗，保留旧无绑定列 schema 兼容。库存、田地与回执的拒写或改写各自保留具体原因/对象，整笔回滚后原消息可恢复。
成功回执优先于成熟/清田前置，同消息换田仍冲突；提交后发送失败可重放，不重复扣物或缩短时间，没有新增独立 pending 表。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`dongfu_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the Dongfu repository/schema tests and the full architecture gate. Verify operation replay after startup migration and verify that requests without `dongfu.004` fail closed without creating tables.

## 个人洞府收获记录

`我的洞府收获记录`（别名 `洞府收获记录`、`灵田收获记录`）补齐玩家侧逐次收获的可见性：
`我的洞府` 只展示当前灵田与作物，看不到最近收获了哪些物品。

- 数据源是真实结算回执 `dongfu_harvest_operations`（game DB）：`payload` 为
  `[user_id, expected_slots, slot_numbers, reward_rows, failed_slots]`，
  `reward_rows` 保存 `[物品ID, 名称, 类型, 数量]`，`created_at` 可排序；
  不新增表、列、迁移、索引或写入路径。
- `DongfuHarvestHistoryApplication` 在 `read_only=True` 的 `DatabaseUnitOfWork` 中按
  `json_extract(payload, '$[0]') = 当前 user_id` 查询近 7 天（只用下界）成功收获行，
  按 `created_at DESC, rowid DESC` 排序；缺库、缺表、缺列、坏 JSON 或读失败返回空态，
  不建表、不补列。
- 输出：收获次数、总件数、物品种类数，以及最近 10 次明细（时间、最多 4 种物品及数量、
  未成熟灵田数）；空态显示“近7天没有洞府收获记录。”。
- 帮助与发现性：`洞府帮助` 增补条目，`修仙帮助`「流水与总览」登记；manifest 通过
  `commands_for("dongfu")` 自动纳入新命令，不需要新增路由或配置。
- 只读展示；不改种植、收获、施肥、催熟、扩建、巡山、潜入或任何资产写路径。

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
- `command: 我的洞府收获记录`
- `alias: 洞府收获记录`
- `alias: 灵田收获记录`
