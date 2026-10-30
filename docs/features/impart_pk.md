# Impart training

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.impart_pk.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

### `player_data.statistics` 统计列契约
`虚神界闭关` 与 `投影虚神界` 各累计一个统计列，落在共享的 `player_data.statistics` 上：

- `虚神界闭关次数`：每次成功进入闭关 +1（`closing_enter_repository`）。
- `虚神界投影次数`：每次成功投影加入 +1（`project_join_repository`）。

`statistics` 由 `features/base`、`training`、`auction`、`boss` 等迁移以
`CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)` 先建好，因此**任何**
`CREATE TABLE IF NOT EXISTS statistics(user_id ..., "<列>" ...)` 都是 no-op，内联的业务列
永远不会被创建。这两个仓储早期就是这样写的，导致真实库上分别抛
`no such column: 虚神界闭关次数` 与 `table statistics has no column named 虚神界投影次数`，
而两个 handler 没有 try/except —— 玩家收不到任何回复。

现在两处统一走 `features/impart_pk/statistics_columns.py`：

- `ensure_statistics_column()`：`PRAGMA table_info(statistics)` 取真实列，缺列才
  `ALTER TABLE ... ADD COLUMN "<列>" INTEGER DEFAULT 0`；不 `DROP`、不改已有列类型。
- `increment_statistics()`：`UPDATE ... COALESCE("<列>",0)+? WHERE user_id=?`，
  `rowcount==0`（玩家还没有统计行）时 `INSERT` 补行，返回累加后的真实值。

两列的类型与默认值、以及"缺列补列 + 缺行补行"的行为，与
`closing_settlement_repository`（虚神界出关）、`past_life/start_repository`（前尘往事次数）
及旧 `_increment_entry_stat` 完全一致。补齐在调用方已有的写事务内完成，不新增迁移版本、
不改已应用迁移函数；旧库若已有该列则跳过 `ALTER` 直接读写，干净库首次写入时自动补列补行。


## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`impart_pk_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 投影虚神界`
- `command: 探索虚神界`
- `alias: 虚神界探索`
- `command: 虚神界信息`
- `command: 虚神界修炼`
- `command: 虚神界出关`
- `command: 虚神界列表`
- `command: 虚神界对决`
- `command: 虚神界排行榜`
- `command: 虚神界闭关`
