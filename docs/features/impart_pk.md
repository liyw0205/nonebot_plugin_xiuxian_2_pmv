# Impart training

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.impart_pk.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

### 回执/投影表的启动迁移（请求路径不执行 DDL）
六条虚神界动作原先都在请求事务内 `CREATE TABLE IF NOT EXISTS`，与仓库级
「请求路径不执行 DDL」合同冲突，且把 DDL 混进资产/统计写入。现在按写入者所在库拆成五条启动迁移：

- `impart_pk.002`（game DB）：`impart_closing_enter_operations`、`impart_closing_operations`、
  `impart_explore_operations`、`impart_training_operations` 四张回执表。
- `impart_pk.003`（player DB）：`impart_pk_state`、`impart_project_members`、
  `impart_project_meta`、`impart_project_join_operations` 四张投影表。
- `impart_pk.004`（impart DB，`_IMPART_DATABASE_MIGRATION_VERSIONS` 白名单内）：
  `impart_battle_batch_operations` —— 战斗批回执的写事务以 impart 库为主连接，不能落到 game 库。
- `impart_pk.005`（player DB，`_PLAYER_DATABASE_MIGRATION_VERSIONS` 白名单内）：
  `statistics` 的 `虚神界闭关时长` / `虚神界闭关修为` / `虚神界闭关祝福时长` 三列。
- `impart_pk.006`（player DB，白名单同上）：`statistics` 的 `虚神界闭关次数` /
  `虚神界投影次数` 两列。

六条请求路径只校验 schema：`operations_ready(uow, table, schema)` 同时检查表存在与本次回读用到的列
（`operation_id`/`payload`/`result_json`；投影表各自的主键列）。缺表**或缺任一读取列**都返回
`schema_missing` 且零资产/零统计变化，不建表、不补列。迁移语句保持 `IF NOT EXISTS`，
已按惰性路径建表的历史库保留全部既有回执行。

`impart_pk.002`/`impart_pk.003`/`impart_pk.004`/`impart_pk.005` 都不在 feature manifest 的
`migration_version` 上（仍为 `legacy.impart_pk.001`），由 `features/_legacy_migrated.py` 的
`MIGRATIONS` 与 `plugin.py` 的库路由共同登记。

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

现在五个统计列全部由 player DB 启动迁移持有，`features/impart_pk/statistics_columns.py`
提供共享契约：

- `CLOSING_SETTLEMENT_FIELDS`（`虚神界闭关时长` / `虚神界闭关修为` / `虚神界闭关祝福时长`）
  → `impart_pk.005`。
- `IMPART_STATISTICS_FIELDS`（`虚神界闭关次数` / `虚神界投影次数`）→ `impart_pk.006`。
- `ensure_statistics_column()`：建 `statistics` 并补列（`INTEGER DEFAULT 0`），仅供启动迁移
  与显式回滚入口使用，不 `DROP`、不改已有列类型。
- `statistics_columns_ready()`：`PRAGMA` 只读校验表与列，空列集视为缺表。
- `increment_statistics_verified()`：请求路径的无 DDL 累加；
  `UPDATE ... COALESCE("<列>",0)+?`，`rowcount==0`（玩家还没有统计行）时 `INSERT` 补行。

`closing_enter_repository.enter`、`project_join_repository.join` 与
`closing_settlement_repository.settle` 都在写事务首步、任何资产/回执行写入之前调用
`statistics_columns_ready`；缺表或缺列返回 `schema_missing`，不建表、不补列。
旧库已有列与历史行保持不变；legacy
`xiuxian_impart_pk/transaction_service.py::_increment_stat` / `_increment_entry_stat` /
`_increment_projection_stat` 仍是显式回滚入口。


## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

### `虚神界信息` 的闭关状态行

玩家进入 `虚神界闭关` 后 `user_cd.type=4`，但信息面板此前只显示修炼时间/效率/次数，
无法确认是否在闭关、已入定多久、祝福余额与本境修为上限余量。现在信息面板追加只读状态行：

- `虚神界闭关中（已入定 N 分钟）`：`N` 由 `elapsed_minutes_from_cd_time` 计算，
  坏/空 `create_time` 记 0，不阻塞面板；
- `今日祝福余额：X 分钟`：取 `xiuxian_impart.exp_day`，与 `虚神界出关` 的
  `available_exp_day` 同源，负值按 0；
- `本境修为上限余量：Y`：`set_closing_type(level) × closing_exp_upper_limit − 当前修为`，
  下限 0；上限配置缺失（`max_exp<=0`）时省略该行，避免误导；
- `发送【虚神界出关】结算本次闭关`：下一步行动提示。

渲染集中在纯函数 `features/impart_pk/closing_status.py::closing_status_lines`（无 I/O、
无 NoneBot），命令适配器只提供 `user_cd`/`exp_day`/当前修为/上限。非闭关状态返回空元组，
因此未闭关玩家的面板逐字不变；不新增表、列、迁移、配置键或命令。

### `虚神界出关` 的祝福消耗与余额回执
`ImpartClosingSettlementResult` 一直携带 `blessing_cost` 与 `exp_day_remaining`，
但出关回执只渲染分钟数与修为。现在 applied 与 duplicate（重放）两条路径都在正文后
追加一行纯渲染说明（`features/impart_pk/closing_status.py::closing_settlement_lines`）：

- 有消耗：`本次消耗虚神界祝福 N 分钟，剩余 M 分钟`（applied 追加 `（本次闭关 X 分钟）`）；
- 无消耗（祝福已耗尽或本次全为普通修炼）：`本次未消耗虚神界祝福，剩余 M 分钟`。

数值来自结算结果的同一字段，渲染层不改扣减、修为、概率或 `result_json`
（仍写 `[exp_gain, blessing_cost, exp_day_remaining]` 三个整数，旧行重放照常渲染）。
由 `tests/test_impart_closing_receipt.py` 钉住。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`impart_pk_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

回执/投影表切片：`python -m unittest tests.test_impart_pk_operations_schema -q` —— 覆盖三条迁移各自的
建表形状与列集、六条动作在未迁移库上返回 `schema_missing` 且零资产/零统计变化、半成品旧表被拒、
历史回执行保留、迁移后同一操作号可成功并重放。

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

旧 `transaction_service` 的六个兼容 service（`ImpartTrainingSettlementService`、
`ImpartExploreSettlementService`、`ImpartBattleBatchService`、`ImpartClosingSettlementService`、
`ImpartClosingEnterService`、`ImpartProjectJoinService`）保留为显式回滚入口，它们仍在自身事务内
建回执/投影表；默认命令路径与 feature 仓储都已改为只校验启动迁移建的 schema。

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
