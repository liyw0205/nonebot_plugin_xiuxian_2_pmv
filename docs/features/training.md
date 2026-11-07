# Training

## 用户流程
The compatibility command remains available while the new application boundary is enabled. The administrator command runs `TrainingApplication.reset_limits` in the existing background chunk worker; the legacy reset service is only a fallback when no player database is supplied.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.training.001` records the historical slice in `game_db`. Event settlement uses `training.001` on `game_db` for the replay projection and `training.002` on `player_db` for the training/statistics schema. Shop exchange adds game-only `training.003` for its replay ledger. Administrator reset adds game-only `training.004` for the frozen target and progress tables. `training.005` on `game_db` persists a resolved event plan before settlement; `training.006` on `player_db` adds expression indexes for bounded numeric leaderboards. `TrainingEventSqlRepository`, `TrainingPurchaseSqlRepository` and `TrainingResetSqlRepository` perform their state CAS, resources, inventory, statistics, replay and resumable reset writes in attached SQLite transactions; request paths do not create or alter tables. Existing two-column event rows, legacy purchase payloads and older reset operation columns remain readable after startup migration.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Event resolution is frozen before settlement and removed in the same attached transaction that records the replay result; a failed settlement leaves the plan available for retry without rerolling rewards. Event settlement records the legacy-compatible replay payload and result in `training_event_operations`; shop exchange records quantity/cost/points/inventory in `training_purchase_operations`. Administrator reset freezes the full user set on its first chunk, then resumes pending targets with the same operation id; completed requests replay and operator conflicts reject. Duplicate payloads replay and conflicting payloads reject; limit, points and capacity failures leave both databases unchanged. A late SQL error rolls back both attached databases, including a failed reset chunk. This is process-level rollback: SQLite WAL crash recovery cannot promise a cross-file atomic commit at every power-loss point, so startup backup/recovery/reconcile remains required. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`training_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application, event, purchase and reset repository tests plus the full architecture gate. Verify applied, duplicate, conflict, frozen-target chunk resume, skipped users, CAS rejection, inventory/resource rejection, late-failure rollback, legacy-row replay and game/player migration routing. Repeat the same operation ID to verify replay.

## 个人历练记录

`我的历练记录`（别名 `历练记录`）补齐玩家侧逐次历练事件的可见性：`历练状态` 只有
当前进度与上一次事件，看不到近几天的资源变化与掉落。

- 数据源是真实结算回执 `training_event_operations`：`payload` 为
  `[user_id, expected_state, state, expected_user, stone_delta, exp_delta, hp_delta,
  rewards, max_goods_num]`，`result_json` 保存 `status` 与事件文本 `message`；
  不新增表、列、迁移、索引或写入路径。
- `TrainingEventHistoryApplication` 在 `read_only=True` 的 `DatabaseUnitOfWork` 中按
  `json_valid(payload)` + `json_extract(payload,'$[0]') = 当前 user_id` 查询近 7 天
  （只用下界）`status='applied'` 的行，按 `created_at DESC, rowid DESC` 排序；
  缺库、缺表、缺列、坏 JSON 或读失败返回空态，不建表、不补列。
- 输出：事件次数、累计灵石/修为/气血变化、奖励总件数，以及最近 10 次明细
  （时间、折叠后的事件文本、三项变化与奖励摘要）；空态显示“近7天没有历练记录。”。
- 帮助与发现性：`历练帮助` 增补条目，`修仙帮助`「流水与总览」登记；manifest 通过
  `commands_for("training")` 自动纳入新命令。
- 只读展示；不改历练事件抽取、资源结算、背包、商店或任何资产写路径。

## 灰度开关、回滚和已知限制
Legacy event settlement, shop exchange and administrator reset remain available only as explicit repository fallbacks when no player database is supplied. The normal composition root uses the feature repositories; the compatibility hit counter determines when the fallback can be removed.

The default event path now persists a resolved plan before settlement and uses a read-only game-database context repository; event resolution no longer calls legacy player/inventory managers. Leaderboards now use SQL top-50 ordering and a short in-process cache instead of a full-roster Python sort plus per-user lookups. The legacy command and help transport adapters remain during compatibility; the static help handler is intentionally retained as an allowed compatibility path. Cross-file power-loss recovery and production latency measurements remain outside this slice.

## 排行榜本人名次
`历练排行榜`（`completed`）与 `历练积分排行榜`（`points`）的前 50 名输出不变，
列表后追加本人名次行，取数走 `TrainingApplication.rank_of`（同一仓储、只读 UoW，
缺库缺表或缺字段返回空 `PersonalRank`，不建库、不补列）。名次与空态口径见
`docs/features/base.md` 的「排行榜本人名次（共享合同）」。

## 列表检索
`历练商店 [关键词|页码]`：商店列表按物品名/ID/描述筛选，翻页提示与按钮携带关键词
（`历练商店 灵草 2`），解析与续查走 `docs/features/base.md` 的「列表检索与翻页续查
（共享合同）」；筛选沿用既有编号排序，不改成就点扣减、周限购或兑换回执。

## Manifest 清单
- `command: 我的历练记录`
- `alias: 历练记录`
- `command: 历练兑换`
- `command: 历练商店`
- `command: 历练帮助`
- `command: 历练排行榜`
- `command: 历练状态`
- `command: 历练积分排行榜`
- `command: 开始历练`
- `alias: 历练开始`
