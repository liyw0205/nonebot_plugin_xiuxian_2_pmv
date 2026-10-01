# 悬赏令接取与结算

## 用户流程

用户先刷新悬赏令，再执行接取编号。接取只改变当前悬赏状态，不消耗刷新次数；完成后结算会按固定的工作快照发放修为和额外物品。刷新次数、悬赏快照、用户冷却状态和结算奖励在各自事务中校验。

## 命令与别名

- `悬赏令接取`（旧入口使用 `悬赏令接取 1`）
- `悬赏令结算`
- `悬赏令`

## Web API

`POST /api/v1/work/claim`，权限为 `user`，请求体包含 `operation_id`、`user_id`、`expected_count`、`expected_offer`、`task_index` 和 `started_at`。支持 `Idempotency-Key`；成功响应返回任务名、开始时间和剩余刷新次数。

`POST /api/v1/work/settle`，权限为 `user`，请求体包含工作快照、修为增量、可选物品、等级上限和背包上限。支持 `Idempotency-Key`；成功响应返回实际修为、奖励类型和任务名。

## 数据模型与迁移

`work.001` 写入功能迁移标记；`work.003` 在 game DB 创建 `work_item_use_operations`，`work.004` 用 `CREATE TABLE IF NOT EXISTS` 建立兼容现有数据的 `work_offer_snapshots`，`work.005` 预建 `work_refresh_operations` 并保留已有刷新回执，`work.006` 预建 `work_active_snapshots` 和 `work_abort_cleanup_operations`，保留已有快照与清理回执，`work.007` 预建 `work_claim_operations` 并保留已有接取回执，`work.008` 预建 `work_settlement_operations` 并为历史表补齐 `result_json`。所有 work migration 均在启动阶段注册，结算请求只校验 schema，不在请求期建表。管理员 `重置悬赏令` 通过 `WorkAdminRefreshResetApplication -> WorkAdminRefreshResetSqlRepository` 在单一事务中扫描全表，仅更新次数与目标值不同的记录，减少无效 WAL 写入；复用 `platform.001` operation ledger/audit，不新增迁移、不加载用户 ID 列表。它与按业务日冻结用户集合的 scheduler daily reset 保持独立语义。
悬赏令加速道具与追捕令都经 `WorkItemUseApplication -> WorkItemUseSqlRepository`；加速原子扣除一个道具并将已接取悬赏的开始时间置为立即可结算，追捕令原子扣除道具并保存随机 offer 与首次奖励倍率。两种动作都按库存快照校验，捕获令重放返回首次保存的 offer 和倍率。`work.002` 负责每日刷新重置，默认 scheduler 经 `WorkDailyRefreshResetApplication -> WorkDailyRefreshResetRepository` 分块处理冻结用户集合；旧 reset service 仅保留 compatibility rollback API。接取由 `WorkClaimApplication -> WorkClaimSqlRepository` 承担，结算由 `WorkSettlementApplication -> WorkSettlementSqlRepository` 承担，两个 repository 均以单一 immediate UoW 校验状态、更新资产和写 operation receipt；历史 JSON 仅作为兼容读取/展示投影。

## 事务与失败回滚

接取、结算 application 在 `game_db.operation_ledger` 记录请求；接取 repository 以一个 `BEGIN IMMEDIATE` 事务校验用户、冷却和 offer，更新 active snapshot 并写接取回执；`work.007` 缺失时返回 `schema_missing`，请求路径不建表。结算 repository 只在 `work.008` 已迁移时执行，缺 schema 返回 `schema_missing`；校验工作快照、冷却时间和 operation payload，按真实奖励 `id/name/type` 写入背包，校验容量后原子更新修为、背包和 `user_cd` 清理，并持久化 `success_kind/item_msg/scheduled_time` 供 replay；历史 `result_json` 列由启动迁移补齐。旧 `WorkSettlementService` 仅保留在 compatibility 模块作为显式回滚路径。物品 repository 以一个 `BEGIN IMMEDIATE` 事务校验用户、道具库存和工作状态，再原子更新背包、`user_cd.create_time` 或 offer 快照及 operation 结果；旧 `WorkItemUseService` 已移至 compatibility 模块，历史 transaction import 仅作对象身份转发，默认 matcher 不可达。刷新由 `WorkRefreshApplication -> WorkRefreshSqlRepository` 在一个 `BEGIN IMMEDIATE` 事务中校验刷新次数、冷却和旧 offer，原子更新次数、固定快照与刷新回执；`work.005` 缺失时返回 `schema_missing`，请求路径不建表。终止/过期清理/重置由 `WorkAbortCleanupApplication -> WorkAbortCleanupSqlRepository` 在 game DB 的 `BEGIN IMMEDIATE` 中校验冷却、offer 和灵石快照，再原子应用惩罚、清除 cooldown/active/offer projection 并写清理回执；`work.006` 缺失时返回 `schema_missing`，请求路径不建表。惩罚不超过当前灵石，重复操作重放首次结果，状态冲突不改资产，晚期 SQL 错误完整回滚。追捕令随机结果与倍率只在首次请求持久化，随机重抽不破坏同 operation 重放；随后仅更新旧 JSON 展示投影。`reward_data_source` 不再请求期创建 `work_offer_snapshots`；有 schema 时可将历史 JSON 导入数据库，无 schema 时只读旧 JSON，写入需先完成迁移。

## 定时任务

刷新、结算、过期清理和每日重置仍由兼容调度管理，本切片不新增任务。

## 配置项

- `work_claim_enabled` / `XIUXIAN_WORK_CLAIM_ENABLED`：默认启用，可关闭新应用接入。

## 适配器差异

命令适配器负责解析旧的正则命令和 JSON 投影；Web blueprint 负责 DTO、权限、CSRF 和统一响应；application 不发送消息。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.work.tests.test_work_application nonebot_plugin_xiuxian_2.features.work.tests.test_refresh_repository -q`
- `python -m unittest nonebot_plugin_xiuxian_2.features.work.tests.test_abort_cleanup_repository tests.test_work_abort_cleanup -q`
- `python -m unittest tests.test_work_claim_service tests.test_work_settlement_service tests.test_source_quality -q`
- `python -m pytest -p no:cacheprovider tests/test_work_item_use_application.py tests/test_work_item_use_service.py`
- 已注册的 `道具使用 追捕令` matcher 应扣除一个追捕令并保留首次生成的 offer。
- 重复提交相同操作号不应重复改变悬赏状态。

## 灰度开关、回滚和已知限制

关闭 `work_claim_enabled` 可恢复旧 claim/settlement 命令实现。普通/强制刷新、abort/reset cleanup、daily reset、claim、settlement 和 item-use 默认路径已由 feature 边界承担；旧 refresh/cleanup/daily-reset/settlement/item-use service 仅通过 compatibility shim 保留。删除旧服务前需满足完整发布周期、真实数据备份恢复和 reconcile 演练要求。

## 缓存与资源

悬赏结算不建立跨请求物品缓存；物品目录只由命令层按需读取，数据库连接由短生命周期 UoW 管理。测试、compileall 和 recovery 产物必须放在专用临时目录，结束后清理 pytest cache、`__pycache__`、`.pyc` 和 recovery 数据，避免占满磁盘或 RAM；不得清理 `.venv`、`.git`、运行数据和用户文件。
