# 管理员资产调整

## 用户流程

管理员提交目标玩家的灵石或修为快照和增减数量，或提交物品数量快照和发放数量。应用在同一事务中重新校验快照、更新资产并记录经济审计；并发修改会被拒绝，不会覆盖玩家最新状态。

## 命令与别名

`神秘力量`（别名 `管理员灵石`）对应 `admin.stone_adjust` 应用用例。旧命令解析目标用户和广播逻辑保留在兼容适配器中，单人调整已转发到新应用边界。

`修为调整` 命令调用同一 feature 的 exp repository；操作回执由 game DB 启动迁移预建。

`毁灭力量` 单人物品扣除由 feature repository 提交；回执和经济审计 schema 在启动期预建，未就绪时拒绝扣除。

`创造力量` 普通单人物品发放经 admin asset application；回执 schema 在 game DB 启动期预建，未就绪时拒绝并记录 operation ledger 结果。

## Web API

`POST /api/v1/admin/assets/stone`，权限 `admin`，需要 CSRF 和 `Idempotency-Key`（或请求体 `operation_id`）。请求字段：`operator_id`、`user_id`、`expected_stone`、`requested_delta`、可选 `target_name`。余额不足时旧规则仍将余额下限限制为 0；统一响应包含操作号和前后余额。

`POST /api/v1/admin/assets/item` 同样要求 `admin`、CSRF 和幂等键，请求字段为 `operator_id`、`user_id`、`item_id`、`item_name`、`item_type`、`quantity`、`expected_quantity`、`max_goods_num` 和可选 `target_name`。统一响应包含操作号、前后数量和实际发放量。

## 数据模型与迁移

`admin_asset.001` 创建 feature migration 标记；`admin_asset.002` 在启动阶段预建单人灵石调整回执和 `economy_log.trace_id`；`admin_asset.003` 预建全服灵石批次回执与逐用户进度，并用部分唯一索引限制同一管理员/增量最多一个 running 批次；`admin_asset.004` 预建单人修为调整回执；`admin_asset.005` 预建单人物品扣除回执；`admin_asset.006` 预建单人物品发放回执；`admin_asset.007` 预建境界与灵根调整回执；`admin_asset.008` 预建单人传承石回执。这些默认路径不在请求期执行 DDL，缺少启动 schema 时 fail closed。传承石余额仍由 legacy `impart_db.xiuxian_impart.stone_num` 持有，feature 仓储只校验既有 schema，不创建或补列。全服进度行保存冻结的目标集、执行前后余额和实际增量，是操作审计数据而非缓存；删除前必须制定独立保留策略。启动新批次前按用户数预估所需磁盘空间，不足时 fail closed。

## 事务与失败回滚

feature 单人 stone repository 在一个 game DB immediate UoW 中提交余额 CAS、旧格式 operation receipt、`economy_log` 和 trace ID；用户不存在、快照变化、操作号冲突都不会改资产。扣减仍封顶至 0，审计记录实际 delta。统一 operation ledger 若停在 `started`，相同请求可借 repository receipt 安全恢复；晚期 SQL 异常同时回滚余额、receipt 和经济审计。全服调整将首次目标集冻结在 game DB，随后每次最多处理 100 人；每个 chunk 的余额更新与前后值/结果同事务提交。新用户不加入已开始的操作，处理前被删除的用户记为 skipped；增减不封顶，保持旧 SQL 算术。批次创建在 `BEGIN IMMEDIATE` 内检查相同管理员/增量的活动操作；若另一个 operation ID 已有运行批次则返回 `in_progress`，不会再执行一个批次，部分唯一索引提供数据库级兜底。显式注入 `LegacyAdminStoneRepository` 仍可供回滚使用，但不由默认 composition root 创建。
feature 单人 exp repository 在一个 game DB immediate UoW 中校验 `.002/.004` 启动 schema，再提交修为快照 CAS、exp receipt 和 `economy_log.trace_id`；缺表/缺列返回 `schema_missing`，不在请求时建表。扣减仍封顶至 0，operation replay 不重复记账。
单人物品扣除 repository 在 immediate UoW 中校验 `.002/.005` schema，再校验物品快照并原子提交背包扣减、绑定数量更新、receipt 和 `economy_log`；缺表/缺列返回 `schema_missing`，不执行扣减。
普通物品发放 repository 在 immediate UoW 中校验 `.006` 与 `back` schema，再按物品数量快照提交背包增量和 receipt。缺 schema 返回 `schema_missing`；应用将 rejected operation 写入 ledger，同一 operation 重试得到稳定拒绝，不会误报成功或改库存。
单人传承石 repository 从 `impart_db.xiuxian_impart.stone_num` 读取并 CAS 更新余额，在 attached UoW 中将旧格式兼容回执和 `economy_log` 写入 game DB。快照读取使用只读连接；缺数据库/表/列返回未就绪，重复用户行拒绝修改；扣减仍封顶至 0 并记录实际 delta。`.008` 只路由 game DB，因为 impart 余额表仍归 legacy schema owner 管理。

## 定时任务

单人调整是显式请求。全服灵石命令使用后台分块执行；进程中断后，相同管理员和增量可恢复仍在运行的冻结批次。

## 配置项

`admin_asset_enabled`（`XIUXIAN_ADMIN_ASSET_ENABLED`）控制新应用边界，默认启用；关闭后旧管理员服务继续接管。

## 适配器差异

核心应用不导入 NoneBot、Flask 或 SQLite 驱动。命令和 Web adapter 只负责解析上下文、权限和响应；单人及全服灵石仓储通过 feature-owned UoW 写入，显式 rollback 仓储才惰性调用旧管理员事务 service。

## 测试与手工验收

执行 admin asset application、stone repository/batch repository、source contract、progress 和架构/migration tests；Flask client 覆盖匿名拒绝、CSRF 失败、管理员成功、重复操作重放和快照变化拒绝。全服批次回归覆盖冻结目标集、分块恢复、冲突、不同 operation ID 的重复活动请求、晚期失败回滚和磁盘空间不足。

## 灰度开关、回滚和已知限制

单人及全服灵石、单人修为、单人物品发放/扣除、境界和灵根调整默认路径已由 feature repositories 承担；境界/灵根使用 game-only `.007`，单人传承石回执使用 game-only `.008`，真实余额仍在既有 impart DB 表。全服传承石批次继续走显式 legacy service，不随本次单人切片迁移。显式 legacy single-user stone repository 保留作回滚；批次回执及逐用户进度属于持久审计记录，不随测试缓存清理。全服物品和其余管理员资产仍有各自兼容边界。
