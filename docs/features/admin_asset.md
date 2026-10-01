# 管理员资产调整

## 用户流程

管理员提交目标玩家的灵石或修为快照和增减数量，或提交物品数量快照和发放数量。应用在同一事务中重新校验快照、更新资产并记录经济审计；并发修改会被拒绝，不会覆盖玩家最新状态。

## 命令与别名

`神秘力量`（别名 `管理员灵石`）对应 `admin.stone_adjust` 应用用例。旧命令解析目标用户和广播逻辑保留在兼容适配器中，单人调整已转发到新应用边界。

`修为调整` 命令调用同一 feature 的 exp repository；操作回执由 game DB 启动迁移预建。

`毁灭力量` 单人物品扣除由 feature repository 提交；回执和经济审计 schema 在启动期预建，未就绪时拒绝扣除。

`创造力量` 普通单人物品发放经 admin asset application；回执 schema 在 game DB 启动期预建，未就绪时拒绝并记录 operation ledger 结果。

`创造力量` 和 `毁灭力量` 的单人及全服饰品发放/扣除由 `AdminAssetApplication` 与 feature-owned repositories 承担。全服饰品批次冻结目标并按块执行，进度和每位玩家的 child operation receipt 均持久保存，可在中断后续跑。

`修仙适配` 的旧境界批量转换由 `AdminAssetApplication -> AdminLegacyRealmAdaptationSqlRepository` 承担，保留境界阶段后缀映射与管理员结果文案。

`重置新手礼包` 由 `AdminAssetApplication -> AdminNoviceResetSqlRepository` 承担，使用既有 game DB `operation_ledger` 做幂等回执。

`创造力量 all` 与 `毁灭力量 all` 的普通物品分支也由 `AdminAssetApplication -> AdminItemBatchSqlRepository` 执行。名单在 game DB 内冻结，每轮最多处理 100 人；命令层不读取完整 user ID 列表。全服发放保留单人容量上限和经济审计，全服扣除按实际持有量部分扣除。旧运行 grant batch 会导入原冻结名单和已完成进度，超过 64 Mi 字符时拒绝恢复并保留原任务；child receipt 已提交而批次进度失败时可幂等重放恢复。

## Web API

`POST /api/v1/admin/assets/stone`，权限 `admin`，需要 CSRF 和 `Idempotency-Key`（或请求体 `operation_id`）。请求字段：`operator_id`、`user_id`、`expected_stone`、`requested_delta`、可选 `target_name`。余额不足时旧规则仍将余额下限限制为 0；统一响应包含操作号和前后余额。

`POST /api/v1/admin/assets/item` 同样要求 `admin`、CSRF 和幂等键，请求字段为 `operator_id`、`user_id`、`item_id`、`item_name`、`item_type`、`quantity`、`expected_quantity`、`max_goods_num` 和可选 `target_name`。统一响应包含操作号、前后数量和实际发放量。

## 数据模型与迁移

`admin_asset.001` 创建 feature migration 标记；`.002` 至 `.009` 依次预建既有管理员资产回执，其中 `.009` 是单人饰品调整回执；`.010` 预建全服饰品 operation、legacy-compatible progress 和规范化目标表；`.011` 预建全服传承石批次表；`.012` 预建普通物品全服批次 operation、旧 grant progress 和规范化目标表。`.009` 至 `.012` 只路由到 game DB；玩家饰品表由 `accessory_package.player_data.001` 启动迁移管理。全服饰品、传承石和普通物品新任务都把冻结用户写入目标表，operation payload 仅保留请求，恢复时只加载当前处理块；旧 payload 内嵌用户列表的运行任务会在首次恢复时导入目标表，并沿用已完成 progress。普通物品旧进度通过数据库内 set-based 更新导入，不在 Python 中全量读取 progress。默认路径不在请求期执行 DDL，缺少启动 schema 时 fail closed。传承石余额仍由 legacy `impart_db.xiuxian_impart.stone_num` 持有，feature 仓储只校验既有 schema，不创建或补列。全服进度行、目标和 operation receipts 是持久审计/幂等记录，不是可随意清理的缓存；删除前必须制定独立保留策略。批次创建前按名单数量预检磁盘空间并保留 8 MiB 安全余量，不足时 fail closed。

## 事务与失败回滚

feature 单人 stone repository 在一个 game DB immediate UoW 中提交余额 CAS、旧格式 operation receipt、`economy_log` 和 trace ID；用户不存在、快照变化、操作号冲突都不会改资产。扣减仍封顶至 0，审计记录实际 delta。统一 operation ledger 若停在 `started`，相同请求可借 repository receipt 安全恢复；晚期 SQL 异常同时回滚余额、receipt 和经济审计。全服调整将首次目标集冻结在 game DB，随后每次最多处理 100 人；每个 chunk 的余额更新与前后值/结果同事务提交。新用户不加入已开始的操作，处理前被删除的用户记为 skipped；增减不封顶，保持旧 SQL 算术。批次创建在 `BEGIN IMMEDIATE` 内检查相同管理员/增量的活动操作；若另一个 operation ID 已有运行批次则返回 `in_progress`，不会再执行一个批次，部分唯一索引提供数据库级兜底。显式注入 `LegacyAdminStoneRepository` 仍可供回滚使用，但不由默认 composition root 创建。
feature 单人 exp repository 在一个 game DB immediate UoW 中校验 `.002/.004` 启动 schema，再提交修为快照 CAS、exp receipt 和 `economy_log.trace_id`；缺表/缺列返回 `schema_missing`，不在请求时建表。扣减仍封顶至 0，operation replay 不重复记账。
旧境界适配复用 `admin_asset.007` 的 operation receipt schema，在单一 game DB immediate UoW 中按首条用户行映射并更新境界；回执 replay/conflict 可恢复，晚期回执失败回滚全部等级更新，缺 schema 时不改用户数据，也不遍历或缓存完整用户 ID 列表。
新手礼包重置在同一 immediate UoW 中统计并归零 `user_xiuxian.is_novice`，ledger/audit 写入失败会回滚状态；缺少既有 platform ledger 或玩家字段时 fail closed，不新增业务表、不执行请求期 DDL。
单人物品扣除 repository 在 immediate UoW 中校验 `.002/.005` schema，再校验物品快照并原子提交背包扣减、绑定数量更新、receipt 和 `economy_log`；缺表/缺列返回 `schema_missing`，不执行扣减。
普通物品发放 repository 在 immediate UoW 中校验 `.006` 与 `back` schema，再按物品数量快照提交背包增量和 receipt。缺 schema 返回 `schema_missing`；应用将 rejected operation 写入 ledger，同一 operation 重试得到稳定拒绝，不会误报成功或改库存。
单人传承石 repository 从 `impart_db.xiuxian_impart.stone_num` 读取并 CAS 更新余额，在 attached UoW 中将旧格式兼容回执和 `economy_log` 写入 game DB。快照读取使用只读连接；缺数据库/表/列返回未就绪，重复用户行拒绝修改；扣减仍封顶至 0 并记录实际 delta。`.008` 只路由 game DB，因为 impart 余额表仍归 legacy schema owner 管理。全服批次由 `AdminImpartStoneBatchSqlRepository` 在 game DB 内以 `INSERT ... SELECT` 冻结名单，不把全服 ID 拉入 handler 内存或塞进新 payload；每次最多加载一块目标。每位用户仍经单人仓储提交 child operation，批次进度晚写失败时重试同一 child receipt 不会重复变更 impart 余额。恢复旧运行任务时先做磁盘预检，再以 500 条 SQL 写入块导入旧 payload 名单和已完成进度；仅读取固定长度 payload 前缀识别请求，旧 payload 超过 64 Mi 字符时拒绝恢复且保留历史进度，避免展开巨型 JSON 名单。已完成旧批次保留原摘要。`.011` 只路由 game DB，impart 余额表不迁移、不请求期建表。
单人饰品 repository 使用只读快照和 attached UoW 校验 `player_accessory` CAS，在一次请求事务中更新 bag、写入 game DB 的兼容格式 operation receipt 与 `economy_log`；发放校验品质、容量、UID 与 factory 产物，扣除只从 bag 移除并允许按实际持有量部分扣除。全服饰品批次先对 game/player 所在磁盘做空间预检，再冻结目标并每次只加载有限块；child receipt 先于 batch progress 独立提交，进度写入失败时通过 child operation 幂等重放恢复。新批次不把全量名单放入 payload；旧 payload 可恢复迁移。请求期不建表。SQLite attached 多文件事务的 late-SQL rollback 有测试覆盖，但 WAL 下跨文件崩溃原子性不作保证，真实发布前仍须完成备份及 P7 恢复核验。
普通物品全服 batch repository 对 grant/destroy 都在 game DB 里先冻结名单，并仅从目标表加载受限 chunk。每位玩家复用单人仓储；child receipt 和库存/经济日志在单人 immediate UoW 中原子提交，批次 target progress 独立记录，晚写失败时以同 child operation ID 重放。旧运行 grant payload 按固定长度前缀匹配、流式解析名单，最多按 500 行缓冲写入；payload 超过 64 Mi 字符则保留原任务并拒绝恢复。新操作和旧任务导入都先按用户数检查空间。`admin_asset.012` 仅路由 game DB；旧 batch service 仍作为恢复/兼容边界保留，但命令不再调用它。

## 定时任务

单人调整是显式请求。全服灵石命令使用后台分块执行；进程中断后，相同管理员和增量可恢复仍在运行的冻结批次。

## 配置项

`admin_asset_enabled`（`XIUXIAN_ADMIN_ASSET_ENABLED`）控制新应用边界，默认启用；关闭后旧管理员服务继续接管。

## 适配器差异

核心应用不导入 NoneBot、Flask 或 SQLite 驱动。命令和 Web adapter 只负责解析上下文、权限和响应；单人及全服灵石仓储通过 feature-owned UoW 写入，显式 rollback 仓储才惰性调用旧管理员事务 service。

## 测试与手工验收

执行 admin asset application、stone/impart/accessory repository 与 batch repository、source contract、progress 和架构/migration tests；饰品回归覆盖单人 grant/destroy、回执重放/冲突、CAS、容量、无效 UID、缺 schema 和晚 SQL rollback。Flask client 覆盖匿名拒绝、CSRF 失败、管理员成功、重复操作重放和快照变化拒绝。全服批次回归覆盖冻结目标集、分块恢复、冲突、不同 operation ID 的重复活动请求、晚期失败回滚和磁盘空间不足。

## 灰度开关、回滚和已知限制

单人及全服灵石、单人修为、单人及全服普通物品、境界和灵根调整，以及单人和全服饰品、全服传承石默认路径已由 feature repositories 承担；普通物品全服批次使用 game-only `.012`，全服传承石批次使用 `.011`，全服饰品使用 `.010`、单人饰品回执使用 `.009`，玩家饰品 schema 继续沿用既有 player-side migration。显式 legacy services 与旧批次记录仍保留作兼容/恢复边界；operation receipts、批次目标和进度属于持久审计数据，不随测试缓存清理。其他管理员资产仍有各自兼容边界。
