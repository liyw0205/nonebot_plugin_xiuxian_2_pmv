# 管理员资产调整

## 用户流程

管理员提交目标玩家的灵石快照和增减数量，或提交物品数量快照和发放数量。应用在同一事务中重新校验快照、更新余额/背包并记录经济审计；并发修改会被拒绝，不会覆盖玩家最新状态。

## 命令与别名

`神秘力量`（别名 `管理员灵石`）对应 `admin.stone_adjust` 应用用例。旧命令解析目标用户和广播逻辑保留在兼容适配器中，单人调整已转发到新应用边界。

## Web API

`POST /api/v1/admin/assets/stone`，权限 `admin`，需要 CSRF 和 `Idempotency-Key`（或请求体 `operation_id`）。请求字段：`operator_id`、`user_id`、`expected_stone`、`requested_delta`、可选 `target_name`。余额不足时旧规则仍将余额下限限制为 0；统一响应包含操作号和前后余额。

`POST /api/v1/admin/assets/item` 同样要求 `admin`、CSRF 和幂等键，请求字段为 `operator_id`、`user_id`、`item_id`、`item_name`、`item_type`、`quantity`、`expected_quantity`、`max_goods_num` 和可选 `target_name`。统一响应包含操作号、前后数量和实际发放量。

## 数据模型与迁移

`admin_asset.001` 创建 feature migration 标记；`admin_asset.002` 在启动阶段预建单人灵石调整回执和 `economy_log.trace_id`，请求路径不执行 DDL。单人灵石调整复用 `admin_stone_adjustment_operations` 以延续旧回执，普通物品发放仍由其各自 feature repository 管理；`admin_item_grant_operations` 保留为兼容数据。`operation_ledger`/`operation_audit` 是应用级流水。

## 事务与失败回滚

feature stone repository 在一个 game DB immediate UoW 中提交余额 CAS、旧格式 operation receipt、`economy_log` 和 trace ID；用户不存在、快照变化、操作号冲突都不会改资产。扣减仍封顶至 0，审计记录实际 delta。统一 operation ledger 若停在 `started`，相同请求可借 repository receipt 安全恢复；晚期 SQL 异常同时回滚余额、receipt 和经济审计。显式注入 `LegacyAdminStoneRepository` 仍可供回滚使用，但不由默认 composition root 创建。

## 定时任务

无。管理员调整是显式请求，不自动重试或后台批量执行。

## 配置项

`admin_asset_enabled`（`XIUXIAN_ADMIN_ASSET_ENABLED`）控制新应用边界，默认启用；关闭后旧管理员服务继续接管。

## 适配器差异

核心应用不导入 NoneBot、Flask 或 SQLite 驱动。命令和 Web adapter 只负责解析上下文、权限和响应；默认灵石仓储通过 feature-owned UoW 写入，显式 rollback 仓储才惰性调用旧管理员事务 service。

## 测试与手工验收

执行 `python -m unittest tests.test_admin_asset_application tests.test_admin_stone_adjustment_transaction -q`，并运行 admin asset repository/source/progress tests；Flask client 覆盖匿名拒绝、CSRF 失败、管理员成功、重复操作重放和快照变化拒绝。

## 灰度开关、回滚和已知限制

设置 `XIUXIAN_ADMIN_ASSET_ENABLED=false` 后重启可停用新 adapter；不删除旧流水。单人灵石默认路径已由 feature repository 承担，显式 legacy repository 保留作回滚。普通物品发放、全服灵石、全服物品、物品扣除、修为和传承资产仍有各自兼容边界。
