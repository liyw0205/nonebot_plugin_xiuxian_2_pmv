# 管理员资产调整

## 用户流程

管理员提交目标玩家的灵石快照和增减数量，或提交物品数量快照和发放数量。应用在同一事务中重新校验快照、更新余额/背包并记录经济审计；并发修改会被拒绝，不会覆盖玩家最新状态。

## 命令与别名

`神秘力量`（别名 `管理员灵石`）对应 `admin.stone_adjust` 应用用例。旧命令解析目标用户和广播逻辑保留在兼容适配器中，单人调整已转发到新应用边界。

## Web API

`POST /api/v1/admin/assets/stone`，权限 `admin`，需要 CSRF 和 `Idempotency-Key`（或请求体 `operation_id`）。请求字段：`operator_id`、`user_id`、`expected_stone`、`requested_delta`、可选 `target_name`。余额不足时旧规则仍将余额下限限制为 0；统一响应包含操作号和前后余额。

`POST /api/v1/admin/assets/item` 同样要求 `admin`、CSRF 和幂等键，请求字段为 `operator_id`、`user_id`、`item_id`、`item_name`、`item_type`、`quantity`、`expected_quantity`、`max_goods_num` 和可选 `target_name`。统一响应包含操作号、前后数量和实际发放量。

## 数据模型与迁移

`admin_asset.001` 创建 `admin_asset_feature_migrations` 标记。`operation_ledger`/`operation_audit` 是新边界的统一流水；旧 `admin_stone_adjustment_operations`、`admin_item_grant_operations` 与 `economy_log` 继续由兼容仓储维护。

## 事务与失败回滚

旧仓储使用 `BEGIN IMMEDIATE` 和条件更新 `WHERE stone = expected_stone`，用户不存在、快照变化、操作号冲突均不改变资产。应用层在旧事务成功后写统一审计；应用异常会写 `failed` 记录，允许同操作号重试。

## 定时任务

无。管理员调整是显式请求，不自动重试或后台批量执行。

## 配置项

`admin_asset_enabled`（`XIUXIAN_ADMIN_ASSET_ENABLED`）控制新应用边界，默认启用；关闭后旧管理员服务继续接管。

## 适配器差异

核心应用不导入 NoneBot、Flask 或 SQLite 驱动。命令和 Web adapter 只负责解析上下文、权限和响应；仓储通过惰性导入连接现有管理员事务服务。

## 测试与手工验收

执行 `python -m unittest tests.test_admin_asset_application tests.test_admin_stone_adjustment_transaction -q`，并用 Flask client 验证匿名拒绝、CSRF 失败、管理员成功、重复操作重放和快照变化拒绝。

## 灰度开关、回滚和已知限制

设置 `XIUXIAN_ADMIN_ASSET_ENABLED=false` 后重启可回退；不删除旧流水。当前迁移单人灵石和普通物品发放；全服灵石、全服物品、物品扣除、修为和传承资产仍由兼容服务处理。
