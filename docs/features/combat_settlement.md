# 地图战斗结算

## 用户流程

地图战斗完成后，旧地图流程生成战斗快照；结算应用读取快照并一次性更新灵石、背包和每日次数，成功后清空快照。背包已满、每日次数耗尽或快照过期时保持原状态，玩家可稍后重试。

## 命令与别名

应用适配器提供 `节点战斗结算`（别名 `战斗结算`）的 `ReplyPlan` 构造入口。旧地图 matcher 在兼容期仍负责战斗展示和快照生成。

## Web API

`POST /api/v1/combat/settle`，权限 `user`，写请求需要 CSRF。请求体包含 `operation_id`、`user_id`、`expected_daily`、`snapshot`、`daily_limit`、`stone`、`items` 和 `max_goods_num`；也可使用 `Idempotency-Key` 请求头。响应使用统一 API envelope，拒绝返回 HTTP 409。

## 数据模型与迁移

`combat_settlement.001` 在 `game_db` 写入 `combat_settlement_feature_migrations` 标记。旧事务按需创建 `map_combat_settlement_operations`，玩家快照和每日限制仍归 `player_db` 所有。

## 事务与失败回滚

旧仓储在 `game_db` 附加 `player_db`，使用 `BEGIN IMMEDIATE` 同时校验快照、每日限制和背包容量，再更新两库。应用层的 `operation_ledger` 和 `operation_audit` 记录统一操作号、结果和审计类别；相同操作号重试只返回首次结果。

## 定时任务

无。战斗结算由用户动作触发，不运行后台补发任务。

## 配置项

`combat_settlement_enabled`（`XIUXIAN_COMBAT_SETTLEMENT_ENABLED`）控制新应用边界是否启用，默认启用；关闭后由旧兼容入口继续提供服务。

## 适配器差异

业务层不依赖 NoneBot 或 Flask。旧地图服务通过惰性仓储适配，Web 适配器只解析 DTO、鉴权和序列化结果。

## 测试与手工验收

执行 `python -m unittest tests.test_combat_settlement_application tests.test_map_combat_settlement_service -q`，并用 Flask client 验证 CSRF、幂等重放、背包已满拒绝和快照过期拒绝。

## 灰度开关、回滚和已知限制

设置 `XIUXIAN_COMBAT_SETTLEMENT_ENABLED=false` 后重启即可回到旧实现；不删除旧表或快照。当前旧 matcher 仍生成战斗计划，完整命令拆迁和前端战斗页面迁移属于后续兼容期工作。
