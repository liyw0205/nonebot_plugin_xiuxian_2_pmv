# 炼体结算

## 用户流程

用户执行 `炼体结算`（别名 `炼体收获`）后，应用根据炼体状态、上次结算时间和宗门仙府等级计算已积累气血。首次执行只初始化计时；没有完整分钟时返回空结算；有效收益会更新炼体状态并保留结算明细。

## 命令与 API

旧 NoneBot handler 仍负责用户校验、提示文本和上下文解析，但实际写操作统一调用 `TiantiSettlementApplication`。Web 入口为 `POST /api/v1/tianti/settle`，权限为 `user`，写请求需要 CSRF；请求体包含 `operation_id`、`user_id`、`settled_at` 和可选 `sect_fairyland_level`，也可使用 `Idempotency-Key` 请求头。响应使用统一 operation envelope，业务拒绝返回 HTTP 409。

## 数据模型与迁移

`tianti_settlement.001` 在 `game_db` 写入 `tianti_settlement_feature_migrations` 标记。炼体状态和历史兼容操作表仍由旧仓储保存在 `player_db`；新 application 在同一数据库维护 `operation_ledger` 与 `operation_audit`，因此重复请求可以安全重放并可审计。

## 事务与失败回滚

`LegacyTiantiSettlementRepository` 惰性调用旧 `TiantiSettlementService`，旧服务以 `BEGIN IMMEDIATE` 原子更新炼体状态和 `tianti_settlement_operations`。应用先登记操作号，旧事务异常时不返回成功结果并记录 `failed`；相同参数的后续请求可重试。操作号、用户或仙府等级不一致时由 ledger 拒绝冲突请求。

## 定时任务

无。结算由用户命令或 Web 请求触发，不自动发放收益。

## 配置与灰度

`XIUXIAN_TIANTI_SETTLEMENT_ENABLED` 控制新应用边界，默认启用；关闭后 manifest 不注册新路由和新命令边界，旧兼容模块仍可提供服务。回滚不删除 `player_db` 中的旧状态和操作表。

## 适配器与测试

核心 application 不依赖 NoneBot、Flask 或 SQLite 驱动；命令、Web 和旧事务服务分别通过 adapter/repository 连接。测试覆盖成功结果、拒绝结果和幂等重放；Web CSRF/权限、迁移标记和 manifest 由架构与 Web 契约测试覆盖。可运行 `python -m unittest nonebot_plugin_xiuxian_2.features.tianti_settlement.tests.test_tianti_settlement_service -q` 验证 application，完整门禁使用 `python scripts/check_architecture.py`。

## 已知限制

炼体的灵石炼体、药浴、突破和冲窍仍处于兼容服务阶段；本切片只迁移按时间结算气血的动作。待这些动作分别具备独立 application、迁移、审计和灰度开关后再拆除兼容层。

## 命令与别名

命令为 `炼体结算`，别名为 `炼体收获`。

## Web API

`POST /api/v1/tianti/settle`，权限 `user`，需要 CSRF 和 `Idempotency-Key`。

## 数据模型与迁移

迁移版本为 `tianti_settlement.001`，统一 ledger 与审计记录按兼容仓储策略落库。

## 事务与失败回滚

状态更新与历史操作投影在玩家库事务中原子提交；异常标记失败并允许重试。

## 定时任务

无新增定时任务。

## 配置项

`tianti_settlement_enabled` / `XIUXIAN_TIANTI_SETTLEMENT_ENABLED` 控制灰度。

## 适配器差异

命令适配器负责上下文和文案，Web 适配器负责 DTO、权限和 CSRF；application 不依赖框架。

## 测试与手工验收

覆盖初始化、空结算、成功、拒绝、异常回滚和幂等重放，并执行 Flask client 验收。

## 灰度开关、回滚和已知限制

关闭开关可回到旧结算；其他炼体动作仍在兼容层。
