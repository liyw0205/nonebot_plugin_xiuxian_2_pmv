# 世界BOSS资产结算

## 用户流程

世界BOSS兑换和讨伐结算均通过 application 生成唯一 operation_id。旧 NoneBot 命令通过兼容门面调用新 application，返回字段保持兼容。

`世界BOSS兑换` 由 `BossPurchaseCommandApplication` 负责。命令先读取 ledger/旧回执和用户 profile，再读取商品配置、物品目录、周限购与积分快照；原始请求数量写入 operation payload，实际数量只在事务内按剩余周限购裁剪。成功回放不依赖当前配置、目录、积分或周限购，拒绝和不明确回执不会渲染为成功。回执仓储只读、不执行请求期 DDL；`started`、`needs_reconcile`、旧表缺少明确 status 和坏 JSON 均 fail closed。

## 命令与别名

- `世界BOSS兑换`：积分商店兑换。
- `讨伐世界BOSS`：战斗结果结算。

## Web API

- `POST /api/v1/boss/purchase`，权限 `user`，支持 `Idempotency-Key`。
- `POST /api/v1/boss/settle`，权限 `user`，支持 `Idempotency-Key`。

响应使用统一 `OperationOutcome`。业务拒绝返回 HTTP 409，输入错误返回 HTTP 400。

## 数据模型与迁移

`boss.001` 在 `game_db` 创建 `boss_feature_migrations`，`boss.004` 与 `boss.005` 在 `player_db` 启动时创建世界BOSS生命周期、限额和全量刷新回执表。积分、背包和战斗结算继续复用既有跨库表，但默认通过 `BossApplication` 的 feature-owned repository 执行；手动生成、全量刷新与每日限额重置运行期只读校验 schema，缺失时 fail closed，不执行 DDL。

## 事务与失败回滚

资产 application 先写 `operation_ledger`，再执行结算；手动生成使用版本 CAS 与回执幂等，每日限额重置冻结目标并按 chunk 恢复。相同 operation/日期重试只返回首次结果；异常在当前事务回滚并保留可恢复进度。旧 transaction service 仅作为显式兼容/回滚路径。

兑换 writer 复用 `BossApplication.purchase -> BossPurchaseSqlRepository.purchase`，在附加 player DB 的 immediate SQLite 事务中校验积分/周限购/背包容量并同时更新积分、周限购、背包和业务回执；ledger 与业务回执仍保留原 operation identity。已确认的 SQL 回滚记录为 `internal_error`，同 payload 可重试；无法确认的 started/needs_reconcile 不盲目重放。

世界BOSS战斗和“世界BOSS信息”通过 `BossApplication.daily_limit_snapshot` 读取每日讨伐次数、积分和灵石。读取使用只读 UoW，每次最多取一行，不缓存；缺数据库、表、用户行或旧字段时返回零，不创建数据库/表、不补字段，也不插入默认用户行。战斗沿用该快照作为结算 CAS 的期望值；显式限额写入与每日重置仍由各自 mutation 路径负责。

世界BOSS积分排行榜通过 `BossIntegralApplication.top_integrals` 读取长期 `boss_limit.integral` 投影，只返回最多 50 项且不在 Python 中物化全量积分表。重复 user_id 保持与积分发放相同的首 rowid 语义；缺数据库/schema 返回空列表，不建库、不建表。旧积分榜曾读取错误的 `integral.boss_integral` 表，不能作为当前数据源。

## 定时任务

世界BOSS刷新和天罚仍由显式兼容生命周期注册，手动生成、全量刷新与每日重置的持久化由 feature-owned repository 承担；手动与定时全量刷新共享 operation receipt，未在 feature 导入时重复注册。

## 配置项

`boss_enabled`（`XIUXIAN_BOSS_ENABLED`）控制新资产边界，默认启用。

## 灰度开关、回滚和已知限制

关闭开关即可停止新 manifest/API 并保留旧命令。活动联动奖励和读模型仍由旧适配器提供，完整发布周期后再删除兼容层。

## 适配器差异

application 保持框架无关；命令适配器负责战斗快照解析，Web 适配器负责 DTO、权限和 CSRF。

## 测试与手工验收

覆盖兑换、讨伐结算的成功、拒绝、异常回滚和重放，并在隔离数据目录执行 Web client 验收。
