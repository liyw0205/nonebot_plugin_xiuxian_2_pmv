# 灵田傀儡资产操作

## 用户流程

购买灵田傀儡后可升级傀儡等级。购买、升级、开启/关闭状态写入、状态查询和 scheduler 用户分页读取由 `PuppetApplication` 承接；自动收取仍由兼容命令和现有 harvest repository 协调。

## 命令与别名

旧命令保持不变：`购买灵田傀儡`、`购买傀儡`、`灵田傀儡购买`、`灵田傀儡升级`、`傀儡升级`、`升级傀儡`。

## Web API

- `POST /api/v1/puppet/purchase`：字段 `user_id`、`stone_cost`。
- `POST /api/v1/puppet/upgrade`：字段 `user_id`、`upgrade_costs`、`max_level`。

写接口要求 `user` 权限、CSRF 和 `Idempotency-Key`，返回统一 operation outcome；重复请求只回放首次结果。

## 数据模型与迁移

`puppet.001` 写入 `game_db.puppet_feature_migrations`；`puppet.002` 在 game DB 启动迁移中为 `user_xiuxian.puppet_status` 补列，保留既有状态且不在请求路径执行 DDL。开关状态与 operation ledger/audit 在同一 game DB immediate UoW 提交。scheduler 先固定 `rowid` 高水位，再按最多 200 行分页读取启用用户，避免一次性将全体 user ID 装入内存；每个页面查询结束后关闭只读连接。历史傀儡等级/收取时间仍由 player DB `mix_elixir_info` 承载。

## 事务与失败回滚

旧跨库事务由 `LegacyPuppetRepository` 执行，application 在 `game_db` 维护统一 operation ledger 和审计记录。余额不足、傀儡状态变化和兼容库异常均不得产生重复扣除；异常会记录失败操作供对账。

## 定时任务

自动收取任务暂留兼容调度器，拥有 `coalesce=True`、`max_instances=1` 和误触发宽限期；启用用户分页有上界，purchase/upgrade 与 harvest 的事务边界未因本片扩展。

## 配置、灰度与回滚

`puppet_enabled` / `XIUXIAN_PUPPET_ENABLED` 默认开启。关闭后新 application/Web 路由不注册，旧命令兼容入口可按发布策略继续提供回滚路径。

## 测试与验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.puppet.tests.test_puppet_application -q`
- `python -m unittest tests.test_puppet_operation_service tests.test_source_quality -q`
- `python -m compileall -q nonebot_plugin_xiuxian_2 tests`

## 配置项

`puppet_enabled` / `XIUXIAN_PUPPET_ENABLED` 控制新 application、命令和 Web 边界，默认开启。

## 适配器差异

旧跨库事务只存在于 repository adapter；命令和 Web 层不直接访问数据库。

## 测试与手工验收

覆盖购买、升级、余额拒绝、状态冲突和幂等重放；使用 Flask client、窄屏页面和恢复演练验收。

## 灰度开关、回滚和已知限制

关闭开关后旧命令兼容入口继续工作；自动收取与灵田读模型仍待后续迁移。
