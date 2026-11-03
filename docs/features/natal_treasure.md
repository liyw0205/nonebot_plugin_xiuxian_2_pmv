# 本命法宝

## 用户流程

觉醒、重塑、养成、升级道纹、铭刻和遗忘均通过统一操作号提交，旧法宝数据由兼容仓储读取。
六条动作的资产变更与回执写入由 feature 仓储在单个事务内完成：觉醒写在 player DB，其余五条在
附库 player_data 的 game DB 事务内。

## 命令与别名

`本命法宝`、`觉醒本命法宝`、`重塑本命法宝`。

## Web API

`POST /api/v1/natal-treasure/{awaken,reawaken,train,upgrade,engrave,forget}`，权限 `user`，使用 `Idempotency-Key`。

## 数据模型与迁移

迁移标记 `natal_treasure.001`；历史表仍由旧服务维护。

`natal_treasure.002` 在 game DB 预建五张回执表（`natal_reawaken_operations`、
`natal_training_operations`、`natal_effect_upgrade_operations`、`natal_engraving_operations`、
`natal_forget_operations`），`natal_treasure.003` 在 player DB 预建
`natal_awaken_operations`。这六张表此前都在请求事务里 `CREATE TABLE IF NOT EXISTS`
（feature 仓储与旧 `transaction_service` 各写一份），与「请求路径不执行 DDL」的仓库级合同冲突。
迁移语句保持 `IF NOT EXISTS`，已按惰性路径建表的历史库保留全部既有回执行。

请求路径只校验 schema：缺表**或缺任一读取列**都返回 `schema_missing` 且零资产变化，
不再补列、不再建表。`natal_treasure` 本表仍归旧服务维护，本切片不新增它的列或迁移。

## 事务与失败回滚

新层先写 operation ledger，旧事务失败时记录 failed 并可重试。
`schema_missing` 与 `treasure_missing`、`state_changed` 一样是拒绝结果：不扣神秘经书、
不扣灵石、不改法宝状态；命令侧把它们各自渲染成可读提示。

## 定时任务

无。

## 配置项

`natal_treasure_enabled`。

## 适配器差异

消息适配器只负责解析用户和回复计划。

## 测试与手工验收

`python -m unittest tests.test_natal_operations_schema -q`（回执表启动迁移、请求路径无 DDL 与
`schema_missing` 零变化、历史回执行保留、迁移后同一调用可用）；
应用层 fake repository、重复操作号和拒绝结果测试。

## 灰度开关、回滚和已知限制

关闭灰度开关即可回到旧入口；具体数值规则仍由兼容服务提供。旧 `transaction_service` 的六个兼容
service（`AwakenService` / `ReawakenService` / `NatalTrainingService` / `EffectUpgradeService` /
`EngravingService` / `ForgetEffectService`）保留为显式回滚入口，它们仍在自身事务内建回执表；
默认命令路径与 feature 仓储都已改为只校验启动迁移建的 schema。

## Manifest 清单

- `command: 本命法宝`
- `alias: 觉醒本命法宝`
- `alias: 重塑本命法宝`
- `route: POST /api/v1/natal-treasure/awaken`
- `route: POST /api/v1/natal-treasure/reawaken`
- `route: POST /api/v1/natal-treasure/train`
- `route: POST /api/v1/natal-treasure/upgrade`
- `route: POST /api/v1/natal-treasure/engrave`
- `route: POST /api/v1/natal-treasure/forget`
- `config: natal_treasure_enabled`
- `migration: natal_treasure.001`
