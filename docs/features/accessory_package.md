# 饰品礼包

## 用户流程

命令适配器解析礼包和已生成的饰品实例。应用层先在 `game_db` 提交礼包、灵石和普通物品变更，再通过 outbox 在 `player_db` 幂等写入饰品，最后标记操作完成。

## 命令与别名

`使用饰品礼包`、`开启饰品礼包`。旧 `AccessoryPackageService` 保留为兼容 facade，并记录命中次数。

## Web API

`POST /api/v1/accessory-package`，权限 `user`，需要 CSRF。请求使用 `Idempotency-Key` 或 `operation_id`，字段包括 `user_id`、`package_id`、`quantity`、`rewards`、`accessories`、`max_goods_num` 和 `accessory_limit`。

## 数据模型与迁移

`game_db.accessory_package_operations` 保存跨库阶段、请求和补偿快照；`player_db.accessory_package_operations` 保存饰品写入幂等记录。迁移版本为 `accessory_package.001`。

## 事务与失败回滚

SQLite 文件之间不宣称原子事务。游戏库先写 operation ledger 和 domain outbox；玩家库失败时游戏库依据快照恢复礼包、灵石和普通物品，并将操作标记为 `compensated`。无法补偿时保留 `needs_reconcile`，由对账任务处理。

## 定时任务

无新增任务。outbox 事件由统一对账入口处理。

## 配置项

`accessory_package_enabled`（`XIUXIAN_ACCESSORY_PACKAGE_ENABLED`）控制灰度；默认开启。

## 适配器差异

领域、仓储和 application 不依赖 NoneBot、Flask 或直接 SQLite 连接。命令适配器负责随机实例和消息文案。

## 测试与手工验收

覆盖成功、重放、参数冲突、库存/饰品容量拒绝、玩家库写入失败补偿和 outbox 可见性。使用隔离 `XIUXIAN_DATA_DIR` 执行 Flask client 与恢复冒烟。

## 灰度开关、回滚和已知限制

关闭灰度后旧实现仍可用。若玩家库在最终提交后才发生故障，操作会保持 `needs_reconcile`，不得手工 SQL 静默修改资产。
