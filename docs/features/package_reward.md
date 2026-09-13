# Package Reward

## 用户流程

用户通过 `使用礼包` 或 `开启礼包` 提交礼包、数量和奖励 DTO。当前旧命令模块负责消息解析与展示，新 application 负责资产事务和 operation ledger。

## 命令与别名

- `使用礼包`
- `开启礼包`
- permission: `user`

## Web API

- `POST /api/v1/package-reward/open`
- permission: `user`
- `Idempotency-Key` 或 JSON `operation_id` 作为幂等键
- adapter: `adapters/web/blueprints/package_reward.py`

## 数据模型与迁移

- migration: `package_reward.001`
- ledger: `package_reward_operations`
- repository: `features/package_reward/repository.py::PackageRewardRepository`
- 读写 `user_xiuxian` 与 `back` 表，使用已有数据库事务边界

## 事务与失败回滚

`PackageRewardApplication` 使用 `DatabaseUnitOfWork(immediate=True)`、operation ledger 和 savepoint。礼包数量不足、用户缺失、库存满或状态变化时回滚资产变更并保留拒绝结果。

## 定时任务

无。

## 配置项

使用现有背包容量配置 `max_goods_num`，由 application 请求 DTO 显式传入。

## 适配器差异

Web route 已直接调用 `PackageRewardApplication`。旧 NoneBot 命令仍通过 `xiuxian_back.package_reward_service` compatibility facade 进入新 application，消息解析仍属于旧命令边界，尚未删除旧入口。

## 测试与手工验收

- `tests/test_package_reward_web_boundary.py` 验证真实 Flask blueprint、permission boundary、Idempotency-Key 和 rewards DTO。
- application/repository 行为测试与真实 `/srv/old/data` backup、migration dry-run、reconcile、恢复 smoke 仍需持续补齐。
- 当前不把 facade、route 或测试通过当作该切片完成证明。

Migration version: `package_reward.001`.
