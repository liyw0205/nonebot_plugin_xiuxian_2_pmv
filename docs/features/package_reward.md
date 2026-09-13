# 礼包奖励

## 用户流程

旧命令 `使用 <礼包> [数量]` 解析礼包配置并把已解析的奖励交给 `PackageRewardApplication`。随机选择和文案仍属于兼容命令适配器；资产变更只在 application/repository 中完成。

## 命令与别名

Manifest 登记 `使用礼包`、`开启礼包`。历史 `使用` 命令在兼容周期内继续转发。

## Web API（方法、路径、请求/响应、权限、幂等键）

本切片暂不暴露独立 Web 写接口。operation ledger 由统一管理 API 查询；后续饰品跨库奖励会在独立切片中提供 API。

## 数据模型与迁移

`game_db.package_reward_operations` 保存已结算的 operation、礼包数量和固定奖励；`package_reward.001` 由统一迁移 runner 执行。通用 `operation_ledger` 与 `operation_audit` 保存请求哈希、结果和审计摘要。

## 事务与失败回滚

礼包消耗、灵石变化、物品入包和 operation 记录在同一个 `BEGIN IMMEDIATE` Unit of Work 中。用户不存在、礼包不足、灵石不足、背包容量不足和并发状态变化均拒绝且不改变资产。

## 定时任务

无。

## 配置项

沿用 `max_goods_num` 运行配置，由兼容 adapter 注入；不在领域层读取全局配置。

## 适配器差异

NoneBot 兼容层负责解析旧命令、随机礼包奖励和消息文案；application 不依赖 NoneBot、Flask 或 SQL 方言。

## 测试与手工验收

覆盖成功、重复请求、请求参数变化、余额/库存不足、用户不存在、背包满和异常回滚。使用临时 SQLite 执行 `python -m unittest discover -s tests -q`。

## 灰度开关、回滚和已知限制

`XIUXIAN_PACKAGE_REWARD_ENABLED=false` 可切回旧服务。饰品礼包仍由旧的跨库兼容服务处理，待跨库 outbox/reconcile 切片迁移后删除旧实现。
