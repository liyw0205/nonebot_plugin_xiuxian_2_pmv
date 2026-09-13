# 竞技场资产结算

## 用户流程

竞技场战斗匹配、排行榜和赛季任务继续由兼容适配器负责；荣誉兑换、购买挑战次数和挑战结算统一经过 `ArenaApplication`。

## 命令与 Web API

旧命令保持不变：`竞技场兑换`、`竞技场购买次数`、`竞技场挑战`。

- `POST /api/v1/arena/purchase`：荣誉值兑换物品。
- `POST /api/v1/arena/challenge-purchase`：灵石购买挑战次数。
- `POST /api/v1/arena/settle`：提交战斗快照与结算结果。

写接口要求 `user` 权限、CSRF 和 `Idempotency-Key`，返回统一 operation outcome。

## 数据与事务

版本 `arena.001` 写入 `game_db.arena_feature_migrations`。历史竞技场表位于 `player_db`，跨库写入由 `LegacyArenaRepository` 调用已有事务服务；`game_db` 的 operation ledger 负责统一审计和幂等回放。

## 灰度与回滚

`arena_enabled` / `XIUXIAN_ARENA_ENABLED` 默认开启。关闭后新 application 和 Web 路由不注册，旧 handler 可继续提供回滚路径。

## 已知限制

战斗算法、对手搜索、排行榜刷新、挑战券和赛季奖励暂留兼容服务，后续按独立垂直切片迁移。

## 测试

- `python -m unittest nonebot_plugin_xiuxian_2.features.arena.tests.test_arena_application -q`
- `python -m unittest tests.test_arena_purchase_service tests.test_arena_challenge_settlement -q`

## 命令与别名

命令为 `竞技场兑换`、`竞技场购买次数`、`竞技场挑战`，旧别名由兼容适配器保留。

## Web API

写接口为 `POST /api/v1/arena/purchase`、`/challenge-purchase`、`/settle`，权限为 `user`，需要 CSRF 与 `Idempotency-Key`。

## 数据模型与迁移

迁移版本为 `arena.001`；统一 operation ledger 位于 `game_db`，历史竞技场状态仍由 `player_db` 兼容仓储维护。

## 事务与失败回滚

拒绝和异常不修改资产；跨库失败记录失败操作并由对账入口重试。

## 定时任务

排行榜与赛季刷新仍由兼容调度器执行，使用稳定任务 ID。

## 配置项

`XIUXIAN_ARENA_ENABLED` 控制新 application 边界，默认开启。

## 适配器差异

application 不依赖 NoneBot、Flask 或 SQLite 驱动，命令和 Web 适配器只负责输入输出。

## 测试与手工验收

除单元测试外，使用隔离数据目录执行 Flask client、CSRF/权限和恢复冒烟。

## 灰度开关、回滚和已知限制

关闭开关后保留旧命令；战斗匹配、排行榜和赛季任务尚未迁移。

## Manifest 清单
- `alias: 竞技场商店`
