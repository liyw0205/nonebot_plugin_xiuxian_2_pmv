# 副本探索与兑换

## 用户流程

副本商店兑换使用统一资产 application；探索采用准备、结算、重放三步操作边界，战斗算法和队伍读模型暂由兼容适配器提供。

## 命令与别名

- `副本兑换`：兑换副本商店物品。
- `探索副本`：创建并结算一次探索操作。

## Web API

- `POST /api/v1/dungeon/purchase`，权限 `user`，支持 `Idempotency-Key`。
- `POST /api/v1/dungeon/explore/replay`、`prepare`、`settle`，权限 `user`。

## 数据模型与迁移

`dungeon.001` 在 `game_db` 创建 `dungeon_feature_migrations`。历史副本操作表由兼容仓储继续维护。

## 事务与失败回滚

商店兑换由 application 的 operation ledger 和旧仓储事务共同保证幂等。探索计划在准备阶段持久化，结算阶段比较玩家、队伍和背包快照，冲突时只记录拒绝结果，不修改资产。

## 定时任务

每日副本重置由兼容生命周期统一调度。

## 配置项

`dungeon_enabled`（`XIUXIAN_DUNGEON_ENABLED`）控制新边界，默认启用。

## 灰度开关、回滚和已知限制

关闭开关后保留旧命令和数据格式。探索的战斗规则、队伍管理和奖励计算尚未移入新 domain，待后续发布周期完成迁移。

## 适配器差异

命令适配器只组装探索快照，Web 适配器只解析 DTO；领域 application 不依赖 NoneBot、Flask 或 SQLite。

## 测试与手工验收

覆盖准备、结算、重放、快照冲突和库存拒绝；使用 Flask client 与恢复冒烟验证权限和幂等。

## Manifest 清单
- `route: POST /api/v1/dungeon/explore/prepare`
- `route: POST /api/v1/dungeon/explore/settle`
