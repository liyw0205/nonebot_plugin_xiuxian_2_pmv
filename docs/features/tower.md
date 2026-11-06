# 通天塔资产结算

## 用户流程

通天塔的战斗算法、随机 Boss 和奖励计算继续由兼容命令适配器负责；积分兑换、单层挑战和连续挑战的最终资产变更统一经过 `TowerApplication`。连续爬塔在前置体力扣除后因重伤提前结束时，体力返还经 `PlayerStaminaApplication.restore` 完成。

## Web API

- `POST /api/v1/tower/purchase`：积分兑换物品，支持每周限购和背包容量校验。
- `POST /api/v1/tower/settle`：提交塔状态、战斗结果、体力和奖励快照。

两者均要求 `user` 权限、CSRF 和 `Idempotency-Key`，返回统一 JSON envelope。体力返还使用既有 `user_xiuxian.user_stamina` 投影，不创建请求期 schema；缺失 schema 或用户时不写入。

## 数据与事务

`tower.001` 写入 `tower_feature_migrations`。旧跨库事务服务作为惰性仓储适配器；统一 ledger 写入 `game_db`，实际塔状态仍由 `player_db` 的兼容仓储维护。

## 灰度与回滚

`tower_enabled` / `XIUXIAN_TOWER_ENABLED` 可关闭新 application 接入并恢复旧入口。旧重置任务和读模型暂留兼容层，待完整发布周期后删除。

## 验证

- `python -m unittest nonebot_plugin_xiuxian_2.features.tower.tests.test_tower_application -q`
- `python -m unittest tests.test_tower_purchase_service tests.test_tower_settlement_service -q`

## 命令与别名

保留通天塔兑换、单层挑战和连续挑战命令及旧别名。

## 数据模型与迁移

`tower.001` 写入 `game_db.tower_feature_migrations`，旧塔状态仍由兼容仓储维护。

## 事务与失败回滚

统一 ledger 记录请求和结果；体力、积分、背包容量拒绝均不产生资产变更。

## 定时任务

每周限购重置由兼容调度器执行，使用稳定 ID 和业务周快照。

## 配置项

`tower_enabled` / `XIUXIAN_TOWER_ENABLED` 控制新边界，默认开启。

## 适配器差异

战斗算法和奖励计划由兼容命令适配器提供，application 只协调事务和审计。

## 测试与手工验收

覆盖兑换、单层/连续结算、容量拒绝、失败回滚和幂等重放；执行 Flask client 与恢复冒烟。

## 个人进度与里程碑总览

`通天塔信息` 在既有当前层数、历史最高、累计积分与挑战提示后追加【进度与里程碑】：

- `features/tower/progress_view.py` 的 `next_milestone(max_floor)` 返回严格大于历史最高的
  下一个 10 的倍数；`weekly_purchase_summary(weekly_purchases, shop_items)` 汇总有效商品的
  本周已兑/周上限/已兑完项数，忽略 `_last_reset`、非法数量、缺失或非正 `weekly_limit`。
- 面板显示下一里程碑与剩余层数、该层首通积分/灵石/修为预览、本周兑换进度，以及
  每周一 0 点重置提示；没有有效限购商品时显示“暂无有效限购商品”，仍保留里程碑段。
- 数据来自同一 `tower_limit.get_user_tower_info` 快照与静态 `tower_data.config`；不新增
  查询、表、列、迁移、索引、配置键、命令、别名或路由，不写回 `weekly_purchases`。
- 只读展示；不改爬塔战斗、体力、积分/灵石/修为奖励、周限购重置或商店兑换写路径。

## 个人爬塔记录

`我的通天塔记录`（别名 `通天塔记录`、`爬塔记录`）补齐玩家侧逐次爬塔的可见性：
`通天塔信息` 只有当前层数、历史最高与累计积分，看不到最近爬了哪些层、成功还是失败、
拿到了什么。

- 数据源是真实结算回执 `tower_settlement_operations`：`payload` 为
  `[user_id, floor, stamina_cost]`，`result_json` 保存本次积分/灵石/修为、是否战斗胜利、
  体力消耗与掉落列表；不新增表、列、迁移、索引或写入路径。
- 只保留 `floor > 0` 的行；被拒绝的结算（体力不足、状态冲突等）由写路径记为 floor 0，
  因此不会出现在记录里。时间窗为近 7 天且只用下界。
- `TowerHistoryApplication` 在 `read_only=True` 的 `DatabaseUnitOfWork` 中按
  `json_extract(payload, '$[0]') = 当前 user_id` 查询；缺库、缺表、缺列或读失败返回
  空态，不建表、不补列；不支持 JSON1 的旧 SQLite 降级为有界扫描后按 payload 过滤。
- 输出：近 7 天挑战次数、成功/失败、最高层、累计积分/灵石/修为，以及最近 10 次明细
  （时间、层数、成功/失败、积分或体力消耗、掉落摘要）；空态显示
  “近7天没有通天塔挑战记录。”。
- 帮助与发现性：`通天塔帮助` 增补条目与按钮，`修仙帮助`「流水与总览」登记；
  manifest 增加 `CommandSpec`（含别名），命令仍由 `xiuxian_tower` 注册。

## 灰度开关、回滚和已知限制

关闭开关即恢复旧入口；战斗算法、Boss 随机和读模型仍待迁移。

## 列表检索
`通天塔商店 [关键词|页码]`：商店列表按物品名/ID/描述筛选，翻页提示与按钮携带关键词
（`通天塔商店 灵草 2`），解析与续查走 `docs/features/base.md` 的「列表检索与翻页续查
（共享合同）」；筛选只改当次集合，不改积分扣减、周限购或兑换回执。

## Manifest 清单
- `command: 爬塔`
- `alias: 挑战通天塔`
- `command: 我的通天塔记录`
- `alias: 通天塔记录`
- `alias: 爬塔记录`
