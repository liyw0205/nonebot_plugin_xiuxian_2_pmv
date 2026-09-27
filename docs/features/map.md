# 地图探索与战斗
## 用户流程
历史命令包继续作为 transport adapter；移动、回城、交互、战斗、探索、地图委托、种子购买和建设洞府的默认路径均进入 `MapApplication`、feature repositories 或 `CombatSettlementApplication`。
## 命令与别名
`地图`、`探索`、`回家`。
## Web API
`POST /api/v1/map/{move,return_home,interactive_start,interactive_finish,combat_start,combat_settle,explore_start,explore_settle,resource_reward,mission_claim,purchase_seed,build_dongfu}`，权限 `user`。
## 数据模型与迁移
地图平台 schema 与各 asset operation schema 由启动迁移 `map.001` 至 `map.016` 管理，按 game/player 数据库路由；请求路径不依赖旧 transaction service 创建回执表。
## 事务与失败回滚
默认 repository 保留各动作的 operation replay、状态快照校验和失败回滚。旧事务实现集中在 `compatibility/legacy_map_transactions.py`；历史 `xiuxian/xiuxian_map/transaction_service.py` 只 re-export 旧名字。默认路径不调用这些实现；显式注入的 `LegacyMapRepository`、`LegacyCombatSettlementRepository` 或旧 `*_service` wrappers 仍可调用。此隔离未改变 ledger/schema，不需要数据库回滚。
## 定时任务
无。
## 配置项
`map_enabled`。
## 适配器差异
消息层不包含战斗算法。
## 测试与手工验收
覆盖体力不足、状态变化、背包容量和幂等重放。
## 灰度开关、回滚和已知限制
战斗引擎、Items/配置解析和静态地图 JSON 仍由显式旧 provider/transport adapter 提供；这不把事务 service 计入默认执行图。旧 service 仍可经 shim 导入，显式 rollback repositories 保持可用。此切片无 migration；既有 `map.001` 至 `map.016` 若需回退，应先恢复代码版本，不删除迁移数据。

## Manifest 清单
- `route: POST /api/v1/map/move`
- `route: POST /api/v1/map/return_home`
- `route: POST /api/v1/map/interactive_start`
- `route: POST /api/v1/map/interactive_finish`
- `route: POST /api/v1/map/combat_start`
- `route: POST /api/v1/map/combat_settle`
- `route: POST /api/v1/map/explore_start`
- `route: POST /api/v1/map/explore_settle`
- `route: POST /api/v1/map/resource_reward`
- `route: POST /api/v1/map/mission_claim`
- `route: POST /api/v1/map/purchase_seed`
- `route: POST /api/v1/map/build_dongfu`
