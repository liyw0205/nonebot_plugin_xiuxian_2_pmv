# 地图探索与战斗
## 用户流程
移动、回城、交互、战斗、探索、任务和资源领取统一记录操作号。
## 命令与别名
`地图`、`探索`、`回家`。
## Web API
`POST /api/v1/map/{move,return_home,interactive_start,interactive_finish,combat_start,combat_settle,explore_start,explore_settle,resource_reward,mission_claim,purchase_seed,build_dongfu}`，权限 `user`。
## 数据模型与迁移
迁移 `map.001`；地图旧状态和掉落表由兼容仓储持有。
## 事务与失败回滚
新层统一 ledger，跨库旧事务失败进入可对账状态。
## 定时任务
无。
## 配置项
`map_enabled`。
## 适配器差异
消息层不包含战斗算法。
## 测试与手工验收
覆盖体力不足、状态变化、背包容量和幂等重放。
## 灰度开关、回滚和已知限制
关闭开关回退旧地图 handler；战斗数值仍由兼容服务维护。

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
