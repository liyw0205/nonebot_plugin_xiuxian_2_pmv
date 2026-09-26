# 背包与物品
## 用户流程
礼包、物品、装备、技能、修复和炼丹动作通过一个操作号协调。
## 命令与别名
`背包`、`使用物品`、`装备`。
## Web API
`POST /api/v1/back/{open_package,use_item,change_equipment,learn_skill,repair,use_pet_eggs,alchemy,unbind}`，权限 `user`。
## 数据模型与迁移
迁移 `back.001` 至 `back.017`；饰品词条锁定 operation 与通用物品批处理、聚灵旗替换、装备 operation、背包修复任务和宠物蛋 operation 位于 game DB，player DB 仅作为显式 attached
事务参与方；历史背包字段通过兼容服务读取。
## 事务与失败回滚
操作 ledger、审计和异常重试由应用层统一提供。
## 定时任务
无。
## 配置项
`back_enabled`。
## 适配器差异
Web 和命令适配器不直接写库存。
## 测试与手工验收
覆盖重复提交、库存上限、物品不足和异常回滚。
## 灰度开关、回滚和已知限制
关闭开关回退旧背包入口；通用 Web 批处理只负责原子扣除背包物品，具体物品效果仍由各领域 handler/application 维护。

## Manifest 清单
- `route: POST /api/v1/back/open_package`
- `route: POST /api/v1/back/use_item`
- `route: POST /api/v1/back/change_equipment`（`EquipmentApplication` 默认路径，旧服务仅显式兼容）
- `route: POST /api/v1/back/learn_skill`
- `route: POST /api/v1/back/repair`
- `route: POST /api/v1/back/use_pet_eggs`
- `route: POST /api/v1/back/alchemy`
- `route: POST /api/v1/back/unbind`
