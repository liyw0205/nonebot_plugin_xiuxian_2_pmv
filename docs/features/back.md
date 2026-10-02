# 背包与物品
## 用户流程
礼包、物品、装备、技能、修复和炼丹动作通过一个操作号协调。
## 命令与别名
`背包`、`使用物品`、`装备`。
## Web API
`POST /api/v1/back/{open_package,use_item,change_equipment,learn_skill,repair,use_pet_eggs,alchemy,unbind}`，权限 `user`。
## 数据模型与迁移
迁移 `back.001` 至 `back.017`；饰品词条锁定、洗练、分解、升阶和预设 operation 共用 game DB 的饰品操作表，通用物品批处理、聚灵旗替换、装备 operation、背包修复任务和宠物蛋 operation 也位于 game DB。player DB 仅作为显式 attached 事务参与方；attached accessory schema 由 `accessory_package.player_data.001/.002/.003` 启动迁移维护，其中 `.003` 提供三个预设列，历史背包字段通过兼容服务读取。
## 事务与失败回滚
操作 ledger、审计和异常重试由应用层统一提供。
## 定时任务
既有 `daily_reset_day_num` 仍由 scheduler 在每日零点执行，写入改由
`DailyPillUsageResetApplication -> DailyPillUsageResetSqlRepository` 承担。按 scheduler 时区生成
业务日回执；同日重放不重复清零，只有 `goods_type='丹药'` 的 `day_num` 被更新。计数、共享
operation ledger 和 audit 在同一 immediate UoW 提交；缺少既有 platform schema 或背包列时 fail closed，
不请求期建表，不增加业务 migration。
## 配置项
`back_enabled`。
## 适配器差异
Web 和命令适配器不直接写库存。
## 测试与手工验收
覆盖重复提交、库存上限、物品不足和异常回滚。
- `python -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/back/tests/test_daily_pill_usage_reset.py -q`
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
