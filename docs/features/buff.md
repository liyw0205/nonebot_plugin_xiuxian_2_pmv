# 功法与洞天福地
## 用户流程
购买、开垦、改名、闭关和结算操作都通过应用服务提交。
## 命令与别名
`功法`、`洞天福地购买`、`洞天福地查看`。
## Web API
`POST /api/v1/buff/{open,upgrade_field,rename,training_start,training_complete,stone_training,pvp_settle}`，权限 `user`，要求幂等键。
## 数据模型与迁移
迁移 `buff.001` 至 `buff.007`；双修令牌 operation 在 game DB，次数 projection 在 player DB，分别由 `buff.002`/`buff.003` 创建。双修结算 operation 由 game DB `buff.004` 创建；`buff.005` 在 player DB 补齐关系、保护、邀请与统计 schema。普通切磋回执由 game DB `buff.006` 预建，切磋胜负统计列由 player DB `buff.007` 预建。
## 事务与失败回滚
统一 operation ledger 和审计，旧事务异常可重试。
## 定时任务
无。
## 配置项
`buff_enabled`。
## 适配器差异
命令/Web 仅组装 DTO，不直接访问数据库。
## 测试与手工验收
覆盖 fake repository 成功、拒绝、重复和异常路径。
## 灰度开关、回滚和已知限制
关闭开关后使用旧功法入口；双修令牌默认经 `PartnerTokenUseApplication` 原子消费库存并更新次数。双修结算默认经 `PartnerCultivationApplication`，把修为、属性、次数、统计、亲密度和邀请状态置于同一 attached UoW；随机计算仍由领域 handler 预滚。普通切磋默认经 `BuffApplication -> NormalPvpSqlRepository`，在 attached UoW 内完成双方 HP/MP/体力 CAS、胜负统计和回执，缺 schema、用户或快照冲突时 fail closed；请求路径不执行 DDL。旧 `NormalPvpSettlementService` 仅保留显式兼容用途，`pvp_battle` 只承载纯战斗计算适配。双修/师徒默认 handler 的姓名、境界、修为和存在性读取统一经 `PlayerProfileApplication`；动态战斗属性仍由 `get_final_attributes` 兼容边界提供。

## Manifest 清单
- `route: POST /api/v1/buff/open`
- `route: POST /api/v1/buff/upgrade_field`
- `route: POST /api/v1/buff/rename`
- `route: POST /api/v1/buff/training_start`
- `route: POST /api/v1/buff/training_complete`
- `route: POST /api/v1/buff/stone_training`
- `route: POST /api/v1/buff/pvp_settle`
