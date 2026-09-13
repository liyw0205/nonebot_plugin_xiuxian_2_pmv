# 功法与洞天福地
## 用户流程
购买、开垦、改名、闭关和结算操作都通过应用服务提交。
## 命令与别名
`功法`、`洞天福地购买`、`洞天福地查看`。
## Web API
`POST /api/v1/buff/{open,upgrade_field,rename,training_start,training_complete,stone_training,pvp_settle}`，权限 `user`，要求幂等键。
## 数据模型与迁移
迁移 `buff.001`；历史玩家表继续由兼容仓储读写。
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
关闭开关后使用旧功法入口；历史数值规则暂由旧服务持有。

## Manifest 清单
- `route: POST /api/v1/buff/open`
- `route: POST /api/v1/buff/upgrade_field`
- `route: POST /api/v1/buff/rename`
- `route: POST /api/v1/buff/training_start`
- `route: POST /api/v1/buff/training_complete`
- `route: POST /api/v1/buff/stone_training`
- `route: POST /api/v1/buff/pvp_settle`
