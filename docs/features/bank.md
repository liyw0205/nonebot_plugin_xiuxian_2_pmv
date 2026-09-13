# 灵庄资产结算

灵庄的存入、取出、会员升级和手动结息最终写入统一的 `BankApplication`。跨库余额与 `player_db.bankinfo` 更新仍由兼容仓储调用旧事务实现，application 在 `game_db` 维护统一 operation ledger 和审计记录。

## 接口

- `POST /api/v1/bank/deposit`
- `POST /api/v1/bank/withdraw`
- `POST /api/v1/bank/upgrade`
- `POST /api/v1/bank/interest`
- `POST /api/v1/bank/v2/deposit`（仅注入 `bank_first_use` service 时启用）
- `POST /api/v1/bank/v2/upgrade`（仅注入 `bank_first_use_upgrade` service 时启用）

写接口需要 `user` 权限、CSRF 和幂等键；旧 `灵庄` 命令继续由兼容入口负责消息排版。

迁移版本为 `bank.001`，灰度开关为 `bank_enabled` / `XIUXIAN_BANK_ENABLED`。

## 用户流程

用户依次执行存入、取出、会员升级或结息，失败时余额和会员状态保持不变。

## 命令与别名

旧 `灵庄` 命令及其别名全部保留，由兼容命令适配器转发。

## Web API

四个接口均为 `POST /api/v1/bank/{deposit,withdraw,upgrade,interest}`，权限为 `user`，支持 CSRF 与 `Idempotency-Key`。

## 数据模型与迁移

`bank.001` 写入 `game_db.bank_feature_migrations`；余额历史表仍由兼容仓储维护。

## 事务与失败回滚

application 先登记 operation ledger，再调用跨库仓储；异常标记失败并允许对账重试。

## 定时任务

自动结息仍由兼容调度器统一注册，禁止导入期重复注册。

## 配置项

`bank_enabled` / `XIUXIAN_BANK_ENABLED` 控制旧灵庄切片灰度；`bank_first_use_enabled` / `XIUXIAN_BANK_FIRST_USE_ENABLED` 控制新首次存款 Web/application 注入，默认关闭。新 v2 route 只有显式注入 service 时注册，旧路径保持可回滚。

## 适配器差异

命令适配器负责文案，Web 适配器负责 DTO、权限、CSRF 和 JSON envelope。

## 测试与手工验收

覆盖四项资产动作的成功、拒绝、异常回滚和重放；使用临时数据目录运行 Flask client 和恢复演练。

## 灰度开关、回滚和已知限制

关闭开关后旧命令继续可用；跨库历史状态待完整发布周期后再迁移。

## Manifest 清单
- `alias: 灵庄存灵石`
- `alias: 灵庄取灵石`
- `alias: 灵庄升级会员`
- `alias: 灵庄结算`
