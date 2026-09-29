# 灵庄资产结算

## 当前边界

- 旧 v1 Web API 仍由 `BankApplication -> LegacyBankRepository` 调用跨库 legacy transaction service；这条路径尚未完成重构。
- 默认 `灵庄` matcher 优先读取 game DB 的 `bank_accounts` 投影。缺少投影时，完整 legacy `player_db.bankinfo` 记录由 bootstrap 导入；没有 legacy 用户记录时，存款可创建默认账户，首次升级/结息会在同一 game DB 事务中创建默认账户并结算。
- matcher 的账户读取经 `BankAccountInfoApplication -> BankLegacyAccountReadRepository` 使用只读 player DB 查询。已有但不完整的用户记录或无效表结构不会落入写 fallback；显式 `savef` 仅作为兼容 writer 保留。
- v2 Web route 与 first-use matcher 仅在对应 service 显式注入后启用，使用 game DB-owned account applications。

## 接口

- `POST /api/v1/bank/deposit`
- `POST /api/v1/bank/withdraw`
- `POST /api/v1/bank/upgrade`
- `POST /api/v1/bank/interest`
- `POST /api/v1/bank/v2/deposit`（仅注入 `bank_first_use` service 时启用）
- `POST /api/v1/bank/v2/upgrade`（仅注入 `bank_first_use_upgrade` service 时启用）
- `POST /api/v1/bank/v2/interest`（仅注入 `bank_first_use_interest` service 时启用）
- `GET /api/v1/bank/v2/info`（仅注入 `bank_first_use_info` service 时启用）

写接口需要 `user` 权限、CSRF 和幂等键；旧 `灵庄` 命令入口保留消息排版，默认 matcher 已优先走账户投影 application。

`bank.001` 登记旧 bank 功能切片；`bank.002` 在启动阶段创建 game DB-owned `bank_accounts` 与 `bank_account_operations`。账户请求只校验 schema，不执行 DDL。旧账户按用户首次读取时 bootstrap 导入，不做批量 destructive rewrite。

## 用户流程

用户依次执行存入、取出、会员升级或结息，失败时余额和会员状态保持不变。

## 命令与别名

旧 `灵庄` 命令及其别名全部保留；默认 matcher 的新账户路径由 feature application 承载，未被接管的 fallback 仍是兼容边界。

## Web API

四个接口均为 `POST /api/v1/bank/{deposit,withdraw,upgrade,interest}`，权限为 `user`，支持 CSRF 与 `Idempotency-Key`。

## 数据模型与迁移

`bank.001` 写入 `game_db.bank_feature_migrations`。新账户与 operation receipt 由 `bank.002` 预建；历史 `bankinfo` 是只读导入来源，显式兼容 writer 与 v1 Web legacy repository 尚未移除。

## 事务与失败回滚

`BankApplication` v1 Web wrapper 先登记 operation ledger，再调用跨库兼容仓储；game DB account applications 在同一事务中校验余额/会员状态、写账户与回执。首次升级和结息的默认账户创建与资产结算也在同一事务中完成；升级不重置 `updated_at`。

## 定时任务

自动结息仍由兼容调度器统一注册，禁止导入期重复注册。

## 配置项

`bank_enabled` / `XIUXIAN_BANK_ENABLED` 控制旧灵庄切片灰度；`bank_first_use_enabled` / `XIUXIAN_BANK_FIRST_USE_ENABLED` 控制新首次存款 Web/application 注入，默认关闭。新 v2 route 只有显式注入 service 时注册，旧路径保持可回滚。

## 适配器差异

命令适配器负责文案，Web 适配器负责 DTO、权限、CSRF 和 JSON envelope。

## 测试与手工验收

覆盖四项资产动作的成功、拒绝、异常回滚和重放；使用临时数据目录运行 Flask client 和恢复演练。

## 灰度开关、回滚和已知限制

关闭开关后旧 Web 与命令兼容入口继续可用。待办：将 v1 Web 默认调用从 legacy transaction services 切到 feature-owned game DB applications，并补齐正式发布迁移、恢复/对账和灰度回滚证据；在完成前不能宣称 bank 全面重构。

## Manifest 清单
- `alias: 灵庄存灵石`
- `alias: 灵庄取灵石`
- `alias: 灵庄升级会员`
- `alias: 灵庄结算`
