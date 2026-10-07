# 灵庄资产结算

## 当前边界

- v1 Web API 默认由 `BankApplication` 调用 game DB-owned 的存入、取出、升级与结息 application；生产组合根不再注入 `LegacyBankRepository`。该 repository 仍可显式注入供回滚/兼容调用，不进入默认执行图。
- 默认正则 matcher 经 `BankCommandApplication.execute` 编排账户读取、回执查询、自动结息及存入/取出/升级/结息；适配器仅解析消息、构建操作号并调用 `render_bank_reply` 发送结果。v1 Web 保持独立的 `BankApplication` 入口，两个入口复用既有四个 game DB account application，不另造资产 writer。
- 命令 owner 与 Web application 只读取 game DB 的 `bank_accounts`。缺少投影表示新账户：信息查询返回 L1/零存款而不开户；存款、首次升级和结息可在同一 game DB 事务中初始化；取款对未开户账户拒绝。
- 命令优先查询同操作号的已提交回执，再读取当前账户、会员等级与结息快照；重放不受此后等级、余额或上限变化影响。存入、取出和结息向既有 writer 传入存款额、更新时间及等级快照，状态变化时拒绝结算。失败回复先按 `status` 分支，不读取仅成功结果才有的字段。
- v1 存款、取款与结息在 game DB 内核对请求中的存款额、更新时间和会员等级快照；旧账户导入后若快照过期则拒绝结算。空旧账户只接受默认零余额/L1 首次存款；已有但不完整或 schema 无效的旧账户 fail closed，不覆盖历史记录。
- v1 全局 operation ledger 与账户 application 回执分开提交时，重试会由账户回执防止重复资产变更，并补完 ledger 结果。
- `bank.003` 在启动阶段从 `player_db.bankinfo` 只读分批回填 game DB，每批最多 200 行；相同账户保留，已有 game DB 操作回执时保留已前进状态，无回执且冲突则中止迁移并回滚。该迁移预估目标表所需空间，低于预留值时拒绝启动，不改写旧表。
- matcher 不再按请求读取旧账户，也没有针对旧账户的写 fallback。管理员 `同步灵庄` 仅将 JSON 快照插入 game DB 中缺少的账户，已有账户绝不覆盖；显式 `savef` 仅作为兼容 writer 保留。
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

`bank.001` 登记旧 bank 功能切片；`bank.002` 在启动阶段创建 game DB-owned `bank_accounts` 与 `bank_account_operations`；`bank.003` 一次性导入旧账户。账户请求只校验 schema，不执行 DDL，也不访问旧账户存储。

## 用户流程

用户依次执行存入、取出、会员升级或结息，失败时余额和会员状态保持不变。

## 命令与别名

旧正则命令入口及其操作语法保留；账户编排已集中到 feature command owner，不再由 matcher 直接调用四个 writer 或旧存储。manifest 中的 `灵庄` 及别名仍是声明，不能据此声称存在同名 `on_command` matcher。

## Web API

四个接口均为 `POST /api/v1/bank/{deposit,withdraw,upgrade,interest}`，权限为 `user`，支持 CSRF 与 `Idempotency-Key`。生产默认路径由 `BankApplication` 编排 feature-owned game DB account application。

## 数据模型与迁移

`bank.001` 写入 `game_db.bank_feature_migrations`。新账户与 operation receipt 由 `bank.002` 预建；历史 `bankinfo` 是只读导入来源。显式兼容 writer 与 `LegacyBankRepository` 仍保留，但不再由生产 v1 Web 默认注入。

## 事务与失败回滚

`BankApplication` v1 Web wrapper 先登记 operation ledger，再调用 game DB account application；每个 account application 在同一事务中校验余额/账户状态、写账户与回执。首次升级和结息的默认账户创建与资产结算也在同一事务中完成；升级不重置 `updated_at`。如果账户回执已提交但总账未完成，operation 重试从幂等账户回执恢复总账。

## 定时任务

银行没有自动结息 scheduler；命令操作携带的自动结息由 command owner 计算并与账户动作一并提交，显式结息也可由用户命令或 Web 操作触发。`features/bank/jobs.py` 保持空任务表。

## 配置项

`bank_enabled` / `XIUXIAN_BANK_ENABLED` 控制旧灵庄切片灰度；`bank_first_use_enabled` / `XIUXIAN_BANK_FIRST_USE_ENABLED` 控制新首次存款 Web/application 注入，默认关闭。新 v2 route 只有显式注入 service 时注册，旧路径保持可回滚。

## 适配器差异

命令适配器负责消息边界，`command_replies.py` 按 feature 返回的 DTO 格式化成功、拒绝与缺账户信息。Web 适配器负责 DTO、权限、CSRF 和 JSON envelope。

## 测试与手工验收

覆盖四项资产动作的成功、拒绝、异常回滚和重放；使用临时数据目录运行 Flask client 和恢复演练。

`tests/test_bank_progress_contract.py` 绑定实际正则 handler、command owner、回执 reader 与既有账户 application 的源码调用边，并通过内存源码变异防止旧 writer 回流或回执优先级退化。新增实现仍须串行运行对应行为测试和门禁，本文不作为本轮验证通过的记录。

## 灰度开关、回滚和已知限制

关闭新功能开关后兼容 Web 与命令入口仍保留；显式 `savef` 仅供兼容写入。v1 Web 默认执行切换和历史账户启动回填仍需正式发布迁移、恢复/对账与灰度回滚证据；这些证据完成前不能宣称 bank 全面重构。

冻结 v1 中 bank 没有独立受阻命令成员；manifest-only `灵庄` 保持“不可达”。实际正则 matcher 属于共享 `legacy.matcher.non_command_dispatch`，本次 owner 收口不新增冻结成员，也不代表该跨插件 family 已完成。

## Manifest 清单
- `alias: 灵庄存灵石`
- `alias: 灵庄取灵石`
- `alias: 灵庄升级会员`
- `alias: 灵庄结算`
