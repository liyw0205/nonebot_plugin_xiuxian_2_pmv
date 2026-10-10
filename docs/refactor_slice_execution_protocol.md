# 重构有限收口执行入口

状态：2026-10-10 恢复 C3 本地收口和提交推送，当前发布 P7 未完成。唯一执行入口。
执行范围：[refactor-closeout-v1 固定清单](refactor_closeout_scope_v1.md)。
状态与验收结果写入[当前进度](full_refactor_progress.md)，架构契约见
[架构规范](refactor_architecture.md)，旧记录见[历史归档](archive/refactor-2026-10-09/README.md)。

## 目标与停止条件

本轮整合停止时工作树，完成固定清单本地收口及审查，提交推送成功并核验远端后停止。
用户已于 2026-10-10 授权必要修复、提交及普通推送；C3 的必需测试维护属于当前范围。
先修已知八项收集错误和两项 Dongfu 旧源码断言；全量揭示的真实回归按证据最小修复。
C1-C4 本地验收实际通过才可收口；P7 缺真实发布条件独立记录，不阻止本地验收和推送。
不重新启动按玩法持续审计、重构、写账本、选择下一片的循环。
提交数量、剩余旧 helper 数量、目录数量和任意布尔字段都不是完成分母。

可复用 goal：

```text
/goal 完成refactor-closeout-v1本地收口，按唯一入口修复C3并提交推送origin/refactor/full-bottom-layer。
```

具体目标仍是 C1-C5：复用未受影响的已通过证据，修复必要测试阻塞并完成最终根目录全量，
独立记录 P7，按白名单提交到当前重构分支并普通推送 `origin/refactor/full-bottom-layer`。
禁止将 Phase 2 通过当作全量或 P7 通过，禁止新增玩法、从历史“下一片”或任意 false 扩范围。

清单外发现只记录 backlog 的入口、证据、影响和 owner，不自动实施。
改变冻结目标需显式范围版本更新；同一固定项内为修正已证实缺陷所需的小修改属于当前工作。
本轮必需的 collection、夹具和过期证据修复也属于 C3，不是自动开启玩法迁移。
固定项受阻时记录原因和缺失条件，继续可独立完成的固定项；条件未变化不反复跑同一审计。

## 四种状态分别判断

| 范围 | 完成依据 | 本轮边界 |
|:--|:--|:--|
| Phase 2 v1 | 冻结 496 项的四态、调用图、membership/provenance 和完整性门禁 | 有限旧路径清单，不是全仓所有玩法 |
| 技术收口 C1-C4 | 固定改动整合、缺陷回归、P0-P6 与隔离迁移恢复证据 | 通过或受阻均需有证据，不能只读旧记录 |
| 当前发布 P7 / C5 | 当前发布周期的数据目录、版本、恢复/兼容证据 | 独立判定，不沿用 v1.1.0 历史通过结果 |
| 后续玩法与风险 | 另行冻结的 scope 和验收目标 | backlog，不是当前执行队列 |

Phase 2 `496` 项的 `scope_id=phase2-default-runtime-legacy-paths-v1` 不变，
membership SHA256 为
`7787a74a3e0c15c220a5f257921693b14706ef23b224bac9f3dad55a1ff51fac`。
[冻结项](refactor_phase2_legacy_path_items.json)及[scope manifest](refactor_phase2_legacy_paths.json)
是有限分母；不得加条目、改 hash 或调整门禁来制造完成。

`scripts/check_full_refactor_progress.py` 的 `phase2_complete` 和 `exit_ready`
都来自 Phase 2 scope 的 `ready`，`exit_blockers` 也仅反映该 scope。
它返回 0 或 `exit_ready=true` 不证明 P0-P6、P7、真实运行、全量测试或全部玩法完成。
`scripts/refactor_completion_audit.py` 的 P0-P6 和 P7 必须分别报告。

## 固定执行顺序

1. 一次读取本入口、固定清单、当前进度、工作树及相关 owner 证据。
   保存 HEAD、未提交文件白名单和已有失败基线；保留用户修改。
2. 按 C2 逐项确认当前默认入口与最小复现，先辨别真实产品缺陷、测试夹具缺陷和过期证据。
   只修已证实问题，不因为 helper 存在或旧静态字符串缺失再次迁移已关闭入口。
3. 运行与变化对应的 focused 回归；共享 SQLite/NoneBot 夹具和恢复任务串行。
   按 C3/C4 完成统一验收，修改产生新风险或失败时才扩大相关验证。
4. 在当前进度逐项记录实际验收及独立发布阻塞；C3 没有全量通过不能提前 complete 或暂停交付。
   审查本轮相关源码、测试、文档及归档，按明确文件清单暂存，排除用户数据和 `boss_info.json`。
   提交并普通推送当前重构分支，确认 `git ls-remote` 与本地 HEAD 一致后结束。
   只清理本任务实际创建且进程已退出的临时产物；不强推、不合 main、不发版。

## 证据与测试解释

- 布尔字段先查语义、源码和断言的预期值。旧 `sign_in.task_core_legacy`、
  `lottery_core_default_legacy`、`lottery_compatibility_fallback` 表示旧路径存在，
  `false` 可以是正确结果；当前正向替代字段不能与旧字段混用。
- slice 诊断、文本 marker、源码大小和计数是线索，不能代替 gate 的 `ready/errors`
  与行为复现。`compensation` 旧 marker 已进入固定证据核验，不触发新玩法重构。
- 真实缺陷需要输入、默认入口、错误行为和预期行为；测试/导入失败需要区分夹具、
  collection、依赖与真实路径，不得一律归因于业务或一律当作“既有可忽略”。
- 修改证据检查时需保持原行为要求，并用当前调用链与行为回归证明；禁止删断言、
  跳过失败用例、扩大允许名单、改 membership 或降低阈值来转绿。
- 记录实际命令、基线、结果、环境和未运行项，不把测试耗时写成开发工时或生产延迟。
  旧记录只能说明当时结果；任何“通过”必须注明本轮是否复跑。

## 验收与资源边界

C3 包含改动回归、最终根目录全量测试实际执行通过、架构/P0-P6、inventory freshness、
Phase 2 完整性和 diff 检查；先完成已知阻塞的最小修复与定向验证，再跑一份最终全量。
收集报错或只通过定向测试不等于完成全量；不新增 skip/xfail/ignore，不删行为断言降低门槛。
已有 C1/C2/C4 和门禁结果仅在相关代码/内容/配置/依赖状态未变时复用；变化按影响补验。
只在新修改、真实失败或未解风险需要时扩大或重跑，文档变化不重复业务长链。
C4 包含隔离的 backup、migration dry-run/apply、readiness、restore 与 reconcile；
无真实当前发布证据的隔离测试不能冒充 P7。文档单独变化只检查链接、源码要求的
文档 token 与 diff，不重新跑业务测试长链。

RAM available 低于 512 MiB、磁盘可用低于 10 GiB 或 inode 异常时不启动新重型
测试/恢复任务，记录受阻条件；不清空系统缓存或终止服务来制造余量。
测试默认禁用 pytest cacheprovider 与字节码写入，使用任务专属临时目录。
资源不足不免除验收，不得将未运行写成通过。

仅可删除本任务有明确创建记录、所有所属进程已退出且路径逐项核对的临时产物。
绝不删除 `/tmp/codex-daemon-*`、Codex IPC/socket/lock、其它活跃代理的临时目录，
也不对 `/tmp` 做通配清理。unlink 活跃 Unix socket 会使新 RPC 无法连接，即使旧连接仍可用。
解决重构循环不能依靠清空系统缓存、杀 daemon 或删除其它任务目录。
`.venv`、`.git`、`data/`、配置、备份、运行数据库/WAL/SHM、
operation ledger/outbox/receipt/projection/审计记录和用户 `boss_info.json` 均不是清理对象。
`ITEMS_CACHE` 等业务共享缓存不复制、不主动清空。

已授权子代理任务需限定独立文件与边界；主线程统一整合、串行测试和验收。
没有明确委派时不自动增开代理，不让多个代理写同一工作树。
提交、推送和发布遵循当前会话授权及文件白名单，文档不授权自动提交所有脏文件。

## 已冻结 Phase 3 与后续边界

保留已有 scope 证据，仅固定 C2 的闭关缺陷进入本轮，不从此节继续领取玩法：

- `scope_id=phase3-player-lifecycle-v1` / `command:buff:闭关`：
  `BuffApplication.closing_enter` 与原有 game/player ownership；
  当前拒绝结果重放问题需最小回归核验，不能凭历史描述判完成。
- `scope_id=phase3-player-lifecycle-v2` / `command:buff:出关`：
  replay-first legacy snapshot adapter、`ClosingRewardCalculator` 纯收益计算，
  `BuffApplication.closing_settle` 仍为 mutation owner，operation 前缀 `buff-closing-settle:`。
  game receipt/CAS、跨事务恢复与完整出关迁移不由计算测试证明。
- `scope_id=phase3-player-lifecycle-v3` / `command:buff:出关:effects-reconcile`：
  `ClosingEffectsApplication` 的 outbox dispatch/reconcile 与稳定 projection receipts。
  legacy sink、game CAS、请求期隐式初始化与跨库恢复仍按原 scope 排除。

`虚神界出关`、`虚神界闭关`、impart_pk 其它路径、未来玩法和持久历史清理
均保持 backlog；不改 Phase 2 的 496 项，不因发现独立问题自动更新当前清单。
