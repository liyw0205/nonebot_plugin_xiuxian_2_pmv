# 当前项目状态

最后核验：2026-10-17。本文是唯一活跃进度入口；旧重构文档仅作为历史记录，不再定义当前分支或禁止事项。

## CI 稳定化（2026-10-12）

- develop/main 已同步至 `0c55b1fa`：quality job 通过 `fetch-depth: 0` + `fetch-tags: true` 取得真实 tag，`test_repository_has_no_release_after_the_only_tag` 在 run 1497/1498 语境下不再复现。
- run 1498（main）暴露第二个共享缺陷：`DatabaseUnitOfWork.__enter__` 先执行 `PRAGMA journal_mode=WAL` 后才设 `busy_timeout`，且 WAL 模式转换不等锁、直接抛 SQLITE_BUSY，两个并发首次打开同一库会在进入 UOW 时互锁失败（sect 周常进度并发测试复现）。修复：`__enter__` 先设 `busy_timeout`；新增 `_ensure_wal` 只在库头不是 wal 时转换一次，并在 timeout 预算内对 busy 重试；回归测试 `tests/test_uow_attach.py::test_concurrent_first_opens_converge_on_wal_without_busy_error`。
- 工作区 `docs/refactor_phase2_legacy_path_items.json` 已由用户明确删除，保持未暂存、不提交、不推送。

## 活动中心切片进展（2026-10-14）

- 第二切片 `9ad7ee8c`（CI run 38505422761 success）：状态入口映射 `commands_for_status`、单卡聚合+玩法一行汇总、只读个人进度行、QQ 蓝字/降级合同，均限冻结清单范围。
- 时间提示切片：目标文档 `docs/goals/activity-time-hints.md`；`center.time_hint` 配置驱动倒计时（进行中"剩余 N 天/小时"、未开始"N 天后开启"、无限/已结束不显示），渲染在玩法行后、奖励行前；聚焦测试 `tests/test_activity_center.py` 6/6。非目标（阶段倒计时/提醒推送/新配置键）见目标文档。
- 签到状态行切片：目标文档 `docs/goals/activity-center-sign-line.md`；`personal_progress_summary` 复用 `overview_snapshot` 的 `sign.last_sign_date` 与结算侧同一"今日已签"语义，在进度行行首追加"签到 已签/未签"，只读、无新查询/配置键；聚焦测试 `tests/test_activity_center.py` 7/7（历史 6 项断言未改）。
- 投递合同切片：目标文档 `docs/goals/activity-center-delivery-contract.md`；`tests/test_activity_center_delivery_contract.py` 5/5 覆盖 OneBot V11 纯文本降级、QQ markdown 关闭同合同、QQ markdown 开启保留 `mqqapi://aio/inlinecmd` 原生蓝字（换行按既有约定归一为 `\r`）与降级幂等，另含 handler 接线源合同；生产代码零改动，仅修正一处测试期望；受影响 `tests/test_activity_center.py` 7/7。
- 来源级领奖入口完整目标（本轮已完成）：活动中心在同一只读 overview snapshot 上按真实资格追加战令、首领进度、首领排行快捷入口，并把阶段/排行入口收紧为实际可领；五类来源保持一键领取首项、稳定顺序和跨适配器降级合同。无新表/迁移/配置键，缺少可选状态表时保持旧库兼容；`tests/test_activity_center_claim_sources.py` 覆盖单来源、组合去重、缺少状态、旧库兼容和参数化蓝字。正式说明已写入 `docs/features/activity.md`；develop CI run `38883608353`、main CI run `38884198589` 均成功。
- 活动详情 Web 交互闭环：配置驱动的 `detail` 视图复用既有只读读模型；活动页可切换中心/详情/阶段/排行/首领视图并按需输入玩家 ID，个人状态要求现有管理权限。不新增表、迁移、配置或奖励写路径；精简旧库和缺失数据库均保持只读回退。聚焦测试 `tests/test_activity_web_state.py` 13/13；develop CI run `38941319083`、main CI run `38942373734` 均成功。正式 API 与交互合同已写入 `docs/features/activity.md`；本轮不部署。

## 主线与已交付

- `main` 是安装/正式 Release 分支。本轮开始时 `main`、本地 `develop`、`origin/main`、`origin/develop` 均指向 `2fd77d84`；该基线已包含 `c7a9d45b` 阶段/排行入口及来源级领取、活动详情交互，develop/main CI 分别由 run `38941319083`、`38942373734` 验证成功，不重做这些功能。后续合并策略：先在 develop HEAD 完成功能、CI 全绿并通过审查后才普通快进合入 main。
- `develop` 是唯一活动开发分支（`efcad54f` 起自 main 创建并推送）。合并历史：`0c55b1fa`（CI tag 抓取）、`3d7fcb84`（UOW WAL 锁）、`a21649b3`（installer tests 误报修复）已按 CI 门逐段快进。
- C1-C4 与既定重构收口已经合入 main，不重开约 2,000 个旧提交的重构盘点。
- 活动中心第一切片 `910aefbb` 及适配器降级修正 `592fdf7a` 都是 main 的祖先；字体修复 `4344796b` 已在 main。
- 旧 `refactor/full-bottom-layer` 和其他历史分支/worktree 保留，不删除或盲目合并。

## Release 与部署实况

- GitHub 当前真实 latest Release 为 `v1.0.0`，资产 `project.tar.gz`，目标 `97f43acba8dd185111d998c48d4e1cf5a069b117`。
- `v1.1.0` tag 指向 `4083da45ce865ad2e3deb8bad965668fa72d3fba`，`v1.2.0` tag 指向 `efcad54f444a593aab364ce0a0472496a99dc1d3`，两者 Release API 均返回 404（tag 存在但无对应 Release）。全部旧 tag 均不可移动；包含当前修复的新 Release 尚未发布。
- `/root/xiu2` 是目标实例。`/bin/xiu2_start` 使用 `/root/myenv` 并在 `/root/xiu2` 执行 `nb run --reload`；当前控制器是旧脚本，不具备新版安装器的 Release 参数。
- 已安装插件无 `.git` 元数据，不能证明精确 commit/tag。`card.py` 文件时间为 2026-08-20，SHA256 为 `140700ca2fbf67fbd23c7ccc9082d55bd10194d27af391c07cee3b5587fe9a8e`，与 main 字体修复不同。运行中的 screen 会话曾被观察到；部署更新/重启及当前加载版本待验。
- 数据目录 `/root/xiu2/data/xiuxian` 有五个根目录库：`message.db`、`player.db`、`trade.db`、`xiuxian.db`、`xiuxian_impart.db`，另有活动中心的独立 `activity/activity.db`。配置文件为 `.env`、`.env.dev` 和 `data/xiuxian/config.json`；`.env.dev` 含 `QQ_BOTS` 与 PostgreSQL DSN 键。值不读取、不输出。尚未做停机一致备份，因此禁止覆盖部署。
- QQ 官方机器人配置键存在不等于平台连接通过；QQ 连接状态需从本轮启动证据单独核验。官方 QQ 消息呈现/权限未做真机验收，不发送真机消息，也不将其宣称通过。
- 本轮先核实 Release 质量门和实例的真实加载/数据路径；停机冷备份并验证复制一致后，才可下载 Release 覆盖插件代码。不得运行会改写共享 `/root/myenv` 的依赖升级。

## 已完成目标：活动签到 Web 代办

本轮沿活动中心路线图推进到受保护的签到写交互，不重做来源级领奖入口、阶段/排行快捷入口、详情视图或已通过的 QQ/OneBot 投递合同。只允许管理员对已选玩家 ID 发起代签到；不引入未绑定 Web 身份的玩家自助写入口。

- `POST /api/v1/activity/sign-in` 以 CSRF 和 `Idempotency-Key` 保护，复用现有 `ActivityApplication -> claim_sign`。活动开关/阶段、每日唯一、奖励配置、用户存在和背包上限仍以现有业务事务为准。
- `activity_sign_settlement_operations` 与签到状态/奖励写入保持既有事务；外层 `activity.claim_sign` 留在 `started` 而签到回执已提交时可恢复为 duplicate。仅扩展该动作的恢复白名单，不放宽其他动作。
- 请求前只读检查签到、玩法事件及 operation/audit schema；缺失文件或旧 schema 返回 503，不请求期建库、建表、补迁移或写独立旧活动库。不改数据模型、奖励语义或配置。
- 管理员活动面板要求显式玩家 ID，复用幂等键重试可恢复错误，完成后刷新玩家状态。正式接口与状态合同在 `docs/features/activity.md`。
- 新聚焦套件 `tests/test_activity_sign_web.py` 7/7 通过，包含真实隔离库结算、奖励只发一次、外层收据恢复、权限/CSRF 和旧库失败关闭；ES module 语法、目标 Python 编译及 `git diff --check` 通过。
- 目标提交 `e9ebc4d4` 已推送 develop 并快进合入 main；develop CI run `38947837210`、main CI run `38948727873` 均成功。未部署、未触碰真实游戏数据。

## 已完成目标：活动中心 Web 一键代领

沿活动中心路线图推进到管理员活动面板的一键代领闭环；阶段/排行入口、来源资格、详情视图、签到 Web 代办与消息投递合同均是已完成基线，不重做。玩家本人仍通过既有 bot `活动领取` 命令领奖；本目标不新增 Web 玩家身份模型。

- Web 领奖路由与 feature manifest 收紧为 `admin`，要求 CSRF、显式玩家 ID 和稳定 operation ID；仅接受管理员代办，修正既有 `user` 权限可由普通请求任意指定 `user_id` 的边界。
- 管理员活动页复用当前玩家 ID 调用既有 `ActivityRewardApplication` 六步协调器，保持来源顺序、child operation ID、逐步回执、旧四步回执兼容回填及 bot 命令路径；同键重放稳定，部分失败续跑时不重发已完成子操作。
- 请求前只读检查 game DB 文件、`operation_ledger`、`operation_audit` 与六步协调器 schema；缺库、读错误或旧 schema 返回 503，不请求期创建文件、表或迁移；不新增表、版本、奖励语义或配置键。
- 面板提供空玩家禁用、操作忙碌时锁定玩家/视图、保持同一玩家和键的可恢复重试、业务结果反馈及操作后状态刷新。接口、权限、错误 envelope 与交互写入 `docs/features/activity.md`；`docs/refactor_inventory.json` 同步管理员权限。
- `tests/test_activity_reward_web.py` 与 `tests/test_activity_reward_application.py` 聚焦套件 `15/15` 通过，覆盖权限/CSRF、参数、缺库旧 schema 无 DDL、六步顺序、成功/replay、异玩家 operation 冲突、部分失败续跑、旧 body operation ID、manifest 与面板合同。JS/Python 语法检查、inventory 与 architecture 门禁通过；diff check 在提交前复核。
- 本轮只改本目标白名单中的 API、管理员面板、manifest、测试与正式文档。目标提交 `a2214a29` 已推送 develop 并快进合入 main；develop CI run `39018775656`、main CI run `39019137012` 均成功（quality job 通过，build-and-release 按无 Release 跳过）。未部署，未读取或写入真实游戏库。

## 已完成目标：活动详情视图行动建议对齐一键领取来源

沿活动中心路线图推进到活动详情 Web 视图（`view=detail`）的行动建议段；中心卡片、
一键代领、签到代办、详情视图与投递合同均是已完成基线，不重做。

- 详情视图带 `user_id` 时按配置请求完整领取状态快照（结束阶段窗口、排行来源含 boss 板、
  首领活动 key），行动建议段复用中心 `detail_action_tips` 判定，覆盖任务、战令与追赶、
  集字保底、阶段、排行、首领进度、首领排行，与活动中心一键领取来源集合一致。
- 修复既有首领排行名次读取缺陷：boss 板位此前用主活动 key 查 `activity_boss_damage`，
  生产配置中首领活动 key 不同（如 `festival_world_boss`），名次恒为 0，中心与详情均无法
  提示首领排行可领；现按首领玩法活动自身 key 读取。
- 只读复用既有 `overview_snapshot`，同一单次快照，不新增表、迁移、配置键、查询次数或
  写路径；无 `user_id` 或快照缺来源键时详情输出不变。
- `tests/test_activity_detail_actions.py` 聚焦套件 `4/4` 通过，覆盖四来源单独可领、
  中心/详情一致性、无资格不误报、首领 key 修复；受影响活动套件（web_state、center、
  claim_sources、claimable、reward_web、sign_web）合计 `58/58` 通过。architecture、
  inventory 与 progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `center.py`、`read_model_application.py`、`read_model_repository.py`、
  `stage_participation.py`、新聚焦测试与正式文档。目标提交 `d5a25d32` 与快照单单元修正
  `e64ee107` 已推送 develop 并快进合入 main；develop CI run `39054068758`（首推
  `39042548289` 因快照多单元失败，已修正）、main CI run `39054522726` 均成功。
  未部署，未读取或写入真实游戏库。

## 当前完整目标：NewAPI 签到历史写入收口

沿 Phase 2 backlog 项 `newapi-checkin-remote-history-window` 收口真实风险：远端签到
POST 与本地三条历史写入无法原子提交，进程在远端接受后、历史写入前崩溃会留下
"远端已签、本地无记录"的永久不一致；原实现历史写入失败只降级为无类别提示。

- `_run_checkin_for_target` 的历史写入抽为 `_record_checkin_history_with_recovery`：
  首次失败后同进程补偿重试一次（仅本地历史写入，不重放远端 POST——远端同日重复
  签到返回明确"已签"消息，但补偿丢失历史行只需本地写入）；重试仍失败才降级为带
  原因类别（数据库不可用/历史不可写）的 `_history_warning`，签到结果本身不变。
- 自动签到调度逐账号异常隔离行为不变；不改远端调用次数、历史表结构、3 条上限、
  展示格式或命令合同；不新增配置键或后台任务。
- `tests/test_entertainment_newapi_history_recovery.py` 聚焦套件 `6/6` 通过，覆盖
  重试成功、双失败分类警告、非数据库失败类别、成功不重试、远端错误仍记失败摘要、
  补偿不重放远端；受影响合同套件（checkin/auto/client/owner）合计 `20/20` 通过。
  architecture/inventory/progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `newapi_commands.py`、新聚焦测试、`tests/test_entertainment_checkin_contract.py`
  （AST 合同随委托更新）与正式文档；develop 推送/CI、审查与 main 快进/CI 按发布门
  继续完成。未部署，未读取或写入真实游戏库，不发送真实消息。

## 冻结清单与停止条件

冻结范围仅包括活动 Web 六来源一键代领 API、权限/错误合同、管理员面板、兼容性保护、聚焦测试、路由清单和正式文档。不得新增奖励经济、schema/数据迁移、玩家身份模型、后台活动编辑器、通用 UI 框架或 unrelated refactor。

成功条件：本轮目标的状态、持久化兼容、交互、聚焦测试和正式文档均完成；diff 检查通过；只提交目标代码、测试和正式文档到 develop，CI 成功且审查通过后普通合入 main 并验证 main CI。本轮按用户指示不部署、不停机、不读取或写入真实游戏数据；已知部署验收事实与本次功能发布门禁分开记录。

停止条件：Release 质量门不能在不违反“无全量测试”要求时完成、发现共享环境/数据冲突、冷备份 hash 不一致、迁移试验需要触碰真实库、活动能力缺少既有入口/查询、或验收需扩大到冻结范围之外。此时保留现场并报告具体阻塞，不绕过门禁或推测通过。

安装/更新/重启/测试串行取得 `flock -w 600 /tmp/nonebot-project-validation.lock`；短工作持锁，结束后释放。运行数据、凭据和 `boss_info.json` 永不暂存。
