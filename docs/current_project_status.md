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
- 活动详情 Web 交互闭环（本轮）：配置驱动的 `detail` 视图复用既有只读读模型；活动页可切换中心/详情/阶段/排行/首领视图并按需输入玩家 ID，个人状态要求现有管理权限。不新增表、迁移、配置或奖励写路径；精简旧库和缺失数据库均保持只读回退。聚焦测试 `tests/test_activity_web_state.py` 13/13；正式 API 与交互合同已写入 `docs/features/activity.md`。本地实现与定向验证已完成，develop/main CI 门禁待推送后确认；本轮不部署。

## 主线与已交付

- `main` 是安装/正式 Release 分支。开始本轮前 `main` 与本地 `develop` 指向 `6e16f4e1`，`origin/develop` 指向其祖先 `3d7fcb84`；本轮以 `6e16f4e1`（含 `c7a9d45b` 活动中心阶段/排行入口及已通过验证的后续提交）为基线，不重做这些功能。后续合并策略：先在 develop HEAD 完成功能、CI 全绿并通过审查后才普通快进合入 main，小切片不单独触发。
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

## 当前有限目标

本轮活动目标已完成代码实现和聚焦验证。继续沿已确认路线图开发时，先重新定义完整目标；不重做来源级领奖入口、阶段/排行快捷入口或已通过的 QQ/OneBot 投递合同。

- 活动详情内容仍以版本化活动配置和现有只读业务快照为唯一来源。
- Web 状态路由支持 `center`、`detail`、`stage`、`rank`、`boss`；缺省用户 ID 时只显示公共状态，个人进度查询限既有管理权限。
- 面板视图选择、玩家 ID 查询及纯文本呈现已加入；奖励状态、事务、业务表和配置结构保持不变。
- 精简旧状态库及未创建数据库文件的读路径已验证只读回退；不在请求路径建表或迁移。
- 本轮本地聚焦验证通过；目标提交推送后等待 develop CI，审查后普通快进 main 并验证 main CI。依用户指示不部署、不碰真实游戏数据。

## 冻结清单与停止条件

冻结范围仅包括上述活动详情、状态入口映射、个人进度只读展示、短消息呈现和聚焦测试/文档。不得新增活动奖励经济、后台活动编辑器、活动存储迁移、权限模型、通用 UI 框架或 unrelated refactor。

成功条件：本轮目标的状态、持久化兼容、交互、聚焦测试和正式文档均完成；diff 检查通过；只提交目标代码、测试和正式文档到 develop，CI 成功且审查通过后普通合入 main 并验证 main CI。本轮按用户指示不部署、不停机、不读取或写入真实游戏数据；已知部署验收事实与本次功能发布门禁分开记录。

停止条件：Release 质量门不能在不违反“无全量测试”要求时完成、发现共享环境/数据冲突、冷备份 hash 不一致、迁移试验需要触碰真实库、活动能力缺少既有入口/查询、或验收需扩大到冻结范围之外。此时保留现场并报告具体阻塞，不绕过门禁或推测通过。

安装/更新/重启/测试串行取得 `flock -w 600 /tmp/nonebot-project-validation.lock`；短工作持锁，结束后释放。运行数据、凭据和 `boss_info.json` 永不暂存。
