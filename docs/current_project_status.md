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

## 已完成目标：NewAPI 签到历史写入收口

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
  （AST 合同随委托更新）与正式文档。目标提交 `ed7413ed` 已推送 develop 并快进合入 main；
  develop CI run `39080592491`、main CI run `39080959210` 均成功。未部署，未读取或写入
  真实游戏库，不发送真实消息。

## 已完成目标：多首领排行名次按首领活动区分

沿活动中心路线图收口多首领配置下的排行名次缺陷：快照 `board_positions["boss"]`
只用第一个首领活动的 key 计算，而 `_claimable_boss_rank_count` 用这一个共享名次
匹配所有首领的档位——玩家在首领 A 上榜、首领 B 未上榜时，B 仍可能被误报"排行可领"。

- `overview_snapshot` 为每个首领活动 key 计算名次，新增可选快照键
  `boss_board_positions`（同一只读快照单元，无第二次往返）；旧
  `board_positions["boss"]` 保留为单首领/旧快照回退语义。
- `_claimable_boss_rank_count` 与奖励目录 `reward_catalog_text` 的首领排行段改为
  按各首领活动自己的名次匹配自己的档位；缺键时回退共享名次，现有断言不变。
- 不改领取写路径、`claim_boss_rank_reward` 的 query 语义、表结构或配置键。
- `tests/test_activity_boss_rank_positions.py` 聚焦套件 `6/6` 通过，覆盖 per-activity
  名次快照、A 上榜 B 不误报、双首领独立名次、旧快照回退、已领档不重复计数与单首领
  中心渲染；受影响活动套件合计 `98 passed + 4 subtests`。architecture/inventory/progress
  门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_repository.py`、`center.py`、`read_model_application.py`、
  新聚焦测试与正式文档。目标提交 `44eecf85` 已推送 develop 并快进合入 main；
  develop CI run `39093663915`、main CI run `39094053406` 均成功。未部署，未读取或
  写入真实游戏库，不发送真实消息。

## 已完成目标：多首领领取状态按首领活动区分

沿活动中心路线图继续收口多首领配置缺陷：`_boss_claim_state` 把所有首领的
`milestones_unlocked/claimed` 与 `rank_claimed` 并成无命名空间集合，而领取回执按
`(activity_key, user, key)` 独立——首领 A 已领同名 `m1` 会漏报首领 B 的可领进度，
A 已解锁会冒充 B 未达标的进度，A 已领 `1-3` 排行档会漏报 B 的同档。

- 新增 `_boss_claim_state_by_activity`：按 `boss_progress` 的 activity key 归位
  各首领的 unlocked/claimed/rank_claimed；无 `boss_progress` 的旧 flat 快照回退
  共享投影（单首领语义不变）。
- `_claimable_boss_milestone_count` 逐首领用自己的 unlocked/claimed与自己配置的
  `server_milestones` 求交；`_claimable_boss_rank_count` 逐首领用自己的
  `rank_claimed` 判档（名次沿用 `boss_board_positions`）；奖励目录
  `reward_catalog_text` 的首领进度/排行段已领标记按当前首领自己的状态判定。
- 不改领取写路径、回执结构、表结构或配置键；只读复用同一快照。
- `tests/test_activity_boss_claim_scoping.py` 聚焦套件 `6/6` 通过，覆盖 A 已领不
  抑制 B、A 已解锁不冒充 B、排行档独立、flat 回退、helper 归位与奖励目录分段
  标记；上一切片 `tests/test_activity_boss_rank_positions.py` 的已领档断言随
  per-activity 表示更新。受影响活动套件合计 `104 passed + 4 subtests`。
  architecture/inventory/progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `center.py`、`read_model_application.py`、新聚焦测试、既有聚焦断言与
  正式文档。目标提交 `56391d37` 已推送 develop 并快进合入 main；develop CI run
  `39141994905`、main CI run `39142399243` 均成功。未部署，未读取或写入真实游戏库，
  不发送真实消息。

## 已完成目标：活动中心完整玩家流程——阶段时间线视图

活动中心各玩家流程（发现/详情/签到/任务/战令/阶段/排行/首领/集字/积分/奖励
目录/一键领取/Web 管理面）均已交付并有聚焦测试；本切片补全 `活动阶段` 视图
缺失的完整阶段日程，玩家一次查看全部阶段的进程与本人领奖状态。

- `activity_stage_text` 末尾新增【阶段日程】段：按配置顺序列出全部阶段的状态
  标记（进行中/已结束/未开始）、时间窗口与剩余倒计时（沿用 `remaining_text`）；
  已结束且配置奖励的阶段按本人参与证据与领取记录标注 `已领取` /
  `可领取｜活动阶段领取` / `未领取`，复用"可领阶段奖励"同一查询路径，无第二
  往返；匿名查看只列公共日程（已配置奖励显示"奖励：已配置"）；无阶段配置或
  活动未开启时输出与现状一致。
- 只读复用既有配置与快照；不新增命令、路由、表、迁移、配置键或写路径。
- `tests/test_activity_stage_timeline.py` 新聚焦套件 `6/6` 通过（多阶段顺序、
  参与证据标注、已领标注、匿名无标注、单阶段/无奖励阶段、关闭与无阶段回归）；
  受影响套件（web_state/stage/center/read_model/detail_actions/claim_sources/
  claimable）合计 `71 passed`。architecture/inventory/progress 门禁真实 exit 0；
  `git diff --check` 干净。
- 本轮只改 `read_model_application.py`、新聚焦测试与正式文档。目标提交 `70465227`
  已推送 develop 并快进合入 main；develop CI run `39244610916`、main CI run
  `39245281531` 均成功。未部署，未读取或写入真实游戏库，不发送真实消息。

## 已完成目标：积分事件每日上限进度闭环

活动中心积分玩法的事件结算带每规则 `daily_limit`（结算按
`activity_point_event_log` 当日累计截断），但玩家侧完全不可见——每日上限是黑盒。
本切片把同一累计渲染为进度。

- `overview_snapshot` 在同一只读单元内按 activity+user+date 有界聚合当日事件积分，
  新增可选快照键 `point_event_daily_totals`（事件日志表缺失时为 `None`，旧库兼容）。
- 共享纯 helper `point_event_daily_parts`（`gameplay_progress.py`）：每个配置
  `daily_limit` 的事件规则产出 `{label} 今日 {earned}/{limit}`（达上限追加
  `已达上限`）；中心进度行积分段在余额后追加 `（今日 …）`。
- `活动积分` 视图（`points_text`）与旧 `build_activity_points_text` 逐字 parity
  锁定，不携带个人进度——进度只在中心卡片渲染（skill 规则：parity-pinned 方法
  不能长数据，用新位置承载）。
- 不改积分结算、每日上限截断、商店购买或任何写路径；不新增命令、表、迁移或
  配置键。
- `tests/test_activity_point_daily_progress.py` 新聚焦套件 `7/7` 通过（进度渲染、
  达上限、跨日/跨用户不计、匿名无进度、旧库缺表、无限制规则、helper 单测）；
  受影响活动套件合计 `121 passed + 4 subtests`（含 read_model parity）。
  architecture/inventory/progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `gameplay_progress.py`、`read_model_repository.py`、
  `read_model_application.py`（仅 snapshot 传参）、`center.py`、新聚焦测试与正式
  文档。目标提交 `dc1d842c` 已推送 develop 并快进合入 main；develop CI run
  `39266368432`、main CI run `39267090259` 均成功。未部署，未读取或写入真实游戏库，
  不发送真实消息。

## 已完成目标：活动中心玩法入口完整覆盖

配置三类玩法（集字+积分+首领）时，`gameplay_entry_commands` 与配置覆盖路径在
映射层硬截断 `[:4]`——首领入口（活动首领/活动首领排行）整体从卡片消失，玩家
只能靠记住命令。入口数量约束属于投递层（消息长度），不该在映射层静默丢入口。

- 移除映射层两处 `[:4]` 截断：默认入口与配置 `commands` 覆盖都完整映射。
- 新增 `gameplay_entry_groups` 与 `ActivityCenterItem.gameplay_entry_groups`：
  按玩法分组的入口（组间按命令去重，主状态入口优先）；渲染层改为紧凑分组行
  ——主状态入口一行，之后每个进行中的玩法一行（`{玩法名}：入口 | 入口`），
  每行条目数只随该组增长，不随玩法总数无界增长。
- 投递载荷核验：最大配置（三类玩法+阶段+榜单+首领里程碑）整卡渲染字节数
  断言远低于 64 KiB（本插件 md模板输入自有上限）；QQ 原生 markdown 与
  OneBot 纯文本降级都完整覆盖全部入口（蓝字按 command 参数断言，纯文本按
  label 断言）。
- 不改投递层 `handle_send` 合同、不新增命令/路由/表/迁移/配置键、不改写路径。
- `tests/test_activity_gameplay_entry_coverage.py` 新聚焦套件 `8/8` 通过（三类
  玩法 6 入口、配置覆盖完整渲染、去重、未开启玩法、非进行中状态、最大配置
  分组行、载荷上限、纯文本降级）；受影响活动套件合计 `89 passed + 4 subtests`
  （含 delivery contract）。architecture/inventory/progress 门禁真实 exit 0；
  `git diff --check` 干净。
- 本轮只改 `center.py`、`read_model_application.py`、新聚焦测试与正式文档。
  目标提交 `edfeb9bf` 已推送 develop 并快进合入 main；develop CI run
  `39345702933`、main CI run `39346383377` 均成功。未部署，未读取或写入真实
  游戏库，不发送真实消息。

## 已完成目标：G1 积分商店购买引导闭环（roadmap-index G1）

`point_shop_text` 被 parity 锁定（与旧 `build_activity_shop_text` 逐字相等），
商店视图没有购买命令引导，玩家看到商品不知道发送 `活动购买 <商品名>`；达
个人上限/售罄也无终态标注。

- 新增 sibling `point_shop_guidance_text`：每商品行追加
  `购买：活动购买 {商品名}`；`bought>=limit>0` 标 `已兑换完（x/y）`、
  `stock_limit>0 且 sold>=stock_limit` 标 `已售罄`，两者不再引导；
  `limit<=0` 不限商品始终可购；尾部显示 `可购商品 N 件` 汇总。
- `活动商店` handler（`__init__.py:395`）改调 sibling；`point_shop_text`
  字节不变（parity 回归锁定）。
- 不改购买结算、上限/库存校验、拒绝语义；不新增命令/表/迁移/配置键。
- `tests/test_activity_shop_guidance.py` 新聚焦套件 `6/6` 通过（可购引导、
  个人上限终态、售罄终态、不限商品、parity 不变、汇总行）；受影响套件
  （point_shop_purchase/read_model/center/point_daily_progress/gameplay_consistency）
  合计 `51 passed + 4 subtests`。三门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_application.py`（新增 sibling）、`__init__.py`（handler
  改调）、新聚焦测试与正式文档。目标提交 `7c68ffce` 与门禁字面量修正
  `5f8bc2f1` 已推送 develop；develop CI run `39393218002`（首推因
  activity_points_shop matcher 字面量翻转失败）、`39394735034` 均已核验，
  后者 success。

## 已完成目标：G2 集字兑换就绪状态与缺口明细闭环（roadmap-index G2）

`collect_bag_text` 被 parity 锁定（与旧 `build_collect_bag_text` 逐字相等），
背包视图无就绪标记、无缺字明细，达上限仍引导兑换。

- 新增 sibling `collect_bag_guidance_text`：字牌齐+未达上限标注
  `可兑换｜活动兑换 {词组}`；缺字列出 `还缺 {char}x{count}`（≤4 项，其余
  `等 N 项`），与结算侧 `tokens_insufficient` 计算同源；达上限标注
  `已兑换完` 且不再显示兑换命令；`limit<=0` 不限词组永不显示已兑换完。
- `活动背包` handler（`__init__.py:353`）改调 sibling（主视图 + parity 回退
  表达式保留 gate 字面量）；`collect_bag_text` 字节不变（parity 回归锁定）。
- 不改兑换结算事务、幂等回执、拒绝语义；不新增命令/表/迁移/配置键。
- `tests/test_activity_exchange_readiness.py` 新聚焦套件 `6/6` 通过（就绪
  标注、缺字明细、达上限终态、不限词组、parity 不变）；受影响套件
  （collect_exchange/read_model/gameplay_consistency/center）合计
  `47 passed + 4 subtests`。三门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_application.py`（新增 sibling）、`__init__.py`（handler
  改调）、新聚焦测试与正式文档。目标提交 `42b4a1e4` 已推送 develop 并快进合入
  main；develop CI run `39421168893`、main CI run `39423466337` 均成功。未部署，
  未读取或写入真实游戏库，不发送真实消息。

## 当前完整目标：G3 阶段领取引导与时间线标注闭环（roadmap-index G3）

阶段时间线已交付，但 `未领取`（无参与证据）阶段没有提示参与记录来源，玩家
不知道为什么领不了；`活动阶段领取` 无 query 的语义（领取全部可领阶段）已
核验但未在文档固定。

- 时间线 `未领取` 标注细化为
  `未领取（参与记录来自活动期间的签到/任务/玩法记录）`；`可领取`/`已领取`
  标注不变。
- `活动阶段领取` 无 query 语义（无 query = 领取全部可领阶段）由行为测试固定
  （`claim(uid, query="")` 实际领取后时间线翻转已领取）。
- 只读渲染改动；不改 claim 过滤语义、领取写路径、参与证据判定或分状态拒绝
  文案；不新增命令/表/迁移/配置键。
- `tests/test_activity_stage_claim_guidance.py` 新聚焦套件 `5/5` 通过（未领取
  参与提示、可领取不变、已领取不变、匿名无标注、无 query 领取行为对照）；
  受影响套件（stage/stage_timeline/stage_gate/center/web_state）合计
  `48 passed`。三门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_application.py`（时间线标注）、新聚焦测试与正式文档；
  develop 推送/CI、审查与 main 快进/CI 按发布门继续完成。未部署，未读取或
  写入真实游戏库，不发送真实消息。

## 已完成目标：G5 活动玩法个人进度视图（roadmap-index G5）

`gameplay_text` 被 parity 锁定、无 `user_id` 参数，玩家看不到本人集字保底、
积分当日进度与首领伤害；目标文档 `docs/goals/goal-gameplay-personal-state.md`。

- 新增 sibling `gameplay_personal_text(user_id)`：复用 `gameplay_text` 的
  配置/阶段/玩法摘要，按玩法追加本人状态段（集字复用
  `collect_pity_entries`/`closest_pity_entry`；积分余额+今日进度复用
  `point_event_daily_parts` 与快照键 `point_event_daily_totals`；首领复用
  `boss_snapshot`）；数据来自一次 `overview_snapshot`（传 `now`），无
  `user_id` 时输出等价 `gameplay_text()`。
- `活动玩法` handler 改调新方法并传入登录用户 id；`gameplay_text` 字节不变
  （parity 回归锁定）；旧库缺可选表时对应段省略；只读，无新命令/路由/表/
  迁移/配置键，不改写路径。
- `tests/test_activity_gameplay_personal.py` 新聚焦套件 `4/4` 通过（三玩法
  本人状态段、单快照、parity、handler 源合同；时钟经
  `patch.object(application.clock, "now", …)` 固定为与插入记录同日的 aware
  时间戳，测试与服务器日期无关）；受影响套件 `test_activity_read_model.py`
  `17/17`（含两个 G5 测试 `test_personal_gameplay_uses_one_snapshot_for_all_progress`
  与 `test_personal_gameplay_ignores_missing_legacy_inventory_table`）、
  `test_activity_gameplay_consistency.py` `9/9`、
  `test_activity_point_daily_progress.py` `7/7` 全绿。三门禁真实 exit 0；
  `git diff --check` 干净。
- 本轮只改 `read_model_application.py`、`read_model_repository.py`、
  `__init__.py`（handler 改调）、新聚焦测试与 `test_activity_read_model.py`
  两个 G5 测试。目标提交 `bf85e010` 已推送 develop 并快进合入 main；
  develop CI run `39998595819`、main CI run `40001772187` 均成功（headSha
  均为 `bf85e010`）。未部署，未读取或写入真实游戏库，不发送真实消息。

## 已完成目标：G6 活动视图投递合同全覆盖（roadmap-index G6）

`test_activity_center_delivery_contract.py` 只通过共享 `handle_send` 缝隙证明过中心
卡片；其余玩家视图没有投递层测试，实测确实存在合同缺口。目标文档
`docs/goals/goal-view-delivery-contracts.md`。

- 新增 `tests/test_activity_view_delivery_contracts.py`（`7/7`）：以随包默认配置
  （集字+积分+首领三玩法、阶段日程、首领行动提示、购买引导、就绪标注）渲染
  stage/boss/shop/bag/reward/gameplay 六视图，逐个走
  `handle_send(native_markdown=True, fallback_msg=strip_md_command_links(text))`：
  OneBot 与关闭 markdown 的 QQ 只收到一次纯文本（无 `**`、无 `mqqapi`、入口 label
  与关键行完整、降级幂等）；QQ 原生 markdown 收到 `\n`→`\r` 归一后消息、保留
  `](mqqapi`；多行布局（玩法段之间、多集字段之间）两条路径都保留空行。handler 源
  合同逐个钉住该视图自己的 handler，另有商店/背包运行期接线合同。
- 合同暴露并修复的缺口（只读渲染/接线）：`活动首领` 曾先 `_ensure_user`，未注册
  用户拿到「请先修仙」而不是状态块，且从未走原生缝隙 → 改为按事件用户 id 渲染并
  走共享缝隙；`活动商店`/`活动背包` 只发纯文本（QQ 原生下购买/兑换引导无蓝字）→
  改走共享缝隙；`活动玩法`/`活动奖励` 的 `fallback_msg` 曾是未剥链接的 `text` →
  统一 `strip_md_command_links(text)`；`活动玩法` 玩法段与「今日进度」后缺空行、
  `活动背包` 多集字时相邻段出现两个空行 → 新增 `_tighten_blank_lines` 折叠为
  「段后恰一空行、段间不连空行」。
- 不改投递层 `handle_send` 的换行归一行为、不改领取/购买/兑换/讨伐写路径与
  `claim_*` query 语义；不新增命令/路由/表/迁移/配置键。
- 受影响 `tests/test_activity_*` 合计 `352 passed`，加上 `test_refactor_progress`
  `23/23` 共 `375` 通过；architecture/inventory/progress 三门禁真实 exit 0；
  `git diff --check` 干净；`compileall` 通过。按用户指示未跑全量测试、未部署、
  未读写真实游戏库、未发真实消息。
- 本轮只改 `read_model_application.py`、`xiuxian_activity/__init__.py`、新聚焦测试与
  正式文档。目标提交 `5c88cd3a` 已推送 develop 并快进合入 main；develop CI run
  `40260273593`、main CI run `40261122013` 均成功（headSha 均为 `5c88cd3a`，
  build-and-release 按分支规则 skipped）。未部署，未读取或写入真实游戏库，不发送
  真实消息。

## 已完成目标：活动一键领取中断重试回执可见性

活动中心 roadmap（G1-G6）已全部交付，本轮改从仓库证据另选切片：`docs/features/activity_reward.md`
承诺 `retryable_failure`「保留已完成步骤、重试只补跑未完成步骤」，持久化行与重试语义确实成立
（实测中途失败后重试只跑剩下的 3/6 步），但玩家文案只写「活动奖励领取未完成，请重试 + 失败步骤」，
从不说明前面几步已经发放——玩家无法判断奖励是否已到账，只能靠客服或补偿核对。

- 旧兼容协调器 `ActivityClaimAllService.run` 与新仓储 `ActivityClaimAllRepository.run` 共用同一
  文案规则：失败行后追加 `已发放：任务、战令、…（重试不会重复发放）`；无步骤落发放时写
  `暂无已发放步骤，重试从第一步开始`。文案只列步骤标签，不回显奖励明细（明细在重试结算后按
  已落账数据重出）。
- 不改六步固定计划、步骤顺序、`retryable_failure` 状态码、外层 ledger/审计、重试补跑语义、
  表结构、迁移或配置键；不改 `apply/duplicate/operation_conflict` 分支与其他结果文案。
- 新聚焦套件 `tests/test_activity_claim_all_retry_message.py` `8/8`（旧/新两条路径各覆盖：多步成功
  后失败列出全部已发放步骤、首步即失败写无已发放提示、已完成段按固定六步顺序、单步成功不列
  未执行步骤、文案变更不影响重试只补跑未完成步骤）；受影响 claim-all/reward 套件 `24 passed`。
  三门禁真实 exit 0，`git diff --check` 干净。未跑全量测试、未部署、未读写真实游戏库。

## 已完成目标：文档权限与 manifest 一致性门禁（架构门禁补强）

`check_manifest_documentation()` 只要求文档**提到**路由/命令，从不校验文档写下的权限等级。
`activity_reward` 因而长期把 `POST /api/v1/activity/rewards/claim` 写成 `权限 user`，而 manifest 与
蓝图实际都按 `admin` 执行（`adapters/web/static/pages/admin.js` 是唯一调用方），运维按文档
配置权限会直接踩空。

- `scripts/check_architecture.py` 新增 `_documented_permission_drift()` 与
  `check_documented_permission_levels()`：逐行读取功能文档里 `权限 \`<level>\`` 的声明，
  与 manifest 的 `RouteSpec`/`CommandSpec.permission` 比对，不一致即报错；不写权限的
  文档行不受影响（存在性仍由原检查负责）。
- 修正唯一一处真实偏差：`docs/features/activity_reward.md` 的 Web API 段改为 `admin`，
  并写明该端点只供管理员面板代玩家领取、玩家本人走 bot 命令。
- 不改任何执行层权限、路由、命令、表或迁移；只加门禁与文档更正。
- `tests/architecture/test_architecture_gate_checks.py` 新增 `ManifestPermissionDriftTests`
  `5/5`（路由漂移上报、命令漂移上报、一致的声明不报、未写权限的行不报、真实仓库保持零错误）；
  反射对照确认把文档改回 `user` 时门禁确实报错。
- 三门禁真实 exit 0；`git diff --check` 干净；`compileall` 通过。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：活动 Web 玩家视图与 bot 六视图对齐

bot 侧六个玩家视图（`活动阶段`/`活动首领`/`活动商店`/`活动背包`/`活动奖励`/`活动玩法`）
在 G6 已有统一投递合同，但 Web 的 `GET /api/v1/activity/state` 与管理员面板下拉都只有
`center|detail|stage|rank|boss`；实测 `?view=shop|bag|reward|gameplay` 一律
`400 unknown view`，运营无法在面板查看商店购买引导、背包就绪标注、奖励目录与玩法个人状态。
四个读模型本身可直接渲染，能力已存在、Web 未暴露。

- `adapters/web/blueprints/activity.py` 视图映射补 4 项：`shop`/`bag`/`reward`/`gameplay`
  分别指向 `point_shop_guidance_text`/`collect_bag_guidance_text`/`reward_catalog_text`/
  `gameplay_personal_text`；路由直接返回读模型文本，不重渲染。
- `adapters/web/templates/pages/admin.html` 的 `data-activity-view` 补齐同名四个选项。
  权限语义（无 `user_id` 公共、带 `user_id` 要求 `admin` 否则 403）、未知视图 400、缺
  `application` 的兼容响应、缺可选表的 `schema_missing`（503）失败关闭合同全部沿用既有视图；
  `center`/`detail` 在缺可选表时本就返回 503，新视图行为与其一致。
- 只读对齐：不新增路由、命令、表、迁移或配置键；不改写路径；未部署、未读写真实游戏库、
  不发送真实消息。
- 新聚焦套件 `tests/test_activity_web_views.py` `12/12`（四视图公共/个人两态、逐字等于读模型
  文本、九视图枚举、面板选项与路由映射一致、旧库只读不建文件、缺表失败关闭）；受影响
  `test_activity_web_state`/`test_activity_center_delivery_contract`/
  `test_activity_view_delivery_contracts`/`test_activity_center`/`test_activity_read_model`
  合计 `53 passed`。三门禁真实 exit 0；`git diff --check` 干净；`compileall` 通过。目标提交
  `52f62c09` 已推送 develop 并快进合入 main；develop CI run `40371340167`、main CI run
  `40372486961` 均成功（headSha 均为 `52f62c09`）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：娱乐房间重启后超时恢复

三个娱乐房间在重启后仍然存在（快照持久化于 `entertainment_game_rooms`，
启动时 `load_rooms()` 恢复），但**超时任务不随进程保留**：一个等待对手的
五子棋房间（`ROOM_TIMEOUT=180s`）重启后不再自动关闭，创建者永远
「你已在其它棋局中」，无法开新局；进行中的五子棋/扫雷/十点半同样不再超时收敛。

- 新增只读纯规则模块 `features/entertainment/room_lifecycle.py`：
  `timeout_seconds`/`room_deadline`/`is_expired`/`remaining_seconds`/
  `expired_rooms`。按类型取时间锚点（五子棋 `playing` 用落子时钟、否则用加入
  时钟；十点半用创建时钟；扫雷用操作时钟），`waiting`/`playing` 之外的状态一律
  不参与，缺字段或坏时间戳按「不清理」处理。到期时刻本身不算过期（运行期任务
  睡满整段才动作），剩余时间返回余量，过期返回 0。
- `room_repository.py` 增加只读批量能力：`list_all_states()` 在**单个只读 UoW**
  内取回三类快照（`list_states` 语义、行/字节预算与 fail-closed 检查不变，抽出的
  `_select_rows` 两者共用）；`EntertainmentApplication.room_states_all()` 暴露该
  能力。
- 新增 `mod/room_restore.py`：`restore_entertainment_rooms()` 用一次批量快照依次
  调用三个 manager 的 `load_rooms(states)`，再执行过期清理与剩余时间重建；进程内
  只跑一次（三个启动钩子中的第一个真正执行，其余为空操作），供测试重置的
  `reset_restore_state()`。
- 三个 `restore_*_rooms()` 改为 async 并委派给该协调器；`load_rooms(states=None)`
  增加可选入参（不传时仍按原路径逐类型读取，既有调用语义不变）。过期房间走**与
  运行期相同的清理分支**（仅抑制通知）：五子棋 `_close_waiting_room`/
  `_finish_stale_move`、十点半 `_close_waiting_room`（人数不足关闭 vs 自动结算）、
  扫雷 `_expire_game`（`status="closed"` + 落盘），其余房间按**剩余时间**重建
  计时器；重复重建先取消旧任务，不叠加。`_finish_stale_move` 接受可选 `moment`，
  使恢复用与过期判定同一时刻比较而非墙钟。
- `start_*_timeout` 增加关键字参数 `delay`（默认 None = 原完整时长，在线行为
  不变）并允许 `bot/event` 为 None（仅清理与落盘，不投递）。运行期超时文案与
  触发条件未改。
- 不改写路径、表结构、迁移或配置键；未新增命令/路由。
- 新聚焦套件 `nonebot_plugin_xiuxian_2/features/entertainment/tests/test_room_lifecycle.py`
  `20/20`（锚点选择、非候选状态、缺/坏时间戳、到期边界、余量、按类型分组）、
  `tests/test_entertainment_room_restore_timeout.py` `15/15`（过期释放并可
  重新开局、走真实关闭分支、存活房间保留且重建计时器、无投递上下文不发送消息、
  重复重建不叠加、启动钩子可 await）与 `tests/test_entertainment_room_batch_restore.py`
  `9/9`（批量读只开一个只读 UoW、覆盖三类且空类型仍在、保留预算与 fail-closed、
  恢复只跑一次且不逐类型读、无 bot 不发消息）；`test_room_repository.py` 新增
  3 项批量读断言（`9 passed`）。受影响套件 `test_entertainment_room_managers`/
  `test_half_ten_providers`/`test_minesweeper_providers`/`test_source_quality`
  与全部 entertainment feature 测试（129 项）合计 `270 passed`。三门禁真实
  exit 0；`git diff --check` 干净；`compileall` 通过。
- 交付提交：`d814465d`（纯规则模块 + 首次接入，develop CI run `40447332383`
  success，已快进 main，main CI run `40450117536` success）、`5ab2db4e`（文档，
  run `40448212171` success）、`060ca806`（回到 develop/CI 记录，run
  `40449259010` success）、`fdb8ae8f`（单快照批量读 + 协调器，run `40461069946`
  success）、`bcaf80a5`（文档，run `40461855992` success）、`80ba1cb9`（过期房间
  走真实关闭分支，run `40468586654` success）、`42401305`（最终记录，develop CI
  run `40472172908` success，已快进 main，main CI run `40473193062` success）；
  全部 develop run 的 `quality` job 均 success、`build-and-release` skipped。
  整卡验收通过前未把 `fdb8ae8f` 之后的提交合入 main。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：鬼市订单生命周期闭环（N1→N2→N3）

鬼市三个入口（存/取灵石、求购、摆摊）此前已迁到 feature application，但订单从创建
到成交、到期、退款从未闭环：`鬼市求购` 的回执明写「次日 8 点自动取消订单，并退还
未购得物品的灵石」，而仓库里没有任何求购到期路径（guishi 仓储零 `created_at`/
`expire` 读取），冻结灵石只在手动取消或管理员 `清空鬼市` 时才回来；`鬼市摆摊`
创建后不触发撮合（对照求购创建后立即调用 `process_guishi_transactions`）；
撮合按摆摊价结算而买家按求购价冻结，差价既不入卖家也不退买家。目标文档
`docs/goals/next-full-feature.md`（local-only，未提交）。

- N1 求购到期自动取消退款：新增纯规则 `features/trade/guishi_lifecycle.py`
  （创建日次日 `GUISHI_BAITAN_END_HOUR` 点到期、`None` 锚点永不过期、混合时区
  不抛错）与 `GuishiQiugouExpiryService`（单次只读 UoW 取候选 + 既有
  `cancel_qiugou` 退款，操作号 `guishi-expire-qiugou:{order_id}` 稳定可重放；
  缺锚点表/缺库返回空且不建表）。锚点复用既有
  `guishi_order_create_operations.created_at`，**未新增表/列/迁移**。接线：
  `clear_expired_baitan_orders_job` 先做全服到期再做摆摊清理（job id 与 cron 不变），
  `鬼市信息` 做本人访问期收敛。
- N2 摆摊即时撮合与差价退还：新增 `GuishiListingFillService`（同物品、求购价不
  低于摆摊价、非本人、按创建时刻 FIFO，操作号 `guishi-fill:{摆摊}:{求购}`），
  `鬼市摆摊` 创建后立即撮合并把成交摘要与 `鬼市成交` 统计写回卖家回执
  （抽 `_record_guishi_match_event` 与既有定时撮合共用）。`guishi_match` 新增
  `price_refund`（`数量 × (求购价 - 摆摊价)`）在同事务退回买家，`GuishiMatchResult`
  默认值保证旧 operation 行重放兼容；价格闸门与部分成交语义不变。
- N3 玩家可见状态与文案：新增纯渲染 `guishi_order_view.py` 与只读
  `GuishiOrderStatusService`，`鬼市信息` 每笔订单显示到期时刻与剩余时长，
  无锚点历史订单显示 `到期：未知（历史订单）`，段尾给出可执行提示；摆摊回执
  「物品不退还」改为与代码一致的「未售出物品退回背包」；`trade_help.py` 补齐
  `鬼市取消求购`/`鬼市取物品` 与到期规则。正式文档 `docs/features/trade.md`、
  `docs/gameplay_notes.md` 已更新。
- 测试：`tests/test_trade_guishi_qiugou_expiry.py` `21/21`、
  `tests/test_trade_guishi_listing_fill.py` `13/13`、
  `tests/test_trade_guishi_order_status.py` `13/13`；
  `tests/test_trade_guishi_match_repository.py` 按新语义原地更新（买家余额
  99→159、回滚不留退款、旧行 `price_refund=0`）；三阶段受影响套件
  `362 passed`、unittest 收集 `293 tests OK`。三门禁真实 exit 0；
  `compileall` 与 `git diff --check` 干净。另用一次性探针跑真实 application
  路径验证价值守恒：1000 → 冻结 120 → 成交净付 80 并退差价 40 → 到期退回 100
  → 买家 920、卖家 80，重复清理不再退款。
- 交付提交：`f020f62d`（N1，develop CI run `40517570714` success）、
  `5319b209`（N2，run `40519162723` success）、`235d9912`（N3，run
  `40520461812` success）、`66e4710b`（状态记录，develop CI run `40521587683`
  success）。三阶段与记录提交齐备后一次性快进合入 main（`main` 与 `develop`
  均为 `66e4710b`），main CI run `40522703643` success（headSha `66e4710b`，
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：拍卖结算物品投递闭环（A1→A2→A3）

拍卖的竞价、上架、下架、场次开始与结算早已由 feature application 承担，但结算的容量
口径在生产接线里恒为 `1`：`AuctionSettlementApplication` 只传 database，
`AuctionSettlementSqlRepository(database)` 的 `max(int(max_goods_num or 1), 1)`
把「未传参」当成单件上限。真实仓储探针（生产接线，非配置默认）实测
`repo.max_goods_num = 1`，因此卖家流拍时只要背包里还有同名物品就判「背包满」：
`auction_current` 行被删、物品不退回、`auction_history` 记 `卖家背包满流拍`，
而玩家日志写的是「已退回背包」；中标者已持有该物品 1 件则 `inventory_full`
拒绝整场结算，所有人的拍品与卖家收入一起卡住，管理员重试也必然失败。目标文档
`docs/goals/next-full-feature.md`（local-only，未提交）。

- A1 结算容量口径修正（`bc404a9d`）：新增 `DEFAULT_MAX_GOODS_NUM = 1000` 与
  `configured_max_goods_num()`（惰性读 `XiuConfig().max_goods_num`，配置不可读才
  回落默认值），`AuctionSettlementSqlRepository` 在未显式注入时走该解析；
  `AuctionSettlementApplication` 可透传 `max_goods_num`。实测修复后
  `max_goods_num = 1000`：卖家持有 2 件时流拍物品退回成 3 件、中标者已持有 1 件
  时正常入包。不改结算价、手续费、卖家收入与败者退款口径。
- A2 未送达入库（`621e558e`）：新迁移 `auction.009`（game DB）创建
  `auction_pending_items`/`auction_pending_claim_operations`。结算在同一事务内
  按 `backpack`/`pending` 投递：卖家流拍且背包满、中标者背包满都写入待领取行
  （`role`+`reason`），历史状态 `待领取`，**不再丢物品、也不再阻塞整场**；
  其余拍品、卖家入账、败者退款照常完成。`item_missing`/`participant_missing`
  仍整事务拒绝；旧库缺表时插入抛错→整个结算回滚、场次保持 active，绝不静默降级
  为销毁。中标/流拍日志改为按 `delivery` 分支，不再谎称「已退回背包」。
- A3 领取闭环（`0d9d378b`）：新增 `AuctionPendingApplication`——`list_for` 只读
  列本人未领取物品，`claim` 在单个 immediate UoW 内逐行做容量校验、原子入包
  （`goods_num/bind_num +1`）、标记 `claimed_at/claim_operation_id`，背包满的
  跳过并保持待领取，同 operation_id 重放不重复发放，缺表 `schema_missing` 失败关闭。
  命令面复用既有 `我的拍卖`（不新增命令名/路由/配置键）：无参数同时列出等待区与
  待领取并给出指令提示，`我的拍卖 领取 [拍卖ID]` 领取全部可领取项或指定一件；
  `trade_help.py` 拍卖帮助补该指令与待领取说明。
- 测试：`tests/test_auction_settlement_delivery.py`（A1 容量 4 项 + A2 入库 5 项 +
  迁移路由）`10/10`、`tests/test_auction_pending_claim.py`（领取/重放/跨用户/
  容量/缺表 + handler 源合同）`15/15`；既有断言按新语义**原地更新**：
  `test_auction_settlement_repository.py` 由「inventory_full 保持 pending」改为
  「settled + 待领取 + 待领取行」，`test_auction_session_finish.py` 同，
  `test_source_quality.py` 的结算默认路径合同改钉
  `self.repository = repository or self._build_repository()` +
  `max_goods_num=self.max_goods_num`。受影响拍卖/交易/源码质量套件 `335 passed`，
  unittest 收集 `271 tests OK`；三门禁真实 exit 0；`compileall` 与
  `git diff --check` 干净；`docs/refactor_inventory.json` 已重新导出（仅新增两张表）。
- 端到端探针（真实 application + 真实仓储，cap=1 复现满包）：结算 `待领取`、
  场次 `settled`、卖家 +630、败者退 500、中标者待领取 1 件；满包时领取 `blocked`
  且仍待领取，腾出空间后领取成功入包（`goods_num=1,bind_num=1`），同 operation_id
  重放返回 `duplicate`、不重复发放。
- 交付与 CI：A1 `bc404a9d` 的 develop run `40529415381` **failure**——唯一失败项是
  `test_source_quality` 仍钉旧字面量 `self.repository = repository or
  AuctionSettlementSqlRepository`，该合同在 A2 提交就地更新（功能代码本身由 A2 的
  绿色 run 覆盖），这里如实记录而不宣称通过；A2 `621e558e` run `40531739039`
  success；A3 `0d9d378b` run `40534016731` success；状态记录 `9993f451` 的
  develop run `40535126193` success，随后一次性快进合入 main（`main` 与 `develop`
  均为 `9993f451`），main CI run `40536296188` success（headSha `9993f451`，
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：拍卖竞价可见性与被超价通知（B1→B2）

竞价资产一直是对的：`AuctionBidSqlRepository` 在同一事务锁定新出价并退回上一位领先者，
`refunded_bidder`/`refunded_amount` 落进 `auction_bid_operations`。但玩家侧完全看不到这条链：
`on_bid` 效果签名里没有被超价者与退款额，被超越的人只会发现灵石悄悄回到余额；
`我的拍卖` 只列等待区与待领取，看不到自己参与的竞价。目标文档
`docs/goals/next-full-feature.md`（local-only，未提交）。

- B1 我的竞价视图：`AuctionQuerySqlRepository.my_bids(user_id)` 只读解析
  `auction_current` 与 `auction_bid_operations`——竞价仓储在玩家被超越时会把它从
  `auction_current.bids` 移除，因此**出价只能从操作账本恢复**；返回
  `auction_id/item_name/my_price/current_price/leading/bid_count`，缺库缺表返回空且不建表。
  `AuctionQueryApplication.my_bids` 直接委派。`我的拍卖` 在等待区与待领取之后追加
  `【我的竞价】` 段（领先/已被超价 + `拍卖竞拍 <ID> <价格>` 加价提示），
  竞价投影读取异常时该段按空处理，不影响整条指令。
- B2 被超价通知：`AuctionBidEffects.on_bid`/`NullAuctionBidEffects` 增加带默认值的
  `refunded_bidder`/`refunded_amount`；`_append_effect_event` 把两者写进 outbox payload，
  `reconcile_outbox_event` 读出并透传；`LegacyAuctionBidEffects.on_bid` 在出价行之后，
  若被超价者非空且退款额大于 0，写
  `[拍卖被超价] {物品} 已被超越，原出价 {N} 灵石已退回，拍卖ID:{id}`，
  `event_id=f"auction.outbid:{operation_id}"`（与出价行 `auction.bid:{operation_id}` 分开，
  outbox 重放各自去重）。`reconcile_event` 同步透传两字段。
- 不改扣款/退款金额、竞价校验、operation 与 outbox 结构；不新增表、迁移、命令、路由或配置键。
- 测试：新增 `tests/test_auction_bid_visibility.py` `13/13`（领先/被超价、账本恢复出价、
  只返回本人拍品、`current_price` 不低于最高出价、空玩家、缺库缺表不建表、超价通知两行/
  无超价一行/退款额为 0 不写、事件号稳定可去重、application payload 源合同、
  handler 渲染源合同）；受影响拍卖/交易/源码质量套件 `333 passed`，unittest 收集
  `274 tests OK`；三门禁真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 端到端探针（真实 `AuctionBidSqlRepository` + `AuctionQueryApplication` +
  `LegacyAuctionBidEffects` + 真实日志写入到临时 players 目录）：先由 me 出价 500、
  再由 rival 出价 800 → 回执 `refunded_bidder=me/500`；`my_bids("me")` 返回
  `my_price=500,current_price=800,leading=False`；me 的日志恰好一条
  `[拍卖被超价] 灵剑 已被超越，原出价 500 灵石已退回，拍卖ID:a1`，rival 只有出价行。
- 交付与 CI：`b05c665e` 已推送 develop，develop CI run `40620682579` success；状态记录
  `a2fba70e` 的 develop run `40621226265` success，随后一次性快进合入 main（`main` 与
  `develop` 均为 `a2fba70e`），main CI run `40621911086` success（headSha `a2fba70e`，
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：我的拍卖——本场拍品与个人成交记录闭环

`我的拍卖` 只读等待区/待领取/我的竞价；开场把等待区整体搬进 `auction_current` 并清空
`auction_player_upload` 后，卖家拍品从该命令消失，三源全空时还会误报「没有上架任何
拍卖物品」。结算结果只写一次性私聊日志，玩家错过 `[拍卖成交]`/`[拍卖流拍]` 后无法回查。
目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增只读 `AuctionQuerySqlRepository.my_auction_snapshot(user_id)`（单个只读 UoW）：
  `lots` 返回本人非系统在拍品——当前价取列值与最高出价的较大者，出价次数/人数来自
  `auction_bid_operations`（缺表回退 `bids` 长度），剩余时间取 active `auction_sessions`
  的 `end_time` 差额（缺表为 `None`）；`records` 按 `seller_id OR winner_id` 取最近 5 条
  `auction_history`，由 SQL `ORDER BY end_time DESC LIMIT` 截断。缺库/缺表返回空且不建表。
- 新增纯渲染 `features/auction/my_auction_view.py`；`我的拍卖` 段序固定为
  等待区 →【本场拍品】→【拍卖待领取】→【我的竞价】→【最近拍卖记录】，五源全空才进空态。
  卖出行显示成交价/手续费/实收/买家，拍得行显示成交价/卖家；流拍写「未售出，物品已退回
  背包」，待领取写「物品在拍卖待领取」，不谎称已售出。
- 只读；不改上架/下架/竞价/结算/投递/领取写路径；不新增表、迁移、命令、路由或配置键。
- 测试：`tests/test_auction_my_lots.py` `24/24`（仓储投影、开拍交接回归、双角色记录、
  limit/排序、缺库缺表不建文件、纯渲染、handler/帮助源合同）；受影响拍卖与源码质量套件
  `324 passed`；unittest 收集 `162 tests OK`。architecture/inventory/progress 门禁真实
  exit 0；`compileall` 与 `git diff --check` 干净。
- 目标提交 `ac8cfdc3` 已推送 develop，develop CI run `40680683571` success；快进合入
  main，main CI run `40681683532` success（headSha 均为 `ac8cfdc3`，`build-and-release`
  按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：签到业务日回执与签到日历

`修仙签到` 只依赖可被每日重置清掉的 `user_xiuxian.is_sign`：重置任务若延迟到玩家当天签到
之后执行，会把当天标记清零，玩家可再次签到并重复领灵石；迁移前的 `is_sign=1` 也没有业务
日期回执，无法区分新旧签到，玩家侧也没有签到历史可查。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- 新增 `sign_in.004`（game DB）创建 `sign_in_daily`（`user_id`、`business_date`、`stone`、
  `operation_id`，主键 `(user_id,business_date)`）与 `business_date` 索引；请求路径不建表。
- `SignInApplication.claim` 在事务内用注入时钟固定业务日：当天已有回执时返回既有
  `already_signed` 拒绝（即使 `is_sign` 被清成 0），成功路径在同一 UoW 写入日回执；
  `sign_in_daily` 缺失的旧库跳过回执、保持原领取语义。
- `SignInDailyResetSqlRepository` 在表存在时只清没有当天回执的 `is_sign`，审计记录
  `protected_rows`/`protected_count`；表缺失时保持旧全表清零。同日 operation 重放不重复清零。
- 新增纯渲染 `features/sign_in/calendar.py`：`signed_streak` 与
  `render_sign_in_calendar`（本月网格、已签 `✓`、今天 `·`、`本月已签 N 天｜连续 M 天`）；
  `修仙签到` 成功与已签到回复都追加日历，表缺失或读取异常时静默省略。
- 只读展示，不发额外奖励；不改灵石区间、随机源、ledger/effects/outbox、任务/统计投影或
  Web 响应；不新增命令、别名、路由或配置键。无历史回填（旧 `is_sign` 无法推断日期）。
- 测试：`tests/test_sign_in_daily_calendar.py` `18/18`（日回执、同日重复拒绝、延迟重置保护、
  旧标记清零、重放、缺表兼容、日历/连签、迁移与接线合同）；受影响签到与源码质量套件
  `277 passed`；unittest 签到组 `20 + 19 tests OK`。architecture/inventory/progress 门禁
  真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `03c5a1b6` 的 develop run `40710128801` **failure**——唯一失败项是
  `test_refactor_progress` 仍要求已知限制保留“迁移前/首次重置延迟”原文，文档改写时移除了
  该表述；修正文档 `be0e82b7` 的 develop run `40711177457` success，随后一次性快进合入
  main，main run `40712724666` success（headSha `be0e82b7`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：我的仙肆成交记录与收支汇总

仙肆成交事实早已写入 `xianshi_operations`（买家、卖家、物品、数量、成交总额、时间），但
`我的仙肆` 只读取 `xianshi_item` 当前挂单；物品售罄后命令直接回“没有上架任何物品”，玩家
只能从一次性 `仙肆售出`/`仙肆购买` 日志回忆成交。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- `XianshiQuerySqlRepository.my_trades(user_id, limit=5)` 在单个只读 UoW 内按
  `buyer_id OR seller_id` 取最近 5 笔，`rowid DESC` 排序；字段含 `operation_id、
  listing_id、name、goods_type、quantity、total_cost、role、counterparty、created_at`。
  对手方优先取 `user_xiuxian.user_name`（同名多行取最小 rowid）；缺表时为空，系统卖家
  显示“系统”。缺 `xianshi_operations`/数据库返回空且不建表。
- `TradeApplication.xianshi_my_trades` 直接委托；新增纯渲染
  `features/trade/xianshi_history_view.py`，逐行输出卖出/买入、数量、成交总额、对手方与
  时间，末尾给出最近 N 笔的卖出收入/买入支出汇总。
- `我的仙肆 记录`（也接受 `成交`/`历史`）展示记录；无记录时明确提示。原上架列表、20 条
  分页和 `send_msg_handler` 行为不变，尾部追加记录入口；空上架提示同样带入口。
- 只读；不改购买扣款、卖家到账、上架手续费、挂单数量、系统商品或 operation 回执；不新增
  表、列、迁移、索引、配置键、命令名或路由。
- 测试：`tests/test_trade_xianshi_history.py` `14/14`（角色/对手方、系统卖家、排序/limit、
  缺表缺库不建文件、纯渲染、handler/帮助源合同）；受影响交易与源码质量套件 `263 passed`；
  unittest 交易组 `121 tests OK`。architecture/inventory/progress 门禁真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 目标提交 `6ef35416` 已推送 develop，develop CI run `40731686802` success；快进合入 main，
  main CI run `40732798556` success（headSha 均为 `6ef35416`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：魔修入侵个人战功与领奖状态

魔修入侵的个人战功已持久化在 `world_event_state.participants/claimed`，但状态页此前只显示
本境界本波前五名，榜外玩家看不到自己的出手、伤害、名次与领奖状态。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- 新增纯渲染 `features/world_events/contribution_view.py`：`find_current_record` 兼容
  `realm:wave:user`、`realm:user` 与仅字段匹配的旧记录；`latest_wave` 取本人该境界最新
  波次；`render_personal_contribution` 汇总本期出手/真实伤害、当前波次名次与伤害占比，
  并按 `claimed`（用户 key、记录 key、`:*user` 后缀）和 `reward_ready` 输出已领取/
  可领取/进行中/已结束未领取/未参与。
- `xiuxian_world_events/__init__.py::_build_state_message` 在原有排行后追加【你的战功】段；
  `_personal_panel_lines` 仅在有 `user_info` 时渲染，无当前境界 Boss 时按本人最新波次降级。
  不改 `讨伐魔修`、`领取魔修奖励`、Boss/波次刷新、奖励计算或 claimed 写入；不新增命令、
  别名、路由、表、列、迁移、索引、配置键或查询次数。
- 测试：`tests/test_world_events_personal_contribution.py` `13/13`（含 3 subtests），覆盖
  榜外名次、波次汇总、三种 claimed 旧格式、四种奖励文案、坏字段降级与状态接线源合同。
  Phase2 门禁 `test_world_events_demon_claim_command_owns_atomic_claim_statistic` 改为校验
  证据路径与 `claim_demon_reward_` 调用图，不再硬编码源代码行号；该单测在 HEAD 快照中
  `OK`。`docs/refactor_phase2_legacy_path_items.json` 保持用户删除、未暂存。
- 门禁：architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。正式说明写入 `docs/features/world_events.md`
  与 `docs/gameplay_notes.md`。
- 交付与 CI：功能提交 `1b96bc04` 的 develop run `40738987652` failure——唯一失败项是
  Phase2 门禁仍断言旧行号 `__init__.py:191`；修正提交 `fc7f3d9f`（自然 import 布局 +
  路径级证据断言）的 develop run `40742318934` success，快进合入 main 后 main run
  `40743346266` success（headSha 均为 `fc7f3d9f`，`build-and-release` 按分支规则 skipped）。
  未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：竞技场荣誉结算回执与个人历史

竞技场每日荣誉由 `ArenaSeasonRewardApplication.reset_daily` 自动发放，结算快照已写入
`arena_season_reward_operations`，但 `我的荣誉` 只显示当前荣誉与“明日预计奖励”，玩家
看不到今天实际结算了多少、最近几次是否到账。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- `ArenaSeasonRewardRepository.recent_settlements(user_id, limit=5)` 在只读 UoW 中按
  `season_key DESC, rowid DESC` 取本人最近 N 条；缺 game DB 文件、缺表或读失败返回空，
  不建库、不建表、不补列。新增冻结 `ArenaHonorSettlement`，解析 `payload` 中的基础/
  排名/段位/名次；坏 JSON、缺字段或 `base + bonus != honor` 时只保留总计，不猜测拆分。
- `ArenaSeasonRewardApplication.recent_settlements` 只读委托；纯渲染
  `features/arena/honor_settlement_view.py` 输出【最近荣誉结算】，空记录给出每日 0 点
  结算提示，旧记录标注“明细不可用”。
- `我的荣誉` 保留既有段位/排名/荣誉与“明日预计奖励”，在其后追加最近最多 5 次结算；
  不新增命令、别名、路由、配置键、表、列、迁移或索引，不改自动结算、荣誉公式或资产。
- 测试：`tests/test_arena_honor_settlement_history.py` `10/10`，覆盖真实 claim 写入后
  读回、倒序/limit、用户隔离、缺表/缺文件不建库、坏 payload 降级、应用委托、纯渲染与
  handler 源合同；受影响竞技场/源码质量套件 `283 passed + 10 subtests`。architecture、
  inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；`compileall` 与
  `git diff --check` 干净。
- 交付与 CI：功能提交 `695f1243` 的 develop run `40755596698` success；快进合入 main 后
  main run `40756949336` success（headSha 均为 `695f1243`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：组队邀请待处理面板

组队邀请已持久化到 `dungeon_team_invites` 并有到期清理，但 `查看队伍` 在玩家不在队伍时
直接回复“你不在任何队伍中”，错过的私聊邀请没有主动查询入口。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- `features/dungeon/team_presentation.py` 新增 `TeamPendingInviteView` 与
  `render_pending_invite`：展示队伍名、邀请人道号、发起群、剩余分钟和同意/拒绝指引；
  空名称回退 `未知队伍`/`未知用户`，`remaining_seconds <= 0` 输出过期文案。
- `xiuxian_dungeon/__init__.py::view_team_handler` 的无队伍分支先调用
  `DungeonTeamApplication.pending_invite(user_id, now)`；命中未过期邀请时读取
  `team_info` 与邀请人道号并渲染，按钮为同意/拒绝/队伍帮助；未命中保持原创建队伍提示。
  新 helper 定义在文件末尾，既有 Phase2 证据锚点 `343/346/1126/1223` 未漂移。
- 只读；复用既有 `dungeon_team_invites` 与 `pending_invite`，不新增表、列、迁移、索引、
  配置键、命令、别名或路由；不改邀请 TTL、私聊通知、同意/拒绝或到期清理。
- 测试：`tests/test_dungeon_pending_invite_view.py` `7/7`，覆盖真实 pending/过期/已消费/
  他人邀请、缺表/缺库不建表、应用委托、纯渲染降级与 handler 源合同；受影响组队/源码
  质量套件 `275 passed`。`test_dungeon_frozen_command_owner` 在本地因用户删除
  `docs/refactor_phase2_legacy_path_items.json` 报 KeyError，在 HEAD 快照中单测 `OK`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6c14cb7e` 的 develop run `40787111157` success；快进合入 main 后
  main run `40788335192` success（headSha 均为 `6c14cb7e`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：通天塔个人进度与里程碑总览

`通天塔信息` 此前只显示当前层数、历史最高、累计积分和挑战提示；玩家需要记住帮助里的
每 10 层奖励，也看不到本周兑换总进度。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/tower/progress_view.py`：`next_milestone` 返回严格大于历史最高的
  下一个 10 的倍数；`weekly_purchase_summary` 汇总有效商品的已兑/上限/已兑完项数，
  忽略 `_last_reset`、非法数量、缺失/非正上限；`render_tower_progress` 输出下一里程碑、
  剩余层数、首通积分/灵石/修为预览、本周兑换与重置提示，空配置仍保留里程碑段。
- `通天塔信息` 使用同一 `tower_limit.get_user_tower_info` 快照与 `tower_data.config`
  追加【进度与里程碑】；保留原有当前层数/历史最高/累计积分/挑战提示。零新增查询、
  表、列、迁移、索引、配置键、命令、别名或路由，不写回 `weekly_purchases`。
- 测试：`tests/test_tower_progress_view.py` `9/9`，覆盖里程碑边界、正常/坏/整数键
  `weekly_purchases`、空商品配置、可重复渲染与输入不变、handler 源合同；受影响
  通天塔/源码质量套件 `266 passed`。architecture、inventory `--check`、progress
  `--json`（`failures: []`）真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `fb40e07f` 的 develop run `40801441031` success；快进合入 main 后
  main run `40802372687` success（headSha 均为 `fb40e07f`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：灵庄流水与最近操作

`灵庄信息` 此前只显示当前存款、会员等级和上限；`bank_account_operations` 已保存每次
存入/取出/结息/升级，但玩家没有读取面。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/bank/history_view.py`：`BankOperationView` 与
  `render_bank_history` 输出【最近灵庄记录】，含存入/取出/结息/会员升级的时间、金额或
  利息、操作后存款；空记录显示“暂无记录”，不导入 NoneBot/Flask。
- `BankAccountInfoApplication.recent_operations(user_id, limit=5)` 在只读 UoW 中按
  `created_at DESC,rowid DESC` 取本人最近 N 条；从 payload 的 `withdraw`/`interest`/
  `upgrade` 前缀识别类型，缺失/损坏时按金额符号与利息降级；缺库/缺表/schema 不完整
  返回空，不请求期 DDL。`get_info` 既有字段不变。
- `BankCommandApplication.execute` 的 `info` 结果增加 `operations`；`render_bank_reply`
  仅在该键存在时追加流水，旧调用方与其它 action 输出不变。未改存取、结息、升级、回执、
  余额或资产写路径；无新表、列、迁移、索引、配置键、命令、别名或路由。
- 测试：`tests/test_bank_history_view.py` `8/8`，覆盖真实流水排序/limit/用户隔离、
  四类操作识别、坏 payload、缺表/缺库不建表、info 结果接线、纯渲染与命令源合同；
  受影响 bank/源码质量套件 `340 passed`。architecture、inventory `--check`、progress
  `--json`（`failures: []`）真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `ddfec137` 的 develop run `40828075324` success；快进合入 main 后
  main run `40829025235` success（headSha 均为 `ddfec137`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：活动中心最近一键领取回执

`活动领取` 的六步回执已写入 `activity_claim_all_operations` / `activity_claim_all_steps`，
但 `活动中心` 只渲染当前卡片，玩家错过后无法回看哪些来源已发放、哪些步骤失败。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/activity/claim_receipt_view.py`：`ActivityClaimReceiptView` 与
  `render_claim_receipts` 输出【最近一键领取】，按状态显示已领取来源、未完成步骤或
  “处理中（可重试）”；空输入不输出段。
- `ActivityReadModelSqlRepository.recent_claim_receipts(user_id, limit=5)` 在只读 UoW 中按
  `COALESCE(updated_at,created_at) DESC,rowid DESC` 取本人最近 N 条；解析 `result_json`
  的 `ok/text/steps`，坏 JSON 只保留状态/时间；缺库/缺表返回空，不请求期 DDL。
- `activity_center_text(user_id)` 仅在本人回执非空时追加该段；匿名或没有回执时输出与
  当前一致。未改六步领取计划、`retryable_failure` 文案、回执写入、ledger/审计或资产；
  无新表、列、迁移、索引、配置键、命令、别名或路由。
- 测试：`tests/test_activity_claim_receipt_view.py` `6/6`，覆盖真实回执排序/limit/用户
  隔离、成功/部分失败/未领取/处理中/坏 JSON、缺表/缺库不建表、纯渲染和中心源合同；
  受影响活动/源码质量套件 `292 passed`。architecture、inventory `--check`、progress
  `--json`（`failures: []`）真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `3db163c0` 的 develop run `40842248247` success；快进合入 main 后
  main run `40843463241`（headSha 均为 `3db163c0`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：活动中心最近活动签到

`活动签到` 的逐日记录已写入 `activity_sign_log`，但 `活动` 只显示累计/上次日期，
`活动中心` 没有逐日回看。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/activity/sign_history_view.py`：`ActivitySignDayView` 与
  `render_sign_calendar` 以今天为终点输出最近 7 天 `MM-DD 已签/未签`；空记录或全部
  坏日期不输出段，坏日期跳过。
- `ActivityReadModelSqlRepository.recent_signs(user_id, limit=7)` 在只读 UoW 中按
  `sign_date DESC` 取本人最近 N 条；缺库/缺表/读失败返回空，不请求期 DDL。
- `activity_center_text(user_id)` 仅在本人存在有效签到记录时追加【最近活动签到】；
  匿名或无记录输出与当前一致。未改 `活动签到` 的写入、奖励、去重、累计天数或事务；
  无新表、列、迁移、索引、配置键、命令、别名或路由。
- 测试：`tests/test_activity_sign_history_view.py` `5/5`，覆盖排序/limit/用户隔离、
  缺表/缺库不建表、7 天窗口补缺/坏日期、中心接线与源合同；受影响活动/源码质量套件
  `289 passed`。architecture、inventory `--check`、progress `--json`（`failures: []`）
  真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `c65c20a2` 的 develop run `40848021717` success；快进合入 main 后
  main run `40849339731`（headSha 均为 `c65c20a2`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：灵田傀儡自动收取生产路径修复

`灵田傀儡自动收取`（每小时 `auto_harvest` job → `check_and_harvest` →
`PuppetApplication.harvest` → `PuppetHarvestSqlRepository`）在生产形态下**必然失败**，
且失败被命令层 `return None` 静默吞掉，玩家与日志都收不到任何结果。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- 读取不存在的列：`harvest_repository.py` 的 SELECT 引用 `mix_elixir_info."傀儡等级"`，
  而真实列是 `灵田傀儡`（= 等级，0 表示未拥有）；SQLite 把未解析标识符当**字符串
  字面量**，`int('傀儡等级')` 抛 `ValueError`。生产探针实测 row 为
  `('2026-01-01 00:00:00','1','傀儡等级')`。既有测试自建了带 `傀儡等级` 的表，
  所以本地绿、生产必炸。
- 奖励工厂类型不匹配：仓库按 4 元组解包，生产 `create_rewards` 返回
  `PuppetHarvestReward` 数据类 → `TypeError: cannot unpack non-iterable ...`。
- 时间基准不一致：`收取时间` 是无偏移文本，`SystemClock.now()` 是 aware UTC，
  裸减抛 `TypeError: can't subtract offset-naive and offset-aware datetimes`。
- 口径漂移：成熟窗口应为 `max(时间成本 × (1 - 加速基数 × 药材速度), 0)`、产出数量为
  `灵田数量 + 收取等级 + 收获加成`、驱动灵石按 `灵田傀儡` 自身等级取配置；旧实现用
  `elapsed × (1 + speed_base)`、`remaining // time_cost_hours` 且把不存在的列当等级，
  等级 2/3 的玩家被误判为未拥有。
- 状态码不可用：仓库产出 `not_mature`/`puppet_missing`，facade 只识别兼容实现的
  `not_ready`/`blessed_spot_missing`/`inventory_full`/`stone_insufficient`，未知状态
  直接 `return None`；灵石不足也不回写关闭。

修复内容：

- `PuppetHarvestSqlRepository` 重写：读真实列；`normalize_now` 把 aware 实例归一为
  naive UTC、`parse_harvest_time` 解析锚点；成熟窗口/产出数量/驱动灵石与
  `我的灵田` 展示及兼容实现同源；等级无配置时拒绝收取而不是免费发放；灵石不足把
  `puppet_status` 置 0 并提交；返回冻结数据类 `PuppetHarvestResult` 与 `HarvestReward`
  （替代 `type(...)` 动态类）；奖励工厂同时接受 4 元组与数据类；`_coerce_reward`/
  `as_int` 容错坏字段。保留 `puppet_harvest_operations` 幂等语义。
- facade `check_and_harvest` 补齐状态分支，未完成状态写出可读文案并记日志
  （`puppet_disabled` 是每小时遍历常态，保持静默）；`duplicate` 附"已经处理"提示。
- 零 schema 变更：复用 `mix_elixir_info`/`back`/`puppet_status`/
  `puppet_harvest_operations`；未改已应用的 `puppet.001`/`puppet.002` 函数；无新表、列、
  迁移、索引、配置键、命令名、别名或路由；job 注册与摘要不变。
- 测试：新 `tests/test_puppet_harvest_production_path.py` `20/20`，用**真实 player
  schema + 生产奖励工厂 + aware UTC 时钟**复现并钉住上述缺陷，覆盖收取成功、数量口径、
  `药材速度` 缩短窗口、未成熟剩余时间、naive 时钟兼容、未开启、灵石不足关闭傀儡、
  等级 3 计价、背包满 fail-closed、重复 operation 不重发、缺行、缺洞天福地、
  helper 单测、handler 源合同（不再静默 `return None`）。feature 侧
  `test_harvest_repository.py` 的表结构随真实列原地更新。聚焦套件合计 `330 passed`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净；Phase-2 冻结门禁在 `git archive HEAD`
  快照中 `47 OK`。
- 交付与 CI：功能提交 `fd795c82` 的 develop run `40979221766` success；快进合入 main 后
  main run `40979718077` success（headSha 均为 `fd795c82`；`build-and-release` 按分支
  规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持
  未暂存。

## 已完成目标：炼丹升级闭环修复（升级收取等级 / 升级丹药控火）

`炼丹升级` 两条玩家命令在生产形态下都不可用，且都被自建 schema 的旧测试掩盖。
两条缺陷都源自同一重构提交 `00768565`。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- **`升级丹药控火` 必然抛错**：`features/mixelixir/fire_control_upgrade_repository.py`
  读写 `mix_elixir_info."控火等级"`，而真实列是 `丹药控火`（权威来源
  `xiuxian2_handle.mix_elixir_infoconfigkey`，**没有 `控火等级`**）。SELECT 里 SQLite
  把未解析标识符当**字符串字面量**（探针 row 为 `'控火等级'`，`CAST` 后 0），UPDATE
  直接抛 `OperationalError: no such column: 控火等级`。探针在 `丹药控火=0/1/2` 三种
  等级下全部复现，等级与经验零变化；handler 无 try/except，玩家收不到任何回复。
  既有测试 `features/mixelixir/tests/test_fire_control_upgrade_repository.py` 自建了
  带 `控火等级` 的表，所以本地绿、生产必炸。
- **`升级收取等级` 对钱包非 0 的玩家恒失败**：`features/mixelixir/application.py` 的
  `harvest_level_upgrade` 默认分支把 `expected_stone=0` 硬编码传给仓储，而仓储把它当
  钱包并发快照，`current_stone != expected_stone` 即返回 `state_changed`。探针（真实
  application）实测 `stone=0` → applied、`stone=1`/`stone=5000` → state_changed；
  即只有钱包恰好为 0 的玩家能升级。handler 也从未透传钱包快照（兄弟
  `升级丹药控火` 传了）。旧测试的钱包值恰好等于硬编码快照，才得以通过。

修复内容：

- 两个升级仓储把列名收在类常量（`_LEVEL_FIELD = "丹药控火"` /
  `"收取等级"`，`_EXPERIENCE_FIELD = "炼丹经验"`），SELECT 与 UPDATE 共用，读写不再
  可能漂移；删除全部 `控火等级` 引用。
- `harvest_level_upgrade` 新增 `expected_stone` 参数并原样透传仓储；删除写死
  `expected_stone=0` 与「显式 repository」分支三重复的嵌套三元表达式，改为与
  `fire_control_upgrade` 同形的单一 `call`。`expected_stone=None` 语义保留
  （显式跳过钱包校验）。
- `升级收取等级` handler 传 `expected_stone=user_info['stone']`，与 `升级丹药控火`
  一致。两条命令都只消耗 `炼丹经验`，都不扣灵石。
- 零 schema 变更：复用 `mix_elixir_info`、`user_xiuxian.stone` 与两张既有
  operation 表；未改已应用迁移函数；无新表、列、迁移、索引、配置键、命令名、别名或
  路由；兼容 service 保留为显式回滚路径且列名常量与 feature 仓储一致。
- 测试：新 `tests/test_mixelixir_upgrade_production_path.py` `16/16`（含 7 subtests），
  用**真实 `mix_elixir_info` 列集 + 真实 `MixelixirApplication` + `OperationLedger`**
  复现并钉住两条缺陷，覆盖 `丹药控火=0/1/2`、duplicate 重放不重扣、operation_conflict、
  经验不足、钱包快照过期、钱包 `0/1/5000/极大值` 全通过、`None` 跳过快照、缺表/缺行
  fail-closed、操作行写入失败整体回滚、列名常量与 handler 源合同。feature 侧
  `test_fire_control_upgrade_repository.py` / `test_harvest_level_upgrade_repository.py`
  的表结构与断言同步改为真实列。聚焦套件合计 `334 passed + 9 subtests`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净；`unittest discover -s tests -p 'test_mixelixir*'`
  `46 OK`。
- 交付与 CI：功能提交 `37e87326` 的 develop run `41008196196` success；快进合入 main 后
  main run `41008883828` success（headSha 均为 `37e87326`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：宗门命令契约闭环修复（搜寻/创建/加入/传位/任务结算）

宗门有 8 条玩家命令在生产数据上不可用或把成功渲染成失败。全部源自同一重构接线漂移，
且都被自建 schema / 自建 stub 的旧测试掩盖。目标文档 `.codex-next-goal.md`（local-only，未提交）。
证据方法：用插件自身的迁移目录把 `game_db`/`player_db` 迁到临时库得到真实 schema，
再按 `xiuxian2_handle` 的基础 DDL + `xiuxian_config.py` 的 `sql_sects`/`sql_user_xiuxian`
ALTER 列表得到真实旧表列集，最后调用真实 `SectApplication` 与真实 feature 仓储逐条实测。

- **宗门功法搜寻 / 宗门神通搜寻 必然抛错（三重缺陷）**：
  1. `xiuxian_sect/__init__.py` 以 `owner_position=owner_position` 调用
     `apply_buff_search`，而该方法签名无此关键字 → 实测
     `TypeError: got an unexpected keyword argument 'owner_position'`；
     同文件兄弟方法（`close_mountain`/`inherit_owner`/`open_join`/`disband`/`upgrade_*`）
     都接受 `*, owner_position: int = 0`，唯它漏了。handler 无 try/except，玩家收不到回复。
  2. `buff_search_repository.py` 读写**不存在的列** `sect_mainbuff` / `sect_secbuff`；
     真实 `sects` 列是 `mainbuff` / `secbuff`（权威来源 `xiuxian_config.py` 的 `sql_sects`）。
     实测 `UPDATE sects SET sect_mainbuff=...` 抛
     `OperationalError: no such column: sect_mainbuff`，而
     `SELECT "sect_mainbuff"` 返回字符串字面量 `'sect_mainbuff'`
     （与已修 `控火等级`、`傀儡等级` 同一 SQLite 语义陷阱）。修好 (1) 后命令依然必炸。
     对照：兼容实现 `legacy_sect_membership.py` 的 `{"main": "mainbuff", "secondary": "secbuff"}`
     与 `features/sect/repository.py` 都正确。
  3. 仓储把宗主校验写成 `int(user['sect_position'] or 0) != 0`，硬编码 0；
     兼容实现比较调用方传入的 `owner_position`，故非 0 宗主位的宗门配置永远拒绝。
- **`SectMutationResult.applied` 漏掉真实成功状态**：只认
  `claimed/closed/disbanded/duplicate/granted/inherited/upgraded`，漏掉
  `applied`/`charged`/`created`/`opened`/`transferred`/`settled`。实测
  `创建宗门`(`created`)、`宗门改名`计费(`charged`)、`开放宗门加入`(`opened`)、
  `宗主传位`(`transferred`)、`宗门任务完成`(`settled`)、功法/神通搜寻(`applied`)
  全部 `applied=False`，成功被渲染成失败并要求玩家重试。
- **`宗主传位` 读取不存在的属性**：handler 读 `result.succeeded`，而 `transfer_owner`
  返回 `SectMutationResult`（dict 子类，只有 `.applied`），
  `.succeeded` 走 `__getattr__` 抛 `AttributeError`（实测确认）。

修复内容：

- `features/sect/buff_search_repository.py`：列名收进
  `_BUFF_COLUMNS = {"main": "mainbuff", "secondary": "secbuff"}`，SELECT/UPDATE 共用；
  `apply(...)` 增加 `*, owner_position: int = 0` 并用于宗主校验，去掉硬编码 0。
- `features/sect/application.py`：`apply_buff_search` 增加 `*, owner_position: int = 0`
  并原样透传；`SectMutationResult.applied` 补齐六个真实成功状态。
- `xiuxian/xiuxian_sect/__init__.py`：`宗主传位` 成功判定由 `result.succeeded` 改为
  `result.applied`。
- 零 schema 变更：复用 `sects`、`user_xiuxian`、`sect_buff_search_operations` 及既有
  operation 表；未改已应用迁移函数；无新表、列、迁移、索引、配置键、命令名、别名或路由。
- 测试：新 `tests/test_sect_command_contract_production_path.py` `25/25`（含 35 subtests），
  用**真实旧表列集**（含 `mainbuff`/`secbuff`、**无** `sect_mainbuff`/`sect_secbuff`）
  + 真实 `SectApplication` + 真实 feature 仓储 + 真实任务迁移表，复现并钉住上述缺陷，
  覆盖成功/失败状态词表、搜寻 main/secondary 扣减与幂等重放不二次扣减、非宗主与资材不足
  零变化、`owner_position` 生效与不匹配被拒、创建/改名/开放/关闭/传位/任务结算各状态、
  传位结果无 `succeeded` 属性、handler 源合同。feature 侧
  `test_buff_search_repository.py` 表结构改为真实列并新增 `owner_position` 用例。
  聚焦套件合计 `386 passed + 37 subtests`；`unittest discover -s tests -p 'test_sect*'`
  `222 OK`、`-p 'test_source_quality*'` `246 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。全量 `unittest discover -s tests` 在改动前后
  的失败集合逐名比对**完全一致**（37 个 distinct，全部源于用户有意删除的
  `docs/refactor_phase2_legacy_path_items.json`），确认零回归。
- 交付与 CI：功能提交 `72229709` 的 develop run `41027959408` success（全部步骤 success）；
  快进合入 main 后 main run `41029044007` success（headSha 均 `72229709`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存。

## 已完成目标：砸蛋命令闭环修复（hatch_result keyword-only 调用）

`砸蛋` / `砸宠物蛋` 在生产形态下 100% 不可用。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- **位置参数调用 keyword-only 方法**：`xiuxian/xiuxian_pet/__init__.py:1064` 的前置回放
  检查以 `pet_application.hatch_result(operation_id)` 传位置参数，而
  `features/pet/application.py:152` 定义为 `def hatch_result(self, *, operation_id: str)`
  （keyword-only）。实测 `TypeError: hatch_result() takes 1 positional argument but
  2 were given`。
- **致命位置**：该调用位于 `pet_egg` handler 的灵石判断、容量判断、孵蛋与落库**之前**，
  且 handler 无 try/except，因此**任何**玩家每次发送 `砸蛋` 都在任何业务逻辑执行前抛错、
  收不到任何回复，孵化功能整体失效。
- **对照**：同文件 1185 行（同一次孵化的后置回放）与兼容门面
  `compatibility/pet.py:69` 都用正确的 `hatch_result(operation_id=operation_id)`；
  唯一破损点就是这一处位置参数调用。
- **测试为何未捕获**：既有 `features/pet/tests/test_pet_application.py:34` 的 stub
  写作 `def hatch_result(self, *args, **kwargs)`，接受任意位置参数，故本地绿、生产必炸。
  属已修的签名/语义漂移家族（`控火等级`、`傀儡等级`、`owner_position`、
  `sect_mainbuff`）的**位置参数 vs keyword-only** 变体。

修复内容：

- `xiuxian/xiuxian_pet/__init__.py`：第 1064 行改为
  `pet_application.hatch_result(operation_id=operation_id)`，与同文件后置回放及兼容门面一致。
- `features/pet/tests/test_pet_application.py`：stub 的 `hatch_result` 收紧为与生产一致的
  keyword-only 形态（`def hatch_result(self, *, operation_id)`），使同类漂移可被捕获。
- 零 schema 变更：复用 `pet_hatch_operations` 与既有宠物表；未改已应用迁移函数；
  无新表、列、迁移、索引、配置键、命令名、别名或路由；未改命令名/别名/priority/冷却/文案。
- 测试：新 `tests/test_pet_hatch_production_path.py` `5/5`，用**真实 `PetApplication`**
  （非 stub）+ 真实 `pet_hatch_operations` 表钉住缺陷：位置调用抛 `TypeError`、
  关键字调用返回 `None`、真实落库后经前置回放读出 `duplicate` 且
  `pets`/`updated_meta`/`bag_limit` 与写入一致（`pets` 为冻结元组）、
  缺表时抛错且**不建表**（不写请求期 DDL）、handler 源合同（所有 `hatch_result(` 调用
  均带 `operation_id=` 且无位置参数）。聚焦套件合计 `292 passed`；
  `unittest discover -s tests -p 'test_pet*'` `50 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `40fbbc74` 的 develop run `41033528555` success；
  快进合入 main 后 main run `41034346130` success（headSha 均 `40fbbc74`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存。

## 已完成目标：新增玩家「我的灵石流水」个人经济流水视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：`economy_log` 是全服唯一经济流水账，由真实玩法持续写入
（`features/tasks/claim_repository.py`、`features/dufang/share_repository.py`、
`features/past_life/final_repository.py`、`features/admin_asset/*`，以及
`xiuxian/xiuxian_utils/economy_log.py` 的 `safe_log_economy_change` 7 处调用点），
但玩家完全看不到自己的流水：`features/economy_ledger/manifest.py` 的 `FEATURE`
没有 `commands`；`features/economy_ledger/commands.py` 明确声明只暴露管理员 Web
读取（`COMMANDS = ()`）；唯一入口是需管理员会话与 CSRF 守卫的 Web 蓝图。
数据在、聚合已实现、只有管理员能看，属纯可见性缺口。

真实 schema（用插件自身迁移目录实测，`game_db` 225 条迁移）：`economy_log` 15 列
`id,user_id,sect_id,source,action,stone_delta,exp_delta,sect_contribution_delta,
sect_scale_delta,sect_materials_delta,item_delta,detail,trace_id,created_at,event_id`，
已有 `idx_economy_log_user_source_time(user_id,source,created_at)` 支撑按玩家查询。

交付内容：

- 新玩家命令 `我的灵石流水`（别名 `灵石流水`、`灵石收支`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_base/__init__.py`；展示本人近 7 天灵石收支总览
  （收入/支出/净额/笔数）、主要收入来源前 3、主要支出方向前 3、最近 5 笔明细。
- `EconomyLedgerApplication.stone_flow_summary(user_id)` 复用既有 `query_page`
  （同一仓储、同一 SQL），玩家视图与管理员台账不可能出现口径分歧；
  `user_id` 只由调用方注入，handler 不解析命令参数，**不能查询他人流水**。
- 新纯函数 `format_stone_flow(payload, *, format_number=None)`（无 I/O、无 NoneBot）
  承载文案与省略规则：无收入省略收入段、无支出省略支出段、无记录输出
  `近7天没有灵石收支记录。`、表缺失透传仓储 notice。
- 时间窗**只用下界**：`economy_log.created_at` 由各写入方提供（UTC 与本地时区混用），
  加上界会把「刚刚发生」的本地时间行误判为未来而丢弃；实测该陷阱会把 6 笔近 7 天
  流水错算成 4 笔、收入 9200 错算成 1200。
- 常量集中于 `features/economy_ledger/schemas.py`：
  `STONE_FLOW_DAYS=7`、`STONE_FLOW_TOP_N=3`、`STONE_FLOW_RECENT=5`，无配置键。
- `features/base/manifest.py` 的 `FEATURE.commands` 追加该命令的 `CommandSpec`；
  `docs/features/base.md` 的「命令与别名」与「Manifest 清单」两段登记
  （`check_architecture.py` 要求 feature 文档点名每条命令）。
- 零 schema 变更：只读复用 `economy_log` 与既有索引；不改任何写路径、经济数值、
  Web 路由或已应用迁移函数。
- 清单再生成：`scripts/export_refactor_inventory.py` 使
  `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 641→642、backlog 327→328；
  `tests/test_phase2_legacy_path_gate.py` 按既有活动切片先例同步该期望值
  （活动切片曾 638→641、324→327），**冻结成员文件与其哈希保持不变**
  （实测 `frozen_membership_valid: True`、`integrity_errors: []`、`failures: None`）。
- 测试：新 `tests/test_economy_stone_flow_view.py` `13/13`（`unittest.TestCase`），
  用**真实 `economy_log` 列集** + 真实 `EconomyLedgerApplication` 钉住：汇总口径
  （收入 6200/支出 10000/净额 -3800/4 笔）、**玩家隔离**（他人流水不进任何字段）、
  7 天窗口（窗外行排除）、收入/支出各前 3 排序、最近 5 笔上限、缺表返回 notice 且
  **不建表**、空 `user_id` 拒绝、四种渲染形态（完整/仅收入/仅支出/空）、
  handler 源合同（会话 `user_id` + 不出现 `args`/`extract_plain_text`）。
  聚焦套件合计 `270 passed`；`unittest discover -s tests -p 'test_economy*'` `17 OK`、
  `-p 'test_source_quality*'` `246 OK`、`-p 'test_phase2*'` `47 OK`（冻结文件在位时）。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。全量 `unittest discover -s tests` 在改动前后
  的失败集合逐名比对**完全一致**（37 个 distinct，全部源于用户有意删除的
  `docs/refactor_phase2_legacy_path_items.json`），确认零回归；测试数 3482→3500。
- 交付与 CI：功能提交 `b0b5f3f4` 的 develop run `41043300120` success（全部步骤 success）；
  快进合入 main 后 main run `41044079519` success（headSha 均 `b0b5f3f4`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存。

## 已完成目标：新增玩家「我的资产总览」跨库资产一览视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：玩家「身家」分散在 **两个库、五张表**——灵石（`game_db.user_xiuxian.stone`）、
灵庄存款（`game_db.bank_accounts.saved_stone`）、宗门归属与贡献（`game_db.sects.sect_name` +
`user_xiuxian.sect_contribution`）、背包规模（`game_db.back`）、竞技场段位/积分/荣誉
（`player_db.arena.rank/score/honor_points`）。此前只能靠 `灵石`/`灵庄`/`我的宗门`/
`我的背包`/`我的竞技场` 五条命令各自拼，没有任何合计视图：
`grep -rn "资产|总览|一览" --include=*.py nonebot_plugin_xiuxian_2/xiuxian/ | grep on_command|on_regex`
**零匹配**，`docs/refactor_inventory.json` 的 command 清单也无对应条目。数据齐全、
每条命令都在、只有合计视角缺失，属纯可见性缺口，无需任何新数据。

真实 schema（插件自身迁移 + `xiuxian2_handle` 基础 DDL + `XiuConfig` ALTER 实测）：
`user_xiuxian` 33 列含 `stone/sect_id/sect_contribution`；`sects` 13 列含 `sect_name`；
`back` 13 列含 `goods_id/goods_num/bind_num`；`bank_accounts` 4 列
`user_id,saved_stone,bank_level,updated_at`；`arena` 含 `score/rank/honor_points`。

交付内容：

- 新玩家命令 `我的资产总览`（别名 `资产总览`、`我的资产`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_base/__init__.py`；一次展示灵石、灵庄存款（含等级）、
  宗门（含贡献）、背包（种类/总数）、竞技场（段位·积分·荣誉）。
- 新模块 `features/base/assets_overview.py`：
  `PlayerAssetsOverviewApplication(game_db, player_db).overview(user_id)` 以**只读**
  `DatabaseUnitOfWork(..., read_only=True)` 打开主库并 `attach_database(player_db,
  "player_data")`，每段查询前用 `sqlite_master` + `PRAGMA table_info` 探测表/列，
  旧库缺表缺列自动降级；纯函数 `format_assets_overview(payload, *, format_number=None)`
  （无 I/O、无 NoneBot）承载文案与省略规则。
- **分段省略**：无灵庄开户行、无宗门、背包为空、无竞技场记录时各自独立消失，
  任一段缺失不会让整条回复失败；全部可选段缺失时仍输出标题与「灵石」行。
- **错误响应**：主库不可读或角色不存在 → `未找到角色数据，请先发送【我要修仙】。`；
  `player_db` 文件缺失/无 `arena` 表 → 只省略竞技场段、不抛错；
  `bank_accounts`/`sects`/`back` 缺表 → 只省略对应段，且**只读路径不建任何表**
  （实测 `sqlite_master` 仍无该表）。
- **玩家隔离**：`user_id` 只取 `check_user` 的会话信息，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人资产；测试写入第二名玩家的巨额资产（灵石
  999,999、存款 888,888、贡献 777,777、段位王者）验证零泄漏。
- 零 schema 变更：只读复用上述五张既有表与索引；不新增表、列、索引、迁移版本、
  配置键或 Web 路由；不改任何写路径与经济数值。
- `features/base/manifest.py` 追加该命令的 `CommandSpec`；`docs/features/base.md`
  的「命令与别名」与「Manifest 清单」两段登记（`check_architecture.py` 要求
  feature 文档点名每条命令）；`scripts/export_refactor_inventory.py` 使
  `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 642→643、backlog 328→329；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原 `git show HEAD:` 版本实测
  `exit_ready: True`、`integrity_errors` 无、`exit_blockers` 0 条，随后再次删除）。
- 测试：新 `tests/test_assets_overview_view.py` `17/17`（`unittest.TestCase`），
  用**真实列集**（`user_xiuxian`/`sects`/`back`/`bank_accounts`/`arena`）与真实
  `PlayerAssetsOverviewApplication` 钉住：五段取数（灵石 123,456、存款 500,000 黄金、
  青云宗贡献 8,800、背包 2 种/17 个且 `goods_num=0` 不计、白银 1,520 分/荣誉 3,300）、
  完整渲染五行、**玩家隔离**、四种分段省略、全段缺失仅剩灵石、
  未知/空 `user_id` 引导文案、缺 `bank_accounts`/`arena`/`sects`+`back` 表与
  整个 `player_db` 缺失均降级且不建表、纯函数边界（空等级、无贡献）、
  handler 源合同（命令名/别名/`overview(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `tests/test_assets_overview_view.py` + `test_source_quality.py` +
  `test_phase2_legacy_path_gate.py` + `features/base/tests/` 合计 `401 passed`
  （冻结文件在位时）；`unittest discover -s tests -p 'test_assets*'` `17 OK`、
  `-p 'test_source_quality*'` `246 OK`。architecture、inventory `--check`、
  progress `--json`（`failures: None`）真实 exit 0；`compileall` 与
  `git diff --check` 干净。改动前 stash 比对：`test_phase2_legacy_path_gate.py`
  在干净 HEAD 上同样 `86 failed`（全部源于用户有意删除的冻结文件），确认零回归。
- 交付与 CI：功能提交 `7ac9ea89` 的 develop run `41057871515` success；
  快进合入 main 后 main run `41058553519` success（headSha 均 `7ac9ea89`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、
  不发送真实消息；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与
  `docs/goals/**` 未提交。

## 已完成目标：虚神界统计列契约修复（虚神界闭关 / 投影虚神界）

修复（非新功能）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助能力遇 HTTP 400 不参与，未构成阻塞）。

**缺口证据（真实 schema 实测）**：用插件自身迁移 + `xiuxian2_handle` DDL +
`XiuConfig` ALTER 物化真实库（`game_db` 337 表 / `player_db` 82 表），
`player_data.statistics` 实有 59 列，含 `闭关次数`/`闭关时长`/`闭关修为`/
`闭关灵石消耗`/`虚神界时间获取` 等，但**不含** `虚神界闭关次数`、`虚神界投影次数`。
对真实 schema 直接调用生产仓储，两条玩家命令必然抛错：

- `ImpartClosingEnterSqlRepository.enter()` →
  `OperationalError: no such column: 虚神界闭关次数`
- `ImpartProjectJoinSqlRepository.join()` →
  `OperationalError: table statistics has no column named 虚神界投影次数`

**根因（同一缺陷类）**：两处都用
`CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY, "<列>" INTEGER DEFAULT 0)`
把业务列内联进建表语句；而 `statistics` 已被 `features/base`、`training`、`auction`、
`boss` 等迁移以 `CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)` 先建好，
`IF NOT EXISTS` 分支是 **no-op**，内联列永远不会被创建。两个 handler 在调用处
（`xiuxian_impart_pk/__init__.py:984`、`:236`）都没有 `try/except`，异常直接冒泡，
**玩家收不到任何回复**。既有测试自建带目标列的表，绕开了真实 schema，故本地一直绿。

对照先例（同子系统与其他切片都已做对）：`closing_settlement_repository`（虚神界出关）
先 `PRAGMA` 取列、缺列才 `ALTER`、`UPDATE rowcount==0` 时 `INSERT`；
`past_life/start_repository`（前尘往事次数）同样先 `ALTER`；
旧路径 `_increment_entry_stat` 亦为 `PRAGMA` + `ALTER` + 补行。

交付内容：

- 新模块 `features/impart_pk/statistics_columns.py`：
  `ensure_statistics_column()` 用 `PRAGMA table_info(statistics)` 取真实列，
  缺列才 `ALTER TABLE ... ADD COLUMN "<列>" INTEGER DEFAULT 0`（不 `DROP`、不改已有列类型）；
  `increment_statistics()` 先 `UPDATE ... COALESCE("<列>",0)+? WHERE user_id=?`，
  `rowcount==0` 时 `INSERT` 补行，返回累加后的真实值。两者支持 `schema=""`
  （UoW 主库即 player_db、未 attach 的仓储）。
- `closing_enter_repository.py` 与 `project_join_repository.py` 的统计写入改走该辅助，
  都在既有写事务内执行，事务边界与回滚语义不变。
- 零迁移：不新增迁移版本、不改已应用迁移函数（checksum 不变）、不新增表/索引/配置键/
  命令/路由；不新增或修改任何玩家文案。
- 兼容性：旧库已有该列 → `PRAGMA` 判定存在，跳过 `ALTER` 直接读写；
  干净库首次写入自动补列补行（计数从 1 开始，不再像旧实现那样在无统计行时静默丢失）；
  `statistics` 表完全不存在时兜底建最小表再补列。
- 文档：`docs/features/impart_pk.md` 新增「`player_data.statistics` 统计列契约」一节。
- 测试：新 `tests/test_impart_statistics_column_contract.py` `19/19`（`unittest.TestCase`），
  刻意用**生产形态** `statistics`（只有 `user_id`，绝不内联目标列）钉住：
  两命令 `applied` 且计数 1；闭关两次累加到 2、删除统计行后仍从 1 正确补行；
  重复 `operation_id` 返回 `duplicate` 且计数不增；`busy`/`ineligible`/
  `already_joined`/`pk_exhausted`/`capacity_full` 分支计数不变；投影按玩家隔离；
  旧库预置列（值 5）累加到 6；`statistics` 表缺失时自动创建；
  以及回归防线——断言两个仓储**不得**再把业务列内联进 `CREATE TABLE ... statistics`。
  聚焦套件 `test_impart_statistics_column_contract.py` + `test_impart_closing_enter.py` +
  `features/impart_pk/tests/` + `test_source_quality.py` 合计 `261 passed`；
  `test_phase2_legacy_path_gate.py` `47 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_impart_statistics*'` `19 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 门禁计数：本切片不新增命令，派生计数按预期**保持** `643 / 329` 不变；
  临时还原冻结文件实测 `exit_ready: True`、`exit_blockers` 0 条、无 `integrity_errors`，
  随后再次删除。
- 交付与 CI：功能提交 `c0753bd0` 的 develop run `41071870790` success；
  快进合入 main 后 main run `41072988353` success（headSha 均 `c0753bd0`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的宗门贡献流水」个人贡献收支视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：宗门贡献是玩家用灵石换来的个人额度，`economy_log.sect_contribution_delta`
由真实写路径持续记录——`宗门捐献`（`action="donate"`，贡献 +）、`宗门商店兑换`
（`action="shop_exchange"`，贡献 −）、宗门洞天/炼丹房升级、以及宗门任务结算
（`action="task_complete"`，贡献 +），均位于 `xiuxian/xiuxian_sect/__init__.py`。
但玩家只能看到 `user_xiuxian.sect_contribution` 这一个当前余额，
`grep -rn "贡献流水" --include=*.py` **零匹配**，没有任何命令说明贡献"最近怎么变的"。
数据在、写入点齐全、只有玩家视角缺失，属纯可见性缺口，无需任何新数据。

真实 schema（插件自身迁移实测）：`economy_log` 15 列，含
`sect_contribution_delta`、`source`、`action`、`created_at`；
`user_xiuxian` 33 列含 `sect_contribution`、`sect_id`；`sects` 含 `sect_name`。

交付内容：

- 新玩家命令 `我的宗门贡献流水`（别名 `宗门贡献流水`、`贡献流水`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_sect/__init__.py`；一次展示近 7 天获得/消耗/净变化/笔数、
  主要来源与主要去向各前 3、最近 5 笔明细，以及当前贡献余额与所属宗门。
- 新模块 `features/economy_ledger/contribution_flow.py`：
  `ContributionFlowApplication(game_db, clock=...)` 以只读 UoW 聚合
  `sect_contribution_delta`；`format_contribution_flow()` 为纯函数（无 I/O、无 NoneBot），
  把已知宗门动作映射为中文（捐献/宗门任务/商店兑换/洞天升级/炼丹房升级），
  未知动作回退 `source·action`。
- 口径细节：`COALESCE(sect_contribution_delta,0)<>0` 过滤零变化行，使"笔数"反映真实变动；
  前 3 各按金额降序、笔数次之；最近 5 笔按 `created_at DESC, id DESC`。
- **时间窗只用下界**：`economy_log.created_at` 由各写入方提供、UTC 与本地时区混用，
  加上界会把「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测该陷阱）。
  常量与灵石流水同形，集中在 `features/economy_ledger/schemas.py`
  （`CONTRIBUTION_FLOW_DAYS=7`、`CONTRIBUTION_FLOW_TOP_N=3`、`CONTRIBUTION_FLOW_RECENT=5`）。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人贡献流水。
- **错误/降级响应**：`economy_log` 缺失、缺 `sect_contribution_delta` 列、
  `user_xiuxian` 缺 `sect_contribution` 列、`sects` 表缺失、库不可读——各自只丢失
  它覆盖的部分，均不抛异常；空 `user_id` 返回「缺少玩家身份，无法查询宗门贡献流水。」
  只读路径**不建任何表**（测试断言 `sqlite_master` 前后一致）。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；
  不改任何写路径与经济数值；不新增或修改既有玩家文案。
- `features/sect/manifest.py` 追加该命令的 `CommandSpec`；`docs/features/sect.md`
  的「命令与 Web API」与「命令与别名」两段登记；`scripts/export_refactor_inventory.py`
  使 `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 643→644、backlog 329→330；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原 `git show HEAD:` 版本实测，
  随后再次删除）。
- 测试：新 `tests/test_sect_contribution_flow.py` `16/16`（`unittest.TestCase`），
  用**真实 15 列 `economy_log`** 与真实 `ContributionFlowApplication`（注入固定时钟）钉住：
  汇总口径（获得 1,620 / 消耗 500 / 净 1,120 / 5 笔）、前 3 排序与金额、
  最近 5 笔倒序与上限、零变化行不计笔数、7 天窗外行排除、
  **玩家隔离**（第二名玩家 9,999 零泄漏且各自独立）、中文动作标签与未知动作回退、
  无记录/无角色文案、空 `user_id`、四条降级路径（缺表/缺列/缺宗门表/极旧库）、
  只读不建表，以及 handler 源合同（命令名/别名、`summary(str(user_info["user_id"]))`、
  无 `args`）。聚焦套件 `test_sect_contribution_flow.py` + `test_phase2_legacy_path_gate.py`
  + `test_source_quality.py` + `test_economy_stone_flow_view.py` + `features/sect/tests/`
  合计 `404 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_sect_contribution*'` `16 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `ccd2f6f3` 的 develop run `41077385695` success；
  快进合入 main 后 main run `41078349901` success（headSha 均 `ccd2f6f3`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的物品流水」个人物品收支视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：物品增减已全量记进共享 `economy_log.item_delta`，写入点齐备且语义统一——
背包加减（`xiuxian2_handle.py:2485` 的 `item_add` 与 `:2570` 的 `item_cost`）、
奖励统一发放（`reward_service.py:248`）、宗门商店兑换（`xiuxian_sect/__init__.py:3531`）、
仙肆购买（`xiuxian_trade/__init__.py:642`），元素形状均为
`{"id","name","type","amount",...}`，加物品 `amount` 为正、扣物品为负。
但 `grep -rn "物品流水|道具流水" --include=*.py` **零匹配**，`docs/refactor_inventory.json`
无对应命令；玩家只能翻 `我的背包` 看当前持有，无法回答"今天的丹药/材料怎么来的、花到哪去了"。
属纯可见性缺口，不需要任何新数据。

真实 schema（插件自身迁移实测）：`economy_log` 15 列，`item_delta` 为 **NOT NULL TEXT**，
另有 `source`、`action`、`created_at`、`id`、`user_id`。

交付内容：

- 新玩家命令 `我的物品流水`（别名 `物品流水`、`道具流水`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_back/__init__.py`；一次展示近 7 天获得/消耗件数与种类数、
  变动笔数、主要获得与主要消耗各前 3、最近 5 笔明细。
- 新模块 `features/economy_ledger/item_flow.py`：
  `ItemFlowApplication(game_db, clock=...)` 以只读 UoW 取该玩家近 7 天
  `item_delta` 非空的行，按物品名聚合；`format_item_flow()` 为纯函数
  （无 I/O、无 NoneBot），已知动作映射中文（宗门商店兑换/仙肆购买/物品获得/物品消耗/
  宗门捐献/宗门任务），未知动作回退 `source·action`，最近明细按元素正负标注「获得/失去」。
- 口径细节：`''`/`'[]'`/`'{}'`/`'null'`/`None` 视为无变动、不计笔数，与
  `features/economy_ledger/repository.py` 既有 `item_change` 判定一致；两个方向各按
  累计件数降序、笔数次之取前 3；最近 5 笔按 `created_at DESC, id DESC`。
- **时间窗只用下界**：`economy_log.created_at` 由各写入方提供、UTC 与本地时区混用，
  加上界会把「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测该陷阱）。
  常量与灵石/贡献流水同形，集中在 `features/economy_ledger/schemas.py`
  （`ITEM_FLOW_DAYS=7`、`ITEM_FLOW_TOP_N=3`、`ITEM_FLOW_RECENT=5`）。
- **脏数据容错**：非法 JSON、非 list、元素非 dict、`amount` 非数字——逐行/逐元素跳过，
  其余照常展示；缺 `name` 回退 `物品#<id>`，缺 `type` 记「其他」；早期把单个物品
  直接写成对象的行也兼容。任何异常都在应用层捕获并降级为提示文案，绝不冒泡到 handler。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人物品流水。
- **降级响应**：空 `user_id` → 「缺少玩家身份，无法查询物品流水。」；
  缺 `economy_log` 表或缺 `item_delta` 列 → 「暂无物品流水记录。」；无记录 →
  「近7天没有物品收支记录。」；只读路径**不建表、不补列**（测试断言 `sqlite_master`
  与 `PRAGMA table_info` 前后一致）。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；
  不改任何物品增减逻辑、经济数值与既有玩家文案。
- `features/back/manifest.py` 追加该命令 `CommandSpec`；`docs/features/back.md`
  的「命令与别名」段登记；`scripts/export_refactor_inventory.py` 使
  `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 644→645、backlog 330→331；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原 `git show HEAD:` 版本实测后再次删除）。
- 测试：新 `tests/test_item_flow_view.py` `19/19`（`unittest.TestCase`），
  用**真实 15 列 `economy_log`**（`item_delta` NOT NULL）与真实 `ItemFlowApplication`
  （注入固定时钟）钉住：汇总口径（获得 112 件/2 种、消耗 20 件/1 种、5 笔）、
  四种空形状不计笔数、前 3 排序与笔数、最近 5 笔倒序与上限、7 天窗外排除、
  **玩家隔离**（他人 999 零泄漏且各自独立）、完整渲染、混合方向行双标注、
  未知动作回退、四种脏数据形状、dict 包装遗留行、缺 type 默认「其他」，
  以及无记录/空 `user_id`/缺表/缺列（不补列）/只读不建表五条降级路径，
  外加 handler 源合同（命令名/别名、`summary(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `test_item_flow_view.py` + `test_phase2_legacy_path_gate.py` +
  `test_source_quality.py` + `test_sect_contribution_flow.py` + `features/back/tests/`
  合计 `332 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_item_flow*'` `19 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6485cbfb` 的 develop run `41095115054` success；
  快进合入 main 后 main run `41095961222` success（headSha 均 `6485cbfb`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：帮助文本补齐已上线玩家命令（发现性修复）

修复（非新功能）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：近四个切片新增的四条玩家命令**上线即隐身**——`grep` 三个帮助文本块零命中，
只存在于 matcher 定义与 handler docstring：

| 命令 | 应有帮助入口 | 修复前 |
| --- | --- | --- |
| `我的灵石流水` | 修仙帮助 | 未列出 |
| `我的资产总览` | 修仙帮助 | 未列出 |
| `我的宗门贡献流水` | 宗门帮助 | 未列出 |
| `我的物品流水` | 背包帮助 | 未列出 |

复核命令：
`grep -rn "我的物品流水\|我的灵石流水\|我的资产总览\|我的宗门贡献流水" --include=*.py nonebot_plugin_xiuxian_2/xiuxian/ | grep -v on_command`
仅命中四条 handler docstring。即功能做了但玩家没有任何途径发现，属半个功能缺失；
既有测试只覆盖命令行为，**没有一条**断言"已上线命令必须出现在对应帮助文本里"。

交付内容：

- `修仙帮助`（`help_in_`）新增 `**流水与总览**` 分组（置于 `**功能入口**` 之前），
  列出 `我的灵石流水`（近7天灵石收支与主要来源去向）与
  `我的资产总览`（灵石、灵庄、宗门、背包与竞技场一览）。
- `宗门帮助`（`__sect_help__`）在 `**任务与商店**` 段 `宗门兑换` 之后追加
  `我的宗门贡献流水`（查看本人近7天贡献收支）。
- `背包帮助`（`back_help_`）在 `**查看**` 段 `灵石` 之后追加
  `我的物品流水`（查看本人近7天物品获得与消耗）。
- 统一沿用既有「`- 命令名` + `> 一行说明`」格式；**纯追加**，不改任何既有条目、
  分组名、快捷按钮（`k1..k4` / `button_id`）或 handler 行为；`宗门帮助` 仍走
  `paginate_text_blocks(..., per_page=3)`，不写死页数。
- 零 schema/命令变更：不新增表、列、索引、迁移版本、配置键、命令或 Web 路由；
  派生计数按预期**保持** `645 / 331`（临时还原冻结文件实测 `exit_ready: True`、
  `exit_blockers` 0 条，随后再次删除）。
- 文档：`docs/features/base.md`、`docs/features/sect.md`、`docs/features/back.md`
  各补一句"已登记在对应帮助入口"，保持文档与玩家入口一致。
- 测试：新 `tests/test_help_discoverability.py` `10/10`（`unittest.TestCase`），
  把这件事变成合同：
  - **正向**：四条命令既已注册，又逐字出现在所属帮助块中，且每条带 `>` 一行说明
    （不接受只甩命令名）。
  - **不倒退**：三个帮助块的原有分组名（`**基础指令**`/`**任务与商店**`/
    `**使用与整理**` 等）与抽样既有条目仍在；帮助快捷按钮未改。
  - **反向防漂移**：帮助块里每个 `- ` 条目都必须能落到真实命令上
    （等于主名/别名，或是某个主名/别名的口语简称），因此帮助**不可能**再宣传
    一条不存在的命令。
  - **分页**：`宗门帮助` 首页仍非空，且新条目可经翻页到达。
  - 收集器覆盖整个包（命令也注册在 `adapters/nonebot/commands.py`），并能从
    `"修仙签到" if _legacy_sign_in_enabled else "__legacy_sign_in_disabled__"`
    这类条件首参里取出真实命令名字面量、过滤 `__` 占位符。
  聚焦套件 `test_help_discoverability.py` + `test_source_quality.py` +
  `test_phase2_legacy_path_gate.py` + `features/back/tests/` + `features/sect/tests/`
  合计 `389 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_help_discoverability*'` `10 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6f461149` 的 develop run `41172607668` success；
  快进合入 main 后 main run `41172947130` success（headSha 均 `6f461149`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的签到统计」签到累计视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：每次成功签到都会把统计写进投影，写入点在真实生产路径上——
`features/sign_in/application_effects.py:54` 的
`self.statistics.record(user_id=..., operation_id=f"statistics:{operation_id}",
event_key="修仙签到", occurred_at=now)`，其 effects 由 `plugin.py:1009` 以
`SignInApplicationEffects(..., statistics=SignInStatisticsRepository(str(game_db)))` 装配。
但 `grep -rn "签到统计\|签到记录"` 只命中 newapi 的无关命令，**没有任何玩家命令读它**；
`SignInStatisticsRepository.value(user_id, event_key)`（`features/sign_in/statistics.py:44`）
本就是为这个视图准备的读接口，长期只被测试与写入侧使用。数据在、读接口在、玩家入口完全没有。

真实 schema（插件自身迁移实测，两表都在 **game_db**）：
`sign_in_statistics_events(operation_id,user_id,event_key,created_at)`、
`sign_in_statistics_projection(user_id,event_key,value)`。

交付内容：

- 新玩家命令 `我的签到统计`（别名 `签到统计`、`签到记录`，priority 7，`block=True`，
  注册在 `xiuxian/xiuxian_base/__init__.py` 与 `修仙签到` 同文件）；展示累计签到次数、
  本月签到天数、最近一次签到时间。
- 新模块 `features/sign_in/statistics_view.py`：
  `SignInStatisticsViewApplication(game_db, clock=...)` 以只读 UoW 读投影表取累计次数、
  聚合事件表取本月天数与最近时间；`format_sign_in_statistics()` 为纯函数
  （无 I/O、无 NoneBot）。
- **口径合同**：累计次数与 `SignInStatisticsRepository.value(user_id, "修仙签到")`
  使用同一 SQL、同一 `event_key`；测试在同一夹具上断言两者相等，玩家视图与写入侧
  不可能分歧。
- **本月口径**：按 `created_at` 的 `YYYY-MM` 前缀匹配（不用 `strftime`，避免毫秒/时区
  后缀解析失败），天数用 `substr(created_at,1,10)` 去重——同一天两条签到只算一天。
- **分段降级**：两表都缺 → `暂无签到统计数据。`；只有投影表 → 显示累计、省略本月与最近；
  只有事件表 → 累计按事件行数兜底；`created_at` 为空的行不计天数、不污染最近时间；
  任何异常在应用层捕获并降级为提示文案。只读路径**不建表、不补列**。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人签到统计。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；不改签到写路径、
  奖励、连签规则或 `is_sign` 标记。
- 帮助与文档：`修仙帮助` 的「流水与总览」段追加 `我的签到统计`；
  `docs/features/sign_in.md` 登记命令与口径；既有
  `tests/test_help_discoverability.py` 的 `EXPECTED` 扩到第五条，反向防漂移合同
  继续生效。
- `features/sign_in/manifest.py` 追加该命令 `CommandSpec`；
  `scripts/export_refactor_inventory.py` 使 `docs/refactor_inventory.json` 新增 1 条
  command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 645→646、backlog 331→332；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原后再次删除）。
- 测试：新 `tests/test_sign_in_statistics_view.py` `14/14`（`unittest.TestCase`），
  用**真实两表列集**与真实应用（固定时钟）钉住：汇总口径（累计 5 次、本月 3 天、
  最近 10-30 09:12）、同日去重、跨月排除、其他 `event_key` 忽略、
  **与 `SignInStatisticsRepository.value()` 取值一致**、玩家隔离、
  三种降级形态（双表缺/只有投影/只有事件兜底）、空 `user_id`、脏 `created_at`、
  从未签到引导文案、只读不建表，以及 handler 源合同（命令名/别名、
  `summary(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `test_sign_in_statistics_view.py` + `test_help_discoverability.py` +
  `test_phase2_legacy_path_gate.py` + `test_source_quality.py` +
  `features/sign_in/tests/` 合计 `326 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_sign_in_statistics*'` `16 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `14f9e346` 的 develop run `41180959755` success；
  快进合入 main 后 main run `41181330664` success（headSha 均 `14f9e346`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 冻结清单与停止条件

冻结范围仅包括活动 Web 六来源一键代领 API、权限/错误合同、管理员面板、兼容性保护、聚焦测试、路由清单和正式文档。不得新增奖励经济、schema/数据迁移、玩家身份模型、后台活动编辑器、通用 UI 框架或 unrelated refactor。

成功条件：本轮目标的状态、持久化兼容、交互、聚焦测试和正式文档均完成；diff 检查通过；只提交目标代码、测试和正式文档到 develop，CI 成功且审查通过后普通合入 main 并验证 main CI。本轮按用户指示不部署、不停机、不读取或写入真实游戏数据；已知部署验收事实与本次功能发布门禁分开记录。

停止条件：Release 质量门不能在不违反“无全量测试”要求时完成、发现共享环境/数据冲突、冷备份 hash 不一致、迁移试验需要触碰真实库、活动能力缺少既有入口/查询、或验收需扩大到冻结范围之外。此时保留现场并报告具体阻塞，不绕过门禁或推测通过。

安装/更新/重启/测试串行取得 `flock -w 600 /tmp/nonebot-project-validation.lock`；短工作持锁，结束后释放。运行数据、凭据和 `boss_info.json` 永不暂存。
