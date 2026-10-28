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

## 冻结清单与停止条件

冻结范围仅包括活动 Web 六来源一键代领 API、权限/错误合同、管理员面板、兼容性保护、聚焦测试、路由清单和正式文档。不得新增奖励经济、schema/数据迁移、玩家身份模型、后台活动编辑器、通用 UI 框架或 unrelated refactor。

成功条件：本轮目标的状态、持久化兼容、交互、聚焦测试和正式文档均完成；diff 检查通过；只提交目标代码、测试和正式文档到 develop，CI 成功且审查通过后普通合入 main 并验证 main CI。本轮按用户指示不部署、不停机、不读取或写入真实游戏数据；已知部署验收事实与本次功能发布门禁分开记录。

停止条件：Release 质量门不能在不违反“无全量测试”要求时完成、发现共享环境/数据冲突、冷备份 hash 不一致、迁移试验需要触碰真实库、活动能力缺少既有入口/查询、或验收需扩大到冻结范围之外。此时保留现场并报告具体阻塞，不绕过门禁或推测通过。

安装/更新/重启/测试串行取得 `flock -w 600 /tmp/nonebot-project-validation.lock`；短工作持锁，结束后释放。运行数据、凭据和 `boss_info.json` 永不暂存。
