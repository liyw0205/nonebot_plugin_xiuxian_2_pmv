# Activity lifecycle

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 当前垂直切片：活动中心

`活动中心`（别名 `活动大厅`）是本阶段的最小只读入口。它从版本化活动配置和一次
玩家活动快照生成活动卡片，显示 `未开始`、`进行中`、`已结束` 或当前玩家存在可领取
奖励时的 `奖励领取`。活动名称、时间、描述和奖励摘要均来自配置数据，handler
不内嵌具体活动内容；奖励领取仍交给既有任务、战令和统一活动领取入口。

卡片入口按生命周期映射既有命令：进行中提供签到、任务、战令，以及已配置阶段时的
`活动阶段`、已配置榜单时的 `活动排行`；奖励领取状态保留一键 `活动领取` 为首项，
并提供任务、阶段奖励和排行奖励的既有领取命令。未开始和已结束状态不展示阶段/排行
入口。入口只引用既有命令，不新增路由或玩法；省略排行配置仍沿用默认签到榜，显式
配置 `boards: []` 则表示本活动没有榜单。玩法入口按玩法分组渲染：主状态入口
一行，之后每个进行中的玩法一行（`{玩法名}：入口 | 入口`），全部配置入口都
可达；入口数量约束属于投递层（消息长度），映射层不再截断。

`奖励领取` 的判定与一键 `活动领取` 覆盖的来源一致：任务、战令、阶段、排行、
首领进度和首领排行任一来源可领取即置位，因此只差阶段或排行奖励的玩家同样能看到
领取入口。判定只读复用既有表（`activity_stage_claim`、`activity_rank_claim`、
`activity_boss_milestone`、`activity_boss_milestone_claim`、`activity_boss_rank_claim`、
`activity_boss_damage` 及既有日志表），不新增表、不写库；快照未携带某来源状态时该
来源计 0，旧调用方行为不变。

当卡片状态为 `奖励领取` 且玩家快照明确携带某来源的可领取状态时，入口区在一键
`活动领取` 和任务入口之后，按既有顺序追加来源级快捷入口：阶段、排行、战令、首领
进度、首领排行分别映射 `活动阶段领取`、`活动排行领取`、`活动战令领取`、
`活动首领领奖 进度`、`活动首领领奖 排行`。每个目标最多出现一次；配置存在但没有
资格、已领取、未上榜，或旧库缺少可选状态表时均不展示对应入口。快捷入口只调用既有
领奖命令，交易、幂等和拒绝语义不变，不新增配置键、路由、表或迁移。QQ 蓝字链接携带
完整命令参数，OneBot/无 Markdown 路径仍只显示可读入口名称。

进度行同时展示首领进度：`{首领名} 血量 {pct}%、我的伤害 {damage}`，存在可领取的
进度奖励时追加 `进度奖励 N 项可领`，与一键领取的首领进度计数同源。进度行还包含当前阶段
（`阶段 {名称} 剩余 {倒计时}`，无限或已结束不显示倒计时）与本人榜单名次
（`{榜单名} 第 N 名`），数据来自同一只读快照与活动配置。积分玩法的进度段在余额后
追加当日进度：每个配置 `daily_limit` 的事件规则渲染 `（今日 {label} 今日
{earned}/{limit}）`（达上限追加 `已达上限`），数据与结算侧每日上限同源
（`activity_point_event_log` 当日累计，按 activity+user+date 有界查询）；事件日志表
缺失（旧库）或未配置 `daily_limit` 时不渲染该进度；`活动积分` 视图保持与旧
`build_activity_points_text` 逐字相等，不携带个人进度。

QQ 使用现有原生 Markdown、蓝字命令链接和可选键盘提取；OneBot 或 Markdown 不可用时，
同一文案经公共发送入口降级为纯文本。此切片只读，不新增表、不执行奖励发放、不引入
后台任务，也不覆盖集字、积分商店或活动首领的既有结算逻辑。

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

`活动阶段` 玩法新增两条玩家命令，均只映射既有能力：

- `活动阶段`：只读查看当前阶段（名称、类型、时间、剩余、开放内容、倍率）、下一阶段，
  以及本人可领取的阶段奖励摘要；活动未开启或没有进行中的阶段时给出确定文案。
  视图末尾附【阶段日程】：按配置顺序列出全部阶段的状态标记（进行中/已结束/未开始）、
  时间窗口与剩余倒计时；已结束且配置奖励的阶段按本人参与证据与领取记录标注
  `已领取` / `可领取｜活动阶段领取` / `未领取（参与记录来自活动期间的签到/任务/玩法记录）`
  （复用"可领阶段奖励"同一查询路径，无第二往返；`活动阶段领取` 无 query 时领取
  全部可领阶段，该语义已由行为测试固定），匿名查看只列公共日程不渲染领奖标注；
  未配置阶段时不出现日程段。
- `活动阶段领取`（别名 `领取阶段奖励`）：领取**已结束**且本人在该阶段内有参与记录、
  配置了奖励、尚未领取的阶段奖励；可加阶段名过滤。重复领取不重复发放，活动未开启、
  没有已结束阶段、未配置奖励、无参与记录各自给出明确拒绝文案。

`活动奖励` 给出全部奖励来源的档位与本人状态：签到（每日/累计，含 `我的累计签到 N 天，已达成 M/T 档`）、
每日/周常任务、战令、阶段、排行、首领进度与首领排行，每档标注 `已领取`/`可领取`/`未达成`，
排行段另附 `我的名次`；未配置的来源不渲染该段，未解析到用户时只渲染配置档位。
`rewards_text()`（配置口径，与旧 `build_activity_rewards_text()` 逐字相等）保持不变，
个人状态由新增只读方法 `reward_catalog_text(user_id)` 提供。`活动商店` 查看视图
由 `point_shop_guidance_text` 提供（sibling；`point_shop_text` 保持与旧
`build_activity_shop_text` 逐字相等）：每个商品行追加 `购买：活动购买 {商品名}`
引导；已达个人上限标注 `已兑换完（x/y）`、全服库存售罄标注 `已售罄`，两者不再
显示购买引导；`limit<=0` 不限商品始终可购；视图尾部有可购商品时显示
`可购商品 N 件，购买：活动购买 {商品名}` 汇总。购买结算、
`personal_limit/stock_limit` 校验与拒绝语义不变。

阶段 `features` 声明「开放内容」，对领奖路径一致生效：当前阶段不含 `claim` 时，
`活动阶段领取`、`活动排行领取` 与一键 `活动领取` 的对应步骤同样拒绝，文案为
`当前阶段【{阶段名}】不开放活动领奖`，与旧路径（任务/战令/积分/首领/集字/兑换/商店/签到）
的拒绝文案一致。

阶段配置位于 `extensions.stages[]`，可选 `rewards` 行（`{key?, name, reward}`）；缺省
表示该阶段无奖励，旧配置语义不变。领取记录写入增量表 `activity_stage_claim`
（`activity_key,user_id,stage_key,reward,create_time`），参与证据复用既有日志表
（`activity_sign_log`、`activity_point_event_log`、`activity_pass_event_log`、
`activity_collect_drop_log`、`activity_boss_fight_log`），不新增日志、不改既有表结构。

活动排行榜玩法把 `活动排行` 从单一签到榜扩展为配置驱动的多榜只读视图，并新增
`活动排行领取`（别名 `领取排行奖励`）结算名次奖励：

- `活动排行`：无参数时列出每个已配置榜单的本人名次与参与人数；带榜单名（如
  `活动排行 积分榜`）时展示该榜 top-N、本人名次与达标档位。榜单名支持 `key`、
  `label` 或 `source` 匹配，未知榜单返回可用榜单列表。
- `活动排行领取`：按配置档位匹配本人名次发放奖励；可加榜单名限定。重复领取不重复
  发放，活动未开放、未配置排行奖励、未知榜单、未上榜、未达到名次、已领取各自给出
  明确拒绝文案。

榜单配置位于 `extensions.rank`（可选）：`boards` 为 `[{key,label,source,limit}]`，
`source` 取 `sign`/`points`/`collect`/`pass`/`boss`，`limit` 归一为 1..100；`rewards`
为 `[{board,rank_min,rank_max,key?,name,reward}]`，名次为闭区间，`reward` 复用既有
`parse_reward` 词汇。缺省（无 `rank` 键）时仅保留签到榜且无奖励，旧行为不变。榜单
数据只读复用既有表（`activity_user`、`activity_point_balance`、
`activity_collect_inventory`、`activity_pass_balance`、`activity_boss_damage`），
不新增日志表；领取记录写入增量表 `activity_rank_claim`
（`activity_key,user_id,board_key,tier_key,reward,create_time`），不改既有表结构。

`活动首领` 的状态块除首领名、全服血量、我的累计伤害、今日挑战与活动道具外，还列出
全服进度奖励档位：按 `hp_percent` 降序显示名称、`血量≤X%`、奖励与本人状态
（`已领取` / `可领取` / `未解锁`）；存在可领取档位时给出 `活动首领领奖 进度` 提示。
未配置 `server_milestones` 时不渲染该段。状态只读复用 `activity_boss_milestone`
（已解锁）与 `activity_boss_milestone_claim`（本人已领），不新增表、不写库。

状态块还按配置模式给出行动提示：`item_raid`/`both` 追加
`讨伐：活动讨伐 {首领名} {首个道具名}`，`cooperative`/`both` 追加
`讨伐：活动讨伐`。本人名次达到某排行档且该档未领时追加
`可用 活动首领领奖 排行 领取`；存在多个活跃首领时，排行/进度提示都会带上
首领名（`活动首领领奖 排行 {首领名}` / `活动首领领奖 进度 {首领名} …`），
因为无名称的领奖命令在多首领下会被拒绝。排行档位读取 `activity_boss_rank_claim`
（可选表，缺表时视为全部未领）；名次来自既有 `board_position` 查询。

## Web API
`GET /api/v1/activity/state?user_id=<id>[&view=center|detail|stage|rank|boss|shop|bag|reward|gameplay]` 返回只读活动状态文本：
默认 `view=center` 为活动中心卡片与本人进度行，`detail` 复用配置驱动的 `活动` 详情读模型，
`stage`/`rank`/`boss` 分别复用 `活动阶段`/`活动排行`/`活动首领` 的读模型，`shop`/`bag`/
`reward`/`gameplay` 分别复用 `活动商店`（购买引导）/`活动背包`（就绪标注）/`活动奖励`
（奖励目录）/`活动玩法`（个人状态段）的读模型，与 bot 侧六个玩家视图同源同文（路由不重渲染，
直接返回读模型文本）。未传 `user_id`
时各视图只返回公共活动信息；带 `user_id` 的个人进度查询要求管理权限，避免把未绑定 Web
身份的任意玩家状态公开给匿名请求。视图依赖可选活动表时，缺表按既有失败关闭合同返回
`schema_missing`（503），不臆造数据。未知 `view` 返回 `validation_error`（400），权限不足返回
`forbidden`（403）。面板「活动」页提供上述视图选择、可选玩家 ID 与查询操作，读模型文本通过
纯文本节点呈现；`PAGE_ENDPOINTS["activity"]`/`["reward_center"]` 仍指向该路由。详情视图的
行动建议段与活动中心卡片覆盖完全相同的可领取来源（任务、战令与追赶、集字保底、阶段、排行、
首领进度、首领排行），两者复用同一 `detail_action_tips` 判定，提示与一键领取入口一致；
首领排行名次按首领玩法活动自身的 key 读取（快照 `boss_board_positions` 按首领活动逐个
计算名次，多首领配置下各首领按自己的名次匹配自己的档位；单首领与旧快照回退到共享
`board_positions["boss"]`）。首领进度与排行领取状态同样按首领活动各自归并
（`_boss_claim_state_by_activity`，回执按 `(activity_key, user, key)` 独立），多首领
配置下一个首领的已领/已解锁不再抑制或冒充另一首领的同名档位；单首领与旧 flat 快照
回退到共享投影。详情与状态
读取只使用活动配置及既有只读快照，不建表、不补写迁移、不创建缺失数据库文件；空库无个人
数据时仍可读取公共详情。审计日志仍由 `GET /api/v1/activity` 提供。写入口仍是既有
`POST /api/v1/activity/rewards/claim`（一键领取，带 `Idempotency-Key`，覆盖六来源）。

管理员活动页可对当前填写的玩家 ID 发起代签到：`POST /api/v1/activity/sign-in`，权限为
`admin`，要求 CSRF 与 `Idempotency-Key`，请求字段为 `user_id`。它调用既有
`ActivityApplication` 的 `claim_sign`，签到开关/阶段、每日唯一、配置奖励、玩家存在与背包
上限全部沿用命令规则；成功返回 `200`，确定的业务拒绝返回 `409`。面板在空玩家 ID 时禁用
操作，收到完成结果后刷新相同玩家的活动视图。当前 Web 没有可验证的玩家主体绑定，因此
不提供由匿名/普通 Web 身份任意指定 `user_id` 的玩家自助写入口；管理员个人查询与代办均受
管理员权限保护。

活动页也可为当前玩家 ID 执行六来源一键代领：`POST /api/v1/activity/rewards/claim`，权限为
`admin`，要求 CSRF、非空 `user_id` 与稳定操作 ID（新面板通过 `Idempotency-Key` 传递；旧
body `operation_id` 仍兼容）。它复用既有 `ActivityRewardApplication` 与六步领取协调器，覆盖
任务、战令、首领进度、首领排行、阶段和排行；重复键重放原结果，遇到可恢复的部分失败时保留
同一键续跑，已完成子操作不重复发奖。成功返回 `200`，确定拒绝和可恢复子步骤失败返回结构化
`409`，数据库/schema 不可用返回 `503`。管理员面板完成或收到业务结果后刷新同一玩家的活动视图。
`user` 目前不是可验证的玩家身份，故该 Web 写路由不可由匿名/普通请求代玩家提交；玩家本人继续
通过 bot `活动领取` 命令使用同一业务协调器。请求前只读检查 game DB、外层 ledger/audit 与六步
协调器 schema；缺失或旧 schema 不会触发请求期建库、建表或迁移。

代签到只使用既有 `game_db` 迁移表。请求前只读检查 `operation_ledger`、签到及玩法事件回执、完整
活动状态/进度表、签到日志与角色资产表；数据库不存在或 schema 未就绪返回 `503 migrations_required`，不在
请求路径创建文件、表或迁移，也不回写独立旧 `activity/activity.db`。同 operation ID 重放由外层
ledger 回放；若进程在签到事务提交后、外层 ledger 完成前中断，`claim_sign` 可依据同事务提交的
`activity_sign_settlement_operations` 业务回执续跑并返回 duplicate，不重复发奖。其他活动动作的
started 状态恢复白名单不变。

## 数据模型与迁移
Manifest 保留历史 migration 标记 `legacy.activity.001`；当前启动 migration `activity_state.001/.002` 在 `game_db` 预建活动玩法表，并从 `data/activity/activity.db` 以只读、每批最多 200 行的方式回填。目标身份冲突、旧 schema 不完整或磁盘空间预检不通过时拒绝继续；迁移不会删除旧文件，也不在请求路径建表。活动模块导入不再抢先校验目标表，避免 startup migration 尚未运行时阻断新库/旧库升级。默认签到、集字、积分、道具、任务、战令和活动首领状态读写统一使用 `game_db`。

`活动背包` 通过 `ActivityApplication.read_model` 使用 feature-owned 只读查询，一次 game_db 快照批量读取当前集字活动的库存、兑换次数和保底进度；旧服务 builder 保留为兼容路径。`活动背包` 查看视图由 `collect_bag_guidance_text` 提供（sibling；`collect_bag_text` 保持与旧 `build_collect_bag_text` 逐字相等）：字牌齐且未达上限的词组标注 `可兑换｜活动兑换 {词组}`；缺字词组列出 `还缺 {char}x{count}`（最多 4 项，其余 `等 N 项`），与结算侧 `tokens_insufficient` 计算同源；已达兑换上限的词组标注 `已兑换完` 且不再显示兑换命令；`limit<=0` 不限词组永不显示已兑换完。兑换结算、幂等回执与拒绝语义不变。`活动兑换` 的默认 matcher 经 `ActivityApplication -> ActivityRepository -> ActivityCollectExchangeSqlRepository` 结算，复用 `activity_collect_exchange_operations`，在同一 game_db `BEGIN IMMEDIATE` 中原子更新字牌、领取次数和玩家资产。默认请求不再调用 `ensure_activity_files()`，也不创建新 schema；旧 `ActivityCollectExchangeService` 仅保留显式兼容调用。

旧 `activity.db` 仍承载 `activity_config_*` 配置/事件数据及对应管理路径，并保留作备份与迁移核验来源；不能将整个文件描述为只读或缓存。活动积分、战令、集字掉落和首领战斗日志均参与每日上限、资格判断或审计，不得按缓存清理。

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Collect exchange also commits its existing business receipt in the same transaction as inventory and asset changes. Only this receipt-backed action opts into retrying a matching interrupted `started` operation; recovery checks the business receipt before revalidating current activity configuration, while a missing receipt proceeds through the current request once. Historical two-field receipts remain replayable after backfill. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`activity_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

活动详情 Web 交互闭环的聚焦回归：`tests/test_activity_web_state.py` 覆盖 `detail` 与既有
视图、匿名公共状态/管理员个人查询边界、精简旧状态库只读兼容、空库不建文件和面板控件。
详情行动建议聚焦回归：`tests/test_activity_detail_actions.py` 覆盖详情视图行动建议覆盖
与一键领取相同的六来源、中心卡片与详情同一快照的一致性、无 `user_id`/无资格状态不出现
新提示，以及首领排行名次按首领活动 key 读取。
签到 Web 代办闭环的聚焦回归：`tests/test_activity_sign_web.py` 覆盖管理员/CSRF/参数合同、真实
签到结算与发放一次、外层操作恢复、旧/缺失 schema 失败关闭、manifest 权限与面板幂等交互。
本阶段轻量回归：`tests/test_activity_center.py` 覆盖四种生命周期状态、配置驱动活动卡片、
奖励可领取状态和 Markdown 链接的纯文本降级；`tests/test_activity_center_claim_sources.py`
覆盖五类来源的单独/组合入口、稳定顺序与去重、缺少资格状态、旧库可选表兼容，以及带参数
蓝字的纯文本降级；未运行全量长测或真实 QQ 消息发送。

活动视图投递合同（G6）：`tests/test_activity_view_delivery_contracts.py` 用默认配置
（集字+积分+首领三类玩法、阶段日程、首领行动提示、购买引导、就绪标注）把
`活动阶段`/`活动首领`/`活动商店`/`活动背包`/`活动奖励`/`活动玩法` 六个玩家视图
逐个走一遍 `handle_send(native_markdown=True, fallback_msg=strip_md_command_links(text))`
缝隙，断言 OneBot V11 与关闭 markdown 的 QQ 都只收到一次纯文本（无 `**`、无
`mqqapi`、入口与关键行完整），QQ 原生 markdown 收到 `\n`→`\r` 归一后的消息且保留
`](mqqapi` 蓝字；多行布局（玩法段之间、多集字段之间）在两条路径上都保留空行，
否则 QQ 原生 markdown 的单换行会吞行。handler 源合同与运行期接线合同钉住六个
handler 走同一缝隙。

本轮据此修复的合同缺口（均只读渲染/接线，不改写路径、命令语义、表结构、迁移或
配置键）：

- `活动首领` 旧实现先 `_ensure_user` 再渲染：未注册用户收到「请先修仙」提示而不是
  首领状态块，且从未走原生 markdown 缝隙。现在直接按事件用户 id 渲染状态块并走
  共享缝隙（读模型本身对任意 id 只读）。
- `活动商店`、`活动背包` 只发纯文本，QQ 原生 markdown 下购买/兑换引导没有蓝字命令；
  现改走同一缝隙。
- `活动玩法` 的 `fallback_msg` 曾是 `text` 本身（未剥链），`活动奖励` 同样；现统一为
  `strip_md_command_links(text)`。
- `活动玩法` 的玩法段与「今日进度」行之后缺少空行，QQ 原生 markdown 下下一段会被
  粘到上一行；`活动背包` 多集字活动时相邻段之间出现过两个空行。新增
  `_tighten_blank_lines` 统一「段后恰一空行、段间不连空行」的折叠规则。

活动 Web 玩家视图对齐（G6 后续）：bot 侧的六个玩家视图（阶段/首领/商店/背包/奖励/玩法）
在 Web 只有前三个视图有对应入口 —— `GET /api/v1/activity/state` 的视图映射与管理员面板
下拉都只有 `center|detail|stage|rank|boss`，`?view=shop|bag|reward|gameplay` 一律返回
`400 validation_error`，运营无法在面板查看商店购买引导、背包就绪标注、奖励目录与玩法个人
状态。现在把这四个只读读模型（`point_shop_guidance_text`/`collect_bag_guidance_text`/
`reward_catalog_text`/`gameplay_personal_text`）接入既有视图映射并在面板补齐同名选项，
路由直接返回读模型文本、不重渲染；权限语义（无 `user_id` 公共、带 `user_id` 要求
`admin` 否则 403）、未知视图 400、缺 `application` 兼容响应、缺可选表时的 `schema_missing`
（503）失败关闭合同全部沿用既有视图。只读；不新增路由、命令、表、迁移或配置键。
聚焦测试 `tests/test_activity_web_views.py` 钉住四视图的两态渲染、逐字等于读模型文本、
九视图枚举、面板选项与路由映射一致、旧库只读不建文件与缺表失败关闭。

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 活动中心`
- `alias: 活动大厅`
- `command: 关闭活动`
- `command: 开启活动`
- `command: 活动`
- `alias: 活动信息`
- `alias: 活动日程`
- `alias: 活动进度`
- `command: 活动任务`
- `alias: 活动日常`
- `alias: 活动目标`
- `command: 活动任务领取`
- `alias: 领取活动任务`
- `command: 活动领取`
- `alias: 活动一键领取`
- `alias: 领取活动奖励`
- `alias: 活动领奖`
- `command: 活动阶段`
- `command: 活动阶段领取`
- `alias: 领取阶段奖励`
- `command: 活动兑换`
- `alias: 集字兑换`
- `command: 活动商店`
- `alias: 积分商店`
- `command: 活动奖励`
- `command: 活动帮助`
- `command: 活动战令`
- `alias: 活动活跃`
- `alias: 活动通行证`
- `command: 活动战令领取`
- `alias: 领取活动战令`
- `alias: 领取活动通行证`
- `command: 活动排行`
- `command: 活动排行领取`
- `alias: 领取排行奖励`
- `command: 活动玩法`
- `command: 活动积分`
- `alias: 积分活动`
- `command: 活动签到`
- `alias: 节日签到`
- `command: 活动管理`
- `command: 活动背包`
- `alias: 活动字牌`
- `alias: 集字背包`
- `command: 活动讨伐`
- `alias: 使用烟花`
- `alias: 使用爆竹`
- `alias: 活动首领攻击`
- `command: 活动购买`
- `alias: 活动兑换商品`
- `alias: 活动商城兑换`
- `command: 活动首领`
- `alias: 活动BOSS`
- `alias: 活动Boss`
- `command: 活动首领排行`
- `alias: 活动BOSS排行`
- `alias: 首领伤害榜`
- `command: 活动首领领奖`
- `alias: 领取首领奖励`
- `alias: 首领领奖`
