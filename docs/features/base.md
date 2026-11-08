# 修炼与基础资产
## 用户流程
突破、渡劫、改名、重入仙途、灵石争夺、抢夺和签到统一进入应用服务。
## 命令与别名
`修炼`、`突破`、`修仙签到`。
`我的灵石流水`（别名 `灵石流水`、`灵石收支`）：玩家本人近 7 天灵石收支总览、主要来源/去向与最近 5 笔明细，只读。
`我的资产总览`（别名 `资产总览`、`我的资产`）：玩家本人灵石、灵庄存款、宗门归属与贡献、背包规模、竞技场段位/积分/荣誉的一览，只读，缺失段自动省略。
`修行周报`（别名 `我的周报`、`本周修行`）：玩家本人近 7 天的跨来源汇总——修为、灵石、宗门贡献的获得/消耗/净额与笔数，物品获得与消耗件数及前 3 名，活跃天数、最活跃一天与主要玩法前 3，只读。数据全部来自共享账本 `economy_log`（`exp_delta`/`stone_delta`/`sect_contribution_delta`/`item_delta`/`action`）；时间窗只用下界（`created_at` 由各写入方提供，UTC 与本地混用，加上界会误删「刚刚发生」的行）；缺表或缺列时逐段降级，不建表、不补列、不写库；零记录时明确提示「本周暂无修行记录」。
`我的灵石流水`、`修行周报`、`我的资产总览` 均已登记在 `修仙帮助` 的「流水与总览」段，玩家可从帮助页直接发现。
`我的突破统计`（别名 `突破统计`、`突破记录`）：玩家本人突破次数、成功、失败、成功率与累计损失修为。
数据来自 `player_data.statistics` 的 `突破次数`/`突破成功`/`突破失败`/`突破损失修为`；
成功率分母恒为 `突破次数`，若 `成功+失败 != 次数` 会如实标注不一致而不改数。
该命令同样登记在 `修仙帮助` 的「流水与总览」段。
`战力拆解`（别名 `我的战力`、`战力明细`）：把战力拆成三因子并给出升境预估，只读。
公式来自 `xiuxian2_handle.py::update_power2` 的 `power = round(exp × root_rate × level_spend, 0)`：
`exp` 为当前修为，`root_rate` 为灵根倍率（`命运道果` 走 `compute_fate_root_rate` 阶梯），
`level_spend` 为境界系数 `jsondata.level_data()[level]["spend"]`。视图输出计算战力、账面战力
（差异如实标注，不改数）、距下一境所需修为、同修为突破后的战力预估，以及三项提升手段中
相对增幅最大的一项。舍入与 SQLite `round(x,0)` 一致（.5 远离零）。未知境界、灵根倍率不可用、
缺表时明确降级，不伪造战力、不建表、不写库。该命令登记在 `修仙帮助` 的「常用入口」段。
`修仙帮助 关键词`（例如 `修仙帮助 签到`）：对已登记命令做关键词检索，只读、纯内存。
索引由 `features/base/help_search.py` 扫描 `bootstrap/platform_manifest.py` 与
`features/*/manifest.py` 的 `FEATURE.commands` 构建，并在首次检索时缓存；
匹配强度为「完全相等 → 前缀 → 包含」，命令名与别名都参与匹配，大小写与首尾空白归一化。
返回最多 10 条（超出时提示收窄关键词），无命中时提示换用更短关键词或发送 `修仙帮助` 看完整菜单。
`tests/test_help_search.py` 会把它与 `plugin.build_registry()` 的登记结果逐条比对，
manifest 新增命令而索引漏扫时直接失败。不带参数的 `修仙帮助` 行为不变。
## 列表检索与翻页续查（共享合同）
`features/base/keyword_page.py` 是列表类视图唯一的参数解析与翻页命令构造入口：

- `parse_page_arg(raw)` → `{keyword, page}`：用于 `命令 [关键词] [页码]`。纯数字（且 >0）才是页码，
  其余整体按关键词；关键词可带尾随页码（`灵芝 2`），也允许多词关键词（`A B 3` → 关键词 `A B`）。
- `parse_typed_page_arg(raw, types)` → `{type, keyword, page}`：用于 `命令 类型 [关键词] [页码]`。
  类型按最长优先匹配（`辅修功法`/`合成丹药` 不会被 `功法`/`丹药` 抢先），并兼容旧的
  「类型紧贴页码」写法（`神通2`、`药材2`）；类型不在列表中时返回空串，由各 handler 给既有错误文案。
- `matches_keyword(fields, keyword)`：大小写与首尾空白归一的包含匹配，空关键词恒为真；
  参与匹配的字段由各视图决定（物品名/ID/描述/卡名等）。
- `page_command(命令, keyword, page)`：构造翻页命令。无关键词时输出与改动前逐字相同
  （`命令 2`）；有关键词时输出 `命令 关键词 2`，与解析器往返一致。

约定：筛选只改变当次集合——总页数、MD `next_cmd`、纯文本翻页提示与底部按钮全部基于筛选结果，
所以第 2 页仍是筛选结果而不是退回全量列表；筛选无命中用 `empty_notice(keyword, 视图名)`
与「本来为空」分开提示；排序沿用各视图原有规则。当前接入六个入口：`仙肆查看`、
`查看修仙界物品`、`世界BOSS商店`、`通天塔商店`、`历练商店`、`传承背包`，
由 `tests/test_list_search_remaining_views.py` 钉住真实渲染、翻页续查与帮助文本。
本切片零 schema/命令变更：不新增表、列、迁移、配置键、命令或别名。
## 排行榜本人名次（共享合同）
`features/base/ranking_position.py` 是六个排行榜视图唯一的「本人名次」口径：

- `PersonalRank(rank, total, value)`：`rank` 为**并列名次**（`1 + 严格高于本人的人数`），
  同分同名次；`total` 为有有效记录（数值 > 0）的去重人数；`rank <= 0` 表示本人无有效记录。
- `personal_rank_line(entry, *, label, unit, number)`：渲染
  `你的名次：第N名 / 共M人（{label} {value}{unit}）`；无记录（含 `entry=None`）渲染
  `你还没有{label}记录。`，不伪造名次。`total` 小于 `rank` 时上取到 `rank`，避免自相矛盾。
- 取数在各自 feature 仓储（`rank_of` / `integral_rank`），使用只读 UoW；
  缺库、缺表、缺列时返回空 `PersonalRank`（不抛错、不建表、不补列），
  命令层只拼接渲染行，不自行计算名次。

覆盖六个视图：`世界BOSS积分排行榜`（`BossIntegralApplication.integral_rank`）、
`通天塔排行榜`/`通天塔积分排行榜`（`TowerStateApplication.rank_of`）、
`历练排行榜`/`历练积分排行榜`（`TrainingApplication.rank_of`）、
`竞技场排行榜`（`ArenaStateApplication.rank_of`）。前 N 名列表输出不变，本人名次行追加在列表之后。
本切片零 schema/命令变更：不新增表、列、迁移、配置键、命令或别名；
由 `tests/test_ranking_position.py` 钉住并列名次、榜外名次、空态、缺库缺表降级与六个视图接线。
## 商店每周限购行（共享合同）
`features/base/shop_limit.py` 是四个商店视图（宗门、世界BOSS、通天塔、历练、竞技场）
唯一的「每周限购」渲染口径：

- `shop_limit_line(limit, purchased)`：`limit > 0` → `每周限购：{剩余}/{总量}个`
  （`剩余 = max(limit - purchased, 0)`）；`剩余 == 0` → `每周限购：已售罄（{总量}/{总量}个）`；
  `limit <= 0`（配置未设置或显式不限购）→ `每周限购：不限购`。
- 已购数量一律来自各店自己的读路径（宗门为 `SectApplication.weekly_shop_progress`，
  其余在命令层已有的 `already_purchased`），渲染层不再自算差值或手写字面量。
- 宗门商店此前只写总量 `每周限购：{limit}个`，与其余三家形状不一致；现在补齐
  `已购/剩余`，并对旧库（`sect_shop_weekly_purchases` 未迁移）回退到迁移前的
  个人 JSON 记录（`get_sect_weekly_purchases_snapshot`），与购买路径的
  `legacy_purchased` 同序。
- 只读渲染改动：不改购买校验、周限购判定、扣费、周键或任何回执。
  由 `tests/test_shop_limit_line.py` 钉住渲染形态与五个视图的接线。

## Web API
`POST /api/v1/base/{breakthrough,tribulation,rename,stone_contest,stone_robbery,sign}`，权限 `user`，要求幂等键。
`POST /api/v1/base/xiangyuan/{create,claim}` 与 `GET /api/v1/base/xiangyuan/group`，权限 `user`；写入请求要求幂等键和 CSRF。
## 数据模型与迁移
迁移 `base.001` 创建 feature 标记，`base.002` 在 game DB 启动时预建改名回执表并为旧表补充可空 `payload` 列；`base.003` 预建灵石争夺回执表，并为旧表补齐 theft 字段；`base.004` 在 game DB 预建抢劫回执表，`base.005` 在 player DB 预建抢劫成功/失败统计列；`base.006` 在 game DB 预建仙缘池与操作回执表，`base.007` 在 player DB 预建仙缘发送/领取限额表；`base.008` 仅在 game DB 预建重入仙途操作回执表。玩家奖励经济写入复用既有 `user_xiuxian` schema，由 `PlayerEconomyApplication -> PlayerEconomySqlRepository` 做运行时只读 schema 检查，不新增迁移。历史玩家/修炼表仍由既有 schema owner 持有。
## 事务与失败回滚
`base.009` 在 game DB 预建直接突破核心回执，保留旧回执并补齐 payload；`base.010` 在 game DB 保存冻结计划和关系奖励回执，`base.011` 在 player DB 预建关系预留回执及统计/历史字段；`base.012` 仅在 game DB 预建 `player_stamina_operations` 体力扣减回执表，缺少 `operation_id` 主键等不支持的既有 schema 会让迁移失败并在请求期返回 `schema_missing`，不静默改写。迁移不为历史无 outbox 回执猜测或补造副作用。

`直接突破`（别名 `破`）在冷却检查前查同消息回执，在 game 写事务内冻结成功/失败、惩罚和关系奖励计划，并原子提交核心资产、回执和 `base.direct_breakthrough.effects` outbox。师徒次数先在独立 player 事务中与 prepared 回执一起预留，再提交 game 奖励/回执，最后 player 统计、历史和 applied 回执一起提交；不依赖 WAL 下跨文件 ATTACH 崩溃原子性。中断重放不重抽、不重复计数或发奖，换绑后恢复不覆盖新 count；奖励受当前及冻结修为上限约束，无法兑现时保持待恢复，不取消已预留的旧承诺。

runtime 和 CLI reconcile 都注册 effects handler；每次有效命令最多恢复 5 笔已到期 pending，失败增加 attempts 并按 5/10/20...300 秒写入 `next_attempt_at`，避免长期待恢复记录阻塞队列；同消息显式 replay 仍可立即重试。直接突破的功率、上限和关系规则在进入 game 写事务前读取配置快照，恢复阶段不在持锁路径初始化旧数据库管理器。统计、日志和历史保持稳定事件 ID/时间，持久回执不属于可清缓存。连续突破、渡厄及其他渡劫仍是兼容边界；通用 Web `breakthrough` 返回契约不变。上述代码切换与隔离恢复测试不代表正式发布迁移或 P7 已完成。

改名写入与回执由 `BaseApplication -> BaseRenameSqlRepository` 承担；重放查询也经 application 使用只读 repository。旧回执缺少 `payload` 时仍作为已完成操作返回，不改动玩家或背包数据。请求路径不创建表或补列，缺少 `base.002` 时拒绝写入。
`偷灵石` 的查重与结算由 `BaseApplication -> BaseStoneTheftSqlRepository` 承担，在 game DB 的单一 immediate UoW 中更新灵石、体力并写回执；缺少 `base.003` 或所需玩家 schema 时 fail closed。`抢劫` 的默认 handler 由 `BaseApplication -> BaseStoneRobberySqlRepository` 承担，在 game DB ATTACH player DB 的单一事务中完成快照校验、双玩家 CAS、统计和回执；资产 CAS 失败通过 savepoint 回滚，缺少 `base.004`/`base.005` 或所需 schema 时 fail closed。普通 stone-contest 由 `BaseApplication -> BaseStoneContestSqlRepository` 承担，在 `base.003` 回执表上执行单事务余额 CAS；重复 operation 只读回首次结果，缺少迁移或玩家字段时 fail closed，不在请求期建表。通用命令 `Cooldown` 的体力扣除由 `PlayerStaminaApplication -> PlayerStaminaSqlRepository` 按 profile 首行 `rowid` 做原子 CAS；缺 schema、用户或快照变化时拒绝继续，不在请求期建表。旧 `StoneContestService` 仅保留显式兼容/回滚调用。
`重入仙途` 的自动择优与手动选择都由 `BaseApplication -> BaseRootRerollSqlRepository` 执行，在 `base.008` 回执表和用户首行上用快照 CAS 原子更新灵根、灵根类型、战力与灵石；同一操作重放返回首次结果，手动回复绑定发起用户和初始快照。缺少迁移/玩家字段、灵石不足或快照变化时 fail closed，不在请求期建表；默认 handler 不再调用 `ramaker`。
## 定时任务
每分钟恢复未满体力。任务通过 `recover_player_stamina -> PlayerStaminaApplication -> PlayerStaminaSqlRepository` 执行，按 `XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE` 分批更新并用 `MIN` 封顶；不读取完整用户列表、不在请求或任务路径建表。缺少数据库/schema 时 fail closed，`stamina_recovery_points=0` 直接返回，避免空转。
连续爬塔、世界首领训练入口和突破入口在前置体力扣除后提前结束时，使用同一 application 的 `restore` 单用户返还；按首个 `user_xiuxian` 行做封顶 CAS，缺 schema 或用户时不写入。
通用奖励、补偿兼容奖励、师徒历史奖励和 Rift Boss 兼容适配器的灵石、修为、宗门贡献写入使用 `PlayerEconomyApplication`；成功路径按首个用户行执行 CAS，修为按传入上限封顶，缺 schema 或状态变化时不生成奖励文本。该边界不负责物品库存、统计或经济流水，它们仍由各自 feature/兼容层负责。
修为小数整理命令也经同一 application，在既有 `user_xiuxian.exp` 上按查询快照 CAS 截断小数；缺 schema、用户或快照变化时拒绝写入，不新增迁移。
## 配置项
`base_enabled`。
## 适配器差异
NoneBot 和 Web 只负责输入输出转换。
## 测试与手工验收
覆盖余额不足、状态冲突、重复操作和异常回滚；重入仙途覆盖战力重算、操作重放/冲突、缺 schema、灵石不足和 game-only 迁移；体力恢复覆盖多批更新、上限封顶、零恢复点、缺 schema fail closed，以及有界查询约束。
## 灰度开关、回滚和已知限制
关闭开关可回退旧基础玩法；旧数值算法暂不复制。

## 高境界整数存储边界（已知限制）
`user_xiuxian.exp/power/hp/mp/atk` 与 `sects.sect_materials/combat_power` 为 SQLite NUMERIC 亲和列，
只保证 64 位整数（≤ 9,223,372,036,854,775,807）的精确存储。境界表最高一境（`至高`）的战力为
`10^29`，超出该范围后 SQLite 只能以 REAL（双精度，约 15–17 位有效数字）保存，更低位会被舍入；
这是历史存储口径，本次运行期修复不改表结构、不做压缩或截断。
写入口统一经 `core/numeric.bind_sqlite_param` 归一化越界整数（纯数字 TEXT，避免驱动抛
`OverflowError` 中止整个事务），因此高境界结算不再失败，但超过 64 位的**持久化精度以 REAL 为准**；
结算回执与操作表 `result_json` 中记录的仍是本次计算出的完整整数。
若后续需要 64 位以上的精确持久化，需单独设计（独立精确列或整表迁移），不在本次运行期修复范围。

## Manifest 清单
- `command: 修炼`
- `alias: 突破`
- `command: 我的灵石流水`
- `command: 修行周报`
- `alias: 我的周报`
- `alias: 本周修行`
- `command: 战力拆解`
- `alias: 我的战力`
- `alias: 战力明细`
- `command: 我的资产总览`
- `alias: 灵石流水`
- `alias: 灵石收支`
- `route: POST /api/v1/base/breakthrough`
- `route: POST /api/v1/base/tribulation`
- `route: POST /api/v1/base/rename`
- `route: POST /api/v1/base/stone_contest`
- `route: POST /api/v1/base/stone_robbery`
- `route: POST /api/v1/base/sign`
- `route: POST /api/v1/base/xiangyuan/create`
- `route: POST /api/v1/base/xiangyuan/claim`
- `route: GET /api/v1/base/xiangyuan/group`
