# 宗门成员与资产操作

## 用户流程

宗门成员加入、宗门商店兑换、主/副功法学习、炼体堂领奖、`领取宗门周常` 和确认解散宗门通过 `SectApplication` 进入 feature-owned application/repository。默认宗门任务状态查询、接取与刷新也通过 `SectApplication`，接取/刷新使用操作回执幂等；周常可单项领取或一次领取所有已完成目标。

## 命令与 Web API

- `加入宗门` -> `POST /api/v1/sect/join`
- `宗门商店兑换` -> `POST /api/v1/sect/purchase`
- `学习宗门功法` -> `POST /api/v1/sect/learn-main`
- `POST /api/v1/sect/learn-secondary`
- `领取宗门炼体堂` -> `POST /api/v1/sect/elixir/claim`
- `我的宗门贡献流水`（命令入口，只读个人贡献收支）
- `领取宗门周常 [目标名]`（命令入口）
- `确认解散宗门`（命令入口）

所有接口权限为 `user`，写请求支持 `Idempotency-Key`，业务拒绝返回统一错误码。

## 数据、回滚与限制

`sect.011` 在 game DB 创建/补齐 `sect_weekly_goal` 和 `sect_weekly_reward_operations`；`sect.012` 仅在 player DB 创建/补齐 `boss_limit.integral`，保留已有周常进度与玩家记录；`sect.013` 在 game DB 预建手动解散回执并保留已有操作记录；`sect.016` 预建宗门任务状态表；`sect.017` 在 game DB 预建接取/刷新幂等回执；`sect.018` 在 game DB 预建任务结算回执。周常、手动解散及任务状态/接取/刷新/结算请求只检查已登记 schema，不建表或补列。任务结算时间由注入的 Clock 提供。周常奖励由 `SectWeeklyRewardSqlRepository` 使用 attached game/player transaction 校验宗门归属、目标进度、重复领取与背包容量，再更新玩家、宗门、背包、BOSS 积分及领取标记。晚期 SQL 异常会回滚两个库；SQLite WAL 下 attached 多库事务不承诺进程/主机崩溃时的跨库原子性。手动解散由 `SectManualDisbandSqlRepository` 在 game-db immediate UoW 内重验宗主身份、解绑全部成员、删除宗门并写幂等回执；旧 `SectDisbandService` 仅保留兼容对照。关闭 `XIUXIAN_SECT_ENABLED` 即回退兼容入口。

### 宗门三项修炼升级

`升级攻击修炼`、`升级元血修炼`、`升级灵海修炼` 通过 `SectApplication.upgrade_practice` 进入同一
SQLite 事务：玩家灵石与对应修炼等级、宗门 `sect_materials` 和 `sect_practice_operations`
永久回执必须逐步写入并在提交前回读。玩家的 `sect_used_stone` 不是本次消耗列，不能代替
玩家灵石；每次成功升级只扣玩家灵石与宗门资材。

`RAISE(IGNORE)`、触发器改写或末步回执缺失均返回对应的
`sect_practice_player_write_rejected`、`sect_practice_assets_write_rejected` 或
`sect_practice_receipt_write_rejected`，携 `reason/subject` 回滚整笔事务及副作用；SQLite
`ABORT` 原异常透传。相同消息在玩家等级已推进后按同一入口、升级步数和成本重放为
`duplicate`，不重复扣费；改变数量则保留 `operation_conflict`。参数解析拒绝零、负数、非数字、
多余参数和超过单次上限，职位与资材边界保持原规则。

## 商店每周限购行
`宗门商店` 的商品行渲染 `每周限购：{剩余}/{总量}个`（售罄写 `已售罄（N/N个）`，
不限购写 `不限购`），已购数量读取本周 `sect_shop_weekly_purchases`
（`SectApplication.weekly_shop_progress`），旧库回退迁移前的个人 JSON 记录；
口径与共享合同见 `docs/features/base.md` 的「商店每周限购行（共享合同）」。
只读渲染，不改兑换校验与扣费。

## 命令与别名

保留 `加入宗门`、`宗门商店兑换`、`学习宗门功法`、`领取宗门炼体堂`、`领取宗门周常` 及历史别名。
新增玩家只读命令 `我的宗门贡献流水`（别名 `宗门贡献流水`、`贡献流水`，priority 7）：
展示本人近 7 天宗门贡献的获得/消耗/净变化/笔数、主要来源与主要去向各前 3、
最近 5 笔明细，以及当前贡献余额与所属宗门。数据来自共享 `economy_log` 的
`sect_contribution_delta`（由 `宗门捐献`、`宗门商店兑换`、宗门设施升级等真实写路径记录），
时间窗只用下界以避免 UTC/本地时间混用误删「刚刚发生」的行。`user_id` 只取登录会话，
不接受参数指定他人；只读、无新表/迁移/配置键，`economy_log` 缺列或库不可读时降级为提示文案。
`宗门商店 [页码|关键词]`：参数既可以是页码，也可以是商品名/描述/编号关键词（可带页码，
如 `宗门商店 灵芝 2`）。此前只做 `int(page_input)`，非数字一律静默回退第 1 页，
玩家输入 `宗门商店 灵芝` 看到的是首页商品。现在由 `features/base/keyword_page.py`
的 `parse_page_arg` 区分意图、`matches_keyword` 按商品名/描述/编号匹配，命中后只渲染命中商品
并保持原分页（每页 5 条）；无命中时提示「没有包含该关键词的内容」，与「宗门商店暂无商品」
区分开。兑换仍走 `宗门兑换 编号`，编号语义未改。筛选后的翻页提示与底部按钮由
`keyword_page.page_command` 生成 `宗门商店 关键词 页码`，翻页保持筛选。
该命令已登记在 `宗门帮助` 的「任务与商店」段，玩家可从帮助页直接发现。
`宗门成员查看 [页码|关键词]`（别名 `查看宗门成员`）：参数既可以是页码，也可以是道号/职位名/
境界/UID 关键词（可带页码，如 `宗门成员查看 长老 2`）。此前只做 `int(args)`，非数字一律静默
回退第 1 页；现在走同一解析器，由 `sect_member_utils.filter_sect_member_rows` 做大小写不敏感
的包含匹配。筛选生效时，首页「宗门职位统计」只统计命中成员并标注「筛选「关键词」命中 N 人」；
无关键词时输出与改动前一致。MD 下一页链接、纯文本 footer 与底部按钮都由 `page_command`
生成 `宗门成员查看 关键词 页码`，翻到第 2 页仍是筛选结果。无命中提示「没有包含该关键词的内容」，
与「宗门目前没有成员！」区分；职位权限判定与踢人/变更快捷操作语义未改。
该命令已登记在 `宗门管理` 的「成员」段。

## Web API

接口为 `/api/v1/sect/join`、`purchase`、`learn-main`、`learn-secondary` 和 `elixir/claim`，权限为 `user`，写请求需要 CSRF 与幂等键。

## 数据模型与迁移

`sect.001` 写迁移标记和统一 ledger；`sect.011`、`sect.013`、`sect.016`、`sect.017`、`sect.018` 属于 game DB，`sect.012` 属于 player DB。历史周常进度、已有手动解散回执和任务操作回执由幂等迁移保留。`sect_fairyland.002` 在 player DB 预建炼体堂领取回执与每日状态表，并将旧按宗门展开的领取日期回填到 `sect_fairyland_claim_days`；信息查询和领奖均读取/更新该 feature-owned 状态，不在请求期建表。

### 操作回执表的 schema 归属（`sect.019`）

11 份仓储原先在**业务方法写事务内** `CREATE TABLE IF NOT EXISTS` 建 12 张回执表；现在
`sect.019` 是唯一创建者，`features/sect/schema.py` 是唯一契约来源。12 张表全部在 **game DB**
（这些仓储只持有一个 `database` 连接，不使用附库）：

| 仓储 | 回执表 |
| --- | --- |
| `creation_repository` | `sect_creation_operations` |
| `name_refresh_repository` | `sect_name_refresh_operations` |
| `owner_transfer_repository` | `sect_operations` |
| `disband_repository` | `sect_inactive_disband_operations` |
| `buff_search_repository` | `sect_buff_search_operations` |
| `practice_repository` | `sect_practice_operations` |
| `elixir_room_repository` | `sect_elixir_room_operations` |
| `join_state_repository` | `sect_open_join_operations`、`sect_close_join_operations` |
| `owner_inherit_repository` | `sect_owner_inherit_operations` |
| `close_mountain_repository` | `sect_close_mountain_operations` |
| `daily_maintenance_repository` | `sect_daily_reset_operations` |

请求路径只校验「表存在 + 本次要读的列齐全」，缺表或缺任一列返回 `schema_missing`
且零写入（不动宗门资产、成员、贡献与任何回执行）；半成品回执表不会被补列。
门禁是业务方法内的布尔守卫（`_ready(uow)`），不是 `-> None` 辅助方法的返回值，
避免返回值被调用方丢弃。

## 事务与失败回滚

周常领取以 `sect_weekly_reward_operations` 作为唯一幂等回执，并与奖励状态在同一 attached transaction 提交；不额外写入会跨事务留下 started 状态的通用 ledger。每步写入检查影响行数，末步再回读玩家/宗门资产、背包、BOSS 积分、目标领取状态和永久回执；`RAISE(IGNORE)`、trigger 改写或最终投影不一致会回滚整笔，并以具体 `reason`/`subject` 返回，未知 SQLite 异常保留原异常链。重复领取可重放奖励摘要，operation 冲突、进度变化、库存不足和晚期 SQL 异常不发放部分奖励。

## 宗门命令合同

`宗门丹药领取`及别名`领取宗门丹药`在日重置后读取新的领取次数和丹房状态。
发药/绑定数量、已领标志与永久回执同事务，逐写行数及最后回读，拒写/改写含
trigger副作用整笔回滚。`sect_elixir_item_write_rejected`、
`sect_elixir_user_write_rejected`、`sect_elixir_receipt_write_rejected`及晚期宗门
状态改写的`sect_elixir_eligibility_write_rejected`携具体主体经应用、failed回执
和实际回复保留；原SQLite异常透传，诊断失败不遮原原因/异常。

新写入失败可同请求恢复，started预约可续；贡献和宗门资材仍只校验门槛，不在领取
时扣减。多药、已有绑定、重复物品ID与库存上限按整批校验，大数使用既有精确存储。
聊天随机奖励由服务端生成，不参与玩家请求指纹；显式API奖励参数仍检查冲突。
旧成功回执先回放，保持历史奖励、旧payload和次数；次日旧消息不占新次数，并发
晚到的回放也显示实际历史奖励。业务已提交而诊断失败时，重放仅补完诊断。legacy
保留原DTO、NULL职位拒绝与物品元数据更新语义，写入委派同一feature校验，无新迁移。

`test_sect_elixir_claim_recovery.py`验证三步IGNORE/改写/ABORT、后步篡改前步、整笔
回滚/重建恢复/并发、大数/坏数据/旧兼容、真实handler流程与失败投递及Flask合同。
`sect-elixir-claim`两条真实dispatch/DI/handler衔接每日调度→领取故障→同消息恢复→
别名/跨日/随机多药和旧消息回放；核对三库资产/回执/ledger/统计/任务及完整性。
OneBot/QQ文本、Markdown与降级仅mock，真实QQ展示未验。

主/副功法搜寻（`宗门功法搜寻`、`宗门神通搜寻`）经
`SectApplication.apply_buff_search(..., owner_position=...)` 进入
`SectBuffSearchSqlRepository`，在 `sects` 上写**真实列** `mainbuff` / `secbuff`
（映射常量 `_BUFF_COLUMNS = {"main": "mainbuff", "secondary": "secbuff"}`）。
历史上这里写成了两个并不存在的 `sect_` 前缀列名：SQLite 把未解析标识符在 SELECT
里当字符串字面量、在 UPDATE 里抛 `OperationalError`，因此命令在生产数据上必炸。
宗主校验比较调用方传入的 `owner_position`（不再硬编码 0），以便非 0 宗主位的宗门配置生效；
同一 `operation_id` 重放返回 `duplicate` 且不二次扣减宗门灵石与资材。

`sects` / `user_xiuxian` 的列集以 `xiuxian_config.py` 的 `sql_sects`、
`sql_user_xiuxian` 为权威：`sects` 为
`sect_id,sect_name,sect_owner,sect_scale,sect_used_stone,join_open,closed,combat_power,
sect_fairyland,sect_materials,mainbuff,secbuff,elixir_room_level`。

`SectMutationResult`（`dict` 子类）的成功谓词 `applied` 覆盖全部真实成功状态：
`applied`、`charged`、`created`、`opened`、`transferred`、`settled`，以及既有的
`claimed`、`closed`、`disbanded`、`duplicate`、`granted`、`inherited`、`upgraded`。
谓词遗漏真实成功状态会把成功渲染成失败并要求玩家重试（`创建宗门` 的 `created`、
`宗门改名` 计费的 `charged`、`开放宗门加入` 的 `opened`、`宗主传位` 的 `transferred`、
`宗门任务完成` 的 `settled` 都曾受影响）。命令层统一读 `.applied`；`宗主传位`
曾误读不存在的 `.succeeded`，会抛 `AttributeError`。

失败状态（`not_owner`、`stone_insufficient`、`materials_insufficient`、
`scale_insufficient`、`state_changed`、`name_exists`、`already_member`、
`task_missing`、`membership_changed`、`schema_missing` 等）一律不算成功，且不产生副作用。

## 个人宗门任务记录

### 每日任务交接

日重置后，接取/刷新将任务状态与永久接取回执一笔提交；完成任务将消耗、修为/
贡献奖励、次数、宗门奖励、完成状态与永久结算回执一笔提交。逐步检查行数并在
提交前回读，IGNORE、字段改写和末步篡改均按具体步骤拒绝，事务含trigger副作用
完整回滚。八种`sect_task_{claim,refresh,settlement}_*_write_rejected`分别带玩家、
宗门或请求主体进入应用结果与命令回复，原SQLite异常透传。

聊天先读取本人永久回执，旧消息跨日、退宗或已有新任务时只回放历史，不覆盖新任务、
不再次结算或记统计。前置体力从通用依赖改为该指令调用既有幂等接口，同消息首次
出门消耗3点，结算失败恢复不再重复扣除；该体力事务独立提交，失败不能宣称体力
也已退还。首次无任务、资产不足等原出门规则保留，冷却仍由原依赖处理。

任务日期与完成时间使用应用注入时钟和调度时区（默认上海），naive保留旧本地解释；
刷新/实际聊天结算拒绝跨业务日快照，历史成功回执先回放。新大整数精确存储，旧
REAL/科学记数奖励回执与原小数气血/修为/建设度计算保留；不改奖励生成、每日上限，
无新迁移。旧服务只准备原schema并委派同一写校验，DTO保留且补充reason/subject。

`tests/test_sect_task_daily_handoff.py`覆盖拒写/改写/末步篡改/ABORT、完整回滚和恢复、
旧成功/并发/时区/坏数值与真实handler异常合同；`sect-task-daily`两条真实dispatch/
DI/handler场景串联实际日重置→接取→刷新→两种消耗完成、八步拒写与重建同消息
恢复，核对三库资产/永久回执/统计/完整性。OneBot/QQ文本、Markdown和降级为mock。

任务体力扣减与永久消费回执也逐步验证和最后回读，拒写/篡改使体力事务含副作用
整笔回滚，任务结算不开始；具体`stamina_user_write_rejected`与
`stamina_receipt_write_rejected`携本人/本次体力回执进入实际指令及共享Cooldown。
旧成功消费回执保持权威，恢复体力后重放仍不重扣；快照竞争、坏体力、坏历史回执
分别保留原因。原SQLite异常到安全定位，失败投递透传。保留原管理员免体力规则，
未改变恢复/补充策略或新增迁移。

`test_sect_task_stamina_handoff.py`补两步IGNORE/改写/末步篡改/ABORT、无回执调用、
竞争/旧回放/坏数据和共享依赖失败合同；`sect-task-stamina`真实参数场景覆盖六种
拒写恢复、体力不足→实际定时恢复→原ABORT→同消息恢复以及管理员免消耗，核三库
和统计/永久回执/完整性，24个OneBot/QQ消息合同为mock，不外推线上或真机已通过。

### 每分钟体力恢复

`recover_user_stamina` 保持原interval、每分钟恢复量/上限和rowid分批；每个分批在
SQLite事务内校验实际更新行数并回读目标玩家。拒写、身份改写或删除会回滚当前批次及
trigger副作用，返回具体`stamina_recovery_write_rejected`和玩家subject，并使调度回调
失败；先前已提交批次保留，后续批次停止。缺表/缺列和非法配置分别保留
`schema_missing`/`stamina_recovery_config_invalid`及具体subject，原SQLite异常透传。
恢复量为0仍是无写成功，批大小小于1仍钳为1；NULL、重复玩家记录和封顶规则保持。
后续interval按既有规则形成一次新的恢复，不增加周期回执、断点补跑或补偿。

`tests/test_stamina_schedule_recovery.py` 覆盖实际声明、pending注册失败重试、interval
边界和真实APScheduler触发、缺schema/非法配置、IGNORE/改写/ABORT/超过SQLite参数
分片的末步篡改、部分提交与失败批次回滚。`stamina-recovery`真实参数场景用隔离三库
验证恢复失败时真实宗门任务仍体力不足、后续恢复再消费一次、成功消息只回放，并核对
资产/回执/统计/完整性。OneBot/QQ文本、Markdown和降级合同为mock，未真机验证。

### 历史查询

`我的宗门任务记录`（别名 `宗门任务记录`）补齐玩家侧逐次宗门任务的可见性：玩家只能看到
当前接取的任务与一次完成提示，无法回看最近做过哪些任务、付出了什么、拿到了什么。

- 数据源是真实结算回执 `sect_task_settlement_operations`（game DB）：`user_id`、`period`、
  `cost_type`/`cost`、`exp_reward`、`sect_reward`、`materials_reward` 与 `created_at`
  在 `settle_task` 成功时写入；不新增表、列、迁移、索引或写入路径。
- `SectTaskHistoryApplication` 在 `read_only=True` 的 `DatabaseUnitOfWork` 中按
  `user_id = 当前会话` 查询近 7 天（只用下界）结算行，按 `created_at DESC, rowid DESC`
  排序；缺库、缺表、缺列或读失败返回空态，不建表、不补列。
- 输出：完成次数、累计修为/宗门贡献/宗门资材奖励，以及最近 10 次明细（时间、消耗气血或
  灵石、三项奖励）；空态显示“近7天没有宗门任务完成记录。”。
- 帮助与发现性：`宗门帮助`「任务与商店」增补条目，`修仙帮助`「流水与总览」登记；
  manifest 增加 `CommandSpec`（含别名），命令仍由 `xiuxian_sect` 注册。
- 只读展示；不改任务接取/刷新/完成、消耗、奖励、周常或任何资产写路径。

## 定时任务

`sect_materials_grant` 沿用配置的 cron 小时、coalesce/max_instances=1 和300秒宽限。
业务日期将注入时钟转换为调度时区（默认Asia/Shanghai）；旧无时区时间仍按本地解释。
每天每宗门的永久回执限制重复发放：已完成宗门和随后新增资材不因重试被覆盖，
后续宗门失败时停止本轮，故障解除后同日重试只处理尚未完成部分。正常宗门消失或
失去宗主仍跳过；缺迁移、坏数值、写入拒绝则使实际APScheduler产生ERROR事件，
保留具体reason/subject，失败不记录完成，新发放日志不计duplicate。

资材/战力状态更新与回执插入逐步校验行数并在提交前回读两者。IGNORE、trigger改写
或末步篡改前步时，`sect_material_state_write_rejected` / `sect_material_receipt_write_rejected`
携宗门编号，退出事务完整回滚（含trigger副作用）后才转结果；原SQLite异常透传。
新超int64写入复用`xiuxian_int_store`的ASCII BLOB表示，避免列affinity变成REAL后
无法通过最终校验；成员既有REAL及旧战力回执仍沿用历史整数转换，不恢复已失精度，
不重写旧回执。legacy服务只保留schema准备，写入委派同一feature仓储，无新迁移。

`test_sect_material_recovery.py` 覆盖实际声明/连续注册失败pending恢复、cron边界、
真实触发、上海跨日/同刻跨时区、两步拒写/改写/ABORT、部分提交恢复、大数与历史
回放、坏资材原因和正常目标竞争。`sect-material-schedule`两条真实dispatch/DI/handler
串联发放→我的宗门/宗门信息别名→成员关键词分页→重放/次日，以及第二宗门拒写→
重建应用→只补剩余；核对三库资产/永久回执/统计及integrity_check。OneBot/QQ文本、
Markdown与发送降级仅mock，无真实消息或线上部署验证。

`daily_reset_sect_task` 每日08:00:15调用日重置，业务日期按实际调度时区取值，
默认上海，旧naive时钟按本地解释。全部玩家次数、丹房维护及当日永久回执在同一
事务；玩家UPDATE、资材扣减、降级UPDATE、回执INSERT各查行数并在提交前回读，
拒写/改写/后步篡改前步均整日回滚，包括trigger副作用。四个
`sect_daily_reset_{user,material,level,receipt}_write_rejected`携玩家/宗门/业务日
到实际任务ERROR；坏数值、坏回执和缺迁移各保留原因，原SQLite异常透传。
同日成功回执先回放，不覆盖新领取次数或任务；旧带空格JSON费用payload按结构比较，
费用实际变化仍拒绝。超int64资材复用既有精确存储，无新迁移、旧回执改写或费用变更。
无主/无房/未知等级仍按原规则跳过；旧服务仅准备schema，再委派同一feature校验。

`test_sect_daily_maintenance_recovery.py`覆盖真实注册/pending重试/cron边界/触发、
四步IGNORE/改写/ABORT与整日恢复、业务时区、六种正常状态、历史兼容及大数。
`sect-daily-maintenance`两条真实参数场景连接日重置→宗门查询→别名领取/接任务→
同日重放/次日，以及失败后原状态读取→重建应用恢复；核对三库状态/资产/回执/
outbox/ledger/统计/任务和完整性。OneBot/QQ文本、Markdown及降级仅mock。

宗门日常重置和任务刷新由兼容生命周期统一调度，使用稳定 job ID。
不活跃维护逐宗检查解散、继承、封闭结果；缺迁移、请求冲突和未识别结果保留
`status/action/sect_id` 并使任务失败，已处理或正常竞争仍按具体原因跳过。批次可继续
处理其他宗门，末尾汇总异常并保留原异常链；存在失败时不记录“处理完成”，也不宣称
其他宗门已回滚。日重置的 `schema_missing` / `operation_conflict` 同样向调度器抛错，
不再丢成属性异常或正常返回。受影响失败/恢复/部分成功与回执由隔离真实 SQLite 验证。

## 配置项

`sect_enabled` / `XIUXIAN_SECT_ENABLED` 控制新切片，默认开启。

## 适配器差异

命令层负责事件和文案，Web 层负责 DTO、权限、CSRF 和统一错误码；application 不依赖框架。

## 测试与手工验收

覆盖加入、兑换、功法学习、炼体堂、任务接取/刷新、周常领奖和手动解散的成功、拒绝、异常回滚和重复请求；事务测试还检查 migration 路由、旧数据保留及请求期不建表。

## 灰度开关、回滚和已知限制

关闭开关即回到兼容入口；其他未迁移事务仍保留兼容实现。周常跨库请求事务能回滚 SQL 异常，但 attached SQLite WAL 的崩溃原子性不作保证。
