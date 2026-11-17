# 交易与拍卖

`鬼市存灵石`、`鬼市取灵石`、`鬼市求购`、`鬼市摆摊`、`鬼市取消求购` 和 `鬼市收摊`
默认通过 `TradeApplication` 的
feature-owned use case 结算。存取操作记录在 game DB 的
`trade_guishi_deposit_operations`/`trade_guishi_withdraw_operations`，求购创建操作记录
在 trade DB 的 `guishi_order_create_operations`，撤销操作记录在
`guishi_order_cancel_operations`；鬼市余额和订单投影保留在 trade DB 的
`guishi_info`/`guishi_item`。启动迁移 `trade.002`/`trade.003`/`trade.004`/`trade.005`/
`trade.006`/`trade.010`
按数据库路由建立 schema。取出规则由注入 `Clock` 的 application 决定周末开放窗口，
并保留历史动态手续费公式。

鬼市订单列表与账户信息读取由 `TradeApplication` 的只读 query repository 承载，短生命周期
打开 `trade_db`，不保留缓存连接，也不在请求中补建 schema；数据库或投影表缺失时维持历史空结果。

普通、自动、快速和系统 `仙肆上架` 已通过 `TradeApplication` 结算。game DB immediate transaction
原子创建 listing 并写入 operation；普通/快速上架还会扣除手续费和可交易库存，自动上架会先
校验并扣除 30 点体力及整份计划的费用/库存，快速上架会先校验并扣除 10 点体力。普通/快速操作记录在
`xianshi_listing_operations`，自动计划记录在 `xianshi_plan_listing_operations`。`trade.010`/
`trade.011` 只路由到 game DB，并为历史 operation 表补齐 `stamina_cost` 列。系统 listing 也记录
在 `xianshi_listing_operations`；`quantity=-1` 无限量和指定数量均保持原样，且不扣玩家资产。
## 用户流程
鬼市存取、求购/摆摊创建与撤销、上架/撤回、场次开始/收尾和现世购买统一提交操作号。

鬼市订单生命周期（求购与摆摊）由 feature 层闭环：

- 求购创建按 `单价 × 数量` 冻结鬼市余额；到期时刻取创建操作行的 `created_at`
  （`guishi_order_create_operations`，与订单同事务写入），规则是**创建日次日
  `GUISHI_BAITAN_END_HOUR`（默认 8）点**，由纯规则模块
  `features/trade/guishi_lifecycle.py` 计算。到期由
  `GuishiQiugouExpiryService` 逐条调用既有 `cancel_qiugou` 取消并按
  `(数量 - 已成交) × 求购价` 退款，操作号 `guishi-expire-qiugou:{order_id}`
  稳定可重放。清理触发点有两处：`clear_expired_baitan_orders_job`（同一 8 点
  任务，先做全服求购到期，再做摆摊清理）与 `鬼市信息` 的本人访问期收敛。
- 摆摊创建后立即调用 `GuishiListingFillService`，按同物品、求购价不低于摆摊价、
  非本人、`(数量 - 已成交) > 0` 选候选，按创建时刻升序（无锚点排最后）逐条走
  既有 `guishi_match`，操作号 `guishi-fill:{摆摊ID}:{求购ID}`。摆摊售罄或没有
  候选即停止；成交摘要与 `鬼市成交` 统计同时回到卖家回执。
- 成交按摆摊价结算（`amount = 成交数量 × 摆摊价`），求购冻结价高于成交价的
  差价 `price_refund` 在同事务退回买家鬼市余额，买家最终支出恒等于
  `成交数量 × 成交价`。`GuishiMatchResult.price_refund` 带默认值，历史
  operation 行重放按 0 处理。
- `鬼市信息` 用 `GuishiOrderStatusService` 一次只读快照取本人订单与锚点，再由
  纯渲染 `guishi_order_view.py` 输出到期时刻与剩余时长；无锚点历史订单显示
  `到期：未知（历史订单）`。

## 我的仙肆成交记录

`我的仙肆 记录`（也接受 `成交`/`历史`）读取 `XianshiQuerySqlRepository.my_trades(user_id,
limit=5)`；该查询在单个只读 UoW 内从既有 `xianshi_operations` 取本人为买家或卖家的最近
5 笔成交，按 `rowid DESC` 排序，不建表、不建库：

- 字段：`operation_id/listing_id/name/goods_type/quantity/total_cost/role(seller|buyer)/
  counterparty/created_at`。对手方名称优先取 `user_xiuxian.user_name`（同名多行取最小 rowid），
  `user_xiuxian` 缺失时为空；系统卖家（`seller_id='0'`）显示“系统”。
- 渲染由纯模块 `features/trade/xianshi_history_view.py` 承担：逐行
  `{卖出|买入}｜{物品} x{数量}｜{总额}灵石｜{买家|卖家}:{对手方}｜{MM-DD HH:MM}`，末尾给出
  `最近 N 笔：卖出 X 笔/收入 A｜买入 Y 笔/支出 B`。
- `我的仙肆` 上架列表保留原分页与排序，尾部追加 `使用【我的仙肆 记录】查看最近成交`；
  无上架时提示同样带记录入口。无记录时明确返回“最近没有仙肆成交记录”。
- `我的仙肆 [关键词] [页码]`：按物品名/ID 过滤自己的上架（大小写不敏感），
  有关键词时列表前标注 `筛选「关键词」`，翻页命令携带同一关键词
  （`我的仙肆 灵芝 2`）；筛选无命中走“仙肆摊位里没有包含「…」的内容”，
  与真空摊提示区分。无关键词、纯页码行为不变；只读，只复用既有
  `xianshi_get_items(user_id=...)`。
- 只读；不改购买扣款、卖家到账、上架手续费、挂单数量、系统商品或 operation 回执；
  不新增表、列、迁移、索引、配置键、命令名或路由。

## 仙肆查看检索

`仙肆查看 类型 [关键词] [页码]`：类型内的列表支持按物品名/ID 筛选，翻页时关键词随行
（`仙肆查看药材 灵芝 2`），仍兼容 `药材2` 这种旧写法。解析、筛选与翻页命令统一走
`docs/features/base.md` 的「列表检索与翻页续查（共享合同）」；筛选后总页数按命中集合计算，
无命中提示与「仙肆中暂无该类物品」分开。只读，不改购买、上架、手续费或成交语义。

## 仙肆行情与关注

`仙肆行情 <物品名>`（别名 `仙肆价格`）与 `仙肆关注`（别名 `关注仙肆物品`）只读既有成交账本
`xianshi_operations` 与在售投影 `xianshi_item`，并为玩家新增一张个人关注表。
不新增奖励、不改买入/卖出扣款、手续费、上架数量或系统商品语义。

数据模型（game DB，启动期迁移 `trade.014`）：

- `xianshi_watchlist(user_id, goods_name COLLATE NOCASE, created_at,
  PRIMARY KEY(user_id, goods_name))`。
- `xianshi_watch_operations(operation_id PRIMARY KEY, user_id, action, goods_name,
  result, created_at DEFAULT CURRENT_TIMESTAMP)`。
- 请求期不建表、不补列；缺 `trade.014` 时关注写入返回 `schema_missing`。

成交口径：

- 仅按物品名大小写不敏感匹配；时间窗**只用下界** `created_at >= now - 7 天`。
  `created_at` 由各写入方提供、UTC 与本地时间混用，加上界会把「刚刚发生」的本地时间行
  误判为未来而丢弃。
- 行内单价 = `total_cost / quantity`（四舍五入）；加权均价 = `Σtotal_cost / Σquantity`。
- 跳过脏行：`quantity <= 0`、`total_cost < 0`、空物品名、空 `created_at`。
- 统计笔数、件数、最低/最高单价、最近成交时间与单价（按 `created_at` 取最大）。

在售口径：

- `xianshi_item.price` 是**单价**（购买总额为 `price * quantity`）。
- 统计摊位行数、卖家去重数（`user_id='0'` 视为系统卖家）、件数合计与最低单价。
- 跳过 `price <= 0`、`quantity <= 0` 的行。

命令语义：

- `仙肆关注`：无参数查看关注列表（最多 10 个）与每项行情摘要；
  `仙肆关注 <物品名>` 添加；`仙肆关注 取消 <物品名>`（或 `取关`）取消。
- 名称归一化：去首尾空白、内部连续空白折叠为一个空格；大小写不敏感去重；
  长度 1–24，非空白控制字符拒绝。

状态与幂等（操作号取会话消息 ID + 动作 + 用户 + 物品名，无消息 ID 时用 UUID）：

| 状态 | 含义 | 写入 |
| --- | --- | --- |
| `added` | 新增关注并写回执 | 是 |
| `duplicate` | 同操作号重放，或该物品已关注 | 仅回执 |
| `removed` | 删除关注并写回执 | 是 |
| `absent` | 取消不存在的关注，或同一取消操作重放；不写回执、不消耗操作号 | 否 |
| `limit_reached` | 已达 10 个上限，腾出名额后同一操作号可重试 | 否 |
| `invalid_name` / `invalid_user` | 名称或身份非法 | 否 |
| `operation_conflict` | 同一操作号用于不同用户/动作/物品 | 否 |
| `schema_missing` | 缺 `trade.014` | 否，且不建表 |

事务与降级：

- 每次写入在 game DB 单个 `BEGIN IMMEDIATE` UoW 内完成；上限判定与插入是同一条
  `INSERT ... SELECT ... WHERE COUNT(*) < 10`，并发下不会超限。
- 拒绝分支全部在写入之前返回（`DatabaseUnitOfWork` 正常返回即 commit），
  晚期异常整体回滚，不留半笔状态。
- 行情读取缺库/缺表/读异常时按来源降级：成交显示「成交数据暂不可用」，
  在售显示「在售数据暂不可用」；两者都不可用时显示「行情数据暂不可用」，
  不建库、不建表。
- 关注只影响本人查看列表，不推送提醒，不参与交易撮合。

玩家入口：`交易帮助` 仙肆段登记两条命令；`tests/test_xianshi_market_watch.py` 覆盖写路径、
行情聚合、降级与渲染。

## 命令与别名
`交易`、`寄售`、`鬼市`。
## Web API
`POST /api/v1/trade/{deposit,withdraw,enqueue,dequeue,purchase}` 要求 `user`；`session_start` 和
`session_finish` 要求 `admin`。所有写请求需提供 CSRF token，以及 `Idempotency-Key` header 或
payload 中的 `operation_id`。

- `enqueue` 接收 `item_id`、`item_name`、`start_price`、`user_name`；每人上架数由服务端限制。
- `dequeue` 接收 `item_id`、`item_type`。
- `session_start` 接收 `system_items_config`、`duration_hours`，可选 `system_item_count`（默认 5）；
  管理员配置仅用于此次开场，不写入持久配置。
- `session_finish` 可接收 `end_time`、`fee_rate`、`item_types`；缺省结束时间取注入的 Clock，手续费率默认 0.2。
  结算委托给 `AuctionSettlementApplication`，统计与日志由其 outbox effects 处理。
## 数据模型与迁移
`trade.001` 保留 feature 基线；`trade.002`/`trade.004` 在 game DB 创建鬼市存入/
取出 operation 表；`trade.003` 只在 trade DB 创建或补齐 `guishi_info`；`trade.005`/
`trade.006` 只在 trade DB 创建求购创建/订单撤销 operation 表；`trade.010`/`trade.011` 只在
game DB 创建或升级仙肆普通、自动、快速、系统上架 operation 表，`trade.012` 只在 game DB
创建仙肆撤架 operation 表。鬼市只读查询不拥有 schema migration。生产请求路径不隐式建表。
## 事务与失败回滚
鬼市存取使用 game DB 主事务附加 trade DB，玩家钱包、余额投影和 operation
写入要么全部提交、要么全部回滚。已迁移操作均保留 canonical payload replay/conflict
和旧 `guishi_stone_operations` 回放兼容；求购创建在 trade DB 的 immediate transaction
中原子冻结余额、插入订单和记录 operation，并保留历史 numeric 订单 ID/payload 回放。
存取的完整保存点覆盖钱包、鬼市余额与永久回执；检查写入行数，并在所有写入后重读
钱包、余额及回执字段，防止 `RAISE(IGNORE)` 或 trigger 改写仍返回完成。已知拒绝
完整回滚后保留 `stone_write_rejected`、`guishi_balance_write_rejected`、
`guishi_receipt_write_rejected` 或原数值精度原因及用户 `subject`，命令显示“本人账户”
和具体原因；未知存储异常仍向上传递。拒绝不写业务回执，同消息可在故障解除后重试；
Web 外层 operation ledger 仍保存拒绝结果，同 key 重放原拒绝，恢复使用新 key。
不修改单次/账户10亿上限、成功提款时的既有余额 clamp、手续费取整或历史成功回执。
命令数字解析同时处理不可转换的 Unicode digit 与超长整数，保留可转换全角数字。
寄存取物同样在完整保存点内扣暂存、增加背包和绑定数、写永久回执；检查每步行数，
并在最后回读存货/余额、背包数量/绑定数及回执关键字段。`IGNORE`、trigger改写和
异常均不能留下单边发物。已知拒绝分别保留 `guishi_take_storage_write_rejected`、
`guishi_take_inventory_write_rejected`、`guishi_take_receipt_write_rejected` 和主体；
未知异常保留原异常日志及安全定位回复，失败回复发送失败仍透传。
暂存解析拒绝坏JSON/非对象、重复键、非法或负数数量，分别为 `guishi_storage_invalid`、
`guishi_storage_duplicate`、`guishi_storage_quantity_invalid`；不跳过坏条目再覆盖账户。
有效的未取物品保留原值表示，全部领取所选物品并绑定，保留已有绑定数和库存上限。
非法背包总数/绑定数用 `inventory_invalid` 拒绝；坏历史取物回执用
`guishi_take_receipt_invalid` 拒绝，不改写旧回执或猜测成功数量。
取物消息操作号独立于物品参数，同事务兼容旧带物品编号的操作号，多条历史回执
明确冲突。先回放原成功回执，再判断本地周末/当前库存；应用按注入业务时钟和时区
决定新请求是否开放，纯仓储旧API默认开放保持兼容。未承诺取物提交与独立行为统计
之间的进程崩溃补记或跨库WAL断电原子性。
摆摊创建在 game DB 主事务附加 trade DB，原子扣减可交易库存、插入订单和记录 operation，
保留 `goods_num - state` 可交易量、`bind_num` 下限、库存 CAS 和历史 numeric 订单 ID。
求购撤销在 trade DB 原子退回未成交冻结灵石并删除订单；摆摊收摊在 game DB 主事务附加
trade DB，按背包上限原子退回未售库存、删除订单并记录 operation。求购/摆摊撮合、过期清理和
寄存物品取回，以及拍卖竞价、等待区、场次开始/交接与结算均已切换到 feature application。trade Web
队列与场次 route 也直接使用这些 application；旧 `AuctionSessionService` 已移入
`compatibility/legacy_trade_auction_sessions.py`，仅供显式注入的 rollback repository 使用，且内部复用
feature-owned session/settlement repositories。

取消求购、收摊和到期退物的返还、删单、永久回执均在完整保存点内执行，校验每步
行数和最后重读。IGNORE、trigger改写和回执未落盘不会伪返回 cancelled/cleared；
连同trigger副作用回滚本单，订单保留，可解除故障后重试。具体原因分别为
`guishi_refund_balance_write_rejected`、`guishi_refund_inventory_write_rejected`、
`guishi_refund_order_delete_rejected`、`guishi_refund_receipt_write_rejected`，主体为
订单号，经过应用到命令回复或调度错误。未知SQLite异常保持原链与事务回滚，取消
命令提供安全定位回复；NoneBot流程异常和失败回复投递异常透传。
坏订单数值/已成交量、负余额、坏背包和历史返还回执明确拒绝，不截断或猜账，不
覆盖历史。求购退款保留原暂存物品，退物保留原绑定数及库存上限，只返未成交部分。
逐单提交允许部分成功，已成功订单保持提交，失败单保留，不能把本单回滚称为全批
回滚；重放不重复返还或玩家行为统计。未承诺跨库WAL断电原子性或独立统计崩溃补记。

管理员 `清空鬼市` 复用相同仓储，逐单保留具体拒绝和订单号，未知异常保留原日志与
安全定位并继续其他订单；只将applied计为本次处理/返还，不把旧回放重复列为到账。
完成后重读剩余订单；失败或仍有订单时显示实际处理和剩余数量，不能回复全部成功。
剩余重读失败明确未能核实，名称读取失败仍列出已提交金额/物品，投递失败不宣称业务
未提交。正常已消失订单可跳过；缺物品、未知类型明确保留，旧中文类型走原仓储，
权限与冷却不变。
## 定时任务
场次调度继续由兼容 scheduler 管理。

鬼市摆摊窗口、周末存货/灵石取回和订单到期将 UTC `SystemClock` 投影到 APScheduler 的业务时区
（默认 `Asia/Shanghai`）；摆摊窗口为本地 `[20:00, 次日08:00)`，本地 08:00
即关闭。存储时间和历史 operation 锚点不因此重写。

`TradeApplication.business_timezone` 由 legacy、runtime 和 Web fallback 注入 scheduler
时区；独立构造默认 `Asia/Shanghai`，测试或其他嵌入调用可以显式传 UTC。创建回执由
应用注入时钟取时，与背包更新时间保持一致，避免数据库当前时间绕过固定时钟。
求购按创建的本地日期推算「次日08:00」：凌晨00:01创建仍是下一日08:00到期；
摊位则截止于下一次08:00清理，凌晨摊位当天08:00结束。信息页和实际退款使用同一
业务时间规则。旧 SQLite 无偏移时间按 UTC 解析后投影，ISO 偏移也保留瞬间语义。

自动撮合遇到 `operation_conflict`、无效订单类型或其他非预期结果时保留具体 status
与订单定位并向调度器抛错，不再跳过后记录“交易处理完成”。取消/退物拒写还保留
具体reason和subject。订单已消失、已完成或
不再可撮合属于正常竞争跳过；到期取消/清理也只容忍订单已消失，冲突会抛错。
背包已满仍警告并保留摊位。任务不发真实 QQ 消息。

`clear_expired_baitan_orders_job`（每日 `GUISHI_BAITAN_END_HOUR` 点，job id 未改）
现在按同一顺序做两件事：先全服求购到期取消退款，再清理超时摆摊并退还未售出
物品。两段都是静默的（任务没有 bot/event 上下文，不发群消息），退款与清理结果
只写日志。求购到期同时在 `鬼市信息` 的本人访问期收敛，因此玩家不必等下一次
任务就能看到冻结灵石回到余额。
信息页到期取消遇到已知失败时返回订单定位和具体拒绝原因，不继续展示为正常账户；
不能据此宣称其他订单没有退款，已提交的成功处理仍保留。
## 配置项
`trade_enabled`。
## 适配器差异
Web/命令不直接连接 trade_db。
## 测试与手工验收
覆盖求购/摆摊创建与撤销重复请求、订单 ID 冲突、订单上限、余额/库存不足、背包满、
跨库失败和恢复重试。
## 灰度开关、回滚和已知限制
已完成的鬼市存取、求购/摆摊创建和撤销入口没有隐式回退；旧 `GuishiStoneService` 仅保留给尚未
迁移的交易兼容路径。仙肆普通、自动、快速和系统上架已切换。拍卖竞价、等待区、场次开始/交接与
结算已由 feature application 处理；场次自动调度仍由兼容 scheduler 触发，其余未覆盖拍卖策略与
命令路径仍待迁移。

鬼市生命周期已知限制：

- 求购到期锚点只来自 `guishi_order_create_operations`。早于本功能创建的历史求购订单
  没有该行，因此**不会自动到期**，只能手动 `鬼市取消求购` 或管理员 `清空鬼市`；
  渲染显示 `到期：未知（历史订单）`。这是有意的 fail-safe：宁可不清理，也不猜
  时间误删玩家订单。
- 到期小时沿用 `GUISHI_BAITAN_END_HOUR`，按 scheduler 的业务日期解释；历史锚点
  本身不变。没有锚点的旧摊位仍会在既有08:00清理中退回，信息页显示截止未知。
  尚未做线上历史订单对照或真实 QQ 展示，不以隔离数据回归冒充生产观察。
- `price_refund` 只补「买家按求购价冻结、按摆摊价成交」的差价，不改变价格闸门
  （摆摊价高于求购价仍然不成交）、部分成交语义或手续费。
- legacy `TradeRepository.match_guishi_orders` 保留原语义供兼容测试使用，默认
  路径已停用（进度门禁 `legacy_guishi_matching_disabled`），新语义只由 feature
  路径承担。

## 参数与任务回归矩阵（2026-11-17）

| 入口/路径 | 当前证据 | 仍待覆盖 |
| --- | --- | --- |
| 灵庄、仙肆上架/购买/下架/查看/我的仙肆/行情/关注 | 当前 live matcher 参数组 23 项通过 | 非本组别名、快速链和更多边界沿用既有独立用例 |
| 鬼市存款 → 求购 → 摆摊即时撮合 → 周末取物 | 真实 dispatch/DI/handler、隔离 game/trade/player DB，核对冻结/收入/暂存/库存/取物重放；创建同消息重放/参数冲突/旧操作号兼容、并发matcher预留库存、闭市回放；即时撮合部分失败、重建应用/同消息与实际调度恢复、成交统计事务/重放 | 线上观察 |
| 鬼市取物品 | 成交后全取/绑定、周五23:59拒绝/周六00:00恢复/周一旧消息回放；库存上限相邻/坏库存/坏暂存/历史回执冲突；三步IGNORE和未知SQLite异常整次回滚，重建应用后同消息恢复/重放，真实matcher并发只发一次；具体原因和主体经过OneBot/QQ短合同 | 线上观察；资产与独立行为统计之间崩溃补记未承诺 |
| 鬼市取消求购 | 真实订单部分成交后仅退剩余、保留已得物品；三步IGNORE及SQLite异常整单回滚/重建应用/同消息恢复重放，实际统计只记一次；旧操作号/冲突/权限和坏订单回执明确拒绝 | 线上观察 |
| 鬼市求购/摆摊参数 | 足额账户与真实物品下验证最低价相邻边界、非法数字/多余参数零写；默认1、零/负数截为1、11截为10、数量10、10/11单上限、上限后的旧消息回放与实际统计 | 历史坏订单形态 |
| 未售鬼市摆摊自动退回 | 真实凌晨摆摊 → 信息截止与本轮08:00对齐 → 清理回调 → 库存/回执重读，重复任务幂等，两库 integrity_check | 线上历史订单对照 |
| 鬼市信息/求购到期退款 | 真实命令建单和锚点，07:59展示剩余1分、08:00实际 APScheduler退款、信息重读和重复任务；历史UTC锚点/本地跨日/缺锚点/并发退款/回滚 | 更多历史订单形态和线上观察 |
| 鬼市存取灵石 | 真实命令固定到本地周六零点与周一边界；钱包/账户不足、累计及单次上限相邻边界、小额手续费取整和10亿提款；非法/全角/超长参数、同消息重放/冲突；余额/回执拒写后三库零写、恢复/重放与实际统计；Web具体原因及历史拒绝重放 | 线上观察；资产提交与独立行为统计间崩溃补记未承诺 |
| 鬼市任务注册/触发/失败状态 | live APScheduler 注册、08:00 cron 边界、独立 APScheduler 实际执行/错误事件，正常竞争跳过 | 线上运行观察、真实客户端展示 |
| 鬼市收摊 | 真实部分成交后退未售库存；背包满/目录缺失/三步拒写保留订单及原绑定数，部分成功与恢复、逐单原因/定位、重读/回执/实际统计、重复收摊零额外写；OneBot/QQ短合同 | 线上观察 |
| 拍卖玩家链 | 真实上架→自动开启→活动/竞拍参数→0.5秒/截止→启动结算故障/恢复/重放；开场四步拒写/同消息恢复/流拍返还；三库资产/回执/历史/统计；孤儿拍品任务及管理员/玩家拒绝 | 真机未验；未涵盖所有拍卖规则与资产写路径 |

六条鬼市参数场景由 `tests/test_guishi_parameter_dispatch.py` 接入 unittest CI。
另五条 `market-guishi-take` 场景覆盖完整取物、时间/历史/参数/并发和故障恢复；
快照包含game/trade/player三库资产、永久回执和实际统计，故意SQLite异常只在对应
调用精确断言类型/traceback/安全回复，普通异常监测不关闭，恢复后核对三库完整性。
仓储回归 `test_trade_guishi_take_recovery.py` 补齐六种拒写/改写、trigger副作用、
时钟异常、坏JSON/重复键/数量/背包与历史回执零写、旧操作号和周末回放。
旧成交取物参数夹具现给应用及命令注入同一业务时钟，资产和回执断言保持不变。

三条 `market-guishi-return` 场景经真实dispatch/DI/handler验证取消退款和收摊的
拒写/部分成功/恢复，实际APScheduler在08:00先退款拒写报错，再退款成功但退物
拒写报错，恢复后仅处理剩余单；07:59求购不提前到期、信息页明确失败、重复任务
零额外写。`test_trade_guishi_return_recovery.py` 覆盖两类取消及到期退物的三步
IGNORE/改写、晚触发器破坏余额、缺账户INSERT拒绝、坏订单/背包/历史回执、绑定数
与恰好库存上限、零剩余量及integrity_check。适配器只做mock短合同，未实测真机。
另两条 `market-guishi-clear` 真实参数场景验证matcher权限拒绝零写、管理员部分
提交/拒写/SQLite异常/重放恢复和历史类型，三库实际快照与完整性；
`test_guishi_clear.py` 覆盖消息/名称/剩余读取失败、旧回放不重计、缺目录和未知类型、
OneBot文本/QQ原生Markdown与降级短合同，已进入CI的pytest步骤。
新增七条 `market-guishi-boundaries` 覆盖存取解析、金额/手续费/订单边界与四种故障
恢复；三库快照含钱包、存取回执和统计，恢复后重放零写，并检查三库完整性。
`test_trade_guishi_stone_write_rejection.py` 覆盖钱包/新账户/余额/回执忽略、余额与
回执改写、原数值错误透传、trigger副作用完整回滚及Web拒绝合同。OneBot普通文本、
QQ Markdown蓝字与发送降级保留具体原因、本人账户和已回滚语义，未做真机验证。
另五条 `market-guishi-lifecycle` 场景使用相同包装器进入CI：两条并发matcher在handler
入口会合，共用一次transport补丁生命周期，分别验证同消息幂等和不同消息库存竞争；
不使用相互覆盖全局补丁的并发`run_case`，不以repository并发冒充命令并发。
创建/取消的消息操作号独立于价格/数量/目标订单；收摊继续每单子操作号。仓储在同一
事务内读取旧带参数操作号，只读重放且不改写历史；同消息多条旧回执明确
`operation_conflict`，不猜选一条。旧摆摊回执若仅留截断数量，无法证明原请求数量时
也明确冲突，不重新扣库存。新摆摊在事务内截断库存，保留请求payload与权威实际数量，
回放先于当前库存/订单上限/时间窗。回放不重复统计或触发一次新撮合。
求购与摆摊CAS拒绝会回滚完整保存点，包括本次写入及trigger改动；异常仍由UoW
完整回滚。收摊逐单事务允许部分成功，只记录实际新取消/退回数量，明确剩余订单
与具体失败；不宣称整批回滚。创建后的即时撮合是后续独立事务，不能冒称创建与撮合
整体原子。

`trade.015` 在 trade DB 新增 `guishi_immediate_match_requests`；仅新命令创建时将
pending 与创建/冻结/扣库存一起提交，成功完成第一轮撮合后标记 complete。求购与
摆摊同消息重放可继续 pending，complete 和缺少 pending 的历史回执不另启新撮合。
自动任务先恢复 pending；显式失败抛出具体 reason、subject 和两单 ID，阻止常规
sweep 用另一操作号绕过未解决冲突。正常订单消失/已完成/不再匹配仍是竞争跳过。
部分成交后失败会保留已成交结果和剩余 pending，不能宣称创建或整批结算已回滚；
未知存储异常保留原异常/traceback，命令回复安全定位及查询提示，NoneBot流程异常透传。
失败原因写入再失败时保留具体业务错误并关联持久化异常；不把它改成“未知错误”。

`trade.016` 在 player DB 准备六列成交统计。生产命令与 runtime 注入 player DB，
每对成交附加该库，将买卖统计和资产/订单/永久回执一并提交；旧纯仓储调用仍可不传
player DB。回放不重复统计，旧结果缺少 `statistics_recorded` 仍可读取。缺迁移明确
schema_missing且整对不写，统计trigger失败也回滚整对；完成标记失败保持 pending，
重试依据永久成交回执/当前订单收敛，不重复入账。不宣称跨库WAL具备断电原子性。
创建行为统计仍沿原命令日志路径，未承诺创建提交与该日志之间的进程崩溃补记。

两条 `market-guishi-recovery` 场景验证真实买单创建后异常/重建应用/同消息恢复，
以及摆摊首对已提交、次对冲突后的部分结果与恢复；读取三库余额、库存、回执、
pending/complete及实际statistics。故意注入的异常有精确类型/traceback/回复断言，
不关闭普通参数探针的异常监测。`test_trade_guishi_immediate_match.py` 另覆盖迁移
路由/幂等、旧回执零额外写、统计故障回滚、完成标记/原因记录失败与大整数统计；
实际调度事件和OneBot/QQ失败消息短合同在既有 `test_guishi_schedule.py` 中执行。

撮合恢复和常规sweep均接应用注入的UTC时钟与业务时区。pending入口先判断自身
订单截止；每对真实成交事务内再判断双方截止，防止第一对提交后时钟跨08:00、或
未过期新单消费清理失败留下的旧单。截止仍沿原创建回执锚点：求购创建次日08:00、
摆摊本轮08:00；已到点分别返回 `qiugou_expired` / `baitan_expired`，保留pending
与具体双订单，不改资产或成交统计，不冒充无匹配完成。历史缺失/非法锚点仍不猜测
到期，旧成功成交回执先回放，截止后不重复结算；纯仓储未注入时钟的旧API保持兼容。
到期任务或玩家取消真正删除订单后，下一次恢复将pending标complete，不重退资产。
`market-guishi-cutoff-recovery` 真实链验证07:59摆摊创建后中断、08:00重放拒绝撮合、
取消回执trigger失败产生实际EVENT_JOB_ERROR且整单回滚、解除故障退款退物、自动
撮合收敛pending和同消息/两任务重复零额外写；三库完整性与零成交统计均核对。
`test_trade_guishi_match_deadline.py` 另覆盖首对已提交后的跨界、双方过期、空候选、
显式UTC时区与历史成功回放；过期原因也进入OneBot/QQ Markdown/降级短合同。
`tests/test_guishi_business_time.py` 验证历史 UTC/ISO 锚点、本地日期/月年边界、凌晨摊位、
本地周末、UTC显式配置、Web真实请求、创建时钟故障回滚和同到期竞争只退款一次。
`tests/test_guishi_schedule.py` 覆盖本地窗口/周末边界、已知失败、实际调度和真实
`handle_send` 的 OneBot 文本、QQ Markdown 蓝字与发送失败降级短合同；共享业务链
只执行一次。CI 另执行魔修、鬼市和同类任务的定向 pytest 组。
这些均为隔离库与 mock transport 证据，未验证真实 QQ/WebSocket 或线上数据。

## 拍卖调度与恢复

自动开场按scheduler业务日期生成操作号与last_auto_start_date，empty与已有场次竞争
正常跳过。旧开启回执对应场次已结束、操作冲突及孤儿拍品保留具体reason/subject；
失败进入APScheduler错误事件。收尾和启动对账同时核对场次与拍品，任一单边缺失
明确报错，不猜测重建；默认启动回调不吞异常，生命周期桥保留失败并可重试。

竞价仓储在扣款事务内检查持久场次[start_time,end_time)，距截止不足一秒仍有效，
正好到点即拒绝新扣款。Web用服务端业务时钟，客户端bid_time不能回拨窗口。
旧成功竞价回执先回放，不因截止重复扣款。自动收尾重试以持久截止时间形成稳定
payload；SQLite异常回滚后推进时钟仍可继续。旧冲突回执不自动重写，业务拒绝回执
保持原有回放语义。活动/列表/信息展示scheduler时区并标明截止待结算；未传now的
旧状态API维持进程本地naive返回兼容。

`test_auction_schedule.py` 覆盖live注册/cron/interval、实际任务事件、生命周期桥
失败/恢复、空拍品与禁用、原异常开场回滚、孤儿/缺拍品、有效窗口/坏时间/旧回执、
Web旧时间绕过和结算恢复。两条`market-auction-lifecycle`命令探针核对三库资产、
回执、成交历史和统计，`test_auction_messages.py`覆盖OneBot/QQ文本/Markdown/降级
与未知异常/正常流程/发送失败，均为隔离证据。新pytest进入定时CI，命令包装进入
unittest CI。

开场交接另有`test_auction_start_write_rejection.py`：拍品插入、等待区删除、场次
插入、永久回执插入均检查行数和全部写入后的最终回读。IGNORE/trigger改写明确
返回`auction_start_item_write_rejected`、`auction_start_queue_write_rejected`、
`auction_start_session_write_rejected`或`auction_start_receipt_write_rejected`，保留
拍品/等待区/场次/操作号subject；拒绝跨出UoW后才转为结果，整笔两库和触发器
副作用回滚，故障解除后同操作可重试且成功回放不重交接。实际调度产生错误事件，
手动开启保留具体原因。`market-auction-start-write`经真实上架/管理员四步拒写/
同消息恢复/重复开场及实际到点流拍验证只交接和返还一次，核对三库数据/统计。
这些测试不宣称跨库WAL在断电时原子提交，也不外推所有竞价/结算写入路径。

## Manifest 清单
- `route: POST /api/v1/trade/deposit`
- `route: POST /api/v1/trade/withdraw`
- `route: POST /api/v1/trade/enqueue`
- `route: POST /api/v1/trade/dequeue`
- `route: POST /api/v1/trade/session_start`（admin）
- `route: POST /api/v1/trade/session_finish`（admin）
- `route: POST /api/v1/trade/purchase`
