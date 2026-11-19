# 背包与物品
## 用户流程
礼包、物品、装备、技能、修复和炼丹动作通过一个操作号协调。
## 命令与别名
`背包`、`使用物品`、`装备`。
`我的物品流水`（别名 `物品流水`、`道具流水`，priority 7）：玩家本人近 7 天物品收支概览——
获得/消耗件数与种类数、变动笔数、主要获得与主要消耗各前 3、最近 5 笔明细。
该命令已登记在 `背包帮助` 的「查看」段，玩家可从帮助页直接发现。
数据来自共享 `economy_log.item_delta`（JSON 元素 `{"id","name","type","amount",...}`，
加物品为正、扣物品为负），由背包加减（`item_add`/`item_cost`）、奖励统一发放、
宗门商店兑换与仙肆购买持续写入。时间窗只用下界以避免 UTC/本地时间混用误删「刚刚发生」的行；
`user_id` 只取登录会话，不接受参数指定他人；只读、无新表/迁移/配置键，
缺表、缺列或单行脏 JSON 均降级跳过而不报错。
`背包容量`（别名 `我的背包容量`、`背包空间`，priority 7）：玩家本人的背包容量体检，只读。
上限是配置项 `max_goods_num`，且**按单件物品件数**判定——各玩法在写入前执行
`当前件数 + 本次发放 > 上限` 即返回 `inventory_full`（任务领奖、洞府收获/巡逻/潜入、
战斗结算、宠物出行均如此）。因此视图给出的是每件物品的占用与剩余，而不是背包总件数与上限之比；
展示条目数、总件数、最接近上限的前 3 件（当前件数/剩余可容纳件数/占用百分比），
并把达到 80% 的条目列为警示。`user_id` 只取登录会话；缺 `back` 表或缺 `goods_num` 列时
明确降级，不建表、不补列、不写库，也不改动任何 `inventory_full` 判定。
该命令已登记在 `背包帮助` 的「查看」段。
`我的背包 [页码|关键词]`：参数既可以是页码，也可以是关键词（可带页码，如 `我的背包 丹药 2`）。
此前实现只做 `int(args)`，解析失败即静默当第 1 页——玩家输入 `我的背包 破境丹` 看到的是首页，
参数被无声吞掉。现在由 `features/back/backpack_search.py` 明确区分两种意图：纯数字才是页码，
其余按名称或类型做大小写不敏感的包含匹配；筛选后保持原有排序与分页，空段整段丢弃。
筛选无结果时提示「没有名称或类型包含该关键词的物品」，与真正的空背包提示分开，不误报。
筛选后的翻页（MD `next_cmd`、纯文本「下一页」提示与底部按钮）统一由
`features/base/keyword_page.py::page_command` 生成 `我的背包 关键词 页码`，所以第 2 页仍是
筛选结果，不会退回未筛选列表；`丹药背包`、`药材背包`、`药材背包详情`、`我的装备` 四条同类视图
使用同一规则。
`背包帮助`「查看」段已补该用法。
`查看修仙界物品 类型 [关键词] [页码]`：图鉴列表按物品名/ID 筛选（如 `查看修仙界物品神通 剑法`），
翻页提示与底部按钮改为完整命令 `查看修仙界物品神通 剑法 2`（旧提示走 `查看` 别名 +
类型紧贴页码 `查看神通2`，无法携带关键词）；旧的 `神通2` 写法仍按页码解析。解析与翻页由
`docs/features/base.md` 的「列表检索与翻页续查（共享合同）」统一提供，无命中与空类型分开提示。
## Web API
`POST /api/v1/back/{open_package,use_item,change_equipment,learn_skill,repair,use_pet_eggs,alchemy,unbind}`，权限 `user`。
## 数据模型与迁移
迁移 `back.001` 至 `back.017`；饰品词条锁定、洗练、分解、升阶和预设 operation 共用 game DB 的饰品操作表，通用物品批处理、聚灵旗替换、装备 operation、背包修复任务和宠物蛋 operation 也位于 game DB。player DB 仅作为显式 attached 事务参与方；attached accessory schema 由 `accessory_package.player_data.001/.002/.003` 启动迁移维护，其中 `.003` 提供三个预设列，历史背包字段通过兼容服务读取。
## 事务与失败回滚
操作 ledger、审计和异常重试由应用层统一提供。

`饰品预设 [1/2/3]`（别名 `预设饰品`）无参数时只读查看；保存仍记录当前四槽 UID，可覆盖原预设。
`AccessoryPresetApplication` 在立即 attached 事务内保存 player DB 预设与 game DB 永久操作回执，
逐步核对影响行数、回执 payload/result 和玩家文档最终投影，保留装备、背包及其他预设栏。
状态拒写/改写分别返回 `accessory_preset_state_write_rejected` /
`accessory_preset_state_projection_mismatch`；回执拒写/改写分别返回
`accessory_preset_receipt_write_rejected` / `accessory_preset_receipt_projection_mismatch`。
已知拒写携 `subject=preset:序号` 整笔回滚，玩家回复保留具体原因和预设编号；
解除故障后同消息可恢复，成功回执重放不再写入。未知 SQLite 异常原样透传并回滚，
并发竞争仍按装备/原预设快照拒绝，不改保存或装备规则。

`快速装备饰品 1/2/3` 同样在立即 attached 事务内一次保存换装后的装备、背包、清理后的预设与永久回执。
状态和回执分别核影响行数与最终投影，并在回执后复核完整玩家文档；拒写/改写保留
`accessory_quick_equip_state_write_rejected` / `...state_projection_mismatch` /
`...receipt_write_rejected` / `...receipt_projection_mismatch` 和预设编号，整笔回滚后可同消息恢复。
正常换装、已经装备时跳过、失效 UID 清理和空预设拒绝保持；成功回执优先重放，不重复换装。

`饰品升阶 部位 材料UID1 [材料UID2 ...]`（别名 `升阶饰品`）升级该部位已装备的主饰品；
一/二/三/四阶分别消耗 1/1/2/3 件同阶同款材料，重复参数去重，多余合法材料保留，五阶拒绝。
升阶保留已有词条和合法锁定、补齐新阶词条并重置洗练次数。装备/背包和永久回执在同一保存点内写入，
核影响行数、回执 payload/result 与完整玩家文档最终投影；状态拒写/改写保留
`accessory_upgrade_state_write_rejected` / `...state_projection_mismatch`，回执拒写/改写保留
`accessory_upgrade_receipt_write_rejected` / `...receipt_projection_mismatch`，携主饰品 UID 整笔回滚。
领域的具体材料拒绝原因优先传到玩家回复，重复历史材料 UID 不再误报为材料不足。
解除故障后同消息可恢复，成功回执重放不重复消耗；SQLite ABORT 原异常透传并回滚。
## 定时任务
既有 `daily_reset_day_num` 仍由 scheduler 在每日零点执行，写入改由
`DailyPillUsageResetApplication -> DailyPillUsageResetSqlRepository` 承担。按 scheduler 时区生成
业务日回执；同日重放不重复清零，只有 `goods_type='丹药'` 的 `day_num` 被更新。计数、共享
operation ledger 和 audit 在同一 immediate UoW 提交；缺少既有 platform schema 或背包列时 fail closed，
不请求期建表，不增加业务 migration。
## 配置项
`back_enabled`。
## 适配器差异
Web 和命令适配器不直接写库存。
## 测试与手工验收
覆盖重复提交、库存上限、物品不足和异常回滚。
- `python -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/back/tests/test_daily_pill_usage_reset.py -q`
- `python -m pytest -q tests/test_accessory_preset_application.py tests/test_accessory_preset_parameter_dispatch.py`：
  隔离 SQLite、真实 matcher 参数/别名、拒写/改写/ABORT、恢复/重放与 OneBot/QQ 消息 mock；不代表真实 QQ 联调。
- `python -m pytest -q tests/test_accessory_quick_equip_application.py tests/test_accessory_quick_equip_parameter_dispatch.py`：
  隔离换装/回执投影、状态和回执拒写/改写/ABORT、恢复与重放、1/2/3/空预设/非法参数及 OneBot/QQ 消息 mock。
- `python -m pytest -q tests/test_accessory_upgrade_application.py tests/test_accessory_upgrade_parameter_dispatch.py`：
  隔离两库的升阶/材料/回执投影、拒写/改写/ABORT、恢复/重放、品阶/参数边界、历史 UID 冲突和 OneBot/QQ 消息 mock。
## 灰度开关、回滚和已知限制
关闭开关回退旧背包入口；通用 Web 批处理只负责原子扣除背包物品，具体物品效果仍由各领域 handler/application 维护。

## Manifest 清单
- `route: POST /api/v1/back/open_package`
- `route: POST /api/v1/back/use_item`
- `route: POST /api/v1/back/change_equipment`（`EquipmentApplication` 默认路径，旧服务仅显式兼容）
- `route: POST /api/v1/back/learn_skill`
- `route: POST /api/v1/back/repair`
- `route: POST /api/v1/back/use_pet_eggs`
- `route: POST /api/v1/back/alchemy`
- `route: POST /api/v1/back/unbind`
