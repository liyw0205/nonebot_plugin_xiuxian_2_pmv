# 宠物资产操作

## 用户流程

宠物游历派遣、游历领取、喂食、砸蛋和技能替换五项资产动作通过 `PetApplication` 统一处理。砸蛋与技能替换命令默认直接调用 feature application，兼容门面只保留显式回滚入口；结果适配仍保留原有字段和提示语。

## 命令与别名

- `宠物游历`、`灵宠游历`：派遣宠物。
- `领取宠物游历`、`领取游历奖励`：领取游历奖励。
- `宠物喂食`：消耗背包物品提升宠物经验。
- `砸蛋`、`砸宠物蛋`：消耗灵石孵化宠物。
- `宠物背包 [页码|关键词]`：纯数字为页码，其余按宠物名/品种/稀有度/类型/UID 做大小写不敏感筛选（可带页码，如 `宠物背包 龙 2`）。此前只做 `int(args)`，非数字一律静默回退第 1 页，关键词被无声吞掉；现在由 `features/base/keyword_page.py` 统一解析，筛选后保持原排序与分页，无命中时提示「没有包含该关键词的内容」，与空背包提示分开。筛选后的 MD 下一页链接与纯文本翻页提示同样通过 `keyword_page.page_command` 携带关键词（如 `宠物背包 龙 2`），翻页不会回到未筛选列表。
- `替换宠物技能`、`确认替换宠物技能`：确认替换升星候选技能。
- `保留宠物技能`、`放弃替换宠物技能`：放弃候选技能并保留当前技能。

## Web API

- `POST /api/v1/pet/travel/start`
- `POST /api/v1/pet/travel/claim`
- `POST /api/v1/pet/feed`
- `POST /api/v1/pet/hatch`

所有接口权限为 `user`，写请求支持 `Idempotency-Key`，统一返回 `OperationOutcome`。

## 数据模型与迁移

`pet.001` 在 `game_db` 创建 `pet_feature_migrations`；`pet.002` 在 `game_db` 创建 `pet_hatch_operations`；`pet.003` 在 `player_db` 创建 `pet_skill_replace_operations`。孵化和技能替换 repository 只使用已迁移 schema，不在请求路径建表；宠物历史表仍由既有 player 数据边界管理。

### 回执表的 schema 归属（`pet.005` / `pet.006`）

出战切换、派遣出发、融合突破、喂养、放生、技能洗练六条默认命令路径原先在各自的写事务里
`CREATE TABLE IF NOT EXISTS <回执行>` 惰性建表。现在启动迁移是唯一写入者，
`features/pet/schema.py` 是唯一契约来源：

| 库 | 表 | 迁移 |
| --- | --- | --- |
| player | `pet_active_switch_operations` | `pet.005` |
| player | `pet_travel_start_operations` | `pet.005` |
| player | `pet_fusion_breakthrough_operations` | `pet.005` |
| game | `pet_feed_operations` | `pet.006` |
| game | `pet_release_operations` | `pet.006` |
| game | `pet_skill_reroll_operations` | `pet.006` |

归属按仓储实际打开的主库划分：`active_switch`/`travel_start`/`fusion` 的仓储只接 player 库；
`feed`/`release`/`skill_reroll` 的仓储以 game 为主库、只把 player 作为 `player_data` 附库。
请求路径只校验「表存在 + 本次要读的列齐全」，缺表或缺任一列返回 `schema_missing`
且零写入（不动宠物、背包与任何回执行）；半成品回执表不会被补列。

兼容 `xiuxian/xiuxian_Interactive` 之外的 legacy 宠物服务仍按其既有方式工作；
本切片的定向测试钉住 `features/pet/repository.py` 不含 `CREATE TABLE`/`ALTER TABLE`。

## 事务与失败回滚

application 先调用带 operation replay/CAS 的 feature-owned repository；重复请求只重放首次结果，宠物快照、背包和灵石不匹配时返回拒绝且不修改资产。缺少 `pet.002` 或 `pet.003` 时请求失败并保持 schema 不变。

### 砸蛋回放契约

`PetApplication.hatch_result(*, operation_id)` 是 **keyword-only**：操作 ID 只能以
关键字传递。`砸蛋` handler 在扣灵石/查容量之前先做前置回放（`operation_id=` 取得
`pet_hatch_operations` 回执），命中已成功回执时直接重放同一批宠物、不重掷随机、不重复扣灵石；
孵蛋落库后同一次请求还会用同一 operation ID 做后置回放。历史上前置回放曾以**位置参数**
调用该 keyword-only 方法，导致 `砸蛋` 在真正执行前就抛 `TypeError`、玩家收不到任何回复。
缺少 `pet_hatch_operations` 表时读取直接失败且**不建表**、不写请求期 DDL。

### 砸蛋回执的存储编码（`receipt_codec`）

`pet_hatch_operations.result_json` 只用于回答「这个 operation 已经发过什么」，但历史上写入的是
`_normalize_pet` 生成的整份宠物快照，而该函数同时设置 `pet["skill"] = pet["skills"][0]`，
于是同一份 33 键技能定义每个宠物存两遍。线上实测：`pet_hatch_operations` 占
`xiuxian.db` 94.2 MB / 290 MB（4,600 行，18.4 KB/行），其中每宠 1.30 KB（37.9%）就是这份重复。

`features/pet/receipt_codec.py` 提供 `encode_receipt` / `decode_receipt`（纯函数，无 I/O）：

- 每个宠物的**全部既有键**原样保留（身份、实例状态、未知字段），并保留
  `skill` 与 `skills` 的原始形状：多技能列表、`skill` 不等于 `skills[0]`、
  非映射条目、显式 `None` 都逐字还原；技能定义抽成每条回执一张引用表，按内容去重，
  宠物按索引引用；一批十个蛋共享重复定义。
- 技能只冻结**展示/身份子集**（`skill_id`、`name`、`type`、`scope`、`category`、`desc`）。
  展示文案在写入时冻结，因此之后目录改名不会改写历史回放；`base_power`、`effect`、
  `star_bonus` 等目录属性保持可由 `skill_id` 重新解析，不再逐宠复制进回执。
- JSON 以 `ensure_ascii=False` 写入，中文按 3 字节 UTF-8 而不是 6 字节转义保存；回执只经
  `json.loads` 读回。
- `v: 2` 文档带版本标记；没有标记的旧行按 v1 结构解析。**损坏回执 fail-closed**：
  非法 JSON、未知版本、坏行、越界技能槽位一律抛 `ReceiptDecodeError`，仓储把它转成
  `receipt_unreadable`（`succeeded=False`），绝不伪装成「成功但零宠物」，也绝不再次扣费。

兼容性：`features/pet/repository.py`（`hatch`/`get_result`/duplicate 分支）与
`compatibility/legacy_pet_transactions.py`（`_result_from_row`/`hatch`）共用该编解码器；
`payload`（CAS 令牌）、扣费、落库、`state_changed` 语义均未改。实测 200 行真实回执
平均 18,427 B → 3,993 B（-78.3%，键集完全保留），按 383 行/活跃日计算写入量约
7.06 MB/日 → 1.53 MB/日。旧行不重写（不做后台 UPDATE/VACUUM），空间只通过后续新增行体现。

## 配置项与回滚

`pet_enabled`（`XIUXIAN_PET_ENABLED`）控制灰度。关闭后可以继续使用旧入口，待融合、放生和技能动作迁移完成、兼容命中归零后再删除门面。

## 定时任务

游历到期由兼容调度器处理，任务使用稳定 ID 和幂等领取操作。

## 适配器差异

application 不导入 NoneBot、Flask 或数据库驱动；旧命令和 Web blueprint 分别承担协议转换。

## 测试与手工验收

覆盖派遣、领取、喂食、孵化的成功、库存拒绝、异常回滚和重放，并执行 Web CSRF/权限测试。

## 灰度开关、回滚和已知限制

`XIUXIAN_PET_ENABLED` 关闭后回到旧兼容实现；融合、放生以及查询/展示中的历史 JSON 边界仍保留显式兼容路径。
