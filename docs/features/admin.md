# Administration

## 固定队列与已审边界（2026-10-07）

当前按子插件顺序停在 admin。50 个冻结入口中，开始盘点前已关闭 6 个；剩余 44 个已一次性完成 owner 盘点。配置 6 条和已有资产/重置 13 条均已验收，现剩 25 个未关闭项，后续按下列组验收，不重新从全局 blocker 中挑命令。

| 顺序 | 入口组 | 状态与下一步 |
| --- | --- | --- |
| 1 | 运行配置 6 条：群修仙、私聊、自动灵根、自动宗名、欢迎开/关 | 本组接入 `AdminConfigApplication -> AdminConfigRepository`；`JsonConfig` 继承同一仓储，旧读者与 Web 备注/置顶/全量群共享锁与缓存。修复三处字段错配。 |
| 2 | 已迁移资产/重置 13 条：传承、修为、造化、轮回、创造、毁灭、修仙适配、新手礼包、悬赏、塔、BOSS、仙缘、易名 | 已验收现有 admin_asset/work/tower/boss/base 写终端，未重迁。修复 4 个 helper 重复 status 关键字、易名/BOSS误报、传承并发批次误报，补仙缘剩余退款、跨笔回滚、缺失赠礼者保护。全服饰品目标读取仍无界，不宣称已优化。 |
| 3 | 兼容输出 9 条：修仙手册、广播帮助、艾特测试、按钮测试、消息信息、取链接、取raw、取reply、全量申请 | 已审为无独立业务状态；需补来源绑定及兼容断言后统一归类，勿新建九套 application。共享消息记录不等于零基础设施写入。 |
| 4 | 黑屋 3 条 | SQL is_ban 与 JSON/_BANNED 双状态；注册玩家封禁未同步路由消费，失败结果亦有误报，必须整组修。 |
| 5 | 命令管控 3 条 | command_disable.json、别名注册、禁用列表和路由重建同组。 |
| 6 | 广播 6 条 | 进程任务、取消、目标集合及普通消息补发同组，消息历史和发送使用端口。 |
| 7 | 生成秘境、重载items、用户伪装，各 1 条 | 秘境已有 feature SQL 写与旧 JSON 投影，不重迁；Items 是共享目录缓存；伪装是共享进程身份映射，分别闭合实际消费者。 |
| 8 | 转换QQID 1 条 | 旧四库逐 ID 同步 API 编排待迁；复用已迁移的可恢复 ID 更新，不重复写底层仓储。 |

配置使用原字段 `group/private/root_selection/sect_name/welcome_disabled_groups`，保留未知字段和原 JSON 路径；读缺失文件返回默认值，兼容 `JsonConfig` 构造仍创建默认文件。写入同目录暂存后原子替换，失败不发布缓存；相同开关值不重写。锁只覆盖同一进程内的线程，不提供多进程协调或 operation-ID 回执。全局欢迎关闭时不再谎报本群已开启。notice 的生命周期进程状态不在这六个命令的完成范围内。

资产结果组定向回归 `271 passed, 2 subtests passed`，覆盖真实 helper/handler 函数抽取执行和默认 feature 仓储。已关闭的历练重置仅因发现 `schema_missing` 被报成功这一明确回归而补失败分支，未重迁其 owner。仙缘清池没有 operation-ID 回执，全量读取及 SQL 历史重复玩家行等边界不在本组性能完成声明内。

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.admin.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

管理员 `易名` 使用 `BaseApplication -> BaseRenameSqlRepository` 更新玩家道号，并复用 `base.002` 的操作回执；旧管理器仅用于目标查询，不再执行该写入。

## 事务与失败回滚
SQL 事务写入的回执语义以各仓储为准。运行配置使用绝对值设置和原子 JSON 替换，相同值不重写，没有 operation-ID ledger；写入失败保留旧文件与旧缓存。不能把所有管理操作概括为统一的回放契约。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`admin_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: ID交换`
- `command: ID更新`
- `command: md模板`
- `command: 传承力量`
- `command: 修为调整`
- `command: 修仙手册`
- `alias: 修仙管理`
- `command: 修仙适配`
- `command: 全局广播`
- `command: 全量申请`
- `command: 关闭进群欢迎`
- `alias: 关掉进群欢迎`
- `alias: 禁用进群欢迎`
- `command: 创造力量`
- `command: 取raw`
- `alias: 原始JSON`
- `command: 取reply`
- `alias: 原始reply`
- `alias: 取引用`
- `command: 取消广播`
- `command: 取链接`
- `alias: 提取链接`
- `alias: 获取链接`
- `command: 启用修仙功能`
- `alias: 禁用修仙功能`
- `command: 启用私聊功能`
- `alias: 禁用私聊功能`
- `command: 启用自动宗名`
- `alias: 禁用自动宗名`
- `command: 小黑屋`
- `command: 广播帮助`
- `alias: 广播指令`
- `alias: 广播说明`
- `command: 开启自动灵根`
- `alias: 关闭自动灵根`
- `command: 开启进群欢迎`
- `alias: 启用进群欢迎`
- `alias: 打开进群欢迎`
- `command: 指令列表`
- `command: 指令禁用`
- `command: 指令解禁`
- `command: 按钮测试`
- `command: 易名`
- `command: 查看小黑屋`
- `alias: 小黑屋列表`
- `command: 查看广播`
- `alias: 广播列表`
- `command: 毁灭力量`
- `command: 消息信息`
- `command: 清空仙缘`
- `command: 清空广播`
- `alias: 清除广播`
- `command: 生成秘境`
- `command: 用户伪装`
- `command: 私聊广播`
- `command: 群聊广播`
- `command: 艾特测试`
- `command: 解除小黑屋`
- `alias: 放出小黑屋`
- `alias: 解禁`
- `command: 转换QQID`
- `command: 轮回力量`
- `command: 造化力量`
- `command: 重置世界BOSS`
- `command: 重置历练`
- `command: 重置悬赏令`
- `command: 重置新手礼包`
- `command: 重置状态`
- `command: 重置通天塔`
- `command: 重载items`
