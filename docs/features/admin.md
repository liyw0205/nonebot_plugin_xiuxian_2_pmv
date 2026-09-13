# Administration

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.admin.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

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
