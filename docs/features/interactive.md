# Interactive rewards

## 用户流程
The compatibility command remains available as an import and command facade while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `interactive.001` creates the four interactive projections in `game_db`; historical table names remain readable for compatibility.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger in the same transaction as the reward projection. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`interactive_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
The old service import path remains for one complete release cycle; the compatibility hit counter determines when it is safe to remove the facade.

## Manifest 清单
- `command: 互动`
- `command: 今天吃什么`
- `alias: 吃什么`
- `alias: 吃啥`
- `alias: 推荐美食`
- `alias: 美食推荐`
- `command: 休息`
- `alias: relax`
- `alias: 休憩`
- `alias: 小憩`
- `alias: 放松`
- `alias: 歇会`
- `command: 你好`
- `alias: hello`
- `alias: 你好呀`
- `alias: 哈喽`
- `alias: 嗨`
- `alias: 道友你好`
- `command: 你好吗`
- `alias: howareyou`
- `alias: 别来无恙`
- `alias: 怎么样`
- `alias: 最近怎么样`
- `alias: 近来可好`
- `command: 再见`
- `alias: bye`
- `alias: goodbye`
- `alias: 再会`
- `alias: 后会有期`
- `alias: 告辞`
- `alias: 拜拜`
- `command: 加油`
- `alias: fighting`
- `alias: 冲鸭`
- `alias: 加把劲`
- `alias: 努力`
- `alias: 鼓励`
- `command: 可爱`
- `alias: 卡哇伊`
- `alias: 可爱捏`
- `alias: 好可爱`
- `alias: 萌`
- `alias: 萌萌哒`
- `command: 吃饭`
- `alias: 干饭`
- `alias: 用膳`
- `alias: 进食`
- `alias: 饿了`
- `command: 土味情话`
- `alias: 土味`
- `alias: 情话`
- `alias: 表白`
- `alias: 说情话`
- `command: 天气`
- `alias: 今天天气`
- `alias: 天气预报`
- `alias: 天象`
- `alias: 气象`
- `command: 学习`
- `alias: study`
- `alias: 修行`
- `alias: 用功`
- `alias: 练功`
- `command: 工作`
- `alias: work`
- `alias: 上班`
- `alias: 劳作`
- `alias: 当值`
- `alias: 打工`
- `command: 早安`
- `alias: 早`
- `alias: 早上好`
- `alias: 早啊`
- `alias: 晨安`
- `alias: 道友早`
- `command: 时间`
- `alias: 什么时辰了`
- `alias: 几点了`
- `alias: 几点钟`
- `alias: 当前时间`
- `alias: 时辰`
- `alias: 现在几点`
- `command: 晚安`
- `alias: 安寝`
- `alias: 晚安啦`
- `alias: 睡了`
- `alias: 睡觉`
- `alias: 道友晚安`
- `command: 给点修为`
- `alias: 施舍点修为`
- `alias: 求点修为`
- `alias: 赏点修为`
- `command: 给点灵石`
- `alias: 施舍点灵石`
- `alias: 求点灵石`
- `alias: 赏点灵石`
- `command: 讲个段子`
- `alias: 幽默`
- `alias: 来段搞笑的`
- `alias: 段子`
- `alias: 趣事`
- `command: 讲个笑话`
- `alias: joke`
- `alias: 搞笑`
- `alias: 来点乐子`
- `alias: 笑话`
- `alias: 逗我笑`
- `command: 谢谢`
- `alias: thx`
- `alias: 多谢`
- `alias: 感恩`
- `alias: 感谢`
- `alias: 谢啦`
