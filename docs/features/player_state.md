# Player State

## 用户流程
这是供其他玩法调用的内部能力，不直接接收玩家请求。调用方通过 `PlayerStateApplication` 初始化空的 HP/MP/ATK，或写回战斗后的 HP/MP。

## 命令与别名
无。入口由历练、突破、通天塔、世界事件、BOSS 和战斗兼容层调用。

## Web API
无独立路由；调用方负责其自身的 Web API 与权限边界。

## 数据模型与迁移
读取和更新既有 player DB 的 `user_xiuxian` 表。此 feature 不拥有表结构，也不声明迁移；缺表或缺列时只允许显式兼容回退，不执行请求期 DDL。

## 事务与失败回滚
`PlayerStateRepository` 在单个 SQLite Unit of Work 中校验 schema，并按首个匹配 `rowid` 更新。初始化使用空 HP 与经验值快照做 CAS；生命值写回可选择 HP/MP CAS。仓储失败不创建 schema。

## 定时任务
无。

## 配置项
无。

## 适配器差异
NoneBot 旧模块仅负责准备调用参数，并可显式传入 schema 缺失时的兼容回退；该 feature 不依赖 NoneBot 或 Flask。

## 测试与手工验收
`features/player_state/tests/test_player_state_repository.py` 覆盖重复用户行、空状态初始化、既有状态保护、CAS 和缺 schema 回退。`tests/test_player_fight_lazy_writer.py` 覆盖战斗写入边界。
