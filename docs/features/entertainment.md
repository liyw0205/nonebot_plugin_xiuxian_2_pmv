# Entertainment

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.entertainment.001` records the original feature slice. `legacy.entertainment.002` creates `entertainment_game_rooms` and imports the Gomoku, half-ten and Minesweeper room JSON snapshots once at startup. `legacy.entertainment.003` creates `entertainment_newapi_accounts` and `entertainment_newapi_checkin_history`, importing the existing NewAPI account/history JSON files once. NewAPI import is capped at 4096 files and 64 MiB total; account/history files retain their respective 1 MiB/64 KiB per-file caps. Both imports record a SHA-256 receipt and counts, run transactionally, and retain source JSON. Recognized invalid NewAPI state aborts migration rather than creating a partial receipt.

## 事务与失败回滚
NewAPI bind, delete and auto-check-in toggle mutate SQL state and finish their operation-ledger receipt in one immediate transaction. A legacy `started` operation with unknown outcome is rejected for manual state inspection rather than blindly replayed. Check-in history uses a separate bounded immediate transaction and retains three rows per QQ. Room snapshots instead use type-scoped SQL upserts/deletes in individual immediate transactions; they do not have event-level operation-ID replay receipts. Repository schemas are checked on first use, not on every command. Restore the pre-migration game database backup to roll back; the original JSON snapshots remain available.

## 定时任务
The existing NewAPI daily auto-check-in job remains registered through the compatibility scheduler; it reads feature-owned SQL account snapshots in pages of at most 32. Existing game timeout tasks are not rearmed from restored room snapshots after a process restart.

## 配置项
`entertainment_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the entertainment repository/manager and NewAPI client/owner contract tests, then the phase2 frozen-evidence gate. Operation-ID replay checks apply to NewAPI bind/delete/toggle, not room snapshots or history append.

## 灰度开关、回滚和已知限制
Room handlers still call synchronous SQLite repository methods on the legacy event path, so database writes can briefly occupy the event loop. NewAPI database operations are offloaded from command handlers, but the scheduled job performs selected remote check-ins sequentially; its total duration depends on the number of enabled accounts and remote site latency. Legacy algorithms remain behind the compatibility adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 60S读世界`
- `alias: 60秒读世界`
- `alias: 每日60S`
- `command: Steam喜加一`
- `alias: 喜加一`
- `command: newapi信息`
- `command: newapi删除`
- `alias: newapi解绑`
- `command: newapi帮助`
- `alias: newapi`
- `command: newapi查看`
- `alias: newapi列表`
- `alias: newapi绑定列表`
- `command: newapi签到`
- `command: newapi签到历史`
- `alias: newapi签到记录`
- `command: newapi绑定`
- `command: newapi自动签到`
- `command: webdav信息`
- `alias: 网盘信息`
- `command: webdav列表`
- `alias: 网盘列表`
- `command: webdav删除`
- `alias: webdav解绑`
- `alias: 网盘删除`
- `alias: 网盘解绑`
- `command: webdav帮助`
- `alias: 网盘帮助`
- `command: webdav文件`
- `alias: 网盘文件`
- `command: webdav查看`
- `alias: 网盘查看`
- `command: webdav绑定`
- `alias: 网盘绑定`
- `command: webdav链接`
- `alias: 网盘链接`
- `command: 二次元帮助`
- `alias: 动漫互动帮助`
- `alias: 随机二次元帮助`
- `command: 五子棋帮助`
- `command: 今日番剧`
- `alias: 每日番剧`
- `alias: 番剧日历`
- `command: 今日老婆`
- `command: 今日超能力`
- `command: 加入五子棋`
- `command: 加入十点半`
- `command: 动漫互动`
- `command: 十点半信息`
- `command: 十点半帮助`
- `command: 历史上的今天`
- `command: 哈基米`
- `alias: 哈基米音乐`
- `alias: 随机哈基米`
- `alias: 随机哈基米音乐`
- `command: 娱乐帮助`
- `alias: 娱乐功能`
- `alias: 娱乐菜单`
- `command: 宝可梦图鉴`
- `alias: 宝可梦`
- `alias: 查宝可梦`
- `alias: 精灵图鉴`
- `command: 宝可梦帮助`
- `alias: 宝可梦盲盒帮助`
- `command: 宝可梦盲盒`
- `alias: 今日宝可梦`
- `alias: 随机宝可梦`
- `command: 小游戏帮助`
- `alias: 小游戏菜单`
- `alias: 游戏帮助`
- `alias: 游戏菜单`
- `command: 开始五子棋`
- `command: 开始十点半`
- `command: 开始单人五子棋`
- `command: 开始扫雷`
- `command: 开始猜数字`
- `command: 开始猜数谜`
- `alias: 开始猜数迷`
- `command: 弱智吧问答`
- `alias: 弱智吧`
- `command: 扫雷信息`
- `command: 扫雷帮助`
- `command: 搞笑段子`
- `alias: 随机段子`
- `command: 摸鱼日报`
- `alias: 今日摸鱼`
- `alias: 摸鱼人日历`
- `alias: 摸鱼日历`
- `command: 标记`
- `command: 棋局信息`
- `command: 每日60S图片`
- `alias: 60S图片`
- `command: 每日Bing图`
- `command: 点歌`
- `alias: 5sing原创`
- `alias: 5sing翻唱`
- `alias: QQ点歌`
- `alias: 一听点歌`
- `alias: 全民K歌`
- `alias: 咪咕点歌`
- `alias: 喜马点歌`
- `alias: 百度点歌`
- `alias: 网易云点歌`
- `alias: 网易点歌`
- `alias: 荔枝点歌`
- `alias: 蜻蜓点歌`
- `alias: 酷我点歌`
- `alias: 酷狗点歌`
- `command: 点歌帮助`
- `alias: 点歌说明`
- `alias: 音乐帮助`
- `command: 点歌翻页`
- `alias: 点歌上一页`
- `alias: 点歌下一页`
- `command: 点歌配置`
- `alias: 音乐配置`
- `command: 热榜图片`
- `alias: 微博热榜图片`
- `alias: 百度热榜图片`
- `command: 猜`
- `alias: 猜数字`
- `command: 猜数字信息`
- `command: 猜数字帮助`
- `command: 猜数谜`
- `alias: 猜数迷`
- `command: 猜数谜帮助`
- `alias: 猜数迷帮助`
- `command: 猫猫帮助`
- `alias: 猫图帮助`
- `alias: 猫猫说帮助`
- `command: 猫猫说`
- `alias: 猫猫说话`
- `alias: 猫说`
- `command: 番剧周表`
- `alias: 每周番剧`
- `alias: 番剧总表`
- `command: 番剧盲盒`
- `alias: 动漫盲盒`
- `alias: 随机动漫`
- `alias: 随机番剧`
- `command: 番剧盲盒帮助`
- `alias: 随机番剧帮助`
- `command: 答案之书`
- `alias: 答案书`
- `alias: 问答案之书`
- `alias: 问答案书`
- `command: 结束扫雷`
- `command: 结束猜数字`
- `command: 结算十点半`
- `command: 翻开`
- `command: 肯德基文案`
- `alias: KFC文案`
- `alias: 疯狂星期四`
- `command: 脑筋急转弯`
- `command: 舔狗日记`
- `command: 落子`
- `command: 认输`
- `command: 退出五子棋`
- `command: 退出十点半`
- `command: 选歌`
- `alias: 播放歌曲`
- `alias: 点歌选择`
- `command: 链接解析`
- `alias: 流媒体解析`
- `alias: 视频解析`
- `alias: 解析视频`
- `alias: 解析链接`
- `command: 随机一言`
- `command: 随机二次元`
- `alias: 二次元图`
- `alias: 猫娘图`
- `alias: 随机狐娘`
- `alias: 随机猫娘`
- `alias: 随机老公`
- `alias: 随机老婆`
- `command: 随机小姐姐`
- `alias: 小姐姐`
- `alias: 随机美女视频`
- `command: 随机点歌`
- `command: 随机猫猫`
- `alias: 猫图`
- `alias: 猫猫`
- `alias: 随机猫图`
- `command: 随机语音`
- `alias: 御姐语音`
- `alias: 怼人语音`
- `alias: 绿茶语音`
- `alias: 语音`
- `alias: 随机御姐撒娇语音`
- `alias: 随机绿茶语音`
