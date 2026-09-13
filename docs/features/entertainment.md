# Entertainment

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.entertainment.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`entertainment_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

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
