# 插件目录说明

面向部署与查阅的模块索引（非开发设计文档）。配置入口：`xiuxian_config.py`。

## 基础与运维

| 目录 / 文件 | 说明 |
|:------------|:-----|
| `xiuxian_config.py` | 主配置 |
| `xiuxian_utils` | 通用工具、数据库、路径 |
| `xiuxian_web` | Web 管理面板（含定时任务页） |
| `xiuxian_admin` | 管理员指令 |
| `xiuxian_scheduler` | 定时任务注册与覆盖 |
| `xiuxian_status` | Bot / 系统信息 |
| `xiuxian_compensation` | 补偿 / 礼包 / 兑换码 / 邀请 |
| `xiuxian_adapter` | 适配器 vendor（QQ / OneBot）与选择 |
| `qq_compat` / `adapter_compat` / `messaging` | 跨适配器消息与兼容层 |

## 核心玩法

| 目录 | 说明 |
|:-----|:-----|
| `xiuxian_base` | 签到 / 突破 / 渡劫 / 送灵石 / 送仙缘 |
| `xiuxian_beg` | 新手礼包 / 仙途奇缘 |
| `xiuxian_info` | 修仙信息查询 |
| `xiuxian_back` | 背包 |
| `xiuxian_bank` | 灵庄 |
| `xiuxian_buff` | 双修 / 修炼 / 闭关 / 切磋 / 洞天福地等 |
| `xiuxian_work` | 悬赏令 |
| `xiuxian_map` | 地图探索 |
| `xiuxian_rift` | 秘境（含秘藏令额外进入） |
| `xiuxian_boss` | 世界 BOSS |
| `xiuxian_sect` | 宗门 |
| `xiuxian_trade` | 交易 / 鬼市 / 拍卖 |
| `xiuxian_dongfu` | 洞府 |
| `xiuxian_puppet` | 灵田傀儡（自动收取定时任务） |
| `xiuxian_dungeon` | 组队 / 副本 |
| `xiuxian_mixelixir` | 炼丹 |
| `xiuxian_impart` | 传承 |
| `xiuxian_impart_pk` | 虚神界 |
| `xiuxian_title` | 称号 |
| `xiuxian_tower` | 通天塔 |
| `xiuxian_pet` | 宠物 |
| `xiuxian_fusion` | 合成 |
| `xiuxian_Illusion` | 幻境寻心 |
| `xiuxian_natal_treasure` | 本命法宝 |
| `xiuxian_past_life` | 前尘往事 |
| `xiuxian_tianti` | 炼体 |
| `xiuxian_training` | 历练 |
| `xiuxian_lunhui` | 轮回重修 |
| `xiuxian_world_events` | 世界事件（魔修 / 灵脉等） |
| `xiuxian_activity` / `xiuxian_tasks` | 活动与任务 |

## 娱乐

| 目录 | 说明 |
|:-----|:-----|
| `xiuxian_entertainment` | 娱乐总模块：小游戏、番剧、NewAPI、WebDAV 等 |
| `…/media_parser` | **链接解析**（B 站 / 抖音 / 快手 / 微博 / 小红书 / 小黑盒 / X / TikTok / IG / YouTube 等） |

用户向说明见仓库根目录：

- [README.md](../../../README.md)
- [docs/media_parser.md](../../../docs/media_parser.md)
- [docs/web_panel.md](../../../docs/web_panel.md)
- [docs/gameplay_notes.md](../../../docs/gameplay_notes.md)
