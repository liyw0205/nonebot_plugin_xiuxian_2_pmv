# 消息发送通道约定

玩家回文分两路，**规则不同**，不要混用。
QQ 支持的 Markdown 语法（表格、代码块、公式、行内命令等）以
[`docs/qq-markdown-reference.md`](qq-markdown-reference.md) 为准；判定“能不能用”先查那份清单，
不要凭印象否决。

## 通道对照

| 通道 | 入口 | 适用 | Markdown / `>` | emoji |
|------|------|------|----------------|-------|
| 迁移后的功能命令 | `ReplyPlan.intent="markdown"` → `NoneBotMessageGateway` → `reply_enhanced` | 状态、结果、日常、签到 | 可用 `**`、`表格`、`---`、手写 `>` | 可用状态 emoji |
| 真 MD 卡 | `handle_send` / `handle_send_md` | 状态、结果、日常、签到 | 可用 `**`、`---`、手写 `>` | 可用状态 emoji |
| 代码框 / shell | `send_msg_handler` | 列表、背包、帮助列表 | **不解析** `>`，不要靠 `>` 缩小 | 可有 emoji，但别指望排版 |

迁移切片只在 `ReplyPlan.intent="markdown"` 时才走 Markdown 投递：非 QQ Bot 或缺少 Markdown 能力
会按能力门控回退同一份文案的纯文本；构造或发送失败会记录 warning 后回退。媒体段与默认
`plain` 计划仍走 `reply`。

配置：`XiuConfig().markdown_status` 仅运行配置，不入库。

## 文案规范

1. 修仙游戏风；**只改说明，不改指令名**。
2. 娱乐帮助 / 管理帮助 **不改**。
3. 真 MD：手写 `**` / `---` / `>`；标题语法是**支持**的，但正文约定用 `**短标题**` 控字号，
   这是排版选择而非能力限制；**禁止自动全局插 `>`**。
4. 查看效果：ID 后整段 `>`；物品列表纯文本（代码框）。
5. 结构（日历、属性对比）用真实 Markdown 表格，不用空格补齐列；`---` 前后留空行。
6. 状态 emoji 约定：
   - ✅ 完成 / 成功
   - ❗ 可做 / 待办
   - ⏳ 冷却
   - 🔄 进行中
   - ❌ 失败 / 不足
   - ⬜ 未开启
   - 🌱 成熟
   - 👑 至高

## 代码

- 公共 helper：`xiuxian_utils/status_card.py`（`prefix_status` / `md_title_card` / `md_kv_lines`）
- 投递门面：`messaging/delivery.py`（`allow_plain_fallback` 等媒体策略）
- 平台专有发送只允许存在于适配器后端、合并转发、撤回和 interaction ACK 等明确边界

## handle_send 按钮

业务回文尽量带 `md_type` 与 `k1/v1`（及 k2/v2…），避免空按钮。

推荐用 `status_card.nav_kwargs("work")` / `daily_action_buttons` 生成情境导航，避免各处复制粘贴。

## 结果卡

`status_card.result_card(title, kind=..., summary=..., pairs=...)`  
冷却：`cooldown_msg` / 文案前缀 `⏳`  
媒体失败：`media_fail_hint`（不塞修仙按钮）
