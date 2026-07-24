# 消息发送通道约定

玩家回文分两路，**规则不同**，不要混用。

## 通道对照

| 通道 | 入口 | 适用 | Markdown / `>` | emoji |
|------|------|------|----------------|-------|
| 真 MD 卡 | `handle_send` / `handle_send_md` | 状态、结果、日常、签到 | 可用 `**`、`---`、手写 `>` | 可用状态 emoji |
| 代码框 / shell | `send_msg_handler` | 列表、背包、帮助列表 | **不解析** `>`，不要靠 `>` 缩小 | 可有 emoji，但别指望排版 |

配置：`XiuConfig().markdown_status` 仅运行配置，不入库。

## 文案规范

1. 修仙游戏风；**只改说明，不改指令名**。
2. 娱乐帮助 / 管理帮助 **不改**。
3. 真 MD：手写 `**` / `---` / `>`；**禁止 `#` 标题**；**禁止自动全局插 `>`**。
4. 查看效果：ID 后整段 `>`；物品列表纯文本（代码框）。
5. 状态 emoji 约定：
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
- 迁移清单：`docs/message_delivery_migration.md`

## handle_send 按钮

业务回文尽量带 `md_type` 与 `k1/v1`（及 k2/v2…），避免空按钮。

推荐用 `status_card.nav_kwargs("work")` / `daily_action_buttons` 生成情境导航，避免各处复制粘贴。

## 结果卡

`status_card.result_card(title, kind=..., summary=..., pairs=...)`  
冷却：`cooldown_msg` / 文案前缀 `⏳`  
媒体失败：`media_fail_hint`（不塞修仙按钮）
