# 目标：活动中心消息跨适配器投递合同（activity center delivery contract）

状态：develop 已实施（聚焦测试 `tests/test_activity_center_delivery_contract.py`
5/5 与受影响 `tests/test_activity_center.py` 7/7 全绿；生产代码零改动，
仅按原生通道既有约定修正一处测试期望）。

冻结清单第 4–5 条（修正不适用入口、短消息呈现、QQ/OneBot 既有消息能力
与降级合同）的独立验证切片。

## 范围（单一用户价值）

`活动中心` 命令生成的同一条只读卡片文本，必须在三种真实投递前提下得到
确定且正确的呈现，玩家在任何适配器上都不会收到裸 Markdown 语法或
mqqapi 蓝字 URL 残留：

1. OneBot V11 bot：强制纯文本降级，收到 `strip_md_command_links` 后的
   文本（无 `**`、无 `mqqapi://`）。
2. QQ 且 markdown 开关关闭：走同一纯文本降级合同。
3. QQ 且 markdown 开启、普通群/私聊场景：走原生 Markdown 通道，
   蓝字命令链接 `mqqapi://aio/inlinecmd` 原样保留。

验证方式与冻结条款一致：OneBot 共享业务链只验证一次；QQ 只验证
格式/蓝字/降级合同，不发送真实消息，不触碰适配器传输层
（传输矩阵已有 tests/test_qq_adapter_contracts.py 覆盖）。

## 验收标准

1. 新聚焦测试 `tests/test_activity_center_delivery_contract.py`：
   - 文本源为真实 `ActivityReadModelApplication.activity_center_text()`
     输出（配置驱动，不手写样例文本）。
   - OneBot bot（`adapter.get_name()=="OneBot V11"`）调用共享入口
     `handle_send(..., native_markdown=True, fallback_msg=strip...)`
     后，终端纯文本发送收到 handler 同款降级文本，且不含 `**` 与
     `mqqapi`。
   - QQ markdown 关闭 → 同上纯文本合同。
   - QQ markdown 开启 + group 场景 → 原生 Markdown 通道收到未剥链接的
     原文；按既有 `handle_send_native_markdown` 约定换行归一为 `\r`，
     `[标签](mqqapi://aio/inlinecmd...)` 原样保留。
   - 降级文本幂等：`strip(strip(text)) == strip(text)`。
2. handler 接线源合同：`xiuxian_activity/__init__.py` 中活动中心
   handler 必须以 `native_markdown=True` 且
   `fallback_msg=strip_md_command_links(text)` 调用共享发送入口。
3. 如测试暴露真实缺陷，最小修复共享入口或 handler；否则生产代码零改动。
4. 只跑新增文件与 `tests/test_activity_center.py`（受影响模块），不跑全量。

## 排除

- 不发送真实 QQ/OneBot 消息，不连接任何平台账号。
- 不改适配器传输层、消息网关、按钮键盘渲染实现。
- 不新增配置键、迁移、奖励发放或 UI 框架。
- 不部署 /root/xiu2，不移动 tag，不发 Release。

## 停止条件

- 合同测试暴露需要改动传输层或适配器合同矩阵才能通过 → 停止并报告，
  不扩大冻结范围。
- 若 `handle_send` 决策缝需要重构才能聚焦测试 → 停止并报告，
  不在本切片内做通用消息层重构。

## 实施结果

- 5 项合同测试全部通过；`handle_send` 决策缝无需重构，未触发停止条件。
- 生产代码零改动：OneBot V11 强制 `markdown_status=False` → 纯文本降级，
  QQ markdown 关闭走同一分支，QQ markdown 开启走
  `handle_send_native_markdown`，三条路径均与目标一致。
- 唯一修正为测试期望：原生通道按既有约定把 `\n` 归一为 `\r`，
  断言改为 `text.replace("\n", "\r")` 并显式断言 `](mqqapi` 未被剥离，
  未改动 `utils.py` 或 handler。
- handler 接线源合同（`native_markdown=True` +
  `fallback_msg=strip_md_command_links(text)`）在
  `xiuxian_activity/__init__.py` 中已成立。
