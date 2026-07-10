# 消息投递迁移清单

新增业务发送必须使用 `messaging.delivery_service`。以下直接 Adapter 调用属于迁移期
兼容路径或平台专有能力，不得扩大文件范围：

| 路径 | 允许原因 | 删除条件 |
| --- | --- | --- |
| `adapter_compat.py` | Adapter patch、引用段和底层 QQ endpoint | 兼容层接口替换后 |
| `adapter_message_sender.py` | 投递门面的主动发送后端 | sender 合并入门面后 |
| `messaging/delivery.py` | 唯一投递门面实现 | 保留 |
| `xiuxian_utils/utils.py` | OneBot 合并转发专用 API | 建立 forward 专用门面后 |
| `broadcast_manager.py` | OneBot 合并转发和普通广播专用 API | 建立 forward 专用门面后 |
| `adapter_message_actions.py` | 撤回专用 endpoint | Adapter 提供统一撤回接口后 |
| `qq_compat/interaction.py` | QQ interaction ACK 专用 endpoint | QQ Adapter 契约统一后 |

普通回复、主动群发、主动私聊和媒体响应已统一进入 `MessageDeliveryService`。源码质量测试
只允许投递后端和 Adapter compatibility 实现直接调用发送方法；合并转发、广播、撤回和
interaction ACK 保持明确的平台专用门面。
