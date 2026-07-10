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
| `xiuxian_admin/` | 管理员 Adapter/Markdown 诊断命令 | 诊断 Presenter 迁移后 |
| `xiuxian_back/accessory.py` | 旧 Markdown 响应 | Presenter 迁移后 |
| `xiuxian_boss/__init__.py` | 旧图片与管理响应 | Presenter 迁移后 |
| `xiuxian_entertainment/` | 有界媒体发送运行时 | Q6 媒体门面接入后 |
| `xiuxian_pet/__init__.py` | 旧 Markdown 响应 | Presenter 迁移后 |
| `xiuxian_sect/__init__.py` | 旧 Markdown 响应 | Presenter 迁移后 |
| `xiuxian_utils/lay_out.py` | Matcher 前置提示 | 入口提示迁移后 |
| `xiuxian/__init__.py` | 全局过载提示 | 入口提示迁移后 |

源码质量测试会阻止新的直接发送文件进入该清单之外。每迁移一个路径，应同时缩小
测试允许集合和本表。
