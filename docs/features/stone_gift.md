# 灵石赠送

## 用户流程

用户通过 `送灵石 道号 数量` 转移灵石。发送方按总额扣除，接收方获得扣除手续费后的净额；发送方和接收方不能相同，双方资产必须在同一事务中更新。

## 命令与别名

- `送灵石`

旧 NoneBot handler 保留道号/@解析、等级日限额和消息文案；资产转移通过 `compatibility/stone_gift.py` 转发到新应用。

## Web API

`POST /api/v1/stone-gift`，权限为 `user`，写请求需要 CSRF。请求体字段：`sender_id`、`recipient_id`、`gross_amount`，可选 `fee_rate`、`transfer_date`、`send_limit`、`receive_limit`；推荐通过 `Idempotency-Key` 指定操作号。响应使用统一 API envelope，并返回 `consumed`、`granted`、`before`、`after` 和审计类别。

## 数据模型与迁移

- `user_xiuxian.stone`：玩家资产表。
- `stone_gift_operations`：赠送操作投影，保存总额、净额和手续费。
- `stone_gift_limits`：按 UTC 日期记录用户发送/接收额度，和资产变更在同一事务中更新；旧命令首次触发当天操作时会把 `player.db` 的旧计数作为一次性基线导入。
- `operation_ledger`、`operation_audit`：统一幂等与审计流水。
- 迁移版本：`stone_gift.001`、`stone_gift.002`。

## 事务与失败回滚

应用层在同一 `DatabaseUnitOfWork` 中登记操作、检查接收方、检查每日额度、扣除发送方、增加接收方、递增额度和写操作投影。余额不足、额度不足、接收方不存在、参与方变化均不改变资产；任一投影写入异常会回滚双方余额和额度，并保留失败流水供对账。

## 定时任务

本切片没有新增任务。新额度按日期分区，不需要清空历史；旧 `stone_limit` 的重置任务仍保留用于仙缘兼容投影。

## 配置项

- `XIUXIAN_STONE_GIFT_ENABLED`：新实现灰度开关，默认开启。
- `stone_gift_fee_rate`（`XIUXIAN_STONE_GIFT_FEE_RATE`）：手续费率，默认 `0.1`。

## 适配器差异

领域和 application 不依赖 NoneBot、Flask 或数据库驱动。命令适配器负责解析消息和日限额提示并将等级计算出的额度传入用例；Web 适配器负责权限、CSRF、DTO 和 JSON 序列化。

## 测试与手工验收

服务测试覆盖成功、重复请求、业务拒绝和异常回滚；Web 测试覆盖 CSRF、成功和重放。隔离实例中先获取 CSRF token，再使用同一 `Idempotency-Key` 重试请求，第二次必须返回 `replayed` 且余额不再变化。

## 灰度开关、回滚和已知限制

关闭 `XIUXIAN_STONE_GIFT_ENABLED` 后旧交易服务继续提供兼容行为。`stone_gift` 的赠送/接收额度已迁入主库，历史 `player.db` 计数通过兼容入口按用户/日期一次性种入；仙缘次数仍属于旧 `stone_limit` 投影，需在仙缘切片迁移后才能删除该兼容模块。
