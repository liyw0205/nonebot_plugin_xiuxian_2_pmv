# 开发与交付

## 本地验收

每个独立改动在提交前执行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q nonebot_plugin_xiuxian_2 tests
git diff --check
```

涉及 QQ Adapter 时额外执行：

```sh
python -m unittest tests.test_qq_adapter_contracts -v
```

## 提交边界

- 一个提交只完成一个可验证目标，并同步更新相关开发文档和测试。
- 不提交运行数据库、缓存、日志、备份文件、Bot token、secret、用户 ID 或群 ID。
- 运行时数据统一通过 `XiuxianPaths`，主动发送统一通过 `MessageDeliveryService`。
- 可变 JSON 状态使用中央 JSON Store；协议专用网络客户端需在代码或路线文档说明理由。
- 修改玩法数值、经济资产或权限边界时必须有失败回滚、重试或幂等测试。

## 发布

`main` 分支推送会运行 Quality 工作流。发布工作流使用当前提交生成源码归档，不应包含本地
部署配置或测试凭据。真实 Bot 冒烟测试只读取 Git 忽略的本地配置，不打印任何凭据或标识。
