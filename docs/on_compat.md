# NoneBot Matcher 路由兼容层

本文档对应 `nonebot_plugin_xiuxian_2/xiuxian/on_compat.py`。

`on_compat.py` 是本项目对 NoneBot `on_command`、`on_regex`、`on_message` 等 matcher 注册函数的轻量包装。它的核心目标不是改变命令语义，而是在 `COMMAND_START = [""]` 的空前缀环境下减少大量 matcher 的无效规则检查。

## 为什么需要

本项目约有 500 个 `on_command` matcher，并且 README 推荐配置里允许空命令前缀：

```dotenv
COMMAND_START = [""]
```

在空前缀下，任意文本都可能进入 NoneBot 命令匹配流程。如果所有修仙 matcher 都进入后续检查，高频群聊里容易造成明显的 matcher 遍历压力。

`on_compat.py` 会记录本项目注册的 matcher 与命令文本之间的关系，在事件分发阶段按消息内容先做一次索引筛选。

## 对插件行为的影响

正常使用下不改变命令触发结果。

会产生影响的地方主要是性能与内部调度：

- `on_command("修仙帮助")` 仍按 NoneBot 原生命令规则注册。
- `aliases` 仍按 NoneBot 原生别名规则注册。
- `on_message` 仍作为通用 matcher 保留，不会被命令索引误过滤。
- 无法安全提取字面前缀的 `on_regex` 会作为通用 matcher 保留。
- 带明确 `^字面量` 前缀的 `on_regex` 会进入前缀索引，例如 `^灵庄...`、`^悬赏令...`。
- 非修仙模块的 matcher 不进入本兼容层索引，仍按 NoneBot 原流程处理。

如果需要临时关闭兼容路由，可在 NoneBot 配置中设置：

```dotenv
XIUXIAN_ON_COMPAT_GATE=false
```

关闭后，已注册的包装函数仍存在，但 provider 会直接返回原 matcher 列表。

## 主要机制

### 1. 替换 matcher provider

初始化时调用：

```python
matchers.set_provider(XiuxianOnCompatProvider)
```

`XiuxianOnCompatProvider` 包装 NoneBot 原 matcher 列表，并在 `__getitem__(priority)` 时根据当前事件筛选对应优先级下的修仙 matcher。

### 2. 捕获当前事件

模块会包装 NoneBot 事件入口：

```python
nonebot.message.handle_event
nonebot.adapters.onebot.v11.bot.handle_event
nonebot.adapters.onebot.v12.bot.handle_event
nonebot.adapters.qq.bot.handle_event
```

包装函数把当前事件放入 `ContextVar`，provider 在取 matcher 列表时读取该事件。

### 3. 建立路由索引

matcher 注册时会记录 `_RouteMeta`：

- `commands`：命令及别名。
- `prefixes`：可安全前缀匹配的文本。
- `fullmatches`：完整匹配文本。
- `generic`：通用 matcher，不做命令过滤。

索引结构按 priority 分组：

- `_command_index`
- `_prefix_index`
- `_fullmatch_index`
- `_generic_index`
- `_routed_by_priority`

### 4. 事件路由

每条消息事件会提取：

- NoneBot Trie 命令：来自首个文本段。
- 纯文本：来自 `event.get_plaintext()`，失败时 fallback 到 `raw_message`、`plaintext`、`content`。

然后按 priority 返回筛选后的 matcher：

1. 命中命令索引的 matcher。
2. 命中 fullmatch 索引的 matcher。
3. 命中 prefix 索引的 matcher。
4. 当前 priority 下的 generic matcher。
5. 非修仙 matcher 始终保留。

如果修仙索引中没有任何命中项，则过滤掉当前 priority 下已被路由管理的修仙 matcher，避免大量无效检查。

## 导出函数

```python
from .on_compat import (
    on,
    on_message,
    on_command,
    on_shell_command,
    on_regex,
    on_startswith,
    on_fullmatch,
    on_endswith,
    on_keyword,
    install_on_compat,
    rebuild_on_compat_index,
)
```

项目中主要使用：

- `on_command`
- `on_regex`
- `on_message`

其余函数用于兼容 NoneBot 原生 `nonebot.plugin.on` 的常见入口。

## 注册规则

### on_command

```python
matcher = on_command("修仙帮助", aliases={"修仙菜单"}, priority=12, block=True)
```

会同时索引：

- `("修仙帮助",)`
- `("修仙菜单",)`

如果 `cmd` 传入 list 或 set，第一个值作为主命令，其余值合并到别名集合。

### on_regex

```python
matcher = on_regex(r"^悬赏令(查看|刷新)?", priority=10, block=True)
```

如果正则以 `^` 开头，且开头部分能安全提取纯字面量，会加入 prefix 索引。上例会提取 `悬赏令`。

以下情况会回退为通用 matcher：

- 不以 `^` 开头。
- 前缀部分包含无法静态判断的正则结构。
- 启用了 `re.IGNORECASE`。

### on_message

```python
matcher = on_message(priority=999, block=False, rule=some_rule)
```

始终注册为通用 matcher。项目中的兜底消息处理不会被命令索引过滤掉。

## 当前项目使用情况

当前项目内对 `on_compat` 的主要使用规模：

- `on_command`：大量业务命令。
- `on_regex`：`灵庄`、`悬赏令` 两类前缀命令。
- `on_message`：管理模块里的兜底处理。

因此当前实现对业务命令的影响应是“减少无效 matcher 进入规则检查”，而不是改变玩家输入的命令结果。

## 注意事项

1. 新增命令模块时，应继续从 `.on_compat` 或 `..on_compat` 导入 `on_command`，不要直接从 `nonebot` 导入。
2. 如果新增复杂正则，无法安全提取字面前缀时会自动作为通用 matcher 处理，优先保证不误过滤。
3. 如果新增忽略大小写的 `on_startswith`、`on_fullmatch`、`on_regex`，会作为通用 matcher 处理，避免大小写路由不一致。
4. 该模块依赖 NoneBot 内部 matcher provider API；升级 NoneBot 后应重点验证插件加载和命令触发。
5. `rebuild_on_compat_index()` 可在动态增删 matcher 后手动调用，正常插件启动时会自动刷新一次。

## 快速排查

如果怀疑某个命令没有触发：

1. 确认该模块从 `on_compat.py` 导入注册函数。
2. 确认命令文本与 `aliases` 是否正确。
3. 临时设置 `XIUXIAN_ON_COMPAT_GATE=false`，判断问题是否来自路由层。
4. 查看启动日志中的索引数量：`[修仙 on_compat] 已索引...`。
