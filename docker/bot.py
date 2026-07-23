"""修仙2 Docker 入口 bot。"""
import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
from nonebot.adapters.qq import Adapter as QQAdapter

nonebot.init()

driver = nonebot.get_driver()
driver.register_adapter(OneBotV11Adapter)
driver.register_adapter(QQAdapter)

# 由 pyproject.toml / 环境变量的 plugin_dirs 发现 src/plugins
nonebot.load_from_toml("pyproject.toml")

if __name__ == "__main__":
    nonebot.run()
