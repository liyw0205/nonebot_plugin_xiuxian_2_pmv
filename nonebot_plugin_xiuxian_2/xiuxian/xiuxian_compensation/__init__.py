"""
补偿 / 礼包 / 兑换码 / 邀请 系统入口

这里只负责导入子模块，让 NoneBot 注册命令。
不要在这里写具体命令逻辑。
"""

from . import compensation
from . import gift_package
from . import redeem_code
from . import invitation