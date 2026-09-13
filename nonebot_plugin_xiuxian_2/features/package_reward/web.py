"""礼包开启没有独立 Web 页面；管理面板只展示 operation ledger。"""

from typing import Any


def blueprint(application: Any, *, permission=None):
    return None


__all__ = ["blueprint"]
