from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec

_ACTIONS = ("deposit", "withdraw", "enqueue", "dequeue", "session_start", "session_finish", "purchase")
FEATURE = FeatureManifest(
    key="trade", title="交易与拍卖", owner="economy",
    commands=(CommandSpec("交易", aliases=("寄售", "鬼市"), permission="user"),),
    routes=tuple(RouteSpec(f"/api/v1/trade/{action}", methods=("POST",), permission="user") for action in _ACTIONS),
    config=(ConfigSpec("trade_enabled", "bool", default=True, reloadable=True, description="交易资产新事务灰度开关"),),
    migration_version="trade.001", test_tag="trade",
)

__all__ = ["FEATURE"]
