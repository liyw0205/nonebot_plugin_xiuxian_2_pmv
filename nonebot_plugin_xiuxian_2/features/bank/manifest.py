from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="bank",
    title="灵庄资产结算",
    owner="economy",
    commands=(CommandSpec("灵庄", aliases=("灵庄存灵石", "灵庄取灵石", "灵庄升级会员", "灵庄结算"), permission="user"),),
    routes=(
        RouteSpec("/api/v1/bank/deposit", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/bank/withdraw", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/bank/upgrade", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/bank/interest", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/bank/v2/deposit", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("bank_enabled", "bool", default=True, reloadable=True, description="灵庄新资产结算灰度开关"),),
    migration_version="bank.001",
    test_tag="bank",
)

__all__ = ["FEATURE"]
