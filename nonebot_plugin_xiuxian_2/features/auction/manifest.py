from ...bootstrap.registry import CommandSpec, FeatureManifest, JobSpec, RouteSpec


FEATURE = FeatureManifest(
    key="auction",
    title="拍卖竞价",
    owner="gameplay",
    commands=(CommandSpec("拍卖竞拍", aliases=("竞拍",), permission="user"),),
    routes=(
        RouteSpec("/api/v1/auction/bids", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/auction/settle", methods=("POST",), permission="admin"),
    ),
    jobs=(
        JobSpec(
            id="auction.settle",
            title="拍卖场次收尾结算",
            owner="gameplay",
            schedule="on_demand",
            timeout=120,
            retry_policy="retry:1",
            idempotency_key="auction.settle:{scheduled_at}",
        ),
    ),
    migration_version="auction.004",
    test_tag="auction",
)

__all__ = ["FEATURE"]
