from ...bootstrap.registry import CommandSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="activity_reward",
    title="活动奖励领取",
    owner="gameplay",
    commands=(CommandSpec("活动领取", aliases=("活动一键领取", "领取活动奖励", "活动领奖"), permission="user"),),
    routes=(RouteSpec("/api/v1/activity/rewards/claim", methods=("POST",), permission="user"),),
    migration_version="activity_reward.001",
    test_tag="activity_reward",
)

__all__ = ["FEATURE"]
