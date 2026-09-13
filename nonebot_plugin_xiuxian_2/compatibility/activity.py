from .legacy_feature import build_service


ActivityService = build_service("activity")


__all__ = ["ActivityService"]
