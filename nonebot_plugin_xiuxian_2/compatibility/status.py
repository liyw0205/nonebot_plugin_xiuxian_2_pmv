from .legacy_feature import build_service


StatusService = build_service("status")


__all__ = ["StatusService"]
