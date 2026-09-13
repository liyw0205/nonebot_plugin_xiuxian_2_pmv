from .legacy_feature import build_service


AdminService = build_service("admin")


__all__ = ["AdminService"]
