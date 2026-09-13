from .legacy_feature import build_service


TasksService = build_service("tasks")


__all__ = ["TasksService"]
