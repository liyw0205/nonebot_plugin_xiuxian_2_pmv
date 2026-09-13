from .legacy_feature import build_service


TrainingService = build_service("training")


__all__ = ["TrainingService"]
