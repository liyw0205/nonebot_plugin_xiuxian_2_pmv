from .legacy_feature import build_service


SimulatorService = build_service("simulator")


__all__ = ["SimulatorService"]
