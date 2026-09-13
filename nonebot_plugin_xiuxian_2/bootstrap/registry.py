from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass(frozen=True)
class CommandSpec:
    name: str
    aliases: tuple[str, ...] = ()
    permission: str | None = None

    def __post_init__(self) -> None:
        if not str(self.name).strip():
            raise ValueError("command name is required")
        if any(not str(alias).strip() for alias in self.aliases):
            raise ValueError(f"command aliases must not be empty: {self.name}")
        if not self.permission or not str(self.permission).strip():
            raise ValueError(f"command permission is required: {self.name}")


@dataclass(frozen=True)
class RouteSpec:
    path: str
    methods: tuple[str, ...] = ("GET",)
    permission: str | None = None
    endpoint: str | None = None

    def __post_init__(self) -> None:
        if not str(self.path).startswith("/"):
            raise ValueError(f"route path must start with '/': {self.path}")
        if not self.methods or any(not str(method).strip() for method in self.methods):
            raise ValueError(f"route methods are required: {self.path}")
        if not self.permission or not str(self.permission).strip():
            raise ValueError(f"route permission is required: {self.path}")
        normalized = tuple(str(method).upper() for method in self.methods)
        if len(normalized) != len(set(normalized)):
            raise ValueError(f"duplicate route methods: {self.path}")


@dataclass(frozen=True)
class JobSpec:
    id: str
    title: str
    owner: str
    schedule: str
    misfire_policy: str = "coalesce"
    concurrency_policy: str = "skip"
    timeout: float = 60
    retry_policy: str = "none"
    idempotency_key: str = "{job_id}:{scheduled_at}"

    def __post_init__(self) -> None:
        for field_name in ("id", "title", "owner", "schedule", "idempotency_key"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"job {field_name} is required")
        if self.timeout <= 0:
            raise ValueError("job timeout must be positive")


@dataclass(frozen=True)
class ConfigSpec:
    name: str
    type: str
    default: Any = None
    secret: bool = False
    reloadable: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        if not str(self.name).strip() or not str(self.type).strip():
            raise ValueError("config name and type are required")


@dataclass(frozen=True)
class FeatureManifest:
    key: str
    title: str
    owner: str
    commands: tuple[CommandSpec, ...] = ()
    routes: tuple[RouteSpec, ...] = ()
    jobs: tuple[JobSpec, ...] = ()
    config: tuple[ConfigSpec, ...] = ()
    migration_version: str | None = None
    test_tag: str | None = None
    compatibility: bool = False
    migration_target: str | None = None
    removal_release: str | None = None

    def __post_init__(self) -> None:
        if not self.key.strip() or not self.title.strip() or not self.owner.strip():
            raise ValueError("feature key, title and owner are required")
        if not self.test_tag:
            raise ValueError(f"feature {self.key} must declare a test tag")
        if self.migration_version is not None and not self.migration_version.strip():
            raise ValueError(f"feature {self.key} has an empty migration version")
        if self.compatibility and (not self.migration_target or not self.removal_release):
            raise ValueError(f"compatibility feature {self.key} must declare migration target and removal release")


class ManifestError(ValueError):
    pass


class FeatureRegistry:
    """Collect and validate all feature declarations before wiring adapters."""

    def __init__(self) -> None:
        self._features: dict[str, FeatureManifest] = {}

    @property
    def features(self) -> tuple[FeatureManifest, ...]:
        return tuple(self._features.values())

    def command_index(self) -> dict[str, tuple[str, str]]:
        self.validate()
        return {
            name.strip().casefold(): (feature.key, command.permission or "")
            for feature in self.features
            for command in feature.commands
            for name in (command.name, *command.aliases)
        }

    def route_index(self) -> dict[tuple[str, str], tuple[str, str]]:
        self.validate()
        return {
            (route.path, method.upper()): (feature.key, route.permission or "")
            for feature in self.features
            for route in feature.routes
            for method in route.methods
        }

    def job_index(self) -> dict[str, tuple[str, str]]:
        self.validate()
        return {job.id: (feature.key, job.owner) for feature in self.features for job in feature.jobs}

    def register(self, manifest: FeatureManifest) -> FeatureManifest:
        if manifest.key in self._features:
            raise ManifestError(f"duplicate feature key: {manifest.key}")
        self._features[manifest.key] = manifest
        try:
            self.validate()
        except Exception:
            self._features.pop(manifest.key, None)
            raise
        return manifest

    def register_many(self, manifests: Iterable[FeatureManifest]) -> None:
        for manifest in manifests:
            self.register(manifest)

    def validate(self) -> None:
        commands: dict[str, str] = {}
        routes: dict[tuple[str, str], str] = {}
        jobs: dict[str, str] = {}
        configs: dict[str, str] = {}
        migrations: dict[str, str] = {}
        for feature in self.features:
            for command in feature.commands:
                for name in (command.name, *command.aliases):
                    normalized = name.strip().casefold()
                    owner = commands.get(normalized)
                    if owner:
                        raise ManifestError(f"duplicate command {name!r}: {owner}, {feature.key}")
                    commands[normalized] = feature.key
            for route in feature.routes:
                for method in route.methods:
                    route_key = (route.path, method.upper())
                    owner = routes.get(route_key)
                    if owner:
                        raise ManifestError(f"duplicate route {route_key}: {owner}, {feature.key}")
                    routes[route_key] = feature.key
            for job in feature.jobs:
                owner = jobs.get(job.id)
                if owner:
                    raise ManifestError(f"duplicate job id {job.id}: {owner}, {feature.key}")
                jobs[job.id] = feature.key
            for config in feature.config:
                owner = configs.get(config.name)
                if owner:
                    raise ManifestError(f"duplicate config {config.name}: {owner}, {feature.key}")
                configs[config.name] = feature.key
            if feature.migration_version:
                previous = migrations.get(feature.key)
                if previous and feature.migration_version < previous:
                    raise ManifestError(f"migration version moved backwards: {feature.key}")
                migrations[feature.key] = feature.migration_version

    def export(self) -> dict[str, Any]:
        self.validate()
        return {
            "features": [
                {
                    "key": feature.key,
                    "title": feature.title,
                    "owner": feature.owner,
                    "commands": [
                        {"name": item.name, "aliases": list(item.aliases), "permission": item.permission}
                        for item in feature.commands
                    ],
                    "routes": [
                        {
                            "path": item.path,
                            "methods": [method.upper() for method in item.methods],
                            "permission": item.permission,
                            "endpoint": item.endpoint,
                        }
                        for item in feature.routes
                    ],
                    "jobs": [item.__dict__ for item in feature.jobs],
                    "config": [
                        {
                            "name": item.name,
                            "type": item.type,
                            "default": "***" if item.secret else item.default,
                            "secret": item.secret,
                            "reloadable": item.reloadable,
                            "description": item.description,
                        }
                        for item in feature.config
                    ],
                    "migration_version": feature.migration_version,
                    "test_tag": feature.test_tag,
                    "compatibility": feature.compatibility,
                    "migration_target": feature.migration_target,
                    "removal_release": feature.removal_release,
                }
                for feature in self.features
            ]
        }


__all__ = [
    "CommandSpec",
    "ConfigSpec",
    "FeatureManifest",
    "FeatureRegistry",
    "JobSpec",
    "ManifestError",
    "RouteSpec",
]
