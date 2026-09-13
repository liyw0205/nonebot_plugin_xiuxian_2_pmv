"""Common boundary used while the remaining legacy packages are migrated.

The boundary is deliberately small: legacy algorithms stay behind a repository
port, while operation identity, replay and transport-neutral replies are owned
by the new application layer.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
import asyncio
import importlib
import inspect
import warnings
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from ..core.result import OperationOutcome, ReplyPlan
from ._legacy_application import LegacyApplication


class LegacyFeatureRepository(Protocol):
    def execute(self, operation_id: str, user_id: str, payload: Mapping[str, Any]) -> Any: ...

    def inspect(self, user_id: str) -> Mapping[str, Any]: ...


class CompatibilityRepository:
    """Lazy adapter for a historical package.

    A compatibility slice is still allowed to keep its historical algorithm,
    but the call must cross this repository boundary.  ``execute`` therefore
    resolves the requested action at call time and invokes a real legacy
    function/service when one is available.  Importing the package is kept
    lazy because old modules still contain matcher registration side effects.
    """

    def __init__(
        self,
        feature: str,
        *databases: str | Path,
        legacy_module: str | None = None,
        handlers: Mapping[str, Callable[..., Any]] | None = None,
    ) -> None:
        self.feature = str(feature)
        self.databases = tuple(str(item) for item in databases)
        self.legacy_module = str(legacy_module or "")
        self.handlers = {str(key).casefold(): value for key, value in (handlers or {}).items()}

    @staticmethod
    def _normalize(value: Any) -> dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        if is_dataclass(value):
            return dict(asdict(value))
        if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], bool):
            ok, message = value
            return {"status": "applied" if ok else "rejected", "message": str(message)}
        if isinstance(value, bool):
            return {"status": "applied" if value else "rejected"}
        if value is None:
            return {"status": "applied"}
        if isinstance(value, str):
            return {"status": "applied", "message": value}
        try:
            return dict(vars(value))
        except TypeError:
            return {"status": "applied", "result": value}

    def _resolve(self, action: str) -> Callable[..., Any] | None:
        key = str(action or "execute").strip().casefold()
        direct = self.handlers.get(key) or self.handlers.get("execute" if key == "" else "")
        if direct is not None:
            return direct
        if not self.legacy_module:
            return None
        if "." in key:
            module_name, _, attr_name = key.rpartition(".")
            try:
                submodule = importlib.import_module(f"{self.legacy_module}.{module_name}")
            except Exception:
                submodule = None
            if submodule is not None:
                target = getattr(submodule, attr_name, None)
                if callable(target):
                    return target
        try:
            module = importlib.import_module(self.legacy_module)
        except Exception:
            # Standalone application tests and CLI manifest inspection run
            # without a NoneBot driver.  The action remains visible as an
            # explicit unsupported result instead of failing at import time.
            return None
        candidates = (
            key,
            f"{self.feature}_{key}",
            f"claim_{key}",
            f"handle_{key}",
            f"settle_{key}",
            f"{key}_service",
        )
        for name in candidates:
            target = getattr(module, name, None)
            if callable(target):
                return target
            if target is not None:
                for method_name in (key, "execute", "run", "claim", "settle"):
                    method = getattr(target, method_name, None)
                    if callable(method):
                        return method
        # ``execute`` is the neutral application facade action, not a request
        # to guess which historical service should run.  Without this guard,
        # a module's first ``*_service`` object could be invoked with an
        # incomplete payload and mutate a different legacy workflow.
        if key == "execute":
            return None
        # Historical packages commonly expose ``*_service`` objects.  Look
        # through those objects only after the explicit names above.
        for name, target in vars(module).items():
            if not name.casefold().endswith("_service"):
                continue
            for method_name in (key, "execute", "run", "claim", "settle"):
                method = getattr(target, method_name, None)
                if callable(method):
                    return method
        return None

    @staticmethod
    def _invoke(handler: Callable[..., Any], operation_id: str, user_id: str, payload: Mapping[str, Any]) -> Any:
        values = dict(payload)
        values.setdefault("operation_id", str(operation_id))
        values.setdefault("user_id", str(user_id))
        try:
            signature = inspect.signature(handler)
        except (TypeError, ValueError):
            return handler(**values)
        accepts_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())
        if accepts_kwargs:
            call_kwargs = values
        else:
            call_kwargs = {
                name: values[name]
                for name, parameter in signature.parameters.items()
                if name in values and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            }
        missing = [
            name
            for name, parameter in signature.parameters.items()
            if parameter.default is inspect.Parameter.empty
            and parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            and name not in call_kwargs
        ]
        if missing:
            raise TypeError(f"legacy action requires fields: {', '.join(missing)}")
        result = handler(**call_kwargs)
        if inspect.isawaitable(result):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(result)
            raise RuntimeError("async legacy handlers must be adapted by an async command boundary")
        return result

    def execute(self, operation_id: str, user_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self._record_compatibility_use("execute")
        request = dict(payload)
        action = str(request.pop("action", request.pop("operation", request.pop("method", "execute"))) or "execute")
        handler = self._resolve(action)
        if handler is None:
            return {
                "status": "unsupported",
                "message": f"{self.feature} action is not available: {action}",
                "action": action,
                "feature": self.feature,
                "operation_id": str(operation_id),
                "user_id": str(user_id),
                "payload": request,
                "compatibility": True,
                "legacy_module": self.legacy_module,
            }
        result = self._normalize(self._invoke(handler, operation_id, user_id, request))
        result.setdefault("status", "applied")
        result.update({
            "feature": self.feature,
            "operation_id": str(operation_id),
            "user_id": str(user_id),
            "action": action,
            "payload": request,
            "compatibility": True,
            "legacy_module": self.legacy_module,
        })
        return result

    def execute_callback(
        self,
        operation_id: str,
        user_id: str,
        action: str,
        payload: Mapping[str, Any],
        callback: Callable[[], Any],
    ) -> dict[str, Any]:
        """Keep pre-bound legacy transactions behind this compatibility port."""
        self._record_compatibility_use(str(action))
        return self._normalize(callback())

    def inspect(self, user_id: str) -> dict[str, Any]:
        self._record_compatibility_use("inspect")
        return {"feature": self.feature, "user_id": str(user_id), "compatibility": True, "legacy_module": self.legacy_module}

    def _record_compatibility_use(self, action: str) -> None:
        """Make generic legacy slices observable before their removal release."""
        warnings.warn(
            f"{self.feature} compatibility {action} is deprecated; use the migrated application",
            DeprecationWarning,
            stacklevel=3,
        )
        try:
            from ..compatibility.commands import record_compatibility_hit

            record_compatibility_hit(f"feature:{self.feature}")
        except Exception:
            # Telemetry must not make a historical command fail when its data
            # directory is read-only or has not been configured yet.
            return


class LegacyFeatureApplication(LegacyApplication):
    """Application facade for a feature whose historical rules are still active."""

    def __init__(self, database: str | Path, *, feature: str, repository: LegacyFeatureRepository | None = None) -> None:
        super().__init__(database, repository=repository or CompatibilityRepository(feature, database), feature=feature)

    def execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload or {})
        request["user_id"] = str(user_id)
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action=f"{self.feature}.execute",
            payload=request,
            call=lambda: self.repository.execute(operation_id, user_id, request),
        )

    def execute_legacy_call(
        self,
        *,
        operation_id: str,
        user_id: str,
        action: str,
        call: Callable[[], Any],
        payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        """Run one historical service call behind the application boundary.

        Some legacy handlers already have the fully assembled transaction
        arguments and cannot be resolved safely by name from a module.  They
        pass that call here so the same ledger, replay and audit contract is
        applied without importing transport concerns into the service.
        """
        request = dict(payload or {})
        request["user_id"] = str(user_id)
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action=f"{self.feature}.{action}",
            payload=request,
            call=call,
        )

    def inspect(self, *, user_id: str) -> Mapping[str, Any]:
        return self.repository.inspect(str(user_id))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.execute(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["CompatibilityRepository", "LegacyFeatureApplication", "LegacyFeatureRepository"]
