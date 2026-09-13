"""Explicit legacy service port used by feature-owned repositories.

The port deliberately accepts a declared handler map from each feature.  It
does not search arbitrary modules or select the first service object, which
keeps compatibility calls auditable while the historical schemas are retired.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import warnings
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Mapping


class ServicePort:
    def __init__(self, feature: str, module: str, *, handlers: Mapping[str, Callable[..., Any]] | None = None) -> None:
        self.feature = str(feature)
        self.module = str(module)
        self.handlers = {str(key).casefold(): value for key, value in (handlers or {}).items()}

    @staticmethod
    def _normalize(value: Any) -> Any:
        if isinstance(value, Mapping):
            return dict(value)
        if is_dataclass(value):
            return dict(asdict(value))
        return value

    def _resolve(self, action: str) -> Callable[..., Any] | None:
        target = self.handlers.get(str(action).casefold())
        if target is not None:
            return target
        try:
            module = importlib.import_module(self.module)
        except Exception:
            return None
        # A feature repository may explicitly expose a named operation on its
        # legacy package; no fallback scanning is performed here.
        candidate = getattr(module, str(action), None)
        return candidate if callable(candidate) else None

    @staticmethod
    def _invoke(handler: Callable[..., Any], operation_id: str, user_id: str, payload: Mapping[str, Any]) -> Any:
        values = dict(payload)
        values.setdefault("operation_id", operation_id)
        values.setdefault("user_id", user_id)
        signature = inspect.signature(handler)
        if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()):
            result = handler(**values)
        else:
            kwargs = {
                name: values[name]
                for name, parameter in signature.parameters.items()
                if name in values and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            }
            missing = [
                name for name, parameter in signature.parameters.items()
                if parameter.default is inspect.Parameter.empty
                and parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
                and name not in kwargs
            ]
            if missing:
                raise TypeError(f"feature action requires fields: {', '.join(missing)}")
            result = handler(**kwargs)
        if inspect.isawaitable(result):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(result)
            raise RuntimeError("async legacy handlers must be adapted by an async command boundary")
        return result

    def execute(self, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any]) -> Any:
        self._record_compatibility_use(action)
        handler = self._resolve(action)
        if handler is None:
            return {"status": "unsupported", "feature": self.feature, "action": action, "operation_id": operation_id, "user_id": user_id}
        return self._normalize(self._invoke(handler, operation_id, user_id, payload))

    def execute_callback(
        self,
        operation_id: str,
        user_id: str,
        action: str,
        payload: Mapping[str, Any],
        callback: Callable[[], Any],
    ) -> Any:
        """Run a pre-bound legacy transaction behind the service port.

        Some historical handlers assemble a transaction object before the
        application boundary is entered, so resolving them by action name
        would either lose arguments or guess the wrong service.  Keeping this
        narrow escape hatch on the feature-owned port preserves those exact
        arguments while ensuring the application never invokes legacy code
        directly.  The callback is deliberately not exposed through the
        public payload or action resolver.
        """
        del operation_id, user_id, action, payload
        self._record_compatibility_use("callback")
        return self._normalize(callback())

    def _record_compatibility_use(self, action: str) -> None:
        """Make staged legacy feature traffic visible to the release gate."""
        warnings.warn(
            f"{self.feature} service port {action} is a compatibility boundary",
            DeprecationWarning,
            stacklevel=3,
        )
        try:
            from ..compatibility.commands import record_compatibility_hit

            record_compatibility_hit(f"feature:{self.feature}")
        except Exception:
            # Observability must not turn a read-only data directory into a
            # failed gameplay operation.
            return

    def inspect(self, user_id: str) -> Mapping[str, Any]:
        return {"feature": self.feature, "user_id": str(user_id), "compatibility": True}


__all__ = ["ServicePort"]
