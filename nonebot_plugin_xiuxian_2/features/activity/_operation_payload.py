from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any


_NUMERIC_TEXT = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")


def _normalize_leaf(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            try:
                return int(value)
            except (OverflowError, ValueError):
                return value
        return value
    text = str(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"[+-]?\d+", text):
        try:
            return int(text)
        except (ValueError, OverflowError):
            return text
    if _NUMERIC_TEXT.fullmatch(text):
        try:
            number = Decimal(text)
            return int(number) if number == number.to_integral_value() else float(number)
        except (InvalidOperation, ValueError, OverflowError):
            return text
    return text


def _normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return _normalize_leaf(value)


def _coerce_json(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return ""
    if text.startswith(("{", "[")):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


def operation_payload_matches(stored: Any, expected: Any) -> bool:
    """Compare JSON operation payloads across legacy numeric-string encodings."""
    return _normalize(_coerce_json(stored)) == _normalize(_coerce_json(expected))


__all__ = ["operation_payload_matches"]
