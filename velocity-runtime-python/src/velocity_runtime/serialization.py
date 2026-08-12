"""
Velocity Runtime serialization utilities.

Handles JSON serialization/deserialization of handler inputs, outputs, and state.
"""

import json
import base64
import datetime
from typing import Any, Callable, Optional

from velocity_runtime.errors import SerializationError


def serialize(value: Any) -> Any:
    """Serialize a value to JSON-compatible format.

    Handles:
    - Primitives (str, int, float, bool, None)
    - datetime → ISO 8601 string
    - bytes → base64 string
    - dict, list → recursively serialized
    - Objects with __dict__ → dict
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (list, tuple)):
        return [serialize(item) for item in value]
    if isinstance(value, dict):
        return {str(k): serialize(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return serialize(vars(value))
    if hasattr(value, "to_dict"):
        return serialize(value.to_dict())
    raise SerializationError(f"Cannot serialize type {type(value).__name__}")


def deserialize(data: Any, target_type: Optional[type] = None) -> Any:
    """Deserialize a JSON-compatible value.

    If target_type is provided, attempts to construct an instance.
    """
    if target_type is None:
        return data
    if target_type in (str, int, float, bool) or data is None:
        return data
    if target_type is bytes and isinstance(data, str):
        return base64.b64decode(data)
    if target_type is datetime.datetime and isinstance(data, str):
        return datetime.datetime.fromisoformat(data)
    if target_type is datetime.date and isinstance(data, str):
        return datetime.date.fromisoformat(data)
    if target_type is list and isinstance(data, list):
        return data
    if target_type is dict and isinstance(data, dict):
        return data
    if isinstance(data, dict) and hasattr(target_type, "__init__"):
        try:
            return target_type(**data)
        except TypeError:
            return data
    return data


def to_json(value: Any, pretty: bool = False) -> str:
    """Serialize to JSON string."""
    try:
        serialized = serialize(value)
        if pretty:
            return json.dumps(serialized, indent=2, default=str)
        return json.dumps(serialized, default=str)
    except (TypeError, ValueError) as e:
        raise SerializationError(f"JSON serialization failed: {e}") from e


def from_json(text: str) -> Any:
    """Deserialize from JSON string."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError) as e:
        raise SerializationError(f"JSON deserialization failed: {e}") from e


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge two dicts. Override values take precedence."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
