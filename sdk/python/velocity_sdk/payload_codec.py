"""
VELOCITY-WorkFlow Python SDK - Payload encoding/decoding.

Provides codecs for serializing workflow payloads (JSON, protobuf, raw bytes).
"""

import json
from typing import Any, Protocol
from dataclasses import dataclass


class PayloadCodec(Protocol):
    """Interface for payload encoding/decoding."""

    def encode(self, data: Any) -> bytes:
        """Encode data to bytes."""
        ...

    def decode(self, data: bytes) -> Any:
        """Decode bytes to data."""
        ...


@dataclass
class JsonCodec:
    """JSON payload codec."""

    ensure_ascii: bool = False
    sort_keys: bool = False

    def encode(self, data: Any) -> bytes:
        """Encode data as JSON bytes."""
        return json.dumps(
            data,
            ensure_ascii=self.ensure_ascii,
            sort_keys=self.sort_keys,
        ).encode("utf-8")

    def decode(self, data: bytes) -> Any:
        """Decode JSON bytes to Python object."""
        return json.loads(data.decode("utf-8"))


@dataclass
class BinaryCodec:
    """Raw binary codec (passthrough)."""

    def encode(self, data: bytes) -> bytes:
        """Return data as-is."""
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"BinaryCodec expects bytes, got {type(data).__name__}")
        return bytes(data)

    def decode(self, data: bytes) -> bytes:
        """Return data as-is."""
        return data


class ProtobufCodec:
    """Protocol Buffers codec (requires protobuf library)."""

    def __init__(self, message_type: Any = None):
        """
        Initialize with a protobuf message type.

        Args:
            message_type: Protobuf message class (e.g., MyMessage)
        """
        self.message_type = message_type

    def encode(self, data: Any) -> bytes:
        """Encode protobuf message to bytes."""
        if self.message_type is None:
            raise ValueError("message_type must be set for ProtobufCodec")

        if hasattr(data, "SerializeToString"):
            return data.SerializeToString()

        # Try to create message from dict
        msg = self.message_type()
        if isinstance(data, dict):
            for key, value in data.items():
                setattr(msg, key, value)
        else:
            msg.CopyFrom(data)

        return msg.SerializeToString()

    def decode(self, data: bytes) -> Any:
        """Decode bytes to protobuf message."""
        if self.message_type is None:
            raise ValueError("message_type must be set for ProtobufCodec")

        msg = self.message_type()
        msg.ParseFromString(data)
        return msg


class CodecChain:
    """Chain multiple codecs together (e.g., JSON -> compression)."""

    def __init__(self, codecs: list[PayloadCodec]):
        if not codecs:
            raise ValueError("CodecChain requires at least one codec")
        self.codecs = codecs

    def encode(self, data: Any) -> bytes:
        """Encode through all codecs in order."""
        result = data
        for codec in self.codecs:
            result = codec.encode(result)
        return result if isinstance(result, bytes) else self.codecs[-1].encode(result)

    def decode(self, data: bytes) -> Any:
        """Decode through all codecs in reverse order."""
        result: Any = data
        for codec in reversed(self.codecs):
            result = codec.decode(result)
        return result
