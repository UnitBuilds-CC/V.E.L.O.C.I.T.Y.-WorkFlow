"""
Velocity Runtime transport abstraction.

Provides a clean interface for connecting to the Velocity engine,
with HTTP and in-memory implementations.
"""

import json
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

from velocity_runtime.errors import TransportError, ConnectionError


@dataclass
class TransportRequest:
    """A request to be sent via transport."""
    method: str
    path: str
    body: Optional[dict] = None
    headers: Dict[str, str] = None
    timeout_ms: int = 30_000

    def __post_init__(self):
        if self.headers is None:
            self.headers = {}


@dataclass
class TransportResponse:
    """A response received from transport."""
    status_code: int
    body: Optional[dict] = None
    headers: Dict[str, str] = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = {}

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300


class Transport(ABC):
    """Abstract transport interface for engine communication."""

    @abstractmethod
    async def send(self, request: TransportRequest) -> TransportResponse:
        """Send a request and return the response."""
        ...

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the engine."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if transport is connected."""
        ...


class HttpTransport(Transport):
    """HTTP-based transport for connecting to the Velocity engine."""

    def __init__(self, base_url: str, timeout_ms: int = 30_000, headers: Optional[Dict[str, str]] = None):
        self._base_url = base_url.rstrip("/")
        self._timeout_ms = timeout_ms
        self._default_headers = headers or {}
        self._connected = False
        self._session = None

    async def connect(self) -> None:
        """Establish HTTP connection."""
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=self._timeout_ms / 1000.0)
            self._session = aiohttp.ClientSession(
                base_url=self._base_url,
                timeout=timeout,
                headers=self._default_headers,
            )
            self._connected = True
        except ImportError:
            # Fall back to urllib-based transport
            self._connected = True
        except Exception as e:
            raise ConnectionError(self._base_url) from e

    async def disconnect(self) -> None:
        """Close HTTP connection."""
        if self._session:
            await self._session.close()
            self._session = None
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    async def send(self, request: TransportRequest) -> TransportResponse:
        """Send HTTP request."""
        if not self._connected:
            raise ConnectionError(self._base_url)

        url = f"{self._base_url}{request.path}"
        headers = {**self._default_headers, **request.headers}

        try:
            if self._session:
                return await self._send_aiohttp(url, request, headers)
            else:
                return await self._send_urllib(url, request, headers)
        except Exception as e:
            if isinstance(e, TransportError):
                raise
            raise TransportError(f"HTTP request failed: {e}", endpoint=url) from e

    async def _send_aiohttp(self, url: str, request: TransportRequest, headers: dict) -> TransportResponse:
        """Send via aiohttp."""
        async with self._session.request(
            request.method,
            request.path,
            json=request.body,
            headers=headers,
        ) as resp:
            body = await resp.json() if resp.content_type == "application/json" else None
            return TransportResponse(
                status_code=resp.status,
                body=body,
                headers=dict(resp.headers),
            )

    async def _send_urllib(self, url: str, request: TransportRequest, headers: dict) -> TransportResponse:
        """Send via urllib (fallback)."""
        import urllib.request
        import urllib.error

        data = json.dumps(request.body).encode() if request.body else None
        req = urllib.request.Request(
            url,
            data=data,
            headers={**headers, "Content-Type": "application/json"},
            method=request.method,
        )
        try:
            with urllib.request.urlopen(req, timeout=request.timeout_ms / 1000.0) as resp:
                body = json.loads(resp.read()) if resp.read() else None
                return TransportResponse(status_code=resp.status, body=body)
        except urllib.error.HTTPError as e:
            return TransportResponse(status_code=e.code)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()


class InMemoryTransport(Transport):
    """In-memory transport for testing — routes directly to a handler function."""

    def __init__(self):
        self._connected = False
        self._handler = None
        self._requests: list = []

    def set_handler(self, handler) -> None:
        """Set the handler function that processes requests."""
        self._handler = handler

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    async def send(self, request: TransportRequest) -> TransportResponse:
        if not self._connected:
            raise ConnectionError("in-memory")
        self._requests.append(request)
        if self._handler:
            return await self._handler(request) if asyncio.iscoroutinefunction(self._handler) else self._handler(request)
        return TransportResponse(status_code=200, body={"status": "ok"})

    @property
    def sent_requests(self) -> list:
        return list(self._requests)

    def clear(self) -> None:
        self._requests.clear()
