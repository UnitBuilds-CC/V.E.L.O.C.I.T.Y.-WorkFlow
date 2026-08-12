"""
Velocity Runtime storage backends.

Provides persistent storage for journals, state, and invocation records.
Supports crash recovery: on restart, journals are replayed to restore state.
"""

import json
import os
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class StoredJournal:
    """A persisted journal for a handler invocation."""
    invocation_id: str
    service_name: str
    handler_name: str
    key: str
    entries: List[dict]
    object_state: Dict[str, Any] = field(default_factory=dict)
    output: Any = None
    error: Optional[str] = None
    state: str = "running"  # running, completed, failed
    created_at: float = 0.0
    completed_at: float = 0.0


@dataclass
class StoredKeyState:
    """Persisted state for a Virtual Object key."""
    full_key: str
    state: Dict[str, Any] = field(default_factory=dict)
    updated_at: float = 0.0


class StorageBackend(ABC):
    """Abstract storage backend for the Velocity Runtime.

    Implementations must be thread-safe for concurrent access.
    """

    @abstractmethod
    def save_journal(self, journal: StoredJournal) -> None:
        """Persist a journal entry."""
        ...

    @abstractmethod
    def load_journal(self, invocation_id: str) -> Optional[StoredJournal]:
        """Load a journal by invocation ID."""
        ...

    @abstractmethod
    def load_journals_for_key(self, full_key: str) -> List[StoredJournal]:
        """Load all journals for a given object key (for replay)."""
        ...

    @abstractmethod
    def load_all_journals(self) -> List[StoredJournal]:
        """Load all journals (for full recovery)."""
        ...

    @abstractmethod
    def save_key_state(self, key_state: StoredKeyState) -> None:
        """Persist Virtual Object key state."""
        ...

    @abstractmethod
    def load_key_state(self, full_key: str) -> Optional[StoredKeyState]:
        """Load Virtual Object key state."""
        ...

    @abstractmethod
    def delete_journal(self, invocation_id: str) -> None:
        """Delete a journal (for cleanup)."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear all stored data (for testing)."""
        ...


class InMemoryStorage(StorageBackend):
    """In-memory storage backend — fast but non-persistent.

    Useful for testing and single-process deployments where
    crash recovery is not required.
    """

    def __init__(self):
        self._journals: Dict[str, StoredJournal] = {}
        self._key_states: Dict[str, StoredKeyState] = {}
        self._lock = threading.RLock()

    def save_journal(self, journal: StoredJournal) -> None:
        with self._lock:
            self._journals[journal.invocation_id] = journal

    def load_journal(self, invocation_id: str) -> Optional[StoredJournal]:
        with self._lock:
            return self._journals.get(invocation_id)

    def load_journals_for_key(self, full_key: str) -> List[StoredJournal]:
        with self._lock:
            return [j for j in self._journals.values()
                    if self._journal_key(j) == full_key]

    def load_all_journals(self) -> List[StoredJournal]:
        with self._lock:
            return list(self._journals.values())

    def save_key_state(self, key_state: StoredKeyState) -> None:
        with self._lock:
            self._key_states[key_state.full_key] = key_state

    def load_key_state(self, full_key: str) -> Optional[StoredKeyState]:
        with self._lock:
            return self._key_states.get(full_key)

    def delete_journal(self, invocation_id: str) -> None:
        with self._lock:
            self._journals.pop(invocation_id, None)

    def clear(self) -> None:
        with self._lock:
            self._journals.clear()
            self._key_states.clear()

    @staticmethod
    def _journal_key(journal: StoredJournal) -> str:
        if journal.key:
            return f"{journal.service_name}/{journal.key}"
        return journal.service_name


class FileStorage(StorageBackend):
    """File-based storage backend — persists journals and state to disk.

    Uses a directory structure:
        <base_dir>/journals/<invocation_id>.json
        <base_dir>/state/<full_key_hash>.json

    Suitable for single-node deployments where crash recovery is needed
    but a full database is overkill.
    """

    def __init__(self, base_dir: str = ".velocity_runtime"):
        self._base_dir = base_dir
        self._journal_dir = os.path.join(base_dir, "journals")
        self._state_dir = os.path.join(base_dir, "state")
        self._lock = threading.RLock()
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        os.makedirs(self._journal_dir, exist_ok=True)
        os.makedirs(self._state_dir, exist_ok=True)

    def _journal_path(self, invocation_id: str) -> str:
        safe_id = invocation_id.replace("/", "_").replace("\\", "_")
        return os.path.join(self._journal_dir, f"{safe_id}.json")

    def _state_path(self, full_key: str) -> str:
        import hashlib
        key_hash = hashlib.sha256(full_key.encode()).hexdigest()[:16]
        return os.path.join(self._state_dir, f"{key_hash}.json")

    def save_journal(self, journal: StoredJournal) -> None:
        with self._lock:
            data = {
                "invocation_id": journal.invocation_id,
                "service_name": journal.service_name,
                "handler_name": journal.handler_name,
                "key": journal.key,
                "entries": journal.entries,
                "object_state": journal.object_state,
                "output": journal.output,
                "error": journal.error,
                "state": journal.state,
                "created_at": journal.created_at,
                "completed_at": journal.completed_at,
            }
            with open(self._journal_path(journal.invocation_id), "w") as f:
                json.dump(data, f)

    def load_journal(self, invocation_id: str) -> Optional[StoredJournal]:
        path = self._journal_path(invocation_id)
        if not os.path.exists(path):
            return None
        with self._lock:
            with open(path, "r") as f:
                data = json.load(f)
            return StoredJournal(**data)

    def load_journals_for_key(self, full_key: str) -> List[StoredJournal]:
        journals = self.load_all_journals()
        parts = full_key.split("/", 1)
        service_name = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return [j for j in journals
                if j.service_name == service_name and j.key == key]

    def load_all_journals(self) -> List[StoredJournal]:
        results = []
        with self._lock:
            if not os.path.exists(self._journal_dir):
                return results
            for filename in os.listdir(self._journal_dir):
                if filename.endswith(".json"):
                    path = os.path.join(self._journal_dir, filename)
                    try:
                        with open(path, "r") as f:
                            data = json.load(f)
                        results.append(StoredJournal(**data))
                    except (json.JSONDecodeError, KeyError, TypeError):
                        continue
        return results

    def save_key_state(self, key_state: StoredKeyState) -> None:
        with self._lock:
            data = {
                "full_key": key_state.full_key,
                "state": key_state.state,
                "updated_at": key_state.updated_at,
            }
            with open(self._state_path(key_state.full_key), "w") as f:
                json.dump(data, f)

    def load_key_state(self, full_key: str) -> Optional[StoredKeyState]:
        path = self._state_path(full_key)
        if not os.path.exists(path):
            return None
        with self._lock:
            with open(path, "r") as f:
                data = json.load(f)
            return StoredKeyState(**data)

    def delete_journal(self, invocation_id: str) -> None:
        path = self._journal_path(invocation_id)
        with self._lock:
            if os.path.exists(path):
                os.remove(path)

    def clear(self) -> None:
        with self._lock:
            for d in [self._journal_dir, self._state_dir]:
                if os.path.exists(d):
                    for f in os.listdir(d):
                        os.remove(os.path.join(d, f))
