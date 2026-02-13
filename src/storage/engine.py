import threading
import time
from typing import Any, Optional, Dict

class StorageEngine:
    def __init__(self):
        # The main in-memory store
        self._store: Dict[str, Any] = {}
        # Expiry tracking: key -> timestamp (unix epoch)
        self._expiry: Dict[str, float] = {}
        # Fine-grained locking could be added later; for MVP, a global lock suffices
        self._lock = threading.RLock()

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        with self._lock:
            self._store[key] = value
            if ttl:
                self._expiry[key] = time.time() + ttl
            elif key in self._expiry:
                del self._expiry[key] # Clear expiry if existing key is updated without TTL
            return True

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            # Lazy Expiration Check
            if key in self._expiry:
                if time.time() > self._expiry[key]:
                    self._delete(key)
                    return None
            
            return self._store.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            return self._delete(key)

    def _delete(self, key: str) -> bool:
        """Internal delete helper (assumes lock is held)."""
        if key in self._store:
            del self._store[key]
            if key in self._expiry:
                del self._expiry[key]
            return True
        return False
        
    def exists(self, key: str) -> bool:
        return self.get(key) is not None
