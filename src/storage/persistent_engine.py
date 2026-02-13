from src.persistence.wal import WALLogger
from src.persistence.snapshot import SnapshotManager
import threading
import time
from typing import Any, Optional, Dict

class PersistentStorageEngine:
    def __init__(self, wal_path: str = "data/wal.log", rdb_path: str = "data/dump.rdb"):
        self._store: Dict[str, Any] = {}
        self._lock = threading.RLock()
        
        # Persistence Components
        self.wal = WALLogger(wal_path)
        self.snapshot = SnapshotManager(rdb_path)
        
        # Recover State
        self._recover()

    def _recover(self):
        """Restore state from Snapshot + WAL Replay."""
        print("Recovering data...")
        
        # 1. Load Snapshot
        self._store = self.snapshot.load()
        print(f"Loaded {len(self._store)} keys from snapshot.")
        
        # 2. Replay WAL (Since last snapshot)
        # Simplified: We allow duplicate replay for now (idempotence needed ideally)
        count = 0
        for record in self.wal.recover():
            op, key, val = record['op'], record['key'], record['val']
            if op == "SET":
                self._store[key] = val
            elif op == "DEL" and key in self._store:
                del self._store[key]
            count += 1
        print(f"Replayed {count} WAL entries.")

    def set(self, key: str, value: Any) -> bool:
        with self._lock:
            self._store[key] = value
            # Write Ahead Log
            self.wal.log_operation("SET", key, value)
            return True

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                self.wal.log_operation("DEL", key)
                return True
            return False

    def trigger_snapshot(self):
        """Called periodically by a background thread."""
        with self._lock:
            # Clone data to avoid locking disk I/O
            data_copy = self._store.copy()
        
        self.snapshot.save(data_copy)
        # Ideally, we would truncate/rotate WAL here
