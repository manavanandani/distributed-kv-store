import pickle
import os
import time
from typing import Dict, Any

class SnapshotManager:
    """
    Manages periodic snapshots (RDB style) of the in-memory store.
    """
    def __init__(self, filepath: str = "data/dump.rdb"):
        self.filepath = filepath
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

    def save(self, store_data: Dict[str, Any]):
        """
        Atomically save snapshot: Write to temp file -> Rename.
        This prevents corruption if process crashes mid-write.
        """
        temp_path = f"{self.filepath}.tmp"
        with open(temp_path, "wb") as f:
            pickle.dump(store_data, f)
        
        # Atomic rename
        os.replace(temp_path, self.filepath)
        print(f"[{time.ctime()}] Snapshot saved to {self.filepath}")

    def load(self) -> Dict[str, Any]:
        """
        Load snapshot from disk on startup.
        """
        if not os.path.exists(self.filepath):
            return {}
        
        try:
            with open(self.filepath, "rb") as f:
                return pickle.load(f)
        except (EOFError, pickle.UnpicklingError):
            return {}
