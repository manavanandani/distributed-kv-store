from datetime import datetime
import json
import struct
import io
import os
from typing import Optional

class WALLogger:
    """
    Write-Ahead Logger (WAL) for durability.
    Appends every mutation (SET/DEL) to an append-only file.
    """
    def __init__(self, filepath: str = "data/wal.log"):
        self.filepath = filepath
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.file = open(filepath, "ab") # Append Binary Mode

    def log_operation(self, op: str, key: str, value: Optional[str] = None):
        """
        Format: [timestamp (8 bytes)][op_len (1 byte)][op][key_len (2 bytes)][key][val_len (4 bytes)][val]
        Simplistic binary format for efficiency.
        """
        timestamp = int(datetime.utcnow().timestamp())
        
        # Serialize
        record = {
            "ts": timestamp,
            "op": op,
            "key": key,
            "val": value
        }
        json_record = json.dumps(record) + "\n"
        
        # Write to disk
        self.file.write(json_record.encode('utf-8'))
        self.file.flush() # Ensure it hits OS buffer (fsync for strict durability)

    def recover(self):
        """
        Generator that reads the log line by line for recovery.
        """
        if not os.path.exists(self.filepath):
            return

        with open(self.filepath, "r") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue # Skip corrupted lines
