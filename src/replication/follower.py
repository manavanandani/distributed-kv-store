import asyncio
import structlog
from src.storage.persistent_engine import PersistentStorageEngine

logger = structlog.get_logger()

class FollowerClient:
    """
    Client logic for a Follower node.
    Connects to the Leader and applies received commands to its local engine.
    """
    def __init__(self, leader_host: str, leader_port: int, engine: PersistentStorageEngine):
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.engine = engine
        self.running = False

    async def start(self):
        self.running = True
        while self.running:
            try:
                reader, writer = await asyncio.open_connection(self.leader_host, self.leader_port)
                
                # Handshake: Tell leader we are a follower (Simple AUTH/Command)
                writer.write(b"REPLICATE\n")
                await writer.drain()
                logger.info("connected_to_leader", host=self.leader_host)

                while self.running:
                    data = await reader.read(4096)
                    if not data:
                        break
                    
                    # Apply replicated commands
                    commands = data.decode().strip().split("\n")
                    for cmd in commands:
                        if not cmd: continue
                        await self._apply_command(cmd)
                        
            except Exception as e:
                logger.error("replication_error", error=str(e))
                await asyncio.sleep(5) # Retry backoff

    async def _apply_command(self, cmd_line: str):
        """Parse and execute replicated command on local engine."""
        parts = cmd_line.split(maxsplit=2)
        if not parts: return
        
        op = parts[0].upper()
        if op == "SET" and len(parts) >= 3:
            # Bypass WAL on follower? Or log as strictly replicated?
            # Ideally, follower also logs to its own WAL for durability.
            self.engine.set(parts[1], parts[2])
            logger.debug("replicated_set", key=parts[1])
        elif op == "DEL" and len(parts) == 2:
            self.engine.delete(parts[1])
            logger.debug("replicated_del", key=parts[1])
