import asyncio
import structlog
from typing import List, Set
from src.persistence.wal import WALLogger

logger = structlog.get_logger()

class ReplicationManager:
    """
    Manages Leader-Follower Replication logic on the Leader node.
    Followers connect via TCP, and the leader streams WAL entries to them.
    """
    def __init__(self):
        # Set of active follower writer streams
        self.followers: Set[asyncio.StreamWriter] = set()

    async def register_follower(self, writer: asyncio.StreamWriter):
        """Called when a new follower connects."""
        addr = writer.get_extra_info('peername')
        logger.info("follower_connected", address=addr)
        self.followers.add(writer)

    async def unregister_follower(self, writer: asyncio.StreamWriter):
        if writer in self.followers:
            self.followers.remove(writer)
            logger.info("follower_disconnected")

    async def propagate_command(self, cmd_string: str):
        """
        Multicast a write command to all connected followers.
        Fire-and-forget for async replication (Eventual Consistency).
        """
        if not self.followers:
            return

        payload = f"{cmd_string}\n".encode()
        dead_followers = []

        for writer in self.followers:
            try:
                writer.write(payload)
                await writer.drain()
            except Exception as e:
                logger.warning("replication_failed", error=str(e))
                dead_followers.append(writer)

        # Cleanup dead connections
        for dead in dead_followers:
            await self.unregister_follower(dead)
