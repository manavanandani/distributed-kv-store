import asyncio
import structlog
import sys
import argparse
from typing import Optional
import signal
from src.storage.persistent_engine import PersistentStorageEngine
from src.replication.leader import ReplicationManager
from src.replication.follower import FollowerClient

logger = structlog.get_logger()

class KVServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 6379, role: str = "leader", leader_addr: Optional[str] = None):
        self.host = host
        self.port = port
        self.role = role
        self.leader_addr = leader_addr
        
        self.server: Optional[asyncio.AbstractServer] = None
        self.running = False
        self.engine = PersistentStorageEngine(wal_path=f"data/wal_{port}.log", rdb_path=f"data/dump_{port}.rdb")
        self.replication_mgr = ReplicationManager() if role == "leader" else None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info("client_connected", address=addr)
        
        try:
            while self.running:
                data = await reader.read(4096)
                if not data: break
                
                # Simple Text Protocol: COMMAND key [value]
                parts = data.decode().strip().split(maxsplit=2)
                if not parts: continue
                
                cmd = parts[0].upper()
                response = "ERROR\n"

                # REPLICATION HANDSHAKE (Internal)
                if cmd == "REPLICATE":
                    if self.role == "leader":
                        await self.replication_mgr.register_follower(writer)
                        # Keep connection open for streaming, don't close in finally
                        return 
                    else:
                        writer.write(b"ERROR: I am not a leader\n")
                        await writer.drain()
                        return

                # READ OPS (Allowed on both Leader and Follower)
                if cmd == "GET":
                    val = self.engine.get(parts[1])
                    response = f"{val}\n" if val else "(nil)\n"
                
                # WRITE OPS (Leader Only)
                elif cmd in ("SET", "DEL"):
                    if self.role != "leader":
                        response = "ERROR: Read-only follower\n"
                    else:
                        if cmd == "SET" and len(parts) >= 3:
                            self.engine.set(parts[1], parts[2])
                            await self.replication_mgr.propagate_command(f"SET {parts[1]} {parts[2]}")
                            response = "OK\n"
                        elif cmd == "DEL" and len(parts) == 2:
                            deleted = self.engine.delete(parts[1])
                            await self.replication_mgr.propagate_command(f"DEL {parts[1]}")
                            response = "1\n" if deleted else "0\n"
                
                writer.write(response.encode())
                await writer.drain()
                
        except Exception as e:
            logger.error("connection_error", error=str(e))
        finally:
            writer.close()

    async def start(self):
        # Start Snapshot loop in background (Not implemented in async loop for brevity)
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        self.running = True
        logger.info("server_started", role=self.role, port=self.port)
        
        # If Follower, start replication client in background
        if self.role == "follower" and self.leader_addr:
            l_host, l_port = self.leader_addr.split(":")
            follower_client = FollowerClient(l_host, int(l_port), self.engine)
            asyncio.create_task(follower_client.start())

        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        logger.info("server_stopped")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--role", type=str, default="leader", choices=["leader", "follower"])
    parser.add_argument("--leader", type=str, help="Leader address host:port (required if follower)")
    args = parser.parse_args()
    
    srv = KVServer(port=args.port, role=args.role, leader_addr=args.leader)
    
    # Graceful Shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(srv.stop()))
    
    await srv.start()

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
