import asyncio
import structlog
from typing import Optional
import signal
from src.storage.persistent_engine import PersistentStorageEngine

logger = structlog.get_logger()

class KVServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 6379):
        self.host = host
        self.port = port
        self.server: Optional[asyncio.AbstractServer] = None
        self.running = False
        self.engine = PersistentStorageEngine()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info("client_connected", address=addr)
        
        try:
            while self.running:
                data = await reader.read(4096)
                if not data:
                    break
                
                # Simple Text Protocol: COMMAND key [value]
                parts = data.decode().strip().split(maxsplit=2)
                if not parts:
                    continue
                
                cmd = parts[0].upper()
                response = "ERROR: Unknown Command\n"
                
                if cmd == "GET" and len(parts) == 2:
                    val = self.engine.get(parts[1])
                    response = f"{val}\n" if val else "(nil)\n"
                
                elif cmd == "SET" and len(parts) >= 3:
                    self.engine.set(parts[1], parts[2])
                    response = "OK\n"
                
                elif cmd == "DEL" and len(parts) == 2:
                    deleted = self.engine.delete(parts[1])
                    response = "1\n" if deleted else "0\n"
                
                writer.write(response.encode())
                await writer.drain()
                
        except Exception as e:
            logger.error("connection_error", error=str(e))
        finally:
            writer.close()

    async def start(self):
        # Start Snapshot loop in background (Not implemented in async loop for brevity)
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        self.running = True
        logger.info("server_started_persistent", host=self.host, port=self.port)
        
        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        logger.info("server_stopped")

async def main():
    server = KVServer()
    
    # Graceful Shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(server.stop()))
    
    await server.start()

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
