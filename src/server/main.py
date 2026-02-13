import asyncio
import structlog
from typing import Optional
import signal
import sys

logger = structlog.get_logger()

class KVServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 6379):
        self.host = host
        self.port = port
        self.server: Optional[asyncio.AbstractServer] = None
        self.running = False

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info("client_connected", address=addr)
        
        try:
            while self.running:
                data = await reader.read(1024)
                if not data:
                    break
                
                message = data.decode().strip()
                logger.debug("command_received", cmd=message)
                
                # Mock Processing Logic
                response = f"OK: {message}\n"
                writer.write(response.encode())
                await writer.drain()
        except Exception as e:
            logger.error("connection_error", error=str(e))
        finally:
            logger.info("client_disconnected", address=addr)
            writer.close()
            await writer.wait_closed()

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        self.running = True
        logger.info("server_started", host=self.host, port=self.port)
        
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
