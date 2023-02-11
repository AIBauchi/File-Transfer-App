import asyncio
import websockets
from websockets.legacy.client import WebSocketClientProtocol

from ft_app.base import BaseWebSocket
from ft_app.protocol import (
    Commands,
    Connection,
    Message,
    MessageTypes,
    TaskTypes,
    get_id,
    is_pong,
)


class WebSocketClient(BaseWebSocket):
    server_fqdn: str = ""

    async def scan_receiver(self, prefix="192.168.0.", limit: int = 256):
        """
        Scan range of ips for available listeners.
        """
        for i in range(limit):
            try:
                url = f"ws://{prefix}{i}:{self.port}"
                conn: WebSocketClientProtocol = await asyncio.wait_for(
                    websockets.connect(url, open_timeout=0.5), timeout=0.5
                )
                await conn.send(Commands.ping.encode())
                try:

                    data = await asyncio.wait_for(conn.recv(), timeout=0.4)
                except asyncio.TimeoutError:
                    continue
                data = data.decode().strip()
                if not is_pong(data):
                    continue
                _, server_fqdn = data.split(" ")
                self.server_fqdn = server_fqdn
                await conn.close()
                self.out_queue.put(
                    Message(id=get_id(), type=MessageTypes.open_candidate, data=url)
                )
            except Exception as e:
                continue
        else:
            return

    async def main(self, prefix: str = "192.168.0.", limit: int = 256):
        await self.scan_receiver(prefix, limit)
        self.tasks.append(asyncio.create_task(self._dispatch_tasks()))
        self.tasks.append(asyncio.create_task(self.listen_conn()))
        await asyncio.sleep(2)
        while True:
            if self.connections:

                async with self.lock:
                    try:
                        data = await asyncio.wait_for(
                            self._recv(self.connections[0]), 1
                        )

                    except Exception as e:

                        await asyncio.sleep(0.5)
                        continue
                    if data.strip().decode() == Commands.sendsoon:

                        self.task_queue.put(
                            Message(
                                id=get_id(),
                                type=MessageTypes.task,
                                data={"type": TaskTypes.recv.value},
                            )
                        )
                    else:
                        # Put back at beginning of queue
                        self.connections_msg_queue[self.connections[0]].insert(0, data)
            await asyncio.sleep(0.4)

    async def listen_conn(self):
        """
        Listen to connect requests and create.
        A listener can make only one connection at a time
        """
        while True:
            try:
                data: Message = self.in_queue.get(0)
                if data.type != MessageTypes.connect:
                    await asyncio.sleep(0.5)
                url = data.data
                conn = await websockets.connect(url)
                await asyncio.sleep(1)

                await conn.send(f"{Commands.connect} {self.fqdn}".encode())
                connection = Connection(connection=conn, user=self.server_fqdn)

                self.connections_msg_queue[connection] = []
                self.tasks.append(await self._conc_recv(connection))
                for c in self.connections:
                    c.connection.close()
                self.connections = [connection]
            except:
                await asyncio.sleep(0.5)
            self.out_queue.put(
                Message(
                    id=get_id(), type=MessageTypes.connections, data=self.connections
                )
            )
