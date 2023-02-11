import asyncio
import websockets
from websockets.legacy.server import WebSocketServerProtocol

from ft_app.base import BaseWebSocket
from ft_app.exceptions import MessageTimeoutError
from ft_app.protocol import (
    Commands,
    Connection,
    Message,
    MessageTypes,
    TaskTypes,
    is_valid_conn_request,
    get_id,
)


class WebSocketServer(BaseWebSocket):
    prompt_user_connect: bool = True

    def run_server(self) -> asyncio.Task:
        start_server = websockets.serve(self.server_handler, self.ip, self.port)
        self.tasks.append(asyncio.create_task(self._dispatch_tasks()))

        async def start():
            await start_server

        loop = asyncio.get_event_loop()
        t = loop.create_task(start())
        return t

    async def server_handler(self, websocket: WebSocketServerProtocol, _):
        """
        Handle connections
        """
        connection = Connection(connection=websocket, user=f"")
        self.connections_msg_queue[connection] = []
        conn_task = await self._conc_recv(connection)
        data = await self._recv(connection)

        # First message should be connection request
        user = is_valid_conn_request(data.decode())
        if not user:
            await connection.connection.close(reason="Invalid Connection")
            return

        if self.prompt_user_connect:
            try:
                resp = await self.put_wait_resp(
                    Message(id=get_id(), type=MessageTypes.connect, data=user)
                )
                if resp.type == "false":
                    raise MessageTimeoutError
            except MessageTimeoutError:
                self.out_queue.put(Message(id=id, type=MessageTypes.connect_error))
                await connection.connection.close(reason=MessageTypes.connect_error)

        # Connection successful. Add to connections pool
        connection.user = user
        self.connections.append(connection)

        while True:
            try:
                await asyncio.sleep(0.4)
                try:
                    data = self.connections_msg_queue[connection].pop(0)
                except IndexError:
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
                    self.connections_msg_queue[connection].insert(0, data)
                data: Message = self.in_queue.get(0)
                if data.type == MessageTypes.quit:
                    conn_task.cancel()
                    break
            except Exception as e:
                print(e)
