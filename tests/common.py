from queue import Queue
from typing import Tuple

from ft_app.base import BaseWebSocket
from ft_app.protocol import Connection


async def get_base(
    ip: str, connection: Connection = None
) -> Tuple[BaseWebSocket, Queue, Queue]:
    in_queue = Queue()
    out_queue = Queue()

    sock = BaseWebSocket(ip, out_queue, in_queue, recv_destination="./tests/data")
    if connection:
        sock.connections_msg_queue[get_Connection(connection)] = []
        conn_task = await sock._conc_recv(get_Connection(connection))
    return sock, in_queue, out_queue


def get_Connection(conn, user="") -> Connection:
    return Connection(connection=conn, user=user)


class MockConnection:
    def __init__(self, id) -> None:
        self.id = id
        self.next_msgs = []
        self.recv_msgs = []
        self.host = "test"
        self.port = 4444

        # If True. Don't store sent messages
        self.lock_send = False

    def expects(self, msg: str):
        """
        Store mock response.
        """
        self.next_msgs.append(msg)
        return self

    async def recv(self):
        data = self.next_msgs.pop(-1)
        if hasattr(data, "encode"):
            data = data.encode()
        return data

    async def send(self, msg):
        self.recv_msgs.append(msg)
