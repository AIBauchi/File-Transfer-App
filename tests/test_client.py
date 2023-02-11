import asyncio

import websockets
import queue
import pytest
from ft_app.base import BaseWebSocket

from ft_app.client import WebSocketClient
from ft_app.protocol import Commands, MessageTypes


@pytest.mark.asyncio
async def test_scan():
    in_queue = queue.Queue()
    out_queue = queue.Queue()
    client = WebSocketClient("", out_queue, in_queue)

    async def ping_pong(host):
        async def handler(websocket, path):
            _ = await websocket.recv()
            greeting = f"{Commands.pong} user"
            await websocket.send(greeting.encode())

        start_server = websockets.serve(handler, host, BaseWebSocket.port)
        # raise Exception(dir(start_server))
        await start_server

    loop = asyncio.get_event_loop()
    test_ips = ["127.0.0.1", "127.0.0.3", "127.0.0.10"]
    test_ips_ws = [f"ws://{i}:{client.port}" for i in test_ips]
    tasks = [loop.create_task(ping_pong(i)) for i in test_ips]
    await client.scan_receiver(prefix="127.0.0.")
    seen = []
    while True:
        try:
            m = out_queue.get(0)
            if m.type != MessageTypes.open_candidate:
                assert False
            seen.append(m.data)
        except:
            break
    assert set(test_ips_ws) == set(seen)
    [t.cancel() for t in tasks]


@pytest.mark.asyncio
async def test_scan_nopong():
    in_queue = queue.Queue()
    out_queue = queue.Queue()
    client = WebSocketClient("", out_queue, in_queue)
    await client.scan_receiver(prefix="127.0.0.")
    seen = []
    while True:
        try:
            m = out_queue.get(0)
            if m.type != MessageTypes.open_candidate:
                assert False
            seen.append(m.data)
        except:
            break
    assert seen == []
