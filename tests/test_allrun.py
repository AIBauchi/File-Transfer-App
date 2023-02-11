import asyncio
import os
from pathlib import Path
import queue
import pytest

from ft_app.client import WebSocketClient
from ft_app.protocol import Message, MessageTypes
from ft_app.server import WebSocketServer


@pytest.mark.asyncio
async def test_start():
    for i in os.listdir("tests/data/start_test"):
        p = Path("tests") / "data" / "start_test" / i
        os.remove(str(p))
    loop = asyncio.get_event_loop()

    in_queue_cli = queue.Queue()
    out_queue_cli = queue.Queue()
    client = WebSocketClient(
        "127.0.0.2",
        out_queue_cli,
        in_queue_cli,
        recv_destination="tests/data/start_test",
    )

    in_queue_serve = queue.Queue()
    out_queue_serve = queue.Queue()
    server = WebSocketServer(
        "127.0.0.2",
        out_queue_serve,
        in_queue_serve,
        recv_destination="tests/data/start_test",
    )
    server.prompt_user_connect = False
    t2 = server.run_server()
    t1 = loop.create_task(client.main("127.0.0.", 5))
    in_queue_cli.put(
        Message(
            id="ok", type=MessageTypes.connect, data=f"ws://127.0.0.2:{client.port}"
        )
    )
    files = ["tests/test.txt", "tests/test2.txt"]
    msg = Message(type=MessageTypes.send_file, id="", data=files)
    end = Message(type=MessageTypes.end_send, id="", data="")
    in_queue_serve.put(msg)
    await asyncio.sleep(2)
    await server.send(100)
    for _ in range(10):
        await asyncio.sleep(1)
    contents = [open(f).read() for f in files]
    timeout = 10
    while True:
        if not timeout:
            assert False
        await asyncio.sleep(1)
        timeout -= 1
        try:
            test_contents = [
                open(f).read()
                for f in [
                    Path("tests") / "data" / "start_test" / "test.txt",
                    Path("tests") / "data" / "start_test" / "test2.txt",
                ]
            ]
            if set(test_contents) == set(contents):
                break
        except Exception as e:
            pass
    in_queue_serve.put(end)
    in_queue_cli.put(Message(id="ok", type=MessageTypes.quit))
    in_queue_serve.put(Message(id="ok", type=MessageTypes.quit))
    t1.cancel()
    t2.cancel()
    for t in client.tasks + server.tasks:
        t.cancel()
    await asyncio.sleep(2)
