import asyncio
import bz2
import math
import os
from pathlib import Path
import time

import pytest

from ft_app.exceptions import MessageTimeoutError, ParseError
from ft_app.protocol import (
    Commands,
    LineTerm,
    Message,
    MessageTypes,
    get_filemetadata,
)
from tests.common import MockConnection, get_base, get_Connection


@pytest.mark.asyncio
async def test_wait_for_msgid():
    sock, in_queue, _ = await get_base("")
    msg = Message(id="1", type="Hello", data="Hello")
    in_queue.put(msg)
    message = await sock._wait_for_msg_id("1")
    assert msg == message


@pytest.mark.asyncio
@pytest.mark.skip
async def test_wait_for_msgid_tout():
    sock, in_queue, _ = await get_base("")
    msg = Message(id="1", type="Hello", data="Hello")
    in_queue.put(msg)
    t = time.time()
    tout = 2
    with pytest.raises(MessageTimeoutError):
        await sock._wait_for_msg_id("12", tout)
    assert round(time.time() - t) == tout


@pytest.mark.asyncio
async def test__recv():
    msg = b"Whatever"
    connection = MockConnection("test").expects(msg)
    sock, _, _ = await get_base("", connection)
    msg2 = await sock._recv(get_Connection(connection))
    assert msg == msg2


@pytest.mark.asyncio
async def test__recv_ping_pong():
    msg = b"Whatever"
    connection = MockConnection("test").expects(msg).expects(Commands.ping + " user")
    sock, _, _ = await get_base("", connection)
    msg2 = await sock._recv(get_Connection(connection))
    comm = connection.recv_msgs[0].strip().decode().split(" ")[0]
    assert comm == Commands.pong
    assert msg == msg2


@pytest.mark.asyncio
async def test__send():
    msg = "Whatever"
    connection = MockConnection("test").expects(msg).expects(Commands.ping)
    sock, _, _ = await get_base("", connection)
    sock.connections = [get_Connection(connection)]
    await sock._send(msg)
    assert connection.recv_msgs[0].strip() == msg.encode()


@pytest.mark.asyncio
async def test__send_multi():
    msg = "Whatever"
    connection = MockConnection("test").expects(msg).expects(Commands.ping)
    sock, _, _ = await get_base("", connection)
    connection2 = MockConnection("test2").expects(msg).expects(Commands.ping)
    sock.connections_msg_queue[get_Connection(connection2)] = []
    conn_task = await sock._conc_recv(get_Connection(connection2))
    sock.connections = [get_Connection(connection), get_Connection(connection2)]
    await sock._send(msg)
    assert connection.recv_msgs[0].strip() == msg.encode()
    assert connection2.recv_msgs[0].strip() == msg.encode()


@pytest.mark.asyncio
async def test_put_wait_resp():
    sock, in_queue, _ = await get_base("")
    msg = Message(id="1", type="Hello", data="Hello")
    in_queue.put(msg)
    message = await sock.put_wait_resp(msg)
    assert msg == message


@pytest.mark.asyncio
async def test__gen_file_chunks():
    sock, _, _ = await get_base("")
    file = Path("tests") / "test.txt"
    chunksize = 20
    gen = sock._gen_file_chunks(file, chunksize)
    c = 0
    while True:
        try:
            _ = await gen.__anext__()
        except StopAsyncIteration:
            break
        c += 1
    assert c == math.ceil(file.stat().st_size / chunksize)


@pytest.mark.asyncio
async def test__gen_file_chunks_bzipped():
    sock, _, _ = await get_base("")
    file = Path("tests") / "test.txt"
    chunksize = 20
    gen = sock._gen_file_chunks_bzipped(file, chunksize)
    c = 0
    while True:
        try:
            data = await gen.__anext__()
            comp = bz2.BZ2Decompressor()
            comp.decompress(data)
        except (StopAsyncIteration, RuntimeError):
            break
        c += 1
    assert c == math.ceil(file.stat().st_size / chunksize)


@pytest.mark.asyncio
async def test__gen_file_chunks_bzipped_no_file():
    sock, _, _ = await get_base("")
    file = Path("tests") / "tst.txt"
    chunksize = 20
    gen = sock._gen_file_chunks_bzipped(file, chunksize)
    c = 0
    comp = bz2.BZ2Decompressor()
    while True:
        try:
            data = await gen.__anext__()
            comp.decompress(data)
        except (StopAsyncIteration, RuntimeError):
            break
        c += 1
    assert c == 0


@pytest.mark.asyncio
async def test_send_filedata():
    msg = "Whatever"
    connection = MockConnection("34est")
    sock, _, _ = await get_base("", connection)
    sock.connections = [get_Connection(connection)]
    files = ["tests/test.txt", "tests/test2.txt"]
    await sock.send_filedata(files)
    begin = f"{Commands.sendmetadata}{LineTerm}"
    metadata = "".join(
        [get_filemetadata(Path(file)) for file in files if Path(file).is_file]
    )
    end = f"{Commands.endmetadata}"
    assert connection.recv_msgs[0].strip().decode() == begin + metadata + end


@pytest.mark.asyncio
async def test__send_files():
    msg = "Whatever"
    connection = MockConnection("34est")
    sock, _, _ = await get_base("", connection)
    sock.connections = [get_Connection(connection)]

    # Send all the chunk acknowledgements
    expects = reversed(range(0, 100))
    for i in expects:
        connection = connection.expects(str(i))
    files = [Path("tests/test.txt"), Path("tests/test2.txt")]
    await sock._send_files(files, 100)
    data = {}
    hash = ""
    for i in connection.recv_msgs:
        if i.startswith(Commands.begin_file_send.encode()):
            _, hash = i.strip().decode().split(" ")
            data[hash] = b""
            continue
        if i.startswith(Commands.end_file_send.encode()):
            continue
        dec = bz2.BZ2Decompressor()
        # raise Exception(i)
        i, *body = i.strip().split(b" ")
        body = b" ".join(body)
        data[hash] += dec.decompress(body.strip())
    for f in files:
        assert open(f, "rb").read() in data.values()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_send_files():
    msg = "Whatever"
    connection = MockConnection("34est")
    sock, in_queue, _ = await get_base("", connection)
    sock.connections = [get_Connection(connection)]

    # Send all the chunk acknowledgements
    expects = reversed(range(0, 100))
    for i in expects:
        connection = connection.expects(str(i))
    files = ["tests/test.txt", "tests/test2.txt"]
    msg = Message(type=MessageTypes.send_file, id="", data=files)
    end = Message(type=MessageTypes.end_send, id="", data="")
    in_queue.put(msg)
    in_queue.put(end)
    await sock.send_files(100)
    data = {}
    hash = ""
    recving_file = False
    assert Commands.sendsoon.encode() == connection.recv_msgs[0].strip()
    for i in connection.recv_msgs:
        if i.startswith(Commands.begin_file_send.encode()):
            _, hash = i.strip().decode().split(" ")
            data[hash] = b""
            recving_file = True
            continue
        if i.startswith(Commands.end_file_send.encode()):
            recving_file = False
            continue
        if recving_file:
            dec = bz2.BZ2Decompressor()
            i, *body = i.strip().split(b" ")
            body = b" ".join(body)
            data[hash] += dec.decompress(body.strip())
    for f in files:
        assert open(f, "rb").read() in data.values()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_receive_files():
    for i in os.listdir("tests/data"):
        p = Path("tests") / "data" / i
        if not p.is_dir():
            os.remove(str(p))
    files = ["tests/test.txt", "tests/test2.txt"]
    connection = MockConnection("34est")
    sock, _, out_queue = await get_base("", connection)

    sock.connections = [get_Connection(connection)]

    # Send all the chunk acknowledgements
    expects = reversed(range(0, 50))
    for i in expects:
        connection = connection.expects(str(i))
    files2 = [Path("tests/test.txt"), Path("tests/test2.txt")]
    await sock._send_files(files2, 100)
    file_send_data = connection.recv_msgs
    connection.recv_msgs = []
    connection.next_msgs = []

    # Send metadata
    await sock.send_filedata(files)
    metadata = connection.recv_msgs.pop()
    connection = MockConnection("34est")
    sock, _, out_queue = await get_base("", connection)

    sock.connections = [get_Connection(connection)]
    for data in reversed(file_send_data):
        connection = connection.expects(data)
    connection = connection.expects(metadata)

    try:
        await sock.receive_files(get_Connection(connection))
    except (asyncio.exceptions.CancelledError, asyncio.exceptions.TimeoutError):
        pass
    c = []
    while True:
        try:
            c.append(out_queue.get_nowait())
        except:
            break
    contents = [open(f).read() for f in files]
    test_contents = [
        open(f).read()
        for f in [
            Path("tests") / "data" / "test.txt",
            Path("tests") / "data" / "test2.txt",
        ]
    ]
    assert set(test_contents) == set(contents)


@pytest.mark.asyncio
async def test_receive_files_nosendsoon():

    files = ["tests/test.txt", "tests/test2.txt"]
    connection = MockConnection("34est")
    sock, _, _ = await get_base("", connection)
    sock.connections = [get_Connection(connection)]

    # Send all the chunk acknowledgements
    expects = reversed(range(0, 100))
    for i in expects:
        connection = connection.expects(str(i))
    files2 = [Path("tests/test.txt"), Path("tests/test2.txt")]
    await sock._send_files(files2, 100)
    file_send_data = connection.recv_msgs
    connection.recv_msgs = []

    # Send metadata
    await sock.send_filedata(files)
    metadata = connection.recv_msgs.pop()
    for data in reversed(file_send_data):
        connection = connection.expects(data)
    connection = connection.expects(metadata)
    with pytest.raises(ParseError):
        await sock.receive_files(get_Connection(connection))


# @pytest.mark.asyncio
# async def test_receive_files_nometa():
#     sock, in_queue, out_queue = get_base("")
#     for i in os.listdir("tests/data"):
#         os.unlink(Path("tests") / "data" / i)
#     files = ["tests/test.txt", "tests/test2.txt"]
#     connection = MockConnection("34est")

#     sock.connections = [get_Connection(connection)]

#     # Send all the chunk acknowledgements
#     expects = reversed(range(0, 100))
#     for i in expects:
#         connection = connection.expects(str(i))
#     files2 = [Path("tests/test.txt"), Path("tests/test2.txt")]
#     await sock._send_files(files2, 100)
#     file_send_data = connection.recv_msgs
#     connection.recv_msgs = []

#     # Send metadata
#     await sock.send_filedata(files)
#     metadata = connection.recv_msgs.pop()
# for data in reversed(file_send_data):
#     connection = connection.expects(data)
# connection = connection.expects(metadata)
# connection = connection.expects(Commands.sendsoon.encode())
# await sock.receive_files(get_Connection(connection))
