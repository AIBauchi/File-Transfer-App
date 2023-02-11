import hashlib
from pathlib import Path
import pytest
from ft_app.exceptions import ParseError

from ft_app.protocol import (
    Commands,
    FileInfo,
    LineTerm,
    get_filemetadata,
    is_ping,
    is_pong,
    is_valid_conn_request,
    parse_metadata,
    send_sock,
    valid_beginsend,
)
from tests.common import MockConnection, get_Connection


@pytest.mark.asyncio
async def test_send_sock():
    msg1 = "whatever"
    msg2 = b"whatever"
    connection = MockConnection("test")
    conn = get_Connection(connection)
    await send_sock(msg1, conn)
    await send_sock(msg2, conn)

    assert connection.recv_msgs[0].strip() == msg1.encode()
    assert connection.recv_msgs[1].strip() == msg2


@pytest.mark.asyncio
async def test_is_valid_conn_req():
    t1 = Commands.connect + " " + "user"
    assert is_valid_conn_request(t1) == "user"


@pytest.mark.asyncio
async def test_is_valid_conn_req_invalid():
    t1 = "whatever" + " " + "user"
    assert is_valid_conn_request(t1) is False

    t1 = Commands.connect + " " + ""
    assert is_valid_conn_request(t1) is False

    t1 = Commands.connect
    assert is_valid_conn_request(t1) is False


@pytest.mark.asyncio
async def test_get_filemetadata():
    file = Path("tests/test.txt")
    hash = hashlib.md5(bytes(file.absolute())).hexdigest()
    size = file.stat().st_size
    name = file.name

    assert get_filemetadata(file) == f"{name} {size} {hash}{LineTerm}"


@pytest.mark.asyncio
async def test_is_ping():
    assert is_ping(Commands.ping + " " + "user")
    assert is_ping(Commands.ping + " " + "") is False
    assert is_ping("Ffgfg" + " " + "user") is False


@pytest.mark.asyncio
async def test_is_pong():
    assert is_pong(Commands.pong + " " + "user")
    assert is_pong("Ffgfg" + " " + "user") is False


@pytest.mark.asyncio
async def test_parse_metadata():
    data = "file1 544 hash\r\nfile2 323 hash2\r\nfile3 1323 hash3"
    res = parse_metadata(f"{Commands.sendmetadata}\r\n{data}\r\n{Commands.endmetadata}")
    assert FileInfo(name="file2", size=323, hash="hash2") in res
    assert FileInfo(name="file1", size=544, hash="hash") in res
    assert FileInfo(name="file3", size=1323, hash="hash3") in res
    assert len(res) == 3


@pytest.mark.asyncio
async def test_parse_metadata_invalid():
    with pytest.raises(ParseError):
        data = "file1 544 hash\r\n hash2\r\nfile3 1323 hash3"
        res = parse_metadata(
            f"{Commands.sendmetadata}\r\n{data}\r\n{Commands.endmetadata}"
        )


@pytest.mark.asyncio
async def test_valid_beginsend():
    assert valid_beginsend(Commands.begin_file_send + " ddff")


@pytest.mark.asyncio
async def test_invalid_beginsend():
    with pytest.raises(ParseError):
        assert valid_beginsend(Commands.begin_file_send + "dff")
