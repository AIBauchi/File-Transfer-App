from dataclasses import dataclass
from enum import Enum
import hashlib
from pathlib import Path
import re
from typing import List, Union
from uuid import uuid4
import websockets.legacy.server as wls
import websockets.legacy.client as wlc
from ft_app.config import RETRIES

from ft_app.exceptions import ParseError

LineTerm = "\r\n"


@dataclass
class Commands:
    """Commands to exchange data between sender and recipient"""

    connect = "CONNECT"
    refuse = "REFUSE"
    sendmetadata = "FILESMETADATA"
    sendfile = "SENDFILE"
    endmetadata = "ENDSEND"
    sendsoon = "SENDSOON"
    ping = "PING"
    pong = "PONG"
    accept = "ACCEPT"
    begin_file_send = "BEGIN"
    end_file_send = "END"
    filechunk = "CHUNK"


@dataclass
class MessageTypes:
    """Message types for communicating with frontend/gui"""

    connect = "CONNECT"
    choose_connect = "CHOOSE"
    connect_error = "CONNECT_ERROR"
    error = "ERROR"
    gotmetadata = "GOTTHEMETA"
    send_file = "SENDFILES"
    sent_file = "SENTFILES"
    fileprogress = "FILEPROGRESS"
    filefailed = "FILEFAILED"
    open_candidate = "OPENCANDIDATE"
    quit = "QUIT"
    end_send = "ENDSEND"
    task = "TASK"
    connections = "CONNECTIONS"


class TaskTypes(Enum):
    send = "send"
    recv = "recv"


@dataclass
class Message:
    id: str
    type: str
    data: any = ""


@dataclass
class Connection:
    connection: Union[wls.WebSocketServerProtocol, wlc.WebSocketClientProtocol]
    user: str

    def __hash__(self) -> int:
        data = f"{self.connection.host}{self.connection.port}"
        return hash(data)


@dataclass
class ValidSender:
    ip: str
    port: int
    name: str


@dataclass
class FileInfo:
    name: str
    size: int
    hash: str


get_id = lambda: uuid4().hex


async def send_sock(msg: Union[str, bytes], conn: Connection):
    """
    Send message via connection to reciever
    """
    for _ in range(RETRIES):
        err = None
        try:
            if hasattr(msg, "encode"):
                msg = msg.encode()
            await conn.connection.send(msg + LineTerm.encode())
            break
        except Exception as e:
            err = e
        raise err


def is_valid_conn_request(msg: str) -> Union[str, bool]:
    try:
        command, arg = msg.split(" ")
    except ValueError:
        return False
    if command != Commands.connect:
        return False
    if not arg:
        return False
    return arg


def get_filemetadata(file: Path) -> str:
    name = file.name
    size = file.stat().st_size
    hash = hashlib.md5(bytes(file.absolute())).hexdigest()
    return f"{name} {size} {hash}{LineTerm}"


def is_ping(msg: Union[bytes, str]) -> bool:
    if hasattr(msg, "encode"):
        msg = msg.encode()
    resp = msg.strip().split(b" ")
    return bool(resp[0] == Commands.ping.encode() and len(resp) > 1)


def is_pong(msg: str) -> bool:
    resp = msg.strip().split(" ")
    return bool(resp[0] == Commands.pong and len(resp) > 1)


def is_accepted(msg: str) -> bool:
    resp = msg.strip().split(" ")
    return resp[0] == Commands.accept


def parse_metadata(data: str) -> List[FileInfo]:
    """
    Parse metadata and get files info to be recieved.
    """
    parsed_metadata = []
    data = data.strip()
    match = re.match(
        f"{Commands.sendmetadata}\r\n((.*? \d+ \w+\r\n)+){Commands.endmetadata}",
        data,
        re.M,
    )
    if match is None:
        raise ParseError("Invalid metadata")
    data = match.groups(0)[0]
    data = match.groups(0)[0].strip().split(LineTerm)
    for item in data:
        try:
            name, size, hash = item.strip().split(" ")
            size = int(size)
        except ValueError:
            raise ParseError("parsing metadata" + item)
        parsed_metadata.append(FileInfo(name=name, size=size, hash=hash))
    return parsed_metadata


def valid_beginsend(data: str) -> str:
    data = data.split(" ")
    if len(data) != 2:
        raise ParseError(f"Unexpected command '{data}'")
    command, hash = data
    if command != Commands.begin_file_send:
        raise ParseError(f"Unexpected command '{command}'")
    return hash.strip()
