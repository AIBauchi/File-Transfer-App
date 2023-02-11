import hashlib
import os
import asyncio
import bz2
from typing import AsyncGenerator, Dict
import math
from pathlib import Path
from queue import Empty, Queue
import socket
from time import time
from typing import List
from uuid import uuid4
from ft_app.config import CHUNKSIZE, RETRIES
from ft_app.exceptions import MessageTimeoutError, NoMetadata

from ft_app.protocol import (
    Commands,
    Connection,
    LineTerm,
    Message,
    MessageTypes,
    TaskTypes,
    is_ping,
    parse_metadata,
    get_id,
    get_filemetadata,
    send_sock,
    valid_beginsend,
)


class BaseWebSocket:
    port: int = 44455

    def __init__(
        self,
        ip: str,
        out_queue: Queue,
        in_queue: Queue,
        recv_destination: str = "./data",
    ) -> None:
        self.task_queue: Queue = Queue(200)
        self.tasks: List[asyncio.Task] = []
        self.connections_msg_queue: Dict[Connection, list] = {}
        self.ip = ip
        self.in_queue = in_queue
        self.recv_destination = recv_destination
        self.out_queue = out_queue
        self.connections: List[Connection] = []
        self.connection: Connection = None
        self.fqdn = socket.getfqdn()

        # Lock for receiving
        self.lock = asyncio.Lock()

    async def _wait_for_msg_id(self, id: str, timeout: int = 500) -> Message:
        """
        Wait for message from frontend with id `id`
        """
        t = time()
        while time() - t < timeout:
            try:
                msg: Message = self.in_queue.get_nowait()
                if msg.id != id:
                    self.in_queue.put(msg)
                else:
                    return msg
                await asyncio.sleep(1)
            except Empty:
                pass

        raise MessageTimeoutError(f"Waiting for message {id} response")

    async def _recv(self, connection: Connection):
        """
        Receive from connection
        """
        while True:
            await asyncio.sleep(0.3)
            if not self.connections_msg_queue.get(connection):
                self.connections_msg_queue[connection] = []
            try:
                data = self.connections_msg_queue[connection].pop(0)
            except Exception as e:
                continue
            if is_ping(data.strip()):
                await send_sock(f"{Commands.pong} {self.fqdn}".encode(), connection)
            else:
                break
        return data

    async def _send(self, msg: str):
        """
        Send message to all connections
        """
        for conn in self.connections:
            await send_sock(msg.encode(), conn)

    async def put_wait_resp(self, msg: Message, timeout: int = 120) -> Message:
        self.out_queue.put(msg)
        return await self._wait_for_msg_id(id=msg.id, timeout=timeout)

    async def _read_file_chunks_bziped(self, data: bytes) -> bytes:
        """
        Read and decompress file
        """
        comp = bz2.BZ2Decompressor()
        return comp.decompress(data)

    async def _gen_file_chunks(
        self, file: Path, chunksize: int = 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Generate file chunks for compression and sending
        """
        with open(file.absolute(), "rb") as f:
            while True:
                r = f.read(chunksize)
                if not r:
                    return
                yield r

    async def _gen_file_chunks_bzipped(
        self, file: Path, chunksize: int = 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Generated compressed file chunks
        """
        content_iter = self._gen_file_chunks(file, chunksize)

        while True:
            try:
                content = await content_iter.__anext__()
            except (FileNotFoundError, StopAsyncIteration):
                return
            if not content:
                yield None
            comp = bz2.BZ2Compressor()
            comp.compress(content)
            yield comp.flush()

    async def send_filedata(self, files: list):
        """
        Send all files metadata before sending files
        """
        begin = f"{Commands.sendmetadata}{LineTerm}"
        metadata = "".join(
            [get_filemetadata(Path(file)) for file in files if Path(file).is_file]
        )
        end = f"{Commands.endmetadata}"

        await self._send(begin + metadata + end)

    async def _send_files(self, files: List[Path], chunksize: int = CHUNKSIZE):
        """
        Send files to all connections
        """
        for file in files:
            for conn in self.connections:
                chunk = self._gen_file_chunks_bzipped(file, chunksize)
                hash = hashlib.md5(str(file.absolute()).encode()).hexdigest()
                await send_sock(f"{Commands.begin_file_send} {hash}", conn)
                progress_i = 0
                total = math.ceil(file.stat().st_size / 1024)
                while True:
                    try:
                        chunk_data = await chunk.__anext__()
                    except StopAsyncIteration:
                        chunk_data = None
                    if chunk_data is None:
                        break
                    progress = int(progress_i * 100 / total)
                    for _ in range(RETRIES):
                        try:
                            await send_sock(
                                str(progress_i).encode() + b" " + chunk_data, conn
                            )

                            data = await asyncio.wait_for(self._recv(conn), 2)

                            try:
                                assert int(data.strip()) == progress_i
                            except (AssertionError, ValueError):
                                self.out_queue.put(
                                    Message(id=get_id(), type=MessageTypes.filefailed)
                                )
                                return
                            self.out_queue.put(
                                Message(
                                    id=get_id(),
                                    type=MessageTypes.fileprogress,
                                    data={
                                        "id": str(file.absolute()),
                                        "progress": progress,
                                    },
                                )
                            )
                            break
                        except Exception as e:

                            pass
                    else:
                        raise MessageTimeoutError
                    progress_i += 1
                await send_sock(f"{Commands.end_file_send} {hash}", conn)

    async def send_files(self, chunksize: int = CHUNKSIZE):

        await self._send(Commands.sendsoon)
        await asyncio.sleep(4)
        while True:
            await asyncio.sleep(0.2)
            try:
                msg: Message = self.in_queue.get_nowait()
                if msg.type == MessageTypes.end_send:
                    break
                if msg.type != MessageTypes.send_file:
                    self.in_queue.put(msg)
                    continue
            except Empty:
                await asyncio.sleep(0.5)
                continue
            files: List[str] = msg.data

            await self.send_filedata(files)
            await self._send_files([Path(file) for file in files], chunksize)
            self.out_queue.put(
                Message(id=uuid4().hex, data=files, type=MessageTypes.sent_file)
            )

    async def receive_files(self, connection: Connection):
        """
        Receive files from connection
        """
        target_directory = Path(self.recv_destination)
        metadata = await self._recv(connection)
        hash_metadata = {
            meta.hash: meta for meta in parse_metadata(metadata.strip().decode())
        }

        self.out_queue.put(
            Message(id=get_id(), type=MessageTypes.gotmetadata, data=metadata)
        )
        sent = 0
        async with self.lock:
            while True:
                if sent >= len(hash_metadata):
                    break
                data = await self._recv(connection)

                hash = valid_beginsend(data.decode())

                metadata = hash_metadata.get(hash)
                if metadata is None:
                    raise NoMetadata(hash)
                chunk = b""
                last_i = -1
                failed = False
                # If file exists append timestamp to name
                if os.path.exists(target_directory / metadata.name):
                    metadata.name = metadata.name + "-" + str(int(time()))

                file_dest = target_directory / metadata.name
                fp = open(file_dest, "wb")
                sent += 1
                try:

                    while f"{Commands.end_file_send} {hash}".encode() not in chunk:
                        if chunk:
                            progress_i, *data = chunk.split(b" ")
                            progress_i = int(progress_i)
                            data = b" ".join(data)
                            if not data:
                                return

                            # Fail if received out of other
                            if progress_i != last_i + 1:
                                failed = True
                                raise TypeError(progress_i, last_i)

                            fp.write(await self._read_file_chunks_bziped(data))
                            progress = progress_i * CHUNKSIZE / metadata.size

                            self.out_queue.put(
                                Message(
                                    id=get_id(),
                                    type=MessageTypes.fileprogress,
                                    data={"id": hash, "progress": progress},
                                )
                            )

                            await send_sock(str(progress_i), connection)
                            last_i += 1

                        chunk = await asyncio.wait_for(self._recv(connection), 2)

                except Exception as e:

                    raise e
                finally:
                    fp.close()

                if failed:
                    os.unlink(file_dest)
                    self.out_queue.put(
                        Message(id=get_id(), type=MessageTypes.filefailed)
                    )
                    send_sock("FAILED", connection)
                else:
                    try:
                        fp.flush()
                    except:
                        pass

    async def receive_one(self):
        """
        Wait for user to connect and recieve one round of files
        """
        await asyncio.sleep(1)
        connection = self.connections[0]
        while True:
            try:
                await asyncio.sleep(0.6)
                ret = await self.receive_files(connection)
                break
            except Exception as e:
                pass

    async def _conc_recv(self, connection: Connection) -> asyncio.Task:
        """
        Start a task to recieve messages and add to message queue
        """

        async def _get_msg(connection: Connection):
            while True:
                data = ""
                try:
                    data = await connection.connection.recv()
                    self.connections_msg_queue[connection].append(data)
                except IndexError:
                    pass
                await asyncio.sleep(0.1)

        return asyncio.create_task(_get_msg(connection))

    async def send(self, chunksize: int = CHUNKSIZE):
        """
        Create and push a send file task to task queue
        """
        self.task_queue.put(
            Message(
                id=get_id(),
                type=MessageTypes.task,
                data={"type": TaskTypes.send.value, "args": [chunksize]},
            )
        )

    async def _dispatch_tasks(self):
        """
        Execute task in queue
        """
        while True:
            try:
                task: Message = self.task_queue.get_nowait()
                if task.type != MessageTypes.task:
                    continue
            except Empty:
                await asyncio.sleep(1)
                continue
            type = task.data["type"]
            if type == TaskTypes.recv.value:
                await self.receive_one()
                self.transferring = False
            elif type == TaskTypes.send.value:
                await self.send_files(*task.data["args"])
            else:
                continue
