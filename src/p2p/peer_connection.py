from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Awaitable, Callable, Dict, Optional

from .types import ConnectionStats

log = logging.getLogger("peer_connection")


MessageHandler = Callable[[Dict[str, object], "PeerConnection"], Awaitable[None]]


class PeerConnection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_id: str,
        namespace: str,
        outbound: bool,
        handler: MessageHandler,
        ping_interval: float = 30.0,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.peer_id = peer_id
        self.namespace = namespace
        self.outbound = outbound
        self.handler = handler
        self.ping_interval = ping_interval
        self.stats = ConnectionStats()
        self._read_task: Optional[asyncio.Task[None]] = None
        self._ping_task: Optional[asyncio.Task[None]] = None
        self._closed = asyncio.Event()

    def start(self) -> None:
        self._read_task = asyncio.create_task(self._reader_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _reader_loop(self) -> None:
        try:
            while not self.reader.at_eof():
                raw = await self.reader.readline()
                if not raw:
                    break
                try:
                    msg = json.loads(raw.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    log.warning("Mensagem inválida de %s: %r", self.peer_id, raw)
                    continue
                await self.handler(msg, self)
        except asyncio.CancelledError:
            pass
        finally:
            self._closed.set()

    async def _ping_loop(self) -> None:
        try:
            while not self._closed.is_set():
                await asyncio.sleep(self.ping_interval)
                await self.send_ping()
        except asyncio.CancelledError:
            pass

    async def send_json(self, payload: Dict[str, object]) -> None:
        if self.writer.is_closing():
            raise ConnectionError("Conexão encerrada")
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        self.writer.write(line.encode("utf-8"))
        await self.writer.drain()

    async def send_ping(self) -> None:
        msg_id = str(uuid.uuid4())
        self.stats.last_ping = time.time()
        await self.send_json({"type": "PING", "msg_id": msg_id, "timestamp": time.time(), "ttl": 1})

    async def send_pong(self, msg_id: str) -> None:
        await self.send_json({"type": "PONG", "msg_id": msg_id, "timestamp": time.time(), "ttl": 1})

    async def send_ack(self, msg_id: str) -> None:
        await self.send_json({"type": "ACK", "msg_id": msg_id, "timestamp": time.time(), "ttl": 1})

    async def close(self) -> None:
        if self.writer.is_closing():
            return
        if self._ping_task:
            self._ping_task.cancel()
        if self._read_task:
            self._read_task.cancel()
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass
        self._closed.set()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def rtt(self) -> Optional[float]:
        return self.stats.avg_rtt
