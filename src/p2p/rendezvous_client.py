from __future__ import annotations

import asyncio
import json
import socket
from typing import Any, Dict, List, Optional

from .types import PeerInfo


class RendezvousError(Exception):
    pass


class RendezvousClient:
    def __init__(self, host: str, port: int, timeout: float = 5.0) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout

    async def _send_command(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        return await asyncio.to_thread(self._send_blocking, line)

    def _send_blocking(self, line: str) -> Dict[str, Any]:
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            sock.sendall(line.encode("utf-8"))
            data = self._recv_line(sock)
        try:
            return json.loads(data)
        except Exception as exc:  # pragma: no cover - proteção extra
            raise RendezvousError(f"Resposta inválida: {data}") from exc

    def _recv_line(self, sock: socket.socket) -> str:
        sock.settimeout(self.timeout)
        buf = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
            if b"\n" in buf:
                line, _rest = buf.split(b"\n", 1)
                return line.decode("utf-8", errors="replace")
        return buf.decode("utf-8", errors="replace")

    async def register(self, namespace: str, name: str, port: int, ttl: int = 3600) -> Dict[str, Any]:
        payload = {"type": "REGISTER", "namespace": namespace, "name": name, "port": port, "ttl": ttl}
        return await self._send_command(payload)

    async def unregister(self, namespace: str, name: str, port: int) -> Dict[str, Any]:
        payload = {"type": "UNREGISTER", "namespace": namespace, "name": name, "port": port}
        return await self._send_command(payload)

    async def discover(self, namespace: Optional[str] = None) -> List[PeerInfo]:
        payload: Dict[str, Any] = {"type": "DISCOVER"}
        if namespace:
            payload["namespace"] = namespace
        response = await self._send_command(payload)
        peers = []
        for p in response.get("peers", []):
            peers.append(
                PeerInfo(
                    name=p.get("name", ""),
                    namespace=p.get("namespace", ""),
                    ip=p.get("ip", ""),
                    port=int(p.get("port", 0)),
                    ttl=int(p.get("ttl", 0)),
                    expires_in=p.get("expires_in"),
                )
            )
        return peers
