from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
import uuid
from typing import Dict, Optional

from .peer_connection import PeerConnection
from .rendezvous_client import RendezvousClient
from .types import PendingAck, PeerInfo, PeerMap

log = logging.getLogger("p2p_client")


class P2PClient:
    def __init__(
        self,
        name: str,
        namespace: str,
        listen_host: str,
        listen_port: int,
        rendezvous_host: str,
        rendezvous_port: int,
        ping_interval: float = 30.0,
        discover_interval: float = 30.0,
        ttl: int = 3600,
    ) -> None:
        self.name = name
        self.namespace = namespace
        self.peer_id = f"{name}@{namespace}"
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.rendezvous = RendezvousClient(rendezvous_host, rendezvous_port)
        self.ping_interval = ping_interval
        self.discover_interval = discover_interval
        self.ttl = ttl

        self.server: Optional[asyncio.base_events.Server] = None
        self.running = False
        self.peers: PeerMap = {}
        self.connections: Dict[str, PeerConnection] = {}
        self.pending_acks: Dict[str, PendingAck] = {}
        self._discover_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        await self.register()
        self.server = await asyncio.start_server(self._handle_incoming, host=self.listen_host, port=self.listen_port)
        addr = ", ".join(str(s.getsockname()) for s in self.server.sockets or [])
        log.info("Escutando conexões P2P em %s", addr)
        self.running = True
        self._discover_task = asyncio.create_task(self._discover_loop())

    async def stop(self) -> None:
        self.running = False
        if self._discover_task:
            self._discover_task.cancel()
        for conn in list(self.connections.values()):
            try:
                await self._send_bye(conn)
            except Exception:
                pass
            await conn.close()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        await self.unregister()

    async def register(self) -> None:
        resp = await self.rendezvous.register(self.namespace, self.name, self.listen_port, ttl=self.ttl)
        if resp.get("status") != "OK":
            raise RuntimeError(f"Falha ao registrar: {resp}")
        log.info("Registrado no rendezvous: %s", resp)

    async def unregister(self) -> None:
        try:
            resp = await self.rendezvous.unregister(self.namespace, self.name, self.listen_port)
            log.info("Unregister: %s", resp)
        except Exception as exc:
            log.warning("Falha ao desregistrar: %s", exc)

    async def _discover_loop(self) -> None:
        try:
            while self.running:
                await self.discover_and_connect()
                await asyncio.sleep(self.discover_interval)
        except asyncio.CancelledError:
            pass

    async def discover_and_connect(self) -> None:
        peers = await self.rendezvous.discover(self.namespace)
        for peer in peers:
            if peer.peer_id == self.peer_id:
                continue
            self.peers[peer.peer_id] = peer
            if peer.peer_id not in self.connections:
                await self._connect_peer(peer)

    async def _connect_peer(self, peer: PeerInfo) -> None:
        try:
            reader, writer = await asyncio.open_connection(peer.ip, peer.port)
        except Exception as exc:
            log.warning("Não foi possível conectar em %s: %s", peer.peer_id, exc)
            return
        hello = {
            "type": "HELLO",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": ["ack", "metrics"],
            "ttl": 1,
        }
        writer.write(json.dumps(hello, separators=(",", ":")).encode("utf-8") + b"\n")
        await writer.drain()
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            msg = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as exc:
            log.warning("HELLO_OK inválido de %s: %s", peer.peer_id, exc)
            writer.close()
            await writer.wait_closed()
            return
        if msg.get("type") != "HELLO_OK":
            log.warning("Resposta inesperada ao HELLO de %s: %s", peer.peer_id, msg)
            writer.close()
            await writer.wait_closed()
            return
        conn = PeerConnection(reader, writer, peer.peer_id, peer.namespace, True, self._handle_message, ping_interval=self.ping_interval)
        self.connections[peer.peer_id] = conn
        conn.start()
        log.info("Conexão estabelecida com %s", peer.peer_id)

    async def _handle_incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            hello = json.loads(raw.decode("utf-8", errors="replace"))
            if hello.get("type") != "HELLO":
                log.warning("Primeira mensagem inválida de inbound: %s", hello)
                writer.close()
                await writer.wait_closed()
                return
            peer_id = hello.get("peer_id", "")
            namespace = peer_id.split("@")[-1] if "@" in peer_id else ""
            reply = {
                "type": "HELLO_OK",
                "peer_id": self.peer_id,
                "version": "1.0",
                "features": ["ack", "metrics"],
                "ttl": 1,
            }
            writer.write(json.dumps(reply, separators=(",", ":")).encode("utf-8") + b"\n")
            await writer.drain()
            conn = PeerConnection(reader, writer, peer_id, namespace, False, self._handle_message, ping_interval=self.ping_interval)
            self.connections[peer_id] = conn
            conn.start()
            log.info("Conexão inbound de %s", peer_id)
        except Exception as exc:
            log.warning("Erro na conexão inbound: %s", exc)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_message(self, msg: Dict[str, object], conn: PeerConnection) -> None:
        mtype = msg.get("type")
        if mtype == "PING":
            await conn.send_pong(str(msg.get("msg_id", "")))
            return
        if mtype == "PONG":
            if conn.stats.last_ping:
                rtt = (time.time() - conn.stats.last_ping) * 1000
                conn.stats.update_rtt(rtt)
                log.info("RTT %s: %.1f ms", conn.peer_id, rtt)
            return
        if mtype == "SEND":
            payload = msg.get("payload")
            src = msg.get("src")
            print(f"[MSG] {src}: {payload}")
            if msg.get("require_ack"):
                await conn.send_ack(str(msg.get("msg_id")))
            return
        if mtype == "PUB":
            payload = msg.get("payload")
            src = msg.get("src")
            dst = msg.get("dst")
            print(f"[PUB {dst}] {src}: {payload}")
            return
        if mtype == "ACK":
            msg_id = str(msg.get("msg_id"))
            pending = self.pending_acks.pop(msg_id, None)
            if pending:
                pending.timeout_handle.cancel()
                pending.future.set_result(None)
            return
        if mtype == "BYE":
            reason = msg.get("reason", "")
            print(f"[BYE] {conn.peer_id} encerrou: {reason}")
            await self._send_bye_ok(conn, msg)
            await conn.close()
            self.connections.pop(conn.peer_id, None)
            return

    async def send_message(self, peer_id: str, text: str) -> None:
        conn = self.connections.get(peer_id)
        if not conn:
            raise RuntimeError(f"Sem conexão com {peer_id}")
        msg_id = str(uuid.uuid4())
        payload = {
            "type": "SEND",
            "msg_id": msg_id,
            "src": self.peer_id,
            "dst": peer_id,
            "payload": text,
            "require_ack": True,
            "ttl": 1,
        }
        await conn.send_json(payload)
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        handle = asyncio.get_running_loop().call_later(5.0, self._ack_timeout, msg_id, peer_id)
        self.pending_acks[msg_id] = PendingAck(future=fut, timeout_handle=handle)
        await fut

    async def publish(self, dst: str, text: str) -> None:
        targets = list(self.connections.values())
        for conn in targets:
            if dst.startswith("#") and conn.namespace != dst[1:]:
                continue
            payload = {
                "type": "PUB",
                "msg_id": str(uuid.uuid4()),
                "src": self.peer_id,
                "dst": dst,
                "payload": text,
                "require_ack": False,
                "ttl": 1,
            }
            await conn.send_json(payload)

    def _ack_timeout(self, msg_id: str, peer_id: str) -> None:
        pending = self.pending_acks.pop(msg_id, None)
        if pending and not pending.future.done():
            pending.future.set_exception(TimeoutError(f"ACK não recebido de {peer_id}"))

    async def _send_bye(self, conn: PeerConnection) -> None:
        payload = {
            "type": "BYE",
            "msg_id": str(uuid.uuid4()),
            "src": self.peer_id,
            "dst": conn.peer_id,
            "reason": "Encerrando sessão",
            "ttl": 1,
        }
        await conn.send_json(payload)

    async def _send_bye_ok(self, conn: PeerConnection, bye_msg: Dict[str, object]) -> None:
        payload = {
            "type": "BYE_OK",
            "msg_id": str(bye_msg.get("msg_id", "")),
            "src": self.peer_id,
            "dst": bye_msg.get("src", conn.peer_id),
            "ttl": 1,
        }
        await conn.send_json(payload)

    def list_connections(self) -> Dict[str, str]:
        return {peer_id: ("outbound" if conn.outbound else "inbound") for peer_id, conn in self.connections.items() if not conn.closed}

    def rtts(self) -> Dict[str, float]:
        data: Dict[str, float] = {}
        for peer_id, conn in self.connections.items():
            if conn.stats.avg_rtt is not None:
                data[peer_id] = conn.stats.avg_rtt
        return data


async def run_with_cli(client: P2PClient) -> None:
    loop = asyncio.get_running_loop()
    await client.start()
    stop_event = asyncio.Event()

    def _handle_sigint():
        loop.create_task(_shutdown())

    async def _shutdown():
        await client.stop()
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, _handle_sigint)

    async def cli_loop():
        while not stop_event.is_set():
            try:
                line = await loop.run_in_executor(None, sys.stdin.readline)
            except Exception:
                break
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                await handle_command(line, client, stop_event)
            except Exception as exc:
                print(f"Erro: {exc}")

    await asyncio.gather(cli_loop(), stop_event.wait())


def parse_args(argv: list[str]) -> dict:
    import argparse

    ap = argparse.ArgumentParser(description="Cliente de chat PyP2P")
    ap.add_argument("peer_name", help="Nome do peer (antes do @)")
    ap.add_argument("namespace", help="Namespace do peer")
    ap.add_argument("port", type=int, help="Porta de escuta para conexões P2P")
    ap.add_argument("--listen-host", default="0.0.0.0")
    ap.add_argument("--rdv-host", default="pyp2p.mfcaetano.cc")
    ap.add_argument("--rdv-port", type=int, default=8080)
    ap.add_argument("--ping", type=float, default=30.0, help="Intervalo de PING em segundos")
    ap.add_argument("--discover", type=float, default=30.0, help="Intervalo de discover em segundos")
    ap.add_argument("--ttl", type=int, default=3600, help="TTL do registro no rendezvous")
    return vars(ap.parse_args(argv))


async def handle_command(line: str, client: P2PClient, stop_event: asyncio.Event) -> None:
    if line == "/quit":
        await client.stop()
        stop_event.set()
        return
    if line.startswith("/msg"):
        parts = line.split(" ", 2)
        if len(parts) < 3:
            raise ValueError("Uso: /msg <peer_id> <mensagem>")
        await client.send_message(parts[1], parts[2])
        return
    if line.startswith("/pub"):
        parts = line.split(" ", 2)
        if len(parts) < 3:
            raise ValueError("Uso: /pub <*|#namespace> <mensagem>")
        await client.publish(parts[1], parts[2])
        return
    if line.startswith("/peers"):
        await client.discover_and_connect()
        for pid, info in client.peers.items():
            print(f"- {pid} {info.ip}:{info.port}")
        return
    if line.startswith("/conn"):
        for pid, direction in client.list_connections().items():
            print(f"{pid} ({direction})")
        return
    if line.startswith("/rtt"):
        rtts = client.rtts()
        for pid, rtt in rtts.items():
            print(f"{pid}: {rtt:.1f} ms")
        return
    if line.startswith("/reconnect"):
        for conn in list(client.connections.values()):
            await conn.close()
        client.connections.clear()
        await client.discover_and_connect()
        return
    if line.startswith("/log"):
        parts = line.split(" ", 1)
        level = parts[1].upper() if len(parts) > 1 else "INFO"
        logging.getLogger().setLevel(getattr(logging, level, logging.INFO))
        print(f"Log ajustado para {level}")
        return
    print("Comando desconhecido")


def main(argv: Optional[list[str]] = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args(argv or sys.argv[1:])
    client = P2PClient(
        name=args["peer_name"],
        namespace=args["namespace"],
        listen_host=args["listen_host"],
        listen_port=args["port"],
        rendezvous_host=args["rdv_host"],
        rendezvous_port=args["rdv_port"],
        ping_interval=args["ping"],
        discover_interval=args["discover"],
        ttl=args["ttl"],
    )
    try:
        asyncio.run(run_with_cli(client))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
