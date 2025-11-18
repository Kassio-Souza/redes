from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class PeerInfo:
    name: str
    namespace: str
    ip: str
    port: int
    ttl: int = 0
    expires_in: Optional[int] = None

    @property
    def peer_id(self) -> str:
        return f"{self.name}@{self.namespace}"


@dataclass
class ConnectionStats:
    last_ping: Optional[float] = None
    rtt_samples: list[float] = field(default_factory=list)

    def update_rtt(self, rtt: float) -> None:
        self.rtt_samples.append(rtt)
        # mantém histórico curto
        if len(self.rtt_samples) > 50:
            self.rtt_samples.pop(0)

    @property
    def avg_rtt(self) -> Optional[float]:
        if not self.rtt_samples:
            return None
        return sum(self.rtt_samples) / len(self.rtt_samples)


@dataclass
class PendingAck:
    future: asyncio.Future[None]
    timeout_handle: asyncio.TimerHandle


PeerMap = Dict[str, PeerInfo]
