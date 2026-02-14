from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class ClientSession:
    """Represents a single logical client connection.

    The registry does not depend on any concrete network implementation and
    can therefore be reused across TCP, WebSocket or other transports.
    """

    client_id: str
    group_ids: Set[str] = field(default_factory=set)
    connected_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    messages_sent: int = 0
    acks_received: int = 0


@dataclass
class RegistryStats:
    total_clients: int
    total_groups: int
    messages_sent: int
    acks_received: int

    @property
    def ack_rate(self) -> float:
        if self.messages_sent == 0:
            return 0.0
        return self.acks_received / float(self.messages_sent)


class ConnectionRegistry:
    """In-memory registry that tracks online clients and groups.

    This is a minimal stand-in for a more advanced shared store.
    """

    def __init__(self) -> None:
        self._clients: Dict[str, ClientSession] = {}
        self._groups: Dict[str, Set[str]] = {}
        self._lock = threading.Lock()

    # ---- client lifecycle -------------------------------------------------
    def register_client(self, client_id: str, group_id: Optional[str] = None) -> ClientSession:
        with self._lock:
            session = self._clients.get(client_id)
            if session is None:
                session = ClientSession(client_id=client_id)
                self._clients[client_id] = session
            session.last_heartbeat = time.time()
            if group_id:
                self.add_client_to_group(client_id, group_id)
            return session

    def unregister_client(self, client_id: str) -> None:
        with self._lock:
            session = self._clients.pop(client_id, None)
            if not session:
                return
            # Remove from all groups
            for gid, members in list(self._groups.items()):
                members.discard(client_id)
                if not members:
                    self._groups.pop(gid, None)

    def heartbeat(self, client_id: str) -> None:
        with self._lock:
            session = self._clients.get(client_id)
            if session:
                session.last_heartbeat = time.time()

    # ---- groups -----------------------------------------------------------
    def add_client_to_group(self, client_id: str, group_id: str) -> None:
        with self._lock:
            session = self._clients.setdefault(client_id, ClientSession(client_id=client_id))
            session.group_ids.add(group_id)
            group = self._groups.setdefault(group_id, set())
            group.add(client_id)

    def remove_client_from_group(self, client_id: str, group_id: str) -> None:
        with self._lock:
            session = self._clients.get(client_id)
            if session:
                session.group_ids.discard(group_id)
            members = self._groups.get(group_id)
            if members:
                members.discard(client_id)
                if not members:
                    self._groups.pop(group_id, None)

    def get_group_members(self, group_id: str) -> List[str]:
        with self._lock:
            members = self._groups.get(group_id, set())
            return list(members)

    # ---- stats & queries --------------------------------------------------
    def list_online_clients(self) -> List[str]:
        with self._lock:
            return list(self._clients.keys())

    def record_message_sent(self, client_id: str) -> None:
        with self._lock:
            session = self._clients.get(client_id)
            if session:
                session.messages_sent += 1

    def record_ack_received(self, client_id: str) -> None:
        with self._lock:
            session = self._clients.get(client_id)
            if session:
                session.acks_received += 1

    def get_stats(self) -> RegistryStats:
        with self._lock:
            total_clients = len(self._clients)
            total_groups = len(self._groups)
            messages_sent = sum(s.messages_sent for s in self._clients.values())
            acks_received = sum(s.acks_received for s in self._clients.values())
        return RegistryStats(
            total_clients=total_clients,
            total_groups=total_groups,
            messages_sent=messages_sent,
            acks_received=acks_received,
        )
