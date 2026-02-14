from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, Optional

from autotest_open.core.registry import ConnectionRegistry
from autotest_open.models.message import Message


class MessageDispatcher:
    """Dispatches downstream messages to connected clients.

    The dispatcher itself is transport-agnostic and relies on a mapping from
    ``client_id`` to writer-like objects maintained by the gateway.

    For TCP-based gateways the writer is typically an ``asyncio.StreamWriter``.
    For WebSocket-based gateways it can be a WebSocket protocol object with a
    ``send`` coroutine method. The concrete type is detected at runtime.
    """

    def __init__(self, registry: ConnectionRegistry) -> None:
        self._registry = registry
        # This mapping is populated by the gateway when new clients connect.
        self._writers: Dict[str, Any] = {}

    # ---- registration from gateway ---------------------------------------
    def attach(self, client_id: str, writer: Any) -> None:
        self._writers[client_id] = writer

    def detach(self, client_id: str) -> None:
        self._writers.pop(client_id, None)

    # ---- downstream sending ----------------------------------------------
    async def _send_message(self, msg: Message) -> None:
        writer = self._writers.get(msg.client_id or "")
        if not writer:
            return

        # TCP: asyncio.StreamWriter-like interface (write/drain).
        if hasattr(writer, "write"):
            data = msg.to_json_line().encode("utf-8")
            writer.write(data)
            drain = getattr(writer, "drain", None)
            if callable(drain):
                await drain()
        else:
            # WebSocket-style interface with an async ``send`` method.
            send = getattr(writer, "send", None)
            if callable(send):
                await send(msg.to_json())
            else:  # pragma: no cover - defensive fallback
                return

        if msg.client_id:
            self._registry.record_message_sent(msg.client_id)

    async def push_to_client(
        self,
        client_id: str,
        payload: Dict[str, Any],
        group_id: Optional[str] = None,
        need_ack: bool = True,
    ) -> None:
        msg = Message.downstream(client_id=client_id, group_id=group_id, payload=payload, need_ack=need_ack)
        await self._send_message(msg)

    async def broadcast(
        self,
        client_ids: Iterable[str],
        payload: Dict[str, Any],
        group_id: Optional[str] = None,
        need_ack: bool = True,
    ) -> None:
        tasks = []
        for cid in client_ids:
            msg = Message.downstream(client_id=cid, group_id=group_id, payload=payload, need_ack=need_ack)
            tasks.append(self._send_message(msg))
        if tasks:
            await asyncio.gather(*tasks)

    async def broadcast_to_all(self, payload: Dict[str, Any], need_ack: bool = True) -> None:
        client_ids = self._registry.list_online_clients()
        await self.broadcast(client_ids, payload=payload, group_id=None, need_ack=need_ack)

    async def broadcast_to_group(self, group_id: str, payload: Dict[str, Any], need_ack: bool = True) -> None:
        client_ids = self._registry.get_group_members(group_id)
        await self.broadcast(client_ids, payload=payload, group_id=group_id, need_ack=need_ack)
