from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import websockets
from websockets.server import WebSocketServerProtocol

from autotest_open.core.dispatcher import MessageDispatcher
from autotest_open.core.registry import ConnectionRegistry
from autotest_open.models.message import (
    Message,
    MESSAGE_TYPE_ACK,
    MESSAGE_TYPE_DOWNSTREAM,
    MESSAGE_TYPE_HEARTBEAT,
    MESSAGE_TYPE_HELLO,
    MESSAGE_TYPE_UPSTREAM,
)


async def _handle_client(
    websocket: WebSocketServerProtocol,
    registry: ConnectionRegistry,
    dispatcher: MessageDispatcher,
) -> None:
    """Handle a single WebSocket connection.

    The protocol between client and server is JSON-based. Each frame contains a
    single JSON object compatible with :class:`autotest_open.models.message.Message`.

    Supported message types:
    - ``hello``: client handshake and registration
    - ``heartbeat``: keep-alive
    - ``upstream``: generic client -> server message, used to trigger broadcasts
    - ``downstream``: server -> client push (clients may ACK)
    - ``ack``: client -> server acknowledgement for downstream messages
    """

    remote = websocket.remote_address
    print(f"[ws-gateway] new connection from {remote}", flush=True)
    client_id: str | None = None

    try:
        async for raw in websocket:
            # ``raw`` can be ``str`` (text frame) or ``bytes`` (binary frame).
            if isinstance(raw, bytes):
                try:
                    text = raw.decode("utf-8")
                except Exception as exc:  # pragma: no cover - defensive
                    print(f"[ws-gateway] drop non-utf8 message: {exc}", flush=True)
                    continue
            else:
                text = raw

            try:
                msg = Message.from_json_line(text)
            except Exception as exc:
                print(f"[ws-gateway] failed to parse message: {exc}", flush=True)
                continue

            if msg.type == MESSAGE_TYPE_HELLO:
                # Basic handshake: remember client id.
                client_id = msg.client_id or "anonymous"
                registry.register_client(client_id)
                dispatcher.attach(client_id, websocket)
                print(f"[ws-gateway] registered client {client_id}", flush=True)
            elif msg.type == MESSAGE_TYPE_HEARTBEAT and msg.client_id:
                registry.heartbeat(msg.client_id)
            elif msg.type == MESSAGE_TYPE_ACK and msg.client_id:
                registry.record_ack_received(msg.client_id)
                stats = registry.get_stats()
                print(
                    "[ws-gateway] ACK from %s, messages=%d acks=%d ack_rate=%.2f"
                    % (
                        msg.client_id,
                        stats.messages_sent,
                        stats.acks_received,
                        stats.ack_rate,
                    ),
                    flush=True,
                )
            elif msg.type == MESSAGE_TYPE_UPSTREAM:
                # Upstream messages can either carry generic payloads or
                # structured control operations.
                if msg.client_id:
                    registry.heartbeat(msg.client_id)

                payload_obj: Any = msg.payload or {}
                if isinstance(payload_obj, dict) and "op" in payload_obj:
                    op = str(payload_obj.get("op") or "").lower()
                    target_client_id = payload_obj.get("client_id") or msg.client_id
                    target_group_id = payload_obj.get("group_id")
                    inner_payload: Any = payload_obj.get("payload") or {}

                    if op == "push" and target_client_id:
                        asyncio.create_task(
                            dispatcher.push_to_client(
                                client_id=target_client_id,
                                payload=inner_payload,
                            )
                        )
                        print(
                            f"[ws-gateway] control op=push to client={target_client_id}",
                            flush=True,
                        )
                    elif op == "group" and target_group_id:
                        asyncio.create_task(
                            dispatcher.broadcast_to_group(
                                group_id=target_group_id,
                                payload=inner_payload,
                            )
                        )
                        print(
                            f"[ws-gateway] control op=group to group={target_group_id}",
                            flush=True,
                        )
                    elif op == "broadcast":
                        asyncio.create_task(
                            dispatcher.broadcast_to_all(payload=inner_payload)
                        )
                        print("[ws-gateway] control op=broadcast to all", flush=True)
                    elif op == "query_online":
                        stats = registry.get_stats()
                        stats_payload = {
                            "total_clients": stats.total_clients,
                            "total_groups": stats.total_groups,
                            "messages_sent": stats.messages_sent,
                            "acks_received": stats.acks_received,
                            "ack_rate": stats.ack_rate,
                        }
                        system_message = {
                            "type": "system",
                            "client_id": msg.client_id,
                            "payload": {"stats": stats_payload},
                        }
                        await websocket.send(
                            json.dumps(system_message, separators=(",", ":"))
                        )
                        print("[ws-gateway] control op=query_online replied", flush=True)
                    else:
                        print(
                            f"[ws-gateway] unknown control op={op!r}",
                            flush=True,
                        )
                else:
                    # For demo purposes, generic upstream messages still trigger
                    # a broadcast to all online clients.
                    payload = {
                        "source_client": msg.client_id,
                        "payload": msg.payload or {},
                    }
                    asyncio.create_task(
                        dispatcher.broadcast_to_all(payload=payload, need_ack=True)
                    )
                    print("[ws-gateway] broadcast triggered by upstream", flush=True)
            else:
                # Any other message type still refreshes heartbeat if possible.
                if msg.client_id:
                    registry.heartbeat(msg.client_id)
    except websockets.ConnectionClosed:
        print(f"[ws-gateway] connection closed from {remote}", flush=True)
    finally:
        if client_id is not None:
            print(f"[ws-gateway] disconnect client {client_id}", flush=True)
            dispatcher.detach(client_id)
            registry.unregister_client(client_id)


async def run_ws_gateway(host: str | None = None, port: int | None = None) -> None:
    """Run the WebSocket gateway until interrupted.

    The listening address is resolved in the following order:
    1. Explicit ``host``/``port`` arguments (if provided);
    2. Environment variables ``APP_WS_GATEWAY_HOST`` / ``APP_WS_GATEWAY_PORT``;
    3. Built-in defaults ``127.0.0.1:9100``.
    """

    env_host = os.getenv("APP_WS_GATEWAY_HOST", "127.0.0.1")
    env_port = int(os.getenv("APP_WS_GATEWAY_PORT", "9100"))
    listen_host = host or env_host
    listen_port = port or env_port

    registry = ConnectionRegistry()
    dispatcher = MessageDispatcher(registry)

    print(
        f"[ws-gateway] listening on ws://{listen_host}:{listen_port}",
        flush=True,
    )

    async with websockets.serve(
        lambda ws: _handle_client(ws, registry, dispatcher),
        listen_host,
        listen_port,
    ):
        # Block forever until the process receives an interrupt.
        await asyncio.Future()
