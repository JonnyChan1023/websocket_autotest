from __future__ import annotations

import asyncio
from typing import List

from autotest_open.core.dispatcher import MessageDispatcher
from autotest_open.core.registry import ConnectionRegistry
from autotest_open.internal.config import AppConfig, load_config
from autotest_open.models.message import Message, MESSAGE_TYPE_ACK, MESSAGE_TYPE_HEARTBEAT, MESSAGE_TYPE_HELLO


class TcpGatewayServer:
    """Async TCP gateway that speaks a simple JSON Lines protocol.

    Each line is a single JSON object, deserialized into a Message instance.
    """

    def __init__(self, config: AppConfig, registry: ConnectionRegistry, dispatcher: MessageDispatcher) -> None:
        self._config = config
        self._registry = registry
        self._dispatcher = dispatcher
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self._config.gateway_host,
            port=self._config.gateway_port,
        )
        addr = self._server.sockets[0].getsockname() if self._server.sockets else None
        print(f"[gateway] listening on {addr}", flush=True)

    async def close(self) -> None:
        if self._server is not None:
            print("[gateway] closing server", flush=True)
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        print(f"[gateway] new connection from {addr}", flush=True)
        client_id: str | None = None
        try:
            while True:
                print("[gateway] waiting for line", flush=True)
                line = await reader.readline()
                if not line:
                    print("[gateway] connection closed by peer", flush=True)
                    break
                try:
                    msg = Message.from_json_line(line.decode("utf-8"))
                except Exception as exc:
                    print(f"[gateway] failed to parse message: {exc}", flush=True)
                    continue

                print(
                    f"[gateway] received message type={msg.type!r} expected={MESSAGE_TYPE_HELLO!r} from client={msg.client_id}",
                    flush=True,
                )

                try:
                    if msg.type == MESSAGE_TYPE_HELLO:
                        # Minimal handshake: register client and attach writer.
                        client_id = msg.client_id or "anonymous"
                        print(f"[gateway] registering client {client_id}", flush=True)
                        self._registry.register_client(client_id)
                        self._dispatcher.attach(client_id, writer)
                        print(f"[gateway] registered client {client_id}", flush=True)
                    elif msg.type == MESSAGE_TYPE_HEARTBEAT and msg.client_id:
                        self._registry.heartbeat(msg.client_id)
                    elif msg.type == MESSAGE_TYPE_ACK and msg.client_id:
                        self._registry.record_ack_received(msg.client_id)
                    else:
                        # For other message types we only update heartbeat in this demo.
                        if msg.client_id:
                            self._registry.heartbeat(msg.client_id)
                except Exception as exc:
                    print(f"[gateway] error while handling message: {exc}", flush=True)
        finally:
            if client_id is not None:
                print(f"[gateway] disconnect client {client_id}", flush=True)
                self._dispatcher.detach(client_id)
                self._registry.unregister_client(client_id)
            writer.close()
            await writer.wait_closed()


async def run_gateway(config: AppConfig | None = None) -> None:
    """Run the gateway server until interrupted (Ctrl+C)."""

    if config is None:
        config = load_config()
    registry = ConnectionRegistry()
    dispatcher = MessageDispatcher(registry)
    server = TcpGatewayServer(config=config, registry=registry, dispatcher=dispatcher)
    await server.start()

    # Keep running forever until cancelled.
    await asyncio.Event().wait()


async def run_gateway_for_demo(config: AppConfig, registry: ConnectionRegistry, dispatcher: MessageDispatcher) -> None:
    """Run the gateway as part of a short-lived demo scenario.

    This helper does not block forever; the caller is responsible for closing
    the server via ``server_task.cancel()`` or process shutdown.
    """

    server = TcpGatewayServer(config=config, registry=registry, dispatcher=dispatcher)
    await server.start()
    # Just keep the coroutine alive; the caller controls lifetime through
    # task cancellation.
    try:
        await asyncio.Event().wait()
    finally:
        await server.close()
