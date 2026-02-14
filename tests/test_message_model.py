from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Dict, List

import websockets
from websocket import ABNF

from autotest_open.client.ws_client import WsClient
from autotest_open.models.message import Message


HOST = "127.0.0.1"
PORT = 9120
PATH = "/ws"


async def _ws_handler(websocket: websockets.WebSocketServerProtocol) -> None:
    """Simple test WebSocket handler.

    Behaviour:
    - Expects a JSON text message with ``type=hello`` and ``client_id``.
    - Immediately sends one downstream message back to that client with
      ``payload={"text": "welcome"}`` and ``need_ack=1``.
    - Any ACK messages from the client are accepted and ignored.
    """

    try:
        async for raw in websocket:
            if isinstance(raw, bytes):
                try:
                    text = raw.decode("utf-8")
                except Exception:
                    continue
            else:
                text = raw

            try:
                data = json.loads(text)
            except Exception:
                continue

            msg_type = data.get("type")
            client_id = data.get("client_id")

            if msg_type == "hello" and client_id:
                downstream: Dict[str, Any] = {
                    "type": "downstream",
                    "client_id": client_id,
                    "message_id": "msg-1",
                    "need_ack": 1,
                    "payload": {"text": "welcome"},
                    "timestamp": time.time(),
                }
                await websocket.send(json.dumps(downstream))
            elif msg_type == "ack":
                # For this test we only care that the client can send ACKs;
                # the payload is not validated further.
                continue
    except websockets.ConnectionClosed:
        pass


def _run_ws_server(stop_event: threading.Event) -> None:
    async def main() -> None:
        async with websockets.serve(_ws_handler, HOST, PORT):
            loop = asyncio.get_running_loop()
            # Wait until ``stop_event`` is set from another thread.
            await loop.run_in_executor(None, stop_event.wait)

    asyncio.run(main())


def test_message_roundtrip() -> None:
    """End-to-end test using the generic WebSocket client.

    The test starts a temporary local WebSocket server, creates a
    :class:`WsClient` instance, sends a ``hello`` message and verifies that
    at least one downstream message is received with the expected fields.
    """

    stop_event = threading.Event()
    server_thread = threading.Thread(target=_run_ws_server, args=(stop_event,), daemon=True)
    server_thread.start()

    # Give the server a brief moment to start listening.
    time.sleep(0.5)

    url = (
        f"ws://{HOST}:{PORT}{PATH}?client_id=test-client"
        "&device_platform=python&version_code=1"
    )

    client = WsClient(
        url=url,
        headers=None,
        timeout=5,
        service_name="test",
        config={"ack_on": 1, "ack_return": 1},
    )

    try:
        # Client sends hello first.
        client.send_hello()

        # Then collect messages from the server.
        messages: List[Any] = client.receive_messages(
            message_count=None,
            message_type=ABNF.OPCODE_TEXT,
            timeout=5.0,
        )

        assert messages, "expected at least one message from server"

        found_downstream = False
        for msg in messages:
            if isinstance(msg, bytes):
                try:
                    msg = msg.decode("utf-8")
                except Exception:
                    continue
            if not isinstance(msg, str):
                continue
            try:
                data = json.loads(msg)
            except Exception:
                continue
            if data.get("type") == "downstream" and data.get("client_id") == "test-client":
                found_downstream = True
                break

        assert found_downstream, "no downstream message for client 'test-client' found"
    finally:
        client.close()
        # Gracefully stop the WebSocket server.
        stop_event.set()
        server_thread.join(timeout=5)


def test_message_hello_without_group_id() -> None:
    msg = Message.hello(client_id="test-client")
    assert msg.type == "hello"
    assert msg.client_id == "test-client"
