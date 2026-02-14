from __future__ import annotations

import argparse
import os
import time
from typing import List

from websocket import ABNF

from autotest_open.client.ws_client import WsClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo WebSocket clients for autotest_open")
    parser.add_argument("--clients", type=int, default=3, help="Number of demo clients")
    parser.add_argument("--host", default=None, help="Gateway host (defaults to APP_WS_GATEWAY_HOST)")
    parser.add_argument("--port", type=int, default=None, help="Gateway port (defaults to APP_WS_GATEWAY_PORT)")
    parser.add_argument("--duration", type=float, default=3.0, help="Seconds to wait for broadcasts and ACKs")
    return parser.parse_args()


def build_ws_url(host: str, port: int, client_id: str) -> str:
    query = (
        f"client_id={client_id}"
        f"&device_platform=python&version_code=1"
    )
    return f"ws://{host}:{port}/ws?{query}"


def run_demo_clients(args: argparse.Namespace) -> None:
    host = args.host or os.getenv("APP_WS_GATEWAY_HOST", "127.0.0.1")
    port = args.port or int(os.getenv("APP_WS_GATEWAY_PORT", "9100"))

    clients: List[WsClient] = []

    # Common ACK/QoS configuration for all demo clients.
    ws_config = {
        "ack_on": 1,
        "ack_return": 1,
        "ack_accept": 1,
        "ack_delay": 0,
        "ack_error": 0,
        "ack_mod": 0,
        "qos_support": 1,
    }

    print(f"[ws-client-demo] connecting {args.clients} client(s) to ws://{host}:{port}/ws")

    for i in range(args.clients):
        client_id = f"ws-client-{i + 1}"
        url = build_ws_url(host, port, client_id)
        print(f"[ws-client-demo] client {client_id} connecting to {url}")
        client = WsClient(url=url, headers=None, timeout=10, service_name="demo", config=ws_config)
        clients.append(client)

    # Give the gateway a moment to accept connections.
    time.sleep(1.0)

    print("[ws-client-demo] sending HELLO messages")
    for client in clients:
        client.send_hello()

    # Short pause before sending upstream messages that will trigger a broadcast.
    time.sleep(1.0)

    print("[ws-client-demo] sending upstream messages to trigger broadcast")
    for index, client in enumerate(clients, start=1):
        payload = {"text": f"hello from client {index}"}
        client.send_upstream(payload)

    # Wait for broadcast messages and automatic ACKs to be processed.
    time.sleep(args.duration)

    print("[ws-client-demo] collecting received messages")
    for client in clients:
        messages = client.receive_messages(message_count=None, message_type=ABNF.OPCODE_TEXT, timeout=2.0)
        print(f"[ws-client-demo] client {client.client_id} received {len(messages)} message(s)")
        for msg in messages:
            print(f"[ws-client-demo] message for {client.client_id}: {msg}")

    for client in clients:
        client.close()


def main() -> None:
    args = parse_args()
    run_demo_clients(args)


if __name__ == "__main__":  # pragma: no cover
    main()
