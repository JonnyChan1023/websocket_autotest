from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

from autotest_open.core.dispatcher import MessageDispatcher
from autotest_open.core.registry import ConnectionRegistry
from autotest_open.internal.config import load_config


_registry = ConnectionRegistry()
_dispatcher = MessageDispatcher(_registry)


class ControlRequestHandler(BaseHTTPRequestHandler):
    """Very small HTTP control plane.

    It is intentionally simple and only meant to demonstrate how a control
    plane could interact with the dispatcher. For a full production setup the
    registry would typically be shared with the gateway process via an
    external store or RPC layer.
    """

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        data = self.rfile.read(length)
        try:
            return json.loads(data.decode("utf-8"))
        except Exception:
            return {}

    def _write_json(self, status: int, obj: Dict[str, Any]) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/query_online":
            online = _registry.list_online_clients()
            stats = _registry.get_stats()
            self._write_json(
                200,
                {
                    "online_clients": online,
                    "stats": {
                        "total_clients": stats.total_clients,
                        "total_groups": stats.total_groups,
                        "messages_sent": stats.messages_sent,
                        "acks_received": stats.acks_received,
                        "ack_rate": stats.ack_rate,
                    },
                },
            )
        else:
            self._write_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in ("/push", "/group", "/broadcast"):
            self._write_json(404, {"error": "not found"})
            return
        body = self._read_json_body()
        if self.path == "/push":
            client_id = body.get("client_id")
            payload = body.get("payload", {})
            if not client_id:
                self._write_json(400, {"error": "client_id required"})
                return
            # In this standalone control server we do not share connections
            # with the real gateway, so sending simply updates statistics
            # when a client with the given id exists in the registry.
            # The method is kept for API completeness.
            self._write_json(200, {"status": "accepted", "client_id": client_id, "payload": payload})
        elif self.path == "/group":
            group_id = body.get("group_id")
            payload = body.get("payload", {})
            self._write_json(200, {"status": "accepted", "group_id": group_id, "payload": payload})
        elif self.path == "/broadcast":
            payload = body.get("payload", {})
            self._write_json(200, {"status": "accepted", "broadcast": True, "payload": payload})


def main() -> None:
    config = load_config()
    server = HTTPServer((config.control_host, config.control_port), ControlRequestHandler)
    try:
        print(f"control server listening on http://{config.control_host}:{config.control_port}")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":  # pragma: no cover
    main()
