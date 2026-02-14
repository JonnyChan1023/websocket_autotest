from __future__ import annotations

from contextlib import contextmanager
from queue import Queue, Empty
from typing import Any, Dict, List, Optional
import json
import threading
import time
from urllib.parse import urlparse, parse_qs

import requests
import websocket
from websocket import ABNF, create_connection
from websocket._exceptions import WebSocketBadStatusException, WebSocketTimeoutException

from autotest_open.models.message import Message, MESSAGE_TYPE_ACK, MESSAGE_TYPE_DOWNSTREAM


class WsClient:
    """Generic WebSocket client used by the open demo project.

    This client intentionally mirrors the capabilities of the original
    ``WsClientRefactor`` while avoiding any internal names or private
    dependencies. It focuses on a small, JSON-based protocol with optional
    ACK/QoS behaviour controlled through headers.
    """

    SEC_WEBSOCKET_KEY = "SGVsbG8sIHdvcmxkIQ=="

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        service_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create a new WebSocket client and start the receive loop.

        Args:
            url: Full WebSocket URL, including query parameters. Common
                query fields used in this demo are ``client_id``,
                ``device_platform`` and ``version_code``.
            headers: Optional extra HTTP headers to send during the
                WebSocket handshake.
            timeout: Optional connection timeout in seconds.
            service_name: Optional logical service name (not interpreted
                by the client itself, but useful for callers).
            config: Behaviour switches, all optional::

                {
                  "ack_on": 0 | 1,
                  "ack_return": 0 | 1,
                  "ack_accept": 0 | 1,
                  "ack_delay": seconds (float),
                  "ack_error": 0 | 1,
                  "ack_mod": 0 | 1,
                  "qos_support": 0 | 1,
                }

                - ``ack_on``: whether the client declares ACK capability via
                  headers.
                - ``ack_return``: whether the client automatically sends ACKs
                  when it receives downstream messages that request one.
                - ``ack_accept``: reserved for future use; currently only
                  forwarded as a header.
                - ``ack_delay``: artificial delay (seconds) before sending an
                  ACK, useful to simulate slow clients.
                - ``ack_error``: when set to 1, the client will send ACK
                  messages with an empty ``message_id`` to emulate protocol
                  errors.
                - ``ack_mod``: when set to 1, automatic ACKs are disabled and
                  callers are expected to send ACKs manually.
                - ``qos_support``: toggles QoS-related headers.
        """

        self.url = url
        self.service_name = service_name
        self.config: Dict[str, Any] = config or {}

        # Parse query parameters into a simple dict of first values.
        parsed = urlparse(url)
        query_raw = parse_qs(parsed.query)
        self.query_params: Dict[str, str] = {k: v[0] for k, v in query_raw.items() if v}

        self.client_id: Optional[str] = self.query_params.get("client_id")
        self.device_platform: Optional[str] = self.query_params.get("device_platform")
        self.version_code: Optional[str] = self.query_params.get("version_code")

        self.headers: Dict[str, str] = dict(headers or {})
        self._apply_ack_headers()

        # Underlying websocket connection and receive buffers.
        header_list = [f"{k}: {v}" for k, v in self.headers.items()]
        self.conn = create_connection(self.url, timeout=timeout, header=header_list, enable_multithread=False)

        self._messages: "Queue[Any]" = Queue(maxsize=0)
        self._opcodes: "Queue[int]" = Queue(maxsize=0)

        # Background thread for receiving frames.
        self._recv_thread = threading.Thread(target=self._recv_loop, name="WsClientRecv", daemon=True)
        self._recv_thread.start()

    # ------------------------------------------------------------------
    # Header helpers
    # ------------------------------------------------------------------
    def _apply_ack_headers(self) -> None:
        """Populate ACK/QoS related headers from ``self.config``.

        All header names are generic and free of internal prefixes.
        """

        if self.config.get("ack_on") is not None:
            self.headers["X-Support-Ack"] = str(self.config["ack_on"])
        if self.config.get("ack_accept") is not None:
            self.headers["X-Accept-Ack"] = str(self.config["ack_accept"])

        # QoS support is expressed via a simple version header.
        qos_support = self.config.get("qos_support")
        if qos_support is not None and str(qos_support) == "1":
            self.headers["X-Qos-Version"] = "2"
        else:
            # Explicitly signal "no QoS" in a generic way.
            self.headers.setdefault("X-Qos-Version", "0")

        # Ack mode describes whether the client intends to automatically
        # respond with ACKs or let the application decide.
        if self.config.get("ack_mod") is not None:
            self.headers["X-Ack-Mode"] = str(self.config["ack_mod"])

    # ------------------------------------------------------------------
    # Capability helpers
    # ------------------------------------------------------------------
    def support_qos(self) -> bool:
        return str(self.config.get("qos_support", 0)) == "1"

    def support_ack(self) -> bool:
        return str(self.config.get("ack_on", 0)) == "1"

    def accept_ack(self) -> bool:
        return str(self.config.get("ack_accept", 0)) == "1"

    def _should_auto_ack(self) -> bool:
        if str(self.config.get("ack_return", 0)) != "1":
            return False
        # When ack_mod is 1 we consider ACKs to be fully application-driven.
        if str(self.config.get("ack_mod", 0)) == "1":
            return False
        return True

    # ------------------------------------------------------------------
    # Public helpers for query parameters
    # ------------------------------------------------------------------
    def has_param(self, name: str, value: Optional[str] = None) -> bool:
        if name not in self.query_params:
            return False
        if value is not None:
            return self.query_params.get(name) == str(value)
        return True

    def get_param(self, name: str) -> Optional[str]:
        return self.query_params.get(name)

    def is_background(self) -> bool:
        return self.get_param("is_background") == "1"

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------
    def _recv_loop(self) -> None:
        while self.conn.connected:
            try:
                opcode, message = self.conn.recv_data(control_frame=True)
            except websocket._exceptions.WebSocketConnectionClosedException:
                break
            except WebSocketTimeoutException:
                break
            except websocket._exceptions.WebSocketProtocolException as exc:
                self._messages.put(str(exc))
                break

            if opcode in (ABNF.OPCODE_BINARY, ABNF.OPCODE_TEXT):
                raw_text: Optional[str] = None
                if isinstance(message, bytes):
                    try:
                        raw_text = message.decode("utf-8")
                    except Exception:
                        raw_text = None
                elif isinstance(message, str):
                    raw_text = message

                if raw_text is not None:
                    try:
                        data = json.loads(raw_text)
                    except Exception:
                        data = None
                    if isinstance(data, dict):
                        self._handle_incoming_json(data)

                # Always enqueue the original payload (bytes or text).
                self._messages.put(message)
            elif opcode == ABNF.OPCODE_PING:
                # Reply with PONG but do not enqueue the control frame.
                try:
                    self.conn.pong(message)
                except Exception:
                    # Any error while replying to ping is non-fatal for the
                    # purposes of this demo client.
                    pass
            else:
                self._opcodes.put(opcode)

    def _handle_incoming_json(self, data: Dict[str, Any]) -> None:
        msg_type = data.get("type")
        if msg_type == MESSAGE_TYPE_DOWNSTREAM and self._should_auto_ack():
            self._send_ack_for_message(data)

    def _send_ack_for_message(self, data: Dict[str, Any]) -> None:
        if not self.client_id:
            return

        message_id = data.get("message_id")
        if self.config.get("ack_error") is not None and str(self.config["ack_error"]) == "1":
            # Simulate a protocol error by sending an empty message id.
            message_id = ""

        delay = 0.0
        if self.config.get("ack_delay") is not None:
            try:
                delay = float(self.config["ack_delay"])
            except (TypeError, ValueError):
                delay = 0.0
        if delay > 0:
            time.sleep(delay)

        ack_msg = Message(type=MESSAGE_TYPE_ACK, client_id=self.client_id, message_id=message_id, timestamp=time.time())
        try:
            self.conn.send(ack_msg.to_json())
        except Exception:
            # Ignore ACK send failures in this demo client.
            pass

    # ------------------------------------------------------------------
    # Sending helpers
    # ------------------------------------------------------------------
    def _send(self, message_type: int, payload: Any) -> None:
        if message_type == ABNF.OPCODE_TEXT:
            self.conn.send(payload)
        elif message_type == ABNF.OPCODE_BINARY:
            self.conn.send_binary(payload)
        else:
            raise ValueError(f"Unsupported message_type: {message_type}")

    def send_text_message_and_recv(self, text_message: str, timeout: float = 5.0) -> List[Any]:
        """Send a text frame and wait for responses.

        Returns a list of messages collected within ``timeout`` seconds.
        """

        self._send(ABNF.OPCODE_TEXT, text_message)
        return self.receive_messages(message_count=None, message_type=ABNF.OPCODE_TEXT, timeout=timeout)

    def send_binary_message_and_recv(
        self,
        binary_message: bytes,
        message_count: Optional[int] = 1,
        timeout: float = 5.0,
    ) -> List[Any]:
        """Send a binary frame and wait for up to ``message_count`` messages."""

        self._send(ABNF.OPCODE_BINARY, binary_message)
        return self.receive_messages(message_count=message_count, message_type=ABNF.OPCODE_BINARY, timeout=timeout)

    # ------------------------------------------------------------------
    # Public API for higher level protocol messages
    # ------------------------------------------------------------------
    def send_hello(self) -> None:
        """Send a minimal HELLO message for the demo handshake."""

        if not self.client_id:
            return
        msg = {
            "type": "hello",
            "client_id": self.client_id,
            "timestamp": time.time(),
        }
        self._send(ABNF.OPCODE_TEXT, json.dumps(msg))

    def send_upstream(self, payload: Dict[str, Any]) -> None:
        """Send a generic upstream message with the given JSON payload."""

        if not self.client_id:
            return
        msg = {
            "type": "upstream",
            "client_id": self.client_id,
            "payload": payload,
            "timestamp": time.time(),
        }
        self._send(ABNF.OPCODE_TEXT, json.dumps(msg))

    # ------------------------------------------------------------------
    # Message collection
    # ------------------------------------------------------------------
    def receive_messages(
        self,
        message_count: Optional[int] = 1,
        message_type: int = ABNF.OPCODE_TEXT,
        timeout: float = 9.0,
    ) -> List[Any]:
        """Collect messages from the internal queues.

        Args:
            message_count: How many messages to wait for. If ``None``, the
                client will collect as many messages as available until
                ``timeout`` is reached.
            message_type: Either ``ABNF.OPCODE_TEXT`` / ``ABNF.OPCODE_BINARY``
                to read from the message queue, or any other opcode to read
                from the control opcode queue.
            timeout: Maximum time in seconds to wait for messages.
        """

        queue = self._messages if message_type in (ABNF.OPCODE_TEXT, ABNF.OPCODE_BINARY) else self._opcodes
        results: List[Any] = []

        target = message_count if message_count is not None else float("inf")
        end_time = time.time() + timeout

        while len(results) < target:
            remaining = end_time - time.time()
            if remaining <= 0:
                break
            try:
                item = queue.get(timeout=remaining)
            except Empty:
                break
            results.append(item)

        return results

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self.conn is not None and self.conn.connected:
            try:
                self.conn.close()
            except Exception:
                pass

    def __enter__(self) -> "WsClient":  # pragma: no cover - convenience
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - convenience
        self.close()

    # ------------------------------------------------------------------
    # HTTP-only connection check
    # ------------------------------------------------------------------
    @staticmethod
    def connect_only_with_http(
        uri: str,
        query_params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Validate the HTTP upgrade behaviour without establishing WS state.

        This helper issues a plain HTTP GET with generic WebSocket upgrade
        headers using :mod:`requests` and returns the raw ``Response`` object
        for further inspection by callers.
        """

        ws_headers = {
            "connection": "upgrade",
            "upgrade": "websocket",
            "sec-websocket-version": "13",
            "sec-websocket-key": WsClient.SEC_WEBSOCKET_KEY,
        }
        if headers is not None:
            for k, v in headers.items():
                key = k.lower()
                if key in ws_headers and v is None:
                    ws_headers.pop(key, None)
                else:
                    ws_headers[key] = v

        response = requests.get(url=uri, params=query_params, headers=ws_headers)
        return response


@contextmanager
def create_batch_clients(configs: List[Dict[str, Any]]) -> List[WsClient]:
    """Context manager that creates multiple WebSocket clients.

    Each item in ``configs`` is passed as keyword arguments to
    :class:`WsClient`. All created clients are closed on exit.
    """

    clients: List[WsClient] = []
    try:
        for cfg in configs:
            client = WsClient(
                url=cfg["url"],
                headers=cfg.get("headers"),
                timeout=cfg.get("timeout"),
                service_name=cfg.get("service_name"),
                config=cfg.get("config"),
            )
            clients.append(client)
        yield clients
    finally:
        for client in clients:
            client.close()
