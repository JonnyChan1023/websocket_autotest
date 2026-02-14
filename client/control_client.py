from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from websocket import ABNF

from autotest_open.client.ws_client import WsClient


class ControlClient:
    """Small helper client for sending control signals over WebSocket.

    This client is intentionally lightweight and focuses on a simple
    JSON-based protocol. It reuses :class:`WsClient` for the underlying
    connection handling (threads, ACK/QoS headers etc.).

    Parameters
    ----------
    uri:
        Base WebSocket URI, for example ``ws://127.0.0.1:9100/ws``.
    client_id:
        Logical client identifier used by the gateway and registry.
    headers:
        Optional extra HTTP headers for the WebSocket handshake.
    timeout:
        Optional connection timeout in seconds.
    config:
        Optional behaviour switches passed through to :class:`WsClient`.
        Common keys are ``ack_on``, ``ack_return``, ``ack_accept``,
        ``ack_delay``, ``ack_error``, ``ack_mod`` and ``qos_support``.
    """

    def __init__(
        self,
        uri: str,
        client_id: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.uri = uri
        self.client_id = client_id
        self.headers: Dict[str, str] = dict(headers or {})
        self.config: Dict[str, Any] = dict(config or {})

        # Build full WebSocket URL with query parameters. The demo gateway
        # expects the same fields as the generic client.
        query_items = [("client_id", self.client_id)]
        # Optional, but kept for consistency with other demo clients.
        query_items.append(("device_platform", "python"))
        query_items.append(("version_code", "1"))

        separator = "&" if "?" in self.uri else "?"
        url = f"{self.uri}{separator}{urlencode(query_items)}"

        self._client = WsClient(
            url=url,
            headers=self.headers,
            timeout=timeout,
            service_name="control",
            config=self.config,
        )

    # ------------------------------------------------------------------
    # High-level protocol helpers
    # ------------------------------------------------------------------
    def send_hello(self) -> None:
        """Send a basic ``hello`` message using the underlying client."""

        self._client.send_hello()

    def send_control_signal(
        self,
        op: str,
        payload: Dict[str, Any],
        target_client_id: Optional[str] = None,
        target_group_id: Optional[str] = None,
    ) -> None:
        """Send a control signal wrapped in an ``upstream`` message.

        The upstream message has ``type="upstream"`` and its ``payload``
        contains the following fields::

            {
              "op": op,
              "payload": { ... },
              "client_id": target_client_id or self.client_id,
              # "group_id" is included only when ``target_group_id`` is set.
            }

        The gateway can interpret these fields to perform targeted push,
        group broadcast, global broadcast or online-status queries.
        """

        body: Dict[str, Any] = {
            "op": op,
            "payload": payload or {},
            "client_id": target_client_id or self.client_id,
        }

        self._client.send_upstream(body)

    def receive_messages(self, count: Optional[int] = None, timeout: float = 5.0) -> List[Any]:
        """Pull text messages from the underlying client queue.

        This is mainly useful for verifying acknowledgements or system
        responses after sending control signals.
        """

        return self._client.receive_messages(
            message_count=count,
            message_type=ABNF.OPCODE_TEXT,
            timeout=timeout,
        )

    def close(self) -> None:
        """Close the underlying WebSocket connection."""

        self._client.close()

    def __enter__(self) -> "ControlClient":  # pragma: no cover - convenience
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - convenience
        self.close()
