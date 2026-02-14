from __future__ import annotations

import json
import uuid
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


# Message types exchanged between clients and the gateway.
MESSAGE_TYPE_HELLO = "hello"          # client -> server handshake
MESSAGE_TYPE_DOWNSTREAM = "downstream"  # server -> client push
MESSAGE_TYPE_UPSTREAM = "upstream"    # client -> server generic message
MESSAGE_TYPE_ACK = "ack"              # client -> server ack for downstream
MESSAGE_TYPE_HEARTBEAT = "heartbeat"  # bidirectional heartbeat
MESSAGE_TYPE_SYSTEM = "system"        # server -> client system notification


@dataclass
class Message:
    """Generic JSON-serializable message model.

    The demo uses JSON Lines over TCP: each message is encoded as a single
    JSON object followed by a "\n" character.
    """

    type: str
    client_id: Optional[str] = None
    group_id: Optional[str] = None
    message_id: Optional[str] = None
    # Whether the receiver should respond with an ACK
    need_ack: bool = False
    # Free-form payload; should be JSON-serializable
    payload: Optional[Dict[str, Any]] = None
    # Unix timestamp in seconds
    timestamp: float = 0.0

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex

    @classmethod
    def hello(cls, client_id: str, group_id: Optional[str] = None) -> "Message":
        return cls(type=MESSAGE_TYPE_HELLO, client_id=client_id, group_id=group_id, timestamp=time.time())

    @classmethod
    def downstream(
        cls,
        client_id: str,
        payload: Dict[str, Any],
        group_id: Optional[str] = None,
        need_ack: bool = True,
    ) -> "Message":
        return cls(
            type=MESSAGE_TYPE_DOWNSTREAM,
            client_id=client_id,
            group_id=group_id,
            message_id=cls.new_id(),
            need_ack=need_ack,
            payload=payload,
            timestamp=time.time(),
        )

    @classmethod
    def ack(cls, client_id: str, message_id: str) -> "Message":
        return cls(
            type=MESSAGE_TYPE_ACK,
            client_id=client_id,
            message_id=message_id,
            timestamp=time.time(),
        )

    def to_json(self) -> str:
        """Serialize message to a JSON string (no trailing newline)."""

        data = asdict(self)
        # Remove keys with value None to keep the wire format compact
        compact = {k: v for k, v in data.items() if v is not None}
        return json.dumps(compact, separators=(",", ":"))

    def to_json_line(self) -> str:
        """Serialize message to a JSON line (including trailing "\n")."""

        return self.to_json() + "\n"

    @classmethod
    def from_json_line(cls, line: str) -> "Message":
        """Parse a Message instance from a JSON line.

        Extra fields are ignored; missing fields fall back to defaults.
        """

        obj = json.loads(line)
        return cls(
            type=obj.get("type", ""),
            client_id=obj.get("client_id"),
            group_id=obj.get("group_id"),
            message_id=obj.get("message_id"),
            need_ack=bool(obj.get("need_ack", False)),
            payload=obj.get("payload"),
            timestamp=float(obj.get("timestamp", time.time())),
        )
