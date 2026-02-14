"""Client utilities for autotest_open.

This package exposes a generic WebSocket client implementation
(:class:`WsClient`) that mirrors the behaviour of the original internal
client while using only public dependencies.
"""

from .ws_client import WsClient

__all__ = ["WsClient"]
