from __future__ import annotations

import argparse
import asyncio

from autotest_open.gateway.ws_server import run_ws_gateway


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run WebSocket gateway server")
    parser.add_argument(
        "--host",
        help=(
            "Listen host (defaults to APP_WS_GATEWAY_HOST or 127.0.0.1 if the "
            "environment variable is not set)."
        ),
        default=None,
    )
    parser.add_argument(
        "--port",
        type=int,
        help=(
            "Listen port (defaults to APP_WS_GATEWAY_PORT or 9100 if the "
            "environment variable is not set)."
        ),
        default=None,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    kwargs = {}
    if args.host is not None:
        kwargs["host"] = args.host
    if args.port is not None:
        kwargs["port"] = args.port

    try:
        asyncio.run(run_ws_gateway(**kwargs))
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
