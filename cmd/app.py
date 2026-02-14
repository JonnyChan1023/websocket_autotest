from __future__ import annotations

"""Unified command line entry for the demo project.

Example usage::

    python3 -m autotest_open.cmd.app ws-gateway
    python3 -m autotest_open.cmd.app tcp-gateway
    python3 -m autotest_open.cmd.app control-server
    python3 -m autotest_open.cmd.app ws-client-demo

The subcommands simply delegate to the existing module-level ``main``
functions in :mod:`autotest_open.cmd.ws_gateway`,
:mod:`autotest_open.cmd.gateway`, :mod:`autotest_open.cmd.control_server`
and :mod:`autotest_open.cmd.ws_client_demo`.
"""

import sys
from typing import Callable, Dict, List


SubcommandFunc = Callable[[], None]


def _build_subcommand_table() -> Dict[str, SubcommandFunc]:
    from autotest_open.cmd import control_server, gateway, ws_client_demo, ws_gateway

    return {
        "ws-gateway": ws_gateway.main,
        "tcp-gateway": gateway.main,
        "control-server": control_server.main,
        "ws-client-demo": ws_client_demo.main,
    }


def _print_usage(prog: str, commands: List[str]) -> None:
    joined = " | ".join(commands)
    print(f"usage: {prog} <subcommand> [args]\n")
    print("available subcommands:")
    for name in commands:
        print(f"  - {name}")


def main(argv: List[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    table = _build_subcommand_table()
    if not argv:
        _print_usage(sys.argv[0], sorted(table.keys()))
        return

    subcommand = argv[0]
    handler = table.get(subcommand)
    if handler is None:
        print(f"unknown subcommand: {subcommand}\n", file=sys.stderr)
        _print_usage(sys.argv[0], sorted(table.keys()))
        return

    # Forward remaining arguments to the target module by rebuilding sys.argv.
    sys.argv = [f"{sys.argv[0]} {subcommand}"] + argv[1:]
    handler()


if __name__ == "__main__":  # pragma: no cover
    main()
