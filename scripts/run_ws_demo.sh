#!/usr/bin/env bash
set -euo pipefail

PYTHON=${PYTHON:-python3}

# Start the WebSocket gateway in the background.
${PYTHON} -m autotest_open.cmd.ws_gateway &
GATEWAY_PID=$!

# Ensure the gateway has time to start listening.
sleep 2

# Run the demo WebSocket clients that will connect to the gateway,
# perform a simple hello + upstream exchange, and trigger one broadcast
# with ACK statistics printed on the gateway side.
${PYTHON} -m autotest_open.cmd.ws_client_demo

# Shut down the gateway process when the demo finishes.
kill "${GATEWAY_PID}" 2>/dev/null || true
