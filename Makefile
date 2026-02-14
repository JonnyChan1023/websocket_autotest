PYTHON ?= python3

.PHONY: run_gateway run_control ws_demo run_ws_gateway

run_gateway:
	PYTHONPATH=..:$$PYTHONPATH $(PYTHON) -m autotest_open.cmd.gateway

run_control:
	PYTHONPATH=..:$$PYTHONPATH $(PYTHON) -m autotest_open.cmd.control_server

ws_demo:
	PYTHONPATH=..:$$PYTHONPATH bash scripts/run_ws_demo.sh

run_ws_gateway:
	PYTHONPATH=..:$$PYTHONPATH $(PYTHON) -m autotest_open.cmd.ws_gateway
