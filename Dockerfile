FROM python:3.11-slim

WORKDIR /app

COPY autotest_open ./autotest_open
COPY autotest_open/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "autotest_open.cmd.app", "ws-gateway"]
