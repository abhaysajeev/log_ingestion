FROM python:3.12-slim
WORKDIR /src
COPY fastapi/requirements.txt /tmp/api.txt
COPY worker/requirements.txt /tmp/worker.txt
COPY tests/requirements.txt /tmp/tests.txt
RUN pip install --no-cache-dir -r /tmp/api.txt -r /tmp/worker.txt -r /tmp/tests.txt
