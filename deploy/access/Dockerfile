FROM python:3.13.11-slim-bookworm@sha256:20080e807bfc404f8450b185cf0fc95d553462673598549613735f70a5b4d5d0
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp PATH=/opt/venv/bin:$PATH
RUN python -m venv /opt/venv
COPY apps/workbench_access/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
WORKDIR /opt/glamdb
COPY apps/workbench_access/ ./apps/workbench_access/
# The ignore file admits only the optional telemetry adapters and a stable README.
COPY tools/ ./tools/
USER 10001:10001
CMD ["python", "-m", "apps.workbench_access", "--config", "/etc/glamdb/gateway.json", "--host", "0.0.0.0", "--port", "8080", "--operator-socket", "/state/operator.sock"]
