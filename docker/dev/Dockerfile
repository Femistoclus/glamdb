FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates clang-18 clang-format-18 clang-tidy-18 cmake git \
        libclang-rt-18-dev ninja-build python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Same version/hash as tools/logging/requirements.txt; preserve stdin-only builds.
RUN python3 -m venv /opt/glamdb-python \
    && printf '%s\n' 'python-json-logger==4.2.0 --hash=sha256:158a52126fcd6869e09574d2b66272666f3dc8f468c62637ef9a1fa883719cb9' > /tmp/logging-requirements.txt \
    && /opt/glamdb-python/bin/pip install --require-hashes --only-binary=:all: --no-deps -r /tmp/logging-requirements.txt
ENV PATH="/opt/glamdb-python/bin:${PATH}"

ENV CC=clang-18 CXX=clang++-18 PYTHONDONTWRITEBYTECODE=1
WORKDIR /src
