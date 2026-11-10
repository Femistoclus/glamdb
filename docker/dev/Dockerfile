FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates clang-18 clang-format-18 clang-tidy-18 cmake git \
        libclang-rt-18-dev ninja-build python3 \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang-18 CXX=clang++-18 PYTHONDONTWRITEBYTECODE=1
WORKDIR /src
