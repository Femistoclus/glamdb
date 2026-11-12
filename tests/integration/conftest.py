"""Network integration always keeps disposable loopback traffic off proxies."""

import pytest


@pytest.fixture(autouse=True)
def loopback_bypasses_environment_proxies(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
