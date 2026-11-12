from dataclasses import replace
from pathlib import Path
import pytest

from apps.workbench_access.resources import CatalogError
from apps.workbench_access.settings import SsoSettings, Settings


def test_production_transport_and_typed_resource_bounds(tmp_path):
    sso = SsoSettings(
        "https://identity.example/realms/glamdb", "gateway", tmp_path / "client.secret"
    )
    settings = Settings(
        sso,
        tmp_path / "catalog",
        tmp_path / "csrf.secret",
        tmp_path / "site",
        tmp_path / "theme.json",
        "https://db.example",
    )
    for change in [
        {"test_mode": "false"},
        {"token_max_seconds": True},
        {"timeout_seconds": float("nan")},
        {"issuer": "http://identity.example/realms/glamdb"},
    ]:
        with pytest.raises(CatalogError):
            replace(sso, **change)
    for change in [
        {"max_inflight": True},
        {"max_body_bytes": "4096"},
        {"drain_timeout_seconds": float("inf")},
        {"backend_timeout_seconds": 0},
        {"cerbos_plaintext": "false"},
        {"catalog": settings.site / "assets" / "catalog.sqlite"},
        {"csrf_secret_file": settings.site / "csrf.secret"},
    ]:
        with pytest.raises(CatalogError):
            replace(settings, **change)
