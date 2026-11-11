"""Brand artwork is configurable but never a remote resource or arbitrary path."""
import json
from pathlib import Path

import pytest

from glamdb_mock.theme import ThemeError, load_theme


THEME = Path(__file__).resolve().parents[3] / "ui/config/theme.json"


@pytest.mark.parametrize("asset", ["https://evil/logo.png", "//evil/logo.png", "assets/brand/../key.png", "javascript:alert(1)", "assets/brand/logo.png?secret=1"])
def test_theme_rejects_nonlocal_brand_assets(tmp_path, asset):
    config = json.loads(THEME.read_text())
    config["brand"]["wordmark"] = asset
    path = tmp_path / "theme.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ThemeError):
        load_theme(path)


def test_supplied_logo_paths_and_sizes_load():
    theme = load_theme(THEME)
    assert theme["brand"]["wordmark"] == "assets/brand/wordmark.png"
    assert theme["brand"]["mark"] == "assets/brand/mark.png"
    assert theme["sizing"]["logo_width"] == "196px"


def test_legacy_theme_without_editor_still_loads(tmp_path):
    config = json.loads(THEME.read_text())
    config.pop("editor", None)
    path = tmp_path / "theme.json"
    path.write_text(json.dumps(config))
    assert "editor" not in load_theme(path)


@pytest.mark.parametrize("maximum", [1, 1010, 10000])
def test_optional_editor_query_maximum_loads(tmp_path, maximum):
    config = json.loads(THEME.read_text())
    config["editor"] = {"query_max_rows": maximum}
    path = tmp_path / "theme.json"
    path.write_text(json.dumps(config))
    assert load_theme(path)["editor"]["query_max_rows"] == maximum


@pytest.mark.parametrize("maximum", [0, 10001, True, 1.5, "10000", None, {"value": 10000}])
def test_editor_query_maximum_rejects_invalid_values(tmp_path, maximum):
    config = json.loads(THEME.read_text())
    config["editor"] = {"query_max_rows": maximum}
    path = tmp_path / "theme.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ThemeError):
        load_theme(path)


def test_editor_rejects_unknown_settings(tmp_path):
    config = json.loads(THEME.read_text())
    config["editor"] = {"query_max_rows": 10000, "extra": 1}
    path = tmp_path / "theme.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ThemeError):
        load_theme(path)
