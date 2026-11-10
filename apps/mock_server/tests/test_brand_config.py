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
