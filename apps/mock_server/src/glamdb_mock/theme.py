"""Read editable visual configuration without accepting executable CSS."""

import json
import re
from pathlib import Path


class ThemeError(ValueError):
    pass


_KEY = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_HEX = re.compile(r"#(?:[a-fA-F0-9]{3}|[a-fA-F0-9]{4}|[a-fA-F0-9]{6}|[a-fA-F0-9]{8})\Z")
_FONT = re.compile(r"[A-Za-z0-9 ,'\"._-]{1,256}\Z")
_SIZE = re.compile(r"(?:0|(?:0|[1-9][0-9]{0,3})(?:\.[0-9]{1,4})?(?:px|rem|em|%|vh|vw))\Z")
_SECTIONS = {"version", "brand", "colors", "typography", "sizing", "decoration", "labels"}
_BRAND_ASSET = re.compile(r"assets/brand/[A-Za-z0-9_-]+\.(?:png|svg|webp)\Z")


def _plain_text(value):
    return isinstance(value, str) and len(value) <= 4096 and not any(
        ord(character) < 32 and character not in "\n\t" for character in value
    )


def load_theme(path: Path) -> dict:
    """Reload and validate one bounded JSON file; callers keep their last valid theme."""
    with path.open("rb") as source:
        raw = source.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        raise ThemeError("theme config exceeds 64 KiB")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as error:
        raise ThemeError("theme config must be valid UTF-8 JSON") from error
    if not isinstance(value, dict) or set(value) != _SECTIONS or type(value["version"]) is not int or value["version"] != 1:
        raise ThemeError("theme config requires version 1 and the documented sections")
    for section in _SECTIONS - {"version"}:
        entries = value[section]
        if not isinstance(entries, dict) or len(entries) > 128:
            raise ThemeError(f"{section} must be a plain object with at most 128 entries")
        for key, item in entries.items():
            if not _KEY.fullmatch(key):
                raise ThemeError(f"invalid {section} token name")
            if section == "colors":
                valid = isinstance(item, str) and _HEX.fullmatch(item)
            elif section == "typography":
                valid = isinstance(item, str) and _FONT.fullmatch(item)
            elif section == "sizing":
                valid = isinstance(item, str) and _SIZE.fullmatch(item)
            elif section == "decoration":
                valid = key in {"sparkles", "bows", "kisses", "motion"} and type(item) is bool
            elif section == "brand" and key in {"wordmark", "mark"}:
                valid = isinstance(item, str) and _BRAND_ASSET.fullmatch(item)
            else:
                valid = _plain_text(item)
            if not valid:
                raise ThemeError(f"unsafe or invalid {section}.{key}")
    return value
