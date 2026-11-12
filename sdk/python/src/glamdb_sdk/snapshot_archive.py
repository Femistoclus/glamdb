"""Client-side checks for the per-collection snapshot archive setting and status."""

from typing import Any, Mapping

from .codec import int64
from .errors import ValidationError

MAX_HOT_SNAPSHOTS = 2**40
STATUS_FIELDS = ("root_snapshots", "archived_snapshots", "expired_snapshots", "archive_pending")


def hot_snapshots_value(value: Any) -> int | None:
    """Validate a requested ``hot_snapshots``: None (no setting) or 1..2^40."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValidationError("hot_snapshots must be an integer from 1 through 2^40 or None")
    number = int64(value)
    if not 1 <= number <= MAX_HOT_SNAPSHOTS:
        raise ValidationError("hot_snapshots must be an integer from 1 through 2^40 or None")
    return number


def _decimal(value: Any, label: str) -> int:
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal() or str(int(value)) != value:
        raise ValidationError(f"{label} must be a canonical non-negative decimal string")
    return int(value)


def reported_hot_snapshots(value: Any) -> int | None:
    """Parse a server-reported ``hot_snapshots``: null or a canonical decimal string in range."""
    if value is None:
        return None
    number = _decimal(value, "hot_snapshots")
    if not 1 <= number <= MAX_HOT_SNAPSHOTS:
        raise ValidationError("reported hot_snapshots is outside 1..2^40")
    return number


def validate_archive_status(value: Mapping[str, Any], *, maintenance: bool) -> dict[str, Any]:
    """Check a status or maintenance response; counts stay decimal strings."""
    expected = {"hot_snapshots", *STATUS_FIELDS} | ({"snapshots_archived"} if maintenance else set())
    missing = expected - set(value)
    if missing:
        raise ValidationError("snapshot archive response is missing " + ", ".join(sorted(missing)))
    hot = reported_hot_snapshots(value["hot_snapshots"])
    counts = {name: _decimal(value[name], name) for name in expected - {"hot_snapshots"}}
    if hot is not None and counts["archive_pending"] != max(0, counts["root_snapshots"] - hot):
        raise ValidationError("archive_pending differs from root_snapshots above hot_snapshots")
    if hot is None and counts["archive_pending"] != 0:
        raise ValidationError("archive_pending requires hot_snapshots")
    return dict(value)
