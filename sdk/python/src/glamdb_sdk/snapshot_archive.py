"""Client-side checks for the per-collection snapshot archive setting and status."""

from typing import Any, Mapping

from .codec import int64
from .errors import ValidationError

MAX_HOT_SNAPSHOTS = 2**40
# Explicit "no archive": every retained snapshot stays in the Iceberg root.
KEEP_ALL_SNAPSHOTS_IN_ROOT = 0
STATUS_FIELDS = ("root_snapshots", "archived_snapshots", "expired_snapshots", "archive_pending")
_RANGE = "hot_snapshots must be an integer from 0 through 2^40 or None"


def hot_snapshots_value(value: Any) -> int | None:
    """Validate a requested ``hot_snapshots``.

    None omits the member: on create the server applies its default, on
    update it stops archiving like 0 (``KEEP_ALL_SNAPSHOTS_IN_ROOT``).
    1..2^40 keeps that many root snapshots.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValidationError(_RANGE)
    number = int64(value)
    if not 0 <= number <= MAX_HOT_SNAPSHOTS:
        raise ValidationError(_RANGE)
    return number


def _decimal(value: Any, label: str) -> int:
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal() or str(int(value)) != value:
        raise ValidationError(f"{label} must be a canonical non-negative decimal string")
    return int(value)


def reported_hot_snapshots(value: Any) -> int:
    """Parse a server-reported effective ``hot_snapshots``: a canonical decimal string, "0" without an archive."""
    number = _decimal(value, "hot_snapshots")
    if number > MAX_HOT_SNAPSHOTS:
        raise ValidationError("reported hot_snapshots is outside 0..2^40")
    return number


def reported_collection_hot_snapshots(info: Mapping[str, Any]) -> tuple[int, bool] | None:
    """The effective ``hot_snapshots`` and ``hot_snapshots_defaulted`` of a collection.

    None when the server reports neither (the mock, servers without the
    snapshot archive); one without the other is invalid.
    """
    if "hot_snapshots" not in info and "hot_snapshots_defaulted" not in info:
        return None
    if "hot_snapshots" not in info or not isinstance(info.get("hot_snapshots_defaulted"), bool):
        raise ValidationError("collection must report hot_snapshots with a boolean hot_snapshots_defaulted")
    hot, defaulted = reported_hot_snapshots(info["hot_snapshots"]), info["hot_snapshots_defaulted"]
    if defaulted and hot == KEEP_ALL_SNAPSHOTS_IN_ROOT:
        raise ValidationError("a defaulted hot_snapshots cannot be 0")
    return hot, defaulted


def validate_archive_status(value: Mapping[str, Any], *, maintenance: bool) -> dict[str, Any]:
    """Check a status or maintenance response; counts stay decimal strings."""
    expected = {"hot_snapshots", *STATUS_FIELDS} | ({"snapshots_archived"} if maintenance else set())
    missing = expected - set(value)
    if missing:
        raise ValidationError("snapshot archive response is missing " + ", ".join(sorted(missing)))
    hot = reported_hot_snapshots(value["hot_snapshots"])
    counts = {name: _decimal(value[name], name) for name in expected - {"hot_snapshots"}}
    expected_pending = max(0, counts["root_snapshots"] - hot) if hot else 0
    if counts["archive_pending"] != expected_pending:
        raise ValidationError("archive_pending differs from root_snapshots above hot_snapshots")
    return dict(value)
