"""Small result values retain complete server responses for forward compatibility."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from .codec import Schema
from .errors import ErrorDetail


class _AnySnapshot(Enum):
    ANY = "any"


ANY = _AnySnapshot.ANY


@dataclass(frozen=True)
class Page:
    items: tuple[Mapping[str, Any], ...]
    next_cursor: str | None
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class AppendOutcome:
    outcome: str
    phase: str | None
    operation_id: str
    result: Mapping[str, Any] | None
    error: ErrorDetail | None
    raw: Mapping[str, Any]

    @property
    def terminal(self) -> bool:
        return self.outcome in ("committed", "definitely_not_committed")


@dataclass(frozen=True)
class CreateOutcome:
    outcome: str
    phase: str | None
    collection_id: str
    info: Mapping[str, Any] | None
    error: ErrorDetail | None
    raw: Mapping[str, Any]

    @property
    def terminal(self) -> bool:
        return self.outcome in ("committed", "definitely_not_committed")


@dataclass(frozen=True)
class OperationInProgress:
    operation_id: str
    raw: Mapping[str, Any]
    state: str = "in_progress"


@dataclass(frozen=True)
class ScanResult:
    schema: Schema
    snapshot_id: str | None
    rows: list[dict[str, Any]]
    profile: Mapping[str, Any]
    identity: Mapping[str, Any]
    raw: Mapping[str, Any]
