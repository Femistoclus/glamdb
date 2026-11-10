"""Client failures are separate from authoritative server write outcomes."""

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ErrorDetail:
    code: str
    message: str
    retry_hint: str | None
    raw: Mapping[str, Any]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ErrorDetail":
        if not isinstance(value.get("code"), str) or not isinstance(value.get("message"), str):
            raise ProtocolError("server error detail requires string code and message")
        hint = value.get("retry_hint")
        if hint is not None and not isinstance(hint, str):
            raise ProtocolError("server retry_hint must be a string")
        return cls(value["code"], value["message"], hint, dict(value))


class SDKError(Exception):
    """Base class for local client and non-outcome HTTP failures."""


class ValidationError(SDKError, ValueError):
    pass


class ProtocolError(SDKError):
    """The response did not satisfy the public wire contract."""

    def __init__(self, message: str, *, operation_id: str | None = None,
                 collection_id: str | None = None, raw: Any = None):
        super().__init__(message)
        self.operation_id = operation_id
        self.collection_id = collection_id
        self.raw = raw


class TransportError(SDKError):
    """No authoritative write outcome is implied by this exception."""

    def __init__(self, message: str, *, operation_id: str | None = None,
                 collection_id: str | None = None):
        super().__init__(message)
        self.operation_id = operation_id
        self.collection_id = collection_id


class DeadlineExceeded(TransportError):
    pass


class HTTPError(SDKError):
    def __init__(self, status_code: int, detail: ErrorDetail, *, raw: Any = None,
                 operation_id: str | None = None, collection_id: str | None = None):
        super().__init__(f"{detail.code}: {detail.message}")
        self.status_code = status_code
        self.detail = detail
        self.raw = raw
        self.operation_id = operation_id
        self.collection_id = collection_id


class AuthenticationError(HTTPError):
    pass


class IntegrityError(ProtocolError):
    """The complete Arrow body failed framing, byte count, or digest checks."""
