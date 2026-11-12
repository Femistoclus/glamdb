"""Finite public route actions and the official Cerbos decision client."""

from dataclasses import dataclass
import re
import time

from cerbos.sdk.grpc.client import AsyncCerbosClient
from cerbos.engine.v1 import engine_pb2
from google.protobuf.struct_pb2 import Value

from .identity import AuthError, finish_stage

UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


@dataclass(frozen=True)
class Route:
    action: str
    metric: str
    kind: str | None = None


ROUTES = (
    ("GET", r"/capabilities", Route("metadata", "capabilities")),
    ("GET", r"/storage", Route("metadata", "storage")),
    ("GET", r"/collections", Route("metadata", "collections")),
    ("GET", r"/collections:resolve", Route("metadata", "collection")),
    ("POST", r"/collections", Route("write", "collections", "create")),
    ("GET", rf"/collections/{UUID}", Route("metadata", "collection")),
    ("GET", rf"/collections/{UUID}/schema", Route("metadata", "collection")),
    ("GET", rf"/collections/{UUID}/snapshots", Route("metadata", "snapshots")),
    ("GET", rf"/collections/{UUID}/files", Route("metadata", "files")),
    ("GET", rf"/collections/{UUID}/files/(?:{UUID}|[0-9a-f]{{64}})", Route("metadata", "file")),
    ("GET", rf"/collections/{UUID}/stats", Route("metadata", "statistics")),
    ("POST", rf"/collections/{UUID}/scan", Route("read", "scan")),
    ("POST", rf"/collections/{UUID}/scan/open", Route("read", "scan")),
    ("POST", rf"/collections/{UUID}/scan/cancel", Route("read", "scan")),
    ("POST", rf"/collections/{UUID}/explain", Route("metadata", "scan")),
    ("POST", rf"/collections/{UUID}/write-settings", Route("write", "collection")),
    ("POST", rf"/collections/{UUID}/append", Route("write", "append", "append")),
    (
        "GET",
        rf"/collections/{UUID}/operations/{UUID}",
        Route("metadata", "operation"),
    ),
)


def classify(method, path):
    if "%" in path or "\\" in path or ".." in path:
        raise ValueError("Unknown route")
    for expected, pattern, route in ROUTES:
        if method == expected and re.fullmatch(pattern, path):
            return route
    raise ValueError("Unknown route")


def attributes(values):
    result = {}
    for key, value in values.items():
        if isinstance(value, bool):
            result[key] = Value(bool_value=value)
        elif isinstance(value, str):
            result[key] = Value(string_value=value)
        else:
            raise ValueError("Finite scalar policy attributes required")
    return result


class Authorizer:
    def __init__(self, settings, *, observer=None):
        self.observer = observer
        tls = (
            False
            if settings.cerbos_plaintext
            else (str(settings.cerbos_ca_file) if settings.cerbos_ca_file else True)
        )
        self.client = AsyncCerbosClient(
            settings.cerbos_target,
            tls_verify=tls,
            timeout_secs=2,
            request_retries=0,
            wait_for_ready=False,
            channel_options={"grpc.max_receive_message_length": 1048576},
        )

    async def close(self):
        await self.client.close()

    async def allowed(
        self, principal, action, database=None, organization=None, *, probe_active=False
    ):
        attrs = {"organization": principal.organization_id or ""}
        person = engine_pb2.Principal(
            id=principal.subject, roles=list(principal.roles), attr=attributes(attrs)
        )
        if database is None:
            resource = engine_pb2.Resource(id="operator", kind="control", attr={})
        else:
            attrs = {
                "owner": database.get("organization_id") or "",
                "organization_active": bool(
                    organization
                    and organization["enabled"]
                    and organization["state"] == "active"
                ),
                "database_active": bool(
                    database["enabled"]
                    and (probe_active or database["state"] == "active")
                ),
            }
            resource = engine_pb2.Resource(
                id=database["id"], kind="database", attr=attributes(attrs)
            )
        started = time.perf_counter()
        try:
            return await self.client.is_allowed(action, person, resource)
        except Exception:
            raise AuthError(503, "UNAVAILABLE") from None
        finally:
            finish_stage(self.observer, "pdp", started)
