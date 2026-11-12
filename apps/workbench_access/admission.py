"""Database-specific admission fence; transport cancellation is never a write outcome."""

from contextlib import asynccontextmanager
from uuid import UUID, uuid5
import anyio

from .resources import CatalogError


def child(operation_id, stage):
    return str(uuid5(UUID(operation_id), stage))


class AdmissionGate:
    def __init__(self, catalog, maximum=32):
        self.catalog = catalog
        self.maximum = maximum
        self.condition = anyio.Condition()
        self.active = {}
        self.total = 0

    @asynccontextmanager
    async def enter(self, database_id, organization_id, versions, write=None):
        async with self.condition:
            self.catalog.assert_admission(database_id, organization_id, versions)
            if self.total >= self.maximum:
                raise CatalogError(
                    "RESOURCE_EXHAUSTED", "gateway admission budget exhausted"
                )
            if write:
                self.catalog.admit_write(
                    write["id"],
                    database_id,
                    organization_id,
                    write["kind"],
                    write["operation_id"],
                    write["collection_id"],
                    versions,
                )
            self.active[database_id] = self.active.get(database_id, 0) + 1
            self.total += 1
        try:
            yield
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    if write:
                        self.catalog.finish_write(
                            write["id"], write.get("outcome", "unknown")
                        )
                finally:
                    async with self.condition:
                        self.total -= 1
                        self.active[database_id] -= 1
                        if not self.active[database_id]:
                            del self.active[database_id]
                        self.condition.notify_all()

    async def drain(self, database_id, owner, operation_id, timeout, actor):
        async with self.condition:
            self.catalog.apply(
                {
                    "operation_id": child(operation_id, "drain"),
                    "action": "database.drain",
                    "params": {"id": database_id, "expected_owner": owner},
                },
                actor=actor,
            )
            try:
                with anyio.fail_after(timeout):
                    while self.active.get(database_id, 0):
                        await self.condition.wait()
            except TimeoutError:
                raise CatalogError(
                    "DRAIN_PENDING",
                    "admission remains closed; admitted requests have not finished",
                ) from None
            if self.catalog.unresolved(database_id):
                raise CatalogError(
                    "BACKEND_UNCERTAIN",
                    "admission remains closed until authoritative write reconciliation",
                )
