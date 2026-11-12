"""Optional bounded operational events; transactional audit remains authoritative."""

import importlib
import os

from .resources import CatalogError


class OperationalEvents:
    def __init__(self, directory=None):
        self.logger = None
        directory = directory or os.environ.get("GLAMDB_LOG_DIRECTORY")
        if directory:
            raise CatalogError(
                "INVALID_ARGUMENT",
                "file logging was removed; configure journald or container stderr rotation",
            )
        try:
            module = importlib.import_module("tools.logging.glamdb_logging")
        except ModuleNotFoundError as error:
            if error.name not in (
                "tools",
                "tools.logging",
                "tools.logging.glamdb_logging",
            ):
                raise
            if directory:
                raise CatalogError(
                    "INVALID_STATE",
                    "configured operational logging requires the logging component",
                ) from None
            return
        try:
            self.logger = module.StructuredLogger()
        except (OSError, ValueError):
            raise CatalogError(
                "INVALID_STATE", "operational stderr sink is unavailable"
            ) from None

    def emit(self, event, **fields):
        if self.logger is not None:
            self.logger.emit(event, **fields)

    def mutation(self, receipt):
        action = receipt["action"]
        fields = {
            "revision": receipt["revision"],
            "outcome": "ok" if receipt["applied"] else "error",
        }
        if action.startswith("organization."):
            fields.update(operation="organization", org_id=receipt.get("target"))
            event = {
                "organization.create": "org_created",
                "organization.delete": "org_deleted",
            }.get(action, "org_updated")
        elif action.startswith("database."):
            fields.update(operation="database", database_id=receipt.get("target"))
            event = {
                "database.create": "database_created",
                "database.delete": "database_deleted",
            }.get(action, "org_updated")
        else:
            event, fields["operation"] = "org_updated", "organization"
        self.emit(event, **fields)

    def close(self):
        if self.logger is not None:
            self.logger.emit("runtime_stopped")
            self.logger.close()
            self.logger = None
