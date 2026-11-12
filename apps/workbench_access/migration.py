"""Offline legacy metadata conversion. Password columns are never selected or copied."""

import json
import os
from pathlib import Path
import sqlite3
import stat
from urllib.parse import quote
from uuid import UUID, uuid4, uuid5

from .admission import child
from .resources import (
    ResourceCatalog,
    CatalogError,
    initialize,
    identifier,
    private_output,
    catalog_lease,
    sync_path,
)
from .provision import write_secret


def migrate_legacy(source, target, namespace, mapping):
    source = private_output(source).absolute()
    target = private_output(target).absolute()
    mapping = private_output(mapping).absolute()
    namespace = UUID(identifier(namespace))
    if (
        target.exists()
        or mapping.exists()
        or target.is_symlink()
        or mapping.is_symlink()
    ):
        raise CatalogError(
            "ALREADY_EXISTS", "migration never overwrites catalog or mapping output"
        )
    info = source.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_mode & 0o077
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
    ):
        raise CatalogError(
            "INVALID_STATE", "legacy catalog must be an owned mode0600 regular file"
        )
    temporary = target.with_name(target.name + ".migrating." + str(uuid4()))
    manifest = {
        "namespace": str(namespace),
        "organizations": [],
        "databases": [],
        "credentials_imported": False,
    }
    with catalog_lease(source), sqlite3.connect(
        "file:" + quote(str(source), safe="/") + "?mode=ro", uri=True
    ) as old:
        old.row_factory = sqlite3.Row
        if (
            old.execute("PRAGMA application_id").fetchone()[0] != 0x47444241
            or old.execute("PRAGMA user_version").fetchone()[0] != 1
            or old.execute("PRAGMA quick_check").fetchone()[0] != "ok"
        ):
            raise CatalogError("INVALID_STATE", "unsupported or corrupt legacy catalog")
        orgs = old.execute(
            "SELECT id,name,login,enabled FROM organizations LIMIT 129"
        ).fetchall()
        databases = old.execute(
            "SELECT id,name,url,token_env,token_file,ca_file,organization_id,enabled FROM databases LIMIT 129"
        ).fetchall()
        if len(orgs) > 128 or len(databases) > 128:
            raise CatalogError(
                "RESOURCE_EXHAUSTED", "legacy catalog exceeds resource bounds"
            )
        identities = {
            row["id"]: str(uuid5(namespace, "organization:" + row["id"]))
            for row in orgs
        }
        dbids = {
            row["id"]: str(uuid5(namespace, "database:" + row["id"]))
            for row in databases
        }
        try:
            initialize(temporary)
            with ResourceCatalog(temporary) as catalog:
                for row in orgs:
                    target_id = identities[row["id"]]
                    operation = str(
                        uuid5(namespace, "identity-activation:" + row["id"])
                    )
                    request = {
                        "operation_id": operation,
                        "action": "organization.create",
                        "params": {
                            "id": target_id,
                            "name": row["name"],
                            "login": row["login"],
                            "enabled": bool(row["enabled"]),
                        },
                    }
                    catalog.begin_workflow(request)
                    catalog.apply(
                        {
                            "operation_id": child(operation, "intent"),
                            "action": "organization.create",
                            "params": {"id": target_id, "name": row["name"]},
                        },
                        actor="offline-migration",
                    )
                    catalog.connection.execute(
                        "INSERT INTO aliases VALUES(?,?,?)",
                        ("organization", row["id"], target_id),
                    )
                    manifest["organizations"].append(
                        {
                            "legacy_id": row["id"],
                            "organization_id": target_id,
                            "activation_command": request,
                        }
                    )
                for row in databases:
                    if (
                        row["organization_id"] is not None
                        and row["organization_id"] not in identities
                    ):
                        raise CatalogError("INVALID_STATE", "legacy owner is missing")
                    params = {
                        key: row[key]
                        for key in ("name", "url", "token_env", "token_file", "ca_file")
                        if row[key] is not None
                    }
                    params.update(
                        id=dbids[row["id"]],
                        enabled=bool(row["enabled"]),
                        organization_id=identities.get(row["organization_id"]),
                    )
                    catalog.apply(
                        {
                            "operation_id": str(
                                uuid5(namespace, "database-import:" + row["id"])
                            ),
                            "action": "database.create",
                            "params": params,
                        },
                        actor="offline-migration",
                    )
                    catalog.connection.execute(
                        "INSERT INTO aliases VALUES(?,?,?)",
                        ("database", row["id"], params["id"]),
                    )
                    manifest["databases"].append(
                        {"legacy_id": row["id"], "database_id": params["id"]}
                    )
                # Preserve readable receipts without reusing old content hashes or credentials.
                for row in old.execute("SELECT id,receipt FROM operations"):
                    identifier(row["id"])
                    receipt = json.loads(row["receipt"])
                    allowed = (
                        "operation_id",
                        "action",
                        "target",
                        "revision",
                        "applied",
                        "metadata_committed",
                    )
                    safe = {key: receipt[key] for key in allowed if key in receipt}
                    safe["legacy_read_only"] = True
                    catalog.connection.execute(
                        "INSERT INTO legacy_receipts VALUES(?,?)",
                        (row["id"], json.dumps(safe)),
                    )
                for row in old.execute(
                    "SELECT at,revision,operation_id,action,target,actor_uid,details FROM audit ORDER BY sequence"
                ):
                    details = json.loads(row["details"])
                    safe = {
                        key: value
                        for key, value in details.items()
                        if key
                        in (
                            "from",
                            "to",
                            "enabled",
                            "expected_owner",
                            "organization_id",
                        )
                    }
                    catalog.connection.execute(
                        "INSERT INTO audit(at,revision,operation_id,action,target,actor,details) VALUES(?,?,?,?,?,?,?)",
                        (
                            row["at"],
                            row["revision"],
                            row["operation_id"],
                            "legacy." + row["action"],
                            row["target"],
                            "legacy-uid:" + str(row["actor_uid"]),
                            json.dumps(safe),
                        ),
                    )
                old_revision = old.execute(
                    "SELECT revision FROM metadata WHERE singleton=1"
                ).fetchone()[0]
                catalog.connection.execute(
                    "UPDATE metadata SET revision=?",
                    (max(old_revision, catalog.revision) + 1,),
                )
                manifest["revision"] = catalog.revision
            sync_path(temporary)
            write_secret(mapping, json.dumps(manifest, indent=2).encode())
            os.link(temporary, target, follow_symlinks=False)
            temporary.unlink()
            sync_path(target)
            sync_path(mapping)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    return {
        "catalog_migrated": True,
        "organizations_pending_activation": len(orgs),
        "mapping": str(mapping),
        "credentials_imported": False,
    }
