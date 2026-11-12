"""Database ownership and operation receipts, independent of identity credentials."""

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from urllib.parse import quote, urlsplit
from uuid import UUID

APPLICATION_ID = 0x47444252
SCHEMA_VERSION = 3
ROOT = Path(__file__).resolve().parents[2]


class CatalogError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def identifier(value):
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise CatalogError(
            "INVALID_ARGUMENT", "canonical lowercase UUID required"
        ) from None
    return value


def text(value, maximum=128):
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or any(ord(c) < 32 for c in value)
    ):
        raise CatalogError("INVALID_ARGUMENT", "invalid text field")
    return value


def private_output(path):
    path = Path(path)
    if path.absolute().is_relative_to(ROOT / "ui") or path.resolve().is_relative_to(
        ROOT / "ui"
    ):
        raise CatalogError(
            "INVALID_ARGUMENT", "authorization output must remain outside the UI tree"
        )
    return path


@contextmanager
def catalog_lease(path):
    descriptor = os.open(
        str(private_output(path).resolve()) + ".owner.lock",
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
        0o600,
    )
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CatalogError(
                "STATE_IN_USE", "catalog has an owning process"
            ) from None
        yield descriptor
    finally:
        os.close(descriptor)


SCHEMA = """
CREATE TABLE metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),revision INTEGER NOT NULL);
INSERT INTO metadata VALUES(1,0);
CREATE TABLE organizations(id TEXT PRIMARY KEY,name TEXT NOT NULL,enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),identity_id TEXT UNIQUE,state TEXT NOT NULL CHECK(state IN('pending','active','deleting')),version INTEGER NOT NULL);
CREATE TABLE databases(id TEXT PRIMARY KEY,name TEXT NOT NULL,url TEXT NOT NULL UNIQUE,token_env TEXT,token_file TEXT,ca_file TEXT,organization_id TEXT REFERENCES organizations(id),enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),state TEXT NOT NULL CHECK(state IN('active','draining')),version INTEGER NOT NULL,CHECK((token_env IS NULL)!=(token_file IS NULL)),UNIQUE(organization_id,name));
CREATE TABLE operations(id TEXT PRIMARY KEY,digest TEXT NOT NULL,receipt TEXT NOT NULL);
CREATE TABLE audit(sequence INTEGER PRIMARY KEY,at TEXT NOT NULL,revision INTEGER NOT NULL,operation_id TEXT NOT NULL,action TEXT NOT NULL,target TEXT,actor TEXT NOT NULL,details TEXT NOT NULL);
CREATE TABLE admissions(id TEXT PRIMARY KEY,database_id TEXT NOT NULL REFERENCES databases(id),organization_id TEXT NOT NULL,kind TEXT NOT NULL,operation_id TEXT NOT NULL,collection_id TEXT NOT NULL,outcome TEXT NOT NULL CHECK(outcome IN('pending','unknown')),at TEXT NOT NULL);
CREATE TABLE aliases(kind TEXT NOT NULL,legacy_id TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(kind,legacy_id));
CREATE TABLE workflows(id TEXT PRIMARY KEY,digest TEXT NOT NULL,action TEXT NOT NULL,params TEXT NOT NULL,state TEXT NOT NULL,result TEXT,external_started INTEGER NOT NULL DEFAULT 0);
CREATE TABLE legacy_receipts(id TEXT PRIMARY KEY,receipt TEXT NOT NULL);
"""


def initialize(path):
    path = private_output(path).absolute()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
    except FileExistsError:
        raise CatalogError(
            "ALREADY_EXISTS", "refusing to overwrite a catalog"
        ) from None
    os.close(descriptor)
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=EXTRA")
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        connection.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + "\nCOMMIT;")
    finally:
        connection.close()
    sync_path(path)
    return {"schema_version": SCHEMA_VERSION, "revision": 0}


def sync_path(path):
    with Path(path).open("rb") as source:
        os.fsync(source.fileno())
    directory = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def database_definition(params, *, test_mode=False):
    identifier(params["id"])
    text(params.get("name", params["id"]))
    parsed = urlsplit(text(params["url"], 2048))
    try:
        host = parsed.hostname
        port = parsed.port
        if (
            not host
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError()
        backend_path = ""
        if parsed.path not in ("", "/"):
            scoped = re.fullmatch(r"/databases/([0-9a-f-]{36})/?", parsed.path)
            if scoped is None:
                raise ValueError()
            backend_path = "/databases/" + identifier(scoped[1])
        try:
            local = ipaddress.ip_address(host).is_loopback
        except ValueError:
            local = host == "localhost"
        if parsed.scheme != "https" and not (
            parsed.scheme == "http" and (local or test_mode)
        ):
            raise ValueError()
        if port is not None and not 1 <= port <= 65535:
            raise ValueError()
    except ValueError:
        raise CatalogError(
            "INVALID_ARGUMENT",
            "backend requires verified HTTPS or explicit loopback/test HTTP, optionally scoped to /databases/<UUID>",
        ) from None
    if bool(params.get("token_env")) == bool(params.get("token_file")):
        raise CatalogError(
            "INVALID_ARGUMENT", "select one server-side backend credential source"
        )
    if params.get("token_env") and not re.fullmatch(
        r"[A-Z_][A-Z0-9_]{0,127}", params["token_env"]
    ):
        raise CatalogError(
            "INVALID_ARGUMENT", "invalid backend credential environment key"
        )
    for key in ("token_file", "ca_file"):
        if params.get(key):
            path = private_output(text(params[key], 4096))
            if not path.is_absolute():
                raise CatalogError(
                    "INVALID_ARGUMENT", "credential/CA paths must be absolute"
                )
    host = host.lower()
    host = f"[{host}]" if ":" in host else host
    suffix = (
        ""
        if port is None or port == ({"https": 443, "http": 80}[parsed.scheme])
        else f":{port}"
    )
    return {**params, "url": f"{parsed.scheme}://{host}{suffix}{backend_path}"}


class ResourceCatalog:
    def __init__(self, path, *, test_mode=False):
        self.path = Path(path).absolute()
        self.test_mode = test_mode
        self.lock = threading.RLock()
        try:
            info = self.path.lstat()
        except OSError:
            raise CatalogError(
                "NOT_FOUND", "catalog must be explicitly initialized or migrated"
            ) from None
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
            or info.st_nlink != 1
        ):
            raise CatalogError(
                "INVALID_STATE",
                "catalog must be an owned mode0600 single-link regular file",
            )
        self.connection = sqlite3.connect(
            "file:" + quote(str(self.path), safe="/") + "?mode=rw",
            uri=True,
            isolation_level=None,
            timeout=5,
            check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row
        try:
            if (
                self.connection.execute("PRAGMA application_id").fetchone()[0]
                != APPLICATION_ID
                or self.connection.execute("PRAGMA user_version").fetchone()[0]
                != SCHEMA_VERSION
            ):
                raise CatalogError(
                    "MIGRATION_REQUIRED",
                    "password-bearing catalogs cannot be opened by this gateway; migrate offline",
                )
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA journal_mode=DELETE")
            self.connection.execute("PRAGMA synchronous=EXTRA")
            if self.connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise CatalogError("INVALID_STATE", "catalog integrity check failed")
            for row in self._rows("organizations"):
                identifier(row["id"])
            for row in self._rows("databases"):
                database_definition(row, test_mode=test_mode)
        except BaseException:
            self.connection.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        self.connection.close()

    @property
    def revision(self):
        return self.connection.execute(
            "SELECT revision FROM metadata WHERE singleton=1"
        ).fetchone()[0]

    def _rows(self, table):
        return [
            dict(row)
            for row in self.connection.execute(f"SELECT * FROM {table} ORDER BY id")
        ]

    def _get(self, table, value):
        identifier(value)
        row = self.connection.execute(
            f"SELECT * FROM {table} WHERE id=?", (value,)
        ).fetchone()
        if row is None:
            raise CatalogError("NOT_FOUND", "resource not found")
        return dict(row)

    def organization(self, value):
        with self.lock:
            return self._get("organizations", value)

    def organization_by_identity(self, value):
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM organizations WHERE identity_id=?", (identifier(value),)
            ).fetchone()
            if row is None:
                raise CatalogError("NOT_FOUND", "resource not found")
            return dict(row)

    def database(self, value):
        with self.lock:
            return self._get("databases", value)

    def state(self):
        with self.lock:
            return {
                "revision": self.revision,
                "organizations": self._rows("organizations"),
                "databases": self._rows("databases"),
                "legacy_aliases": [
                    dict(row)
                    for row in self.connection.execute(
                        "SELECT * FROM aliases ORDER BY kind,legacy_id"
                    )
                ],
            }

    def unresolved(self, database_id):
        with self.lock:
            return [
                dict(row)
                for row in self.connection.execute(
                    "SELECT * FROM admissions WHERE database_id=? ORDER BY at",
                    (identifier(database_id),),
                )
            ]

    def receipt(self, value):
        with self.lock:
            workflow = self.connection.execute(
                "SELECT state,result FROM workflows WHERE id=?", (identifier(value),)
            ).fetchone()
            if workflow:
                return (
                    json.loads(workflow["result"])
                    if workflow["result"]
                    else {
                        "operation_id": value,
                        "state": workflow["state"],
                        "applied": False,
                    }
                )
            row = self.connection.execute(
                "SELECT receipt FROM operations WHERE id=?", (identifier(value),)
            ).fetchone()
            if not row:
                row = self.connection.execute(
                    "SELECT receipt FROM legacy_receipts WHERE id=?", (value,)
                ).fetchone()
            if not row:
                raise CatalogError("NOT_FOUND", "receipt not found")
            return json.loads(row[0])

    def begin_workflow(self, request):
        self.fields(
            request, ("operation_id", "action", "params"), ("expected_revision",)
        )
        identity = identifier(request["operation_id"])
        body = json.dumps(
            {"action": request["action"], "params": request["params"]},
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(body.encode()).hexdigest()
        with self.transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM legacy_receipts WHERE id=?", (identity,)
            ).fetchone():
                raise CatalogError(
                    "CONFLICT",
                    "legacy receipt is read-only; use a new operation ID for the new authentication model",
                )
            old = connection.execute(
                "SELECT * FROM workflows WHERE id=?", (identity,)
            ).fetchone()
            if old:
                if old["digest"] != digest:
                    raise CatalogError(
                        "CONFLICT", "operation ID is bound to different command content"
                    )
                if old["state"] == "superseded":
                    raise CatalogError(
                        "CONFLICT",
                        "operation was superseded by an emergency disable; use a new reviewed command",
                    )
                if old["state"] == "failed":
                    raise CatalogError(
                        json.loads(old["result"])["error"]["code"],
                        "operation failed without side effects; use a new corrected command",
                    )
                return json.loads(old["result"]) if old["result"] else None
            if "expected_revision" in request and (
                type(request["expected_revision"]) is not int
                or request["expected_revision"] != self.revision
            ):
                raise CatalogError("CONFLICT", "catalog revision changed")
            if (
                connection.execute(
                    "SELECT COUNT(*) FROM workflows WHERE state='pending'"
                ).fetchone()[0]
                >= 256
            ):
                raise CatalogError(
                    "RESOURCE_EXHAUSTED", "pending lifecycle operation budget exhausted"
                )
            pending = connection.execute(
                "SELECT id FROM workflows WHERE state='pending' AND json_extract(params,'$.id')=? AND action LIKE ?",
                (request["params"]["id"], request["action"].split(".")[0] + ".%"),
            ).fetchall()
            if pending:
                emergency = request["action"] == "organization.disable" or (
                    request["action"] == "database.update"
                    and request["params"]
                    == {"id": request["params"]["id"], "enabled": False}
                )
                if not emergency:
                    raise CatalogError(
                        "CONFLICT",
                        "another lifecycle operation owns this resource; retry its receipt or disable access",
                    )
                for previous in pending:
                    receipt = {
                        "operation_id": previous["id"],
                        "state": "superseded",
                        "applied": False,
                        "superseded_by": identity,
                    }
                    connection.execute(
                        "UPDATE workflows SET state='superseded',result=? WHERE id=?",
                        (json.dumps(receipt), previous["id"]),
                    )
            connection.execute(
                "INSERT INTO workflows(id,digest,action,params,state,result) VALUES(?,?,?,?,'pending',NULL)",
                (
                    identity,
                    digest,
                    request["action"],
                    json.dumps(request["params"], sort_keys=True),
                ),
            )

    def finish_workflow(self, identity, result):
        with self.transaction() as connection:
            connection.execute(
                "UPDATE workflows SET state='complete',result=? WHERE id=?",
                (json.dumps(result), identifier(identity)),
            )

    def mark_external(self, identity):
        with self.transaction() as connection:
            connection.execute(
                "UPDATE workflows SET external_started=1 WHERE id=?",
                (identifier(identity),),
            )

    def fail_empty_workflow(self, request, stages, code):
        identity = identifier(request["operation_id"])
        expected = hashlib.sha256(
            json.dumps(
                {"action": request["action"], "params": request["params"]},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT state,digest,external_started FROM workflows WHERE id=?",
                (identity,),
            ).fetchone()
            if (
                row is None
                or row["state"] != "pending"
                or row["digest"] != expected
                or row["external_started"]
            ):
                return
            if any(
                connection.execute(
                    "SELECT 1 FROM operations WHERE id=?", (stage,)
                ).fetchone()
                for stage in stages
            ):
                return
            result = {
                "operation_id": identity,
                "state": "failed",
                "applied": False,
                "error": {"code": code},
            }
            connection.execute(
                "UPDATE workflows SET state='failed',result=? WHERE id=?",
                (json.dumps(result), identity),
            )

    def audit(self, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise CatalogError("INVALID_ARGUMENT", "audit limit must be1..1000")
        with self.lock:
            rows = [
                dict(row)
                for row in self.connection.execute(
                    "SELECT * FROM audit ORDER BY sequence DESC LIMIT ?", (limit,)
                )
            ]
            for row in rows:
                row["details"] = json.loads(row["details"])
            return rows

    @contextmanager
    def transaction(self):
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
                self.connection.execute("COMMIT")
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    @staticmethod
    def fields(value, required, optional=()):
        if (
            not isinstance(value, dict)
            or not set(required) <= value.keys()
            or value.keys() - set(required) - set(optional)
        ):
            raise CatalogError("INVALID_ARGUMENT", "unknown or missing fields")

    def apply(self, request, *, actor="local-operator"):
        self.fields(
            request, ("operation_id", "action", "params"), ("expected_revision",)
        )
        op = identifier(request["operation_id"])
        action = text(request["action"], 64)
        params = request["params"]
        digest = hashlib.sha256(
            json.dumps(
                {"action": action, "params": params},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        try:
            with self.transaction() as db:
                old = db.execute(
                    "SELECT digest,receipt FROM operations WHERE id=?", (op,)
                ).fetchone()
                if old:
                    if old[0] != digest:
                        raise CatalogError(
                            "CONFLICT",
                            "operation ID is bound to different command content",
                        )
                    return json.loads(old[1])
                if "expected_revision" in request and (
                    type(request["expected_revision"]) is not int
                    or request["expected_revision"] != self.revision
                ):
                    raise CatalogError("CONFLICT", "catalog revision changed")
                self._mutate(action, params)
                if (
                    db.execute("SELECT COUNT(*) FROM organizations").fetchone()[0] > 128
                    or db.execute("SELECT COUNT(*) FROM databases").fetchone()[0] > 128
                ):
                    raise CatalogError(
                        "RESOURCE_EXHAUSTED", "resource catalog limit exceeded"
                    )
                db.execute("UPDATE metadata SET revision=revision+1")
                result = {
                    "operation_id": op,
                    "action": action,
                    "target": params.get("id"),
                    "revision": self.revision,
                    "metadata_committed": True,
                    "applied": True,
                }
                db.execute(
                    "INSERT INTO operations VALUES(?,?,?)",
                    (op, digest, json.dumps(result)),
                )
                details = {
                    key: value
                    for key, value in params.items()
                    if key
                    in ("from", "to", "enabled", "expected_owner", "organization_id")
                }
                db.execute(
                    "INSERT INTO audit(at,revision,operation_id,action,target,actor,details) VALUES(?,?,?,?,?,?,?)",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        self.revision,
                        op,
                        action,
                        params.get("id"),
                        text(actor, 256),
                        json.dumps(details),
                    ),
                )
                return result
        except sqlite3.IntegrityError:
            raise CatalogError(
                "CONFLICT", "duplicate identity/name/origin or invalid ownership"
            ) from None

    def _mutate(self, action, p):
        if not isinstance(p, dict):
            raise CatalogError("INVALID_ARGUMENT", "params must be an object")
        db = self.connection
        if action == "organization.create":
            self.fields(p, ("id", "name"))
            db.execute(
                "INSERT INTO organizations VALUES(?,?,0,NULL,'pending',1)",
                (identifier(p["id"]), text(p["name"])),
            )
        elif action == "organization.activate":
            self.fields(p, ("id", "identity_id"), ("enabled",))
            current = self._get("organizations", p["id"])
            if current["state"] != "pending" or current["identity_id"] is not None:
                raise CatalogError(
                    "CONFLICT", "activation requires a new pending organization"
                )
            if type(p.get("enabled", True)) is not bool:
                raise CatalogError("INVALID_ARGUMENT", "enabled must be boolean")
            db.execute(
                "UPDATE organizations SET identity_id=?,enabled=?,state='active',version=version+1 WHERE id=?",
                (identifier(p["identity_id"]), p.get("enabled", True), p["id"]),
            )
        elif action == "organization.update":
            self.fields(p, ("id",), ("name", "enabled", "state"))
            current = self._get("organizations", p["id"])
            if len(p) == 1:
                raise CatalogError("INVALID_ARGUMENT", "change required")
            if (p.get("enabled") is True or p.get("state") == "active") and (
                current["identity_id"] is None or current["state"] == "deleting"
            ):
                raise CatalogError(
                    "CONFLICT", "identity activation or deletion must complete first"
                )
            for key in ("name", "enabled", "state"):
                if key not in p:
                    continue
                value = p[key]
                if key == "name":
                    text(value)
                if key == "enabled" and type(value) is not bool:
                    raise CatalogError("INVALID_ARGUMENT", "enabled must be boolean")
                if key == "state" and value not in ("pending", "active", "deleting"):
                    raise CatalogError("INVALID_ARGUMENT", "invalid organization state")
                db.execute(
                    f"UPDATE organizations SET {key}=?,version=version+1 WHERE id=?",
                    (value, p["id"]),
                )
        elif action == "organization.delete":
            self.fields(p, ("id",))
            org = self._get("organizations", p["id"])
            if org["enabled"] or org["state"] != "deleting":
                raise CatalogError(
                    "CONFLICT", "disable and mark deletion before deleting identity"
                )
            if db.execute(
                "SELECT COUNT(*) FROM databases WHERE organization_id=?", (p["id"],)
            ).fetchone()[0]:
                raise CatalogError(
                    "NOT_EMPTY", "detach or transfer owned databases first"
                )
            db.execute("DELETE FROM organizations WHERE id=?", (p["id"],))
        elif action == "database.create":
            self.fields(
                p,
                ("id", "name", "url"),
                ("organization_id", "token_env", "token_file", "ca_file", "enabled"),
            )
            p = database_definition(p, test_mode=self.test_mode)
            if p.get("organization_id"):
                self._get("organizations", p["organization_id"])
            if type(p.get("enabled", True)) is not bool:
                raise CatalogError("INVALID_ARGUMENT", "enabled must be boolean")
            db.execute(
                "INSERT INTO databases VALUES(?,?,?,?,?,?,?,?, 'active',1)",
                (
                    p["id"],
                    p["name"],
                    p["url"],
                    p.get("token_env"),
                    p.get("token_file"),
                    p.get("ca_file"),
                    p.get("organization_id"),
                    p.get("enabled", True),
                ),
            )
        elif action == "database.update":
            self.fields(
                p,
                ("id",),
                ("name", "url", "token_env", "token_file", "ca_file", "enabled"),
            )
            current = self._get("databases", p["id"])
            candidate = {**current, **p}
            if "token_file" in p:
                candidate["token_env"] = None
            if "token_env" in p:
                candidate["token_file"] = None
            candidate = database_definition(candidate, test_mode=self.test_mode)
            if "enabled" in p and type(p["enabled"]) is not bool:
                raise CatalogError("INVALID_ARGUMENT", "enabled must be boolean")
            if self.unresolved(p["id"]) and (
                set(p) - {"id", "enabled"} or p.get("enabled") is not False
            ):
                raise CatalogError(
                    "BACKEND_UNCERTAIN",
                    "backend writes require reconciliation before mapping changes",
                )
            for key in ("name", "url", "token_env", "token_file", "ca_file", "enabled"):
                db.execute(
                    f"UPDATE databases SET {key}=? WHERE id=?",
                    (candidate[key], p["id"]),
                )
            db.execute("UPDATE databases SET version=version+1 WHERE id=?", (p["id"],))
        elif action in (
            "database.drain",
            "database.resume",
            "database.transfer",
            "database.detach",
            "database.assign",
            "database.delete",
        ):
            needed = {
                "database.drain": ("id", "expected_owner"),
                "database.resume": ("id",),
                "database.transfer": ("id", "from", "to"),
                "database.detach": ("id", "expected_owner"),
                "database.assign": ("id", "organization_id"),
                "database.delete": ("id", "expected_owner"),
            }[action]
            self.fields(p, needed, ("name",) if action == "database.transfer" else ())
            current = self._get("databases", p["id"])
            if action == "database.transfer":
                identifier(p["from"])
                identifier(p["to"])
            expected = p.get("from", p.get("expected_owner"))
            if (
                action not in ("database.assign", "database.resume")
                and current["organization_id"] != expected
            ):
                raise CatalogError("CONFLICT", "database owner changed")
            if action == "database.drain":
                db.execute(
                    "UPDATE databases SET state='draining',version=version+1 WHERE id=?",
                    (p["id"],),
                )
                return
            if self.unresolved(p["id"]):
                raise CatalogError(
                    "BACKEND_UNCERTAIN", "backend write outcome is not authoritative"
                )
            if action == "database.resume":
                db.execute(
                    "UPDATE databases SET state='active',version=version+1 WHERE id=?",
                    (p["id"],),
                )
                return
            if action == "database.assign":
                if current["organization_id"] is not None:
                    raise CatalogError(
                        "CONFLICT", "use explicit transfer for an owned database"
                    )
                target = identifier(p["organization_id"])
            else:
                if current["state"] != "draining":
                    raise CatalogError(
                        "CONFLICT",
                        "close admission and drain before changing ownership",
                    )
                target = p.get("to")
            if target:
                self._get("organizations", target)
            if action == "database.delete":
                db.execute("DELETE FROM databases WHERE id=?", (p["id"],))
                return
            name = text(p.get("name", current["name"]))
            db.execute(
                "UPDATE databases SET organization_id=?,name=?,state='draining',version=version+1 WHERE id=?",
                (target, name, p["id"]),
            )
        else:
            raise CatalogError("INVALID_ARGUMENT", "unsupported resource action")

    def assert_admission(self, database_id, organization_id, versions=None):
        with self.lock:
            db = self._get("databases", database_id)
            org = self._get("organizations", organization_id)
            if (
                db["organization_id"] != organization_id
                or not db["enabled"]
                or db["state"] != "active"
                or not org["enabled"]
                or org["state"] != "active"
            ):
                raise CatalogError("NOT_FOUND", "resource not found")
            if versions is not None and versions != (db["version"], org["version"]):
                raise CatalogError("CONFLICT", "access changed during authorization")
            return db, org

    def admit_write(
        self,
        request_id,
        database_id,
        organization_id,
        kind,
        operation_id,
        collection_id,
        versions=None,
    ):
        if kind not in ("append", "create"):
            raise CatalogError("INVALID_ARGUMENT", "invalid write kind")
        with self.transaction() as connection:
            self.assert_admission(database_id, organization_id, versions)
            if (
                connection.execute("SELECT COUNT(*) FROM admissions").fetchone()[0]
                >= 4096
            ):
                raise CatalogError(
                    "RESOURCE_EXHAUSTED", "unresolved admission budget exhausted"
                )
            connection.execute(
                "INSERT INTO admissions VALUES(?,?,?,?,?,?,'pending',?)",
                (
                    identifier(request_id),
                    identifier(database_id),
                    identifier(organization_id),
                    kind,
                    identifier(operation_id),
                    identifier(collection_id),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def finish_write(self, request_id, outcome):
        if outcome not in ("committed", "definitely_not_committed", "unknown"):
            raise CatalogError("INVALID_ARGUMENT", "unknown write outcome")
        with self.transaction() as db:
            if outcome == "unknown":
                db.execute(
                    "UPDATE admissions SET outcome='unknown' WHERE id=?",
                    (identifier(request_id),),
                )
            else:
                db.execute(
                    "DELETE FROM admissions WHERE id=?", (identifier(request_id),)
                )

    def backup(self, output):
        output = private_output(output)
        if not output.is_absolute():
            raise CatalogError("INVALID_ARGUMENT", "absolute backup output required")
        try:
            fd = os.open(
                output, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600
            )
        except FileExistsError:
            raise CatalogError("ALREADY_EXISTS", "backup never overwrites") from None
        os.close(fd)
        with self.lock, sqlite3.connect(output) as destination:
            self.connection.backup(destination)
        with ResourceCatalog(output, test_mode=self.test_mode):
            pass
        sync_path(output)
        return {"backup_created": True, "revision": self.revision}
