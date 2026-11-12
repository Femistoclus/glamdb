import json
import sqlite3
from uuid import uuid4
import pytest
from apps.workbench_access.migration import migrate_legacy
from apps.workbench_access.resources import ResourceCatalog, CatalogError


def test_offline_migration_has_stable_identity_disabled_pending_and_no_credentials(
    tmp_path,
):
    old = tmp_path / "old.sqlite"
    with sqlite3.connect(old) as db:
        db.executescript("""PRAGMA application_id=1195655745; PRAGMA user_version=1;
        CREATE TABLE metadata(singleton,revision);INSERT INTO metadata VALUES(1,9);
        CREATE TABLE organizations(id,name,login,password_hash,enabled);
        INSERT INTO organizations VALUES('pink','Pink','pink-login','NEVER_COPY_CREDENTIAL',0);
        CREATE TABLE databases(id,name,url,token_env,token_file,ca_file,organization_id,enabled);
        INSERT INTO databases VALUES('warehouse','Warehouse','https://backend.example','BACKEND_TOKEN',NULL,NULL,'pink',1);
        CREATE TABLE operations(id,receipt);CREATE TABLE audit(sequence,at,revision,operation_id,action,target,actor_uid,details);
        """)
    old.chmod(0o600)
    namespace = str(uuid4())
    target = tmp_path / "new.sqlite"
    mapping = tmp_path / "mapping.json"
    result = migrate_legacy(old, target, namespace, mapping)
    assert result["credentials_imported"] is False
    with ResourceCatalog(target) as catalog:
        org = catalog.state()["organizations"][0]
        assert (
            not org["enabled"]
            and org["state"] == "pending"
            and org["identity_id"] is None
        )
        assert "NEVER_COPY" not in "\n".join(catalog.connection.iterdump())
        assert catalog.state()["databases"][0]["organization_id"] == org["id"]
    manifest = json.loads(mapping.read_text())
    assert (
        manifest["organizations"][0]["activation_command"]["params"]["enabled"] is False
    )
    assert "NEVER_COPY" not in mapping.read_text()
    migrate_legacy(old, tmp_path / "second.sqlite", namespace, tmp_path / "second.json")
    assert json.loads((tmp_path / "second.json").read_text()) == manifest
    with pytest.raises(CatalogError):
        migrate_legacy(old, target, namespace, mapping)
