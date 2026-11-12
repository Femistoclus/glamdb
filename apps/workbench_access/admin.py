"""Local catalog setup/backup and authenticated native lifecycle commands."""

import argparse
import json
from pathlib import Path
import ssl
import sys
from authlib.integrations.httpx_client import OAuth2Client
import httpx
from .resources import CatalogError, ResourceCatalog, catalog_lease, initialize
from .settings import Settings, secret_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--socket", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    commands.add_parser("status")
    commands.add_parser("audit")
    receipt = commands.add_parser("receipt")
    receipt.add_argument("id")
    admissions = commands.add_parser("admissions")
    admissions.add_argument("id")
    apply = commands.add_parser("apply")
    apply.add_argument("--file", required=True, type=Path)
    backup = commands.add_parser("backup")
    backup.add_argument("--output", required=True, type=Path)
    migrate = commands.add_parser("migrate")
    migrate.add_argument("--legacy", required=True, type=Path)
    migrate.add_argument("--namespace", required=True)
    migrate.add_argument("--mapping", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.command in ("init", "backup", "migrate"):
            if not args.catalog:
                parser.error("--catalog is required for offline administration")
            with catalog_lease(args.catalog):
                if args.command == "init":
                    result = initialize(args.catalog)
                elif args.command == "migrate":
                    from .migration import migrate_legacy

                    result = migrate_legacy(
                        args.legacy, args.catalog, args.namespace, args.mapping
                    )
                else:
                    with ResourceCatalog(args.catalog) as catalog:
                        result = catalog.backup(args.output)
        else:
            if not args.config or not args.socket:
                parser.error("--config and --socket are required for online commands")
            settings = Settings.load(args.config)
            sso = settings.sso
            if not sso.directory_client_id or not sso.directory_secret_file:
                raise CatalogError(
                    "INVALID_STATE", "operator client must be configured explicitly"
                )
            verify = (
                ssl.create_default_context(cafile=str(sso.ca_file))
                if sso.ca_file
                else True
            )
            with OAuth2Client(
                sso.directory_client_id,
                secret_file(sso.directory_secret_file),
                verify=verify,
                timeout=5,
                follow_redirects=False,
            ) as oauth:
                token = oauth.fetch_token(
                    sso.endpoint + "/token", grant_type="client_credentials"
                )
            with httpx.Client(
                transport=httpx.HTTPTransport(uds=str(args.socket)),
                base_url="http://localhost",
                headers={"Authorization": "Bearer " + token["access_token"]},
                timeout=60,
                follow_redirects=False,
            ) as client:
                if args.command == "apply":
                    with args.file.open("rb") as source:
                        raw = source.read(65537)
                    if len(raw) > 65536:
                        raise CatalogError("INVALID_ARGUMENT", "command limit exceeded")
                    response = client.post("/operator", json=json.loads(raw))
                else:
                    response = client.get(
                        "/operator",
                        params={
                            "view": args.command,
                            **({"id": args.id} if hasattr(args, "id") else {}),
                        },
                    )
                result = response.json()
                if response.status_code >= 300:
                    print(json.dumps(result))
                    return 1
        print(json.dumps(result))
        return 0
    except Exception as error:
        code = error.code if isinstance(error, CatalogError) else "UNAVAILABLE"
        print(
            json.dumps(
                {
                    "error": {
                        "code": code,
                        "message": (
                            str(error)
                            if isinstance(error, CatalogError)
                            else "Operation did not complete; inspect its receipt before retry"
                        ),
                    }
                }
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
