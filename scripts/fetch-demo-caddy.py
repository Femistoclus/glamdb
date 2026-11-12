#!/usr/bin/env python3
"""Install a checksum-pinned Caddy in ignored artifacts, never system directories."""

import hashlib
import io
from pathlib import Path
import platform
import shutil
import tarfile
import tempfile
import urllib.request


VERSION = "2.11.9"
# SHA-512 from the official release's caddy_2.11.9_checksums.txt.
PACKAGES = {
    ("Darwin", "arm64"): ("mac_arm64",
        "166a8fdd183dc597ebe3a12d71e2e8eb313ab673ae8d3e286cb92bb0078d04233c"
        "23347543f7d7e8285b44136416e6718b0d106afe973de8ede4d81b6809c6bb"),
    ("Linux", "x86_64"): ("linux_amd64",
        "0e85544340290a16ce4bfb76fe7d65e9de1bf3a6e67f5ccfff30b30d27ea4c73e"
        "078077b7bf3e58ae60b6b12e44327705287059c775bca5782e8ee85ea7edd75"),
}
ROOT = Path(__file__).resolve().parents[1]


def main():
    package = PACKAGES.get((platform.system(), platform.machine()))
    if package is None:
        raise SystemExit("Pinned demo installer supports macOS ARM64 and Linux AMD64")
    name, digest = package
    url = (f"https://github.com/caddyserver/caddy/releases/download/v{VERSION}/"
           f"caddy_{VERSION}_{name}.tar.gz")
    with urllib.request.urlopen(url, timeout=60) as response:
        archive = response.read(64 * 1024 * 1024 + 1)
    if len(archive) > 64 * 1024 * 1024 or hashlib.sha512(archive).hexdigest() != digest:
        raise SystemExit("Caddy archive size/checksum verification failed")
    destination = ROOT / "artifacts/tools" / f"caddy-{VERSION}"
    destination.mkdir(parents=True, exist_ok=True)
    # Extract only the regular executable, not arbitrary paths from an archive.
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        member = bundle.getmember("caddy")
        if not member.isfile() or member.size > 128 * 1024 * 1024:
            raise SystemExit("Unexpected Caddy executable in verified archive")
        with bundle.extractfile(member) as source, tempfile.NamedTemporaryFile(
                dir=destination, delete=False) as target:
            temporary = Path(target.name)
            try:
                shutil.copyfileobj(source, target)
                target.flush()
                temporary.chmod(0o755)
                temporary.replace(destination / "caddy")
            finally:
                temporary.unlink(missing_ok=True)
    print(destination / "caddy")


if __name__ == "__main__":
    main()
