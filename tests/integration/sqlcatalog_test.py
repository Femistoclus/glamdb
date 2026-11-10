"""Exercise the real Iceberg catalog, then verify its state in a new process."""

import pathlib
import subprocess
import sys
import tempfile


def main():
    driver = sys.argv[1]
    with tempfile.TemporaryDirectory(prefix="glamdb-sqlcatalog-", dir="/tmp") as parent:
        root = str(pathlib.Path(parent) / "store")
        subprocess.run([driver, root], check=True)
        subprocess.run([driver, "--verify", root], check=True)


if __name__ == "__main__":
    main()
