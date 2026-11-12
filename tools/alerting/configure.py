#!/usr/bin/env python3
"""Create local Telegram secret files without placing secrets in shell history."""

import argparse
import getpass
import os
from pathlib import Path
import re
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secrets-dir", type=Path, default=Path("artifacts/alerting/secrets"))
    parser.add_argument("--rotate", action="store_true", help="replace both secrets; recreate Alertmanager afterwards")
    args = parser.parse_args()
    root = args.secrets_dir
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    names = ("telegram_bot_token", "telegram_chat_id")
    if not args.rotate and any((root / name).exists() for name in names):
        parser.exit(1, "secret files already exist; --rotate explicitly replaces them\n")
    token = getpass.getpass("BotFather token (hidden): ").strip()
    if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token):
        parser.exit(1, "token format is invalid\n")
    chat = input("Numeric destination chat ID (negative IDs are valid): ").strip()
    try:
        chat = str(int(chat))
        if chat == "0":
            raise ValueError()
    except ValueError:
        parser.exit(1, "chat ID must be a nonzero integer\n")
    for name, value in zip(names, (token, chat)):
        fd, temp = tempfile.mkstemp(dir=root)
        try:
            with os.fdopen(fd, "w") as output:
                output.write(value + "\n")
                output.flush()
                os.fsync(output.fileno())
                # Compose local secrets are bind mounts, and file uid/mode
                # remapping is not implemented. A private 0700 parent protects
                # host access; each read-only mount must be readable by the
                # non-root users in the Alertmanager container.
                os.fchmod(output.fileno(), 0o444)
            os.replace(temp, root / name)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)
    print(f"Created two secret files in {root}; values were not printed.")


if __name__ == "__main__":
    main()
