"""Remove a test bearer token from every member of a Playwright trace archive."""
import argparse
import json
import os
from pathlib import Path
import zipfile


def sanitize(source, destination, token):
    if not token or len(token) > 4096 or source == destination:
        raise ValueError('invalid trace sanitization inputs')
    secrets = {token.encode(), json.dumps(token, ensure_ascii=True)[1:-1].encode()}
    with zipfile.ZipFile(source) as reader, zipfile.ZipFile(destination, 'x') as writer:
        os.chmod(destination, 0o600)
        if sum(member.file_size for member in reader.infolist()) > 128 * 1024 * 1024:
            raise ValueError('trace exceeds acceptance budget')
        for member in reader.infolist():
            data = reader.read(member)
            for secret in secrets:
                data = data.replace(secret, b'[redacted]')
            writer.writestr(member, data)
    with zipfile.ZipFile(destination) as reader:
        for member in reader.infolist():
            data = reader.read(member)
            if any(secret in data for secret in secrets):
                raise RuntimeError('trace still contains credentials')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('destination', type=Path)
    parser.add_argument('token_file', type=Path)
    args = parser.parse_args()
    try:
        sanitize(args.source, args.destination, args.token_file.read_text().strip())
    finally:
        args.source.unlink(missing_ok=True)
