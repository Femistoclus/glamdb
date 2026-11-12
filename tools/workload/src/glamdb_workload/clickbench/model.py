"""Bounded metadata and durable identity files; no credentials are persisted."""

from contextlib import contextmanager
import fcntl
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import tempfile
import stat

from glamdb_sdk.request import json_bytes

MAX_METADATA_BYTES = 16 * 1024**2


@contextmanager
def run_lock(path):
    """One local coordinator per immutable state; the lock inode is retained."""
    path = Path(path).absolute()
    descriptor = os.open(path.with_name(path.name + ".lock"),
                         os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("benchmark lock must be a regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("ClickBench run is already in use by another command") from None
        yield
    finally:
        os.close(descriptor)


def exclusive_state(index):
    def decorate(function):
        @wraps(function)
        def locked(*args, **kwargs):
            path = args[index] if len(args) > index else kwargs["path"]
            with run_lock(path):
                return function(*args, **kwargs)
        return locked
    return decorate


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    path = Path(path)
    if path.stat().st_size > MAX_METADATA_BYTES:
        raise ValueError("benchmark metadata exceeds its byte budget")
    def object_pairs(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate benchmark metadata key")
            value[key] = item
        return value
    def invalid(_):
        raise ValueError("benchmark metadata must contain finite JSON values")
    return json.loads(path.read_bytes(), object_pairs_hook=object_pairs, parse_constant=invalid)


def save_json(path, value):
    path = Path(path)
    payload = json_bytes(value) + b"\n"
    if len(payload) > MAX_METADATA_BYTES:
        raise ValueError("benchmark metadata exceeds its byte budget")
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
