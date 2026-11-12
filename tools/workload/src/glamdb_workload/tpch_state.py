"""Durable, credential-free benchmark metadata and exclusive run ownership."""

from contextlib import contextmanager
import fcntl
import json
import math
import os
from pathlib import Path
import tempfile

from glamdb_sdk.request import json_bytes, uuid_string

from .runtime import validate_url
from .tpch_target import collection_mode

SECRET_KEYS = frozenset({'token', 'access_token', 'refresh_token', 'client_secret',
                         'authorization', 'password'})


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('benchmark metadata contains duplicate JSON keys')
        result[key] = value
    return result


def _invalid_number(value):
    raise ValueError('benchmark metadata requires finite JSON numbers')


def read_json(path):
    try:
        return json.loads(Path(path).read_bytes(), object_pairs_hook=_object,
                          parse_constant=_invalid_number)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError('benchmark metadata must be valid UTF-8 JSON') from None


def _no_secrets(value):
    if isinstance(value, dict):
        if any(key.lower() in SECRET_KEYS for key in value):
            raise ValueError('benchmark metadata must not contain credentials; use GLAMDB_TOKEN')
        for item in value.values():
            _no_secrets(item)
    elif isinstance(value, list):
        for item in value:
            _no_secrets(item)


def durable_json(path, value):
    """Sync the new file and directory before reporting a metadata transition."""
    path = Path(path).absolute()
    _no_secrets(value)
    body = json_bytes(value) + b'\n'
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(name)
        except FileNotFoundError:
            pass


def read_state(path):
    value = read_json(path)
    if not isinstance(value, dict) or type(value.get('version')) is not int or value['version'] != 1:
        raise ValueError('benchmark run state requires version 1')
    _no_secrets(value)
    collection_mode(value)
    if not isinstance(value.get('url'), str):
        raise ValueError('benchmark run state requires its original server URL')
    validate_url(value['url'])
    database = uuid_string(value.get('database_id'), 'database_id')
    if uuid_string(value.get('store_id'), 'store_id') != database:
        raise ValueError('run state store_id must match its original database_id')
    return value


def write_state(path, saved):
    durable_json(path, saved)


def client_config(saved, timeout, ca_file=None):
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('timeout must be positive finite seconds')
    return {'url': validate_url(saved['url']), 'database': saved['database_id'],
            'timeout': timeout, 'ca_file': ca_file}


@contextmanager
def run_lock(path):
    """One coordinator may mutate a run, including its cleanup, at a time."""
    lock_path = Path(str(Path(path).resolve()) + '.lock')
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('benchmark run is locked by an active command') from None
        yield
    finally:
        os.close(fd)
