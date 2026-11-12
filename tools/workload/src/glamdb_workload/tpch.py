"""Cleanup of an explicitly saved dedicated TPC-H database identity."""

import json
import math
from pathlib import Path

from glamdb_sdk.request import uuid_string

from .runtime import client_for, validate_url


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("run state contains duplicate JSON keys")
        value[key] = item
    return value


def _invalid_number(value):
    raise ValueError("run state must contain finite JSON values")


def clean(state, *, timeout=30.0, ca_file=None):
    """Send one drop using the original endpoint/IDs; never rewrite the state."""
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive finite seconds")
    try:
        saved = json.loads(Path(state).read_bytes(), object_pairs_hook=_object,
                           parse_constant=_invalid_number)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError("run state must be valid UTF-8 JSON") from None
    if not isinstance(saved, dict) or type(saved.get("version")) is not int or saved["version"] != 1:
        raise ValueError("run state requires version 1")
    if not saved.keys().isdisjoint({"token", "access_token", "refresh_token", "client_secret",
                                   "authorization", "password"}):
        raise ValueError("run state must not contain credentials; use GLAMDB_TOKEN")
    if not isinstance(saved.get("url"), str):
        raise ValueError("run state requires a server URL")
    url = validate_url(saved["url"])
    database = uuid_string(saved.get("database_id"), "database_id")
    store = uuid_string(saved.get("store_id"), "store_id")
    if store != database:
        raise ValueError("run state store_id must match its original database_id")
    config = {"url": url, "timeout": timeout, "ca_file": ca_file}
    with client_for(config, scoped=False) as client:
        return client.drop_database(database, store_id=store, deadline=timeout)
