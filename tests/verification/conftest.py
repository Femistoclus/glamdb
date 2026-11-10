import importlib
import importlib.util

import pytest


@pytest.fixture
def verify():
    assert importlib.util.find_spec("glamdb_verify") is not None, "verification package is missing"
    return importlib.import_module("glamdb_verify")
