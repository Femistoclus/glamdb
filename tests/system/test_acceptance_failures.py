from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import subprocess

import pytest

from tools.acceptance import run


def test_shutdown_failure_during_restore_cannot_leave_original_path_unavailable(tmp_path, monkeypatch):
    source = tmp_path / 'store'
    source.mkdir()
    (source / 'original').write_text('keep')
    harness = run.Acceptance.__new__(run.Acceptance)
    harness.root = source
    harness.directory = tmp_path
    harness.admin = Path('admin')
    harness.state = {'collection_id': 'collection', 'store_id': 'store',
        'receipt_a': {'result': {'snapshot_id': '123'}}}
    harness.report = {'checks': {'before_restart': {name: {'files': []} for name in ('latest', 'initial', 'a')}}}
    harness.command = lambda *args: None
    harness.start = lambda *args: None
    harness.verify = lambda *args: None
    harness.browser = lambda *args: None
    def failed_shutdown():
        raise RuntimeError('injected restored child failure')
    harness.stop = failed_shutdown
    @contextmanager
    def owned(*args):
        yield SimpleNamespace(poll=lambda: None)
    monkeypatch.setattr(run, 'quiesced', owned)
    monkeypatch.setattr(run.subprocess, 'run', lambda *a, **kw:
        subprocess.CompletedProcess([], 1, stderr='STATE_IN_USE'))
    monkeypatch.setattr(run, 'inspect_store', lambda *args:
        {'views': {'latest': {'rows': '1010', 'files': []}, 'initial': {'rows': '0', 'files': []}, '123': {'rows': '1000', 'files': []}}})
    monkeypatch.setattr(run, 'copy_store', lambda _source, destination: destination.mkdir() or {})
    with pytest.raises(RuntimeError, match='restored child failure'):
        harness.backup_restore()
    assert (source / 'original').read_text() == 'keep'
    assert not list(tmp_path.glob('source-unavailable-*'))
