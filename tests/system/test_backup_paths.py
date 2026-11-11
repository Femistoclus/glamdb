from pathlib import Path
import subprocess
import sys

import pytest

from tools.acceptance.backup import copy_store


def test_whole_store_copy_includes_orphans_and_checksums(tmp_path):
    source = tmp_path / 'source'
    (source / 'warehouse').mkdir(parents=True)
    (source / 'store.ready').write_bytes(b'ready')
    (source / 'catalog.sqlite').write_bytes(b'closed-catalog')
    (source / 'warehouse' / 'orphan').write_bytes(b'orphan-must-stay')
    target = tmp_path / 'copy'
    copied = copy_store(source, target)
    assert {row['path'] for row in copied['files']} == {'store.ready', 'catalog.sqlite', 'warehouse/orphan'}
    assert all(len(row['sha256']) == 64 for row in copied['files'])
    assert (target / 'warehouse/orphan').read_bytes() == b'orphan-must-stay'
    assert (target / 'catalog.sqlite').stat().st_mode & 0o777 == 0o600


def test_copy_rejects_existing_destination_without_changes(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    target = tmp_path / 'target'
    target.mkdir()
    marker = target / 'existing'
    marker.write_bytes(b'keep')
    with pytest.raises(FileExistsError):
        copy_store(source, target)
    assert marker.read_bytes() == b'keep'


def test_copy_rejects_overlap_and_symlinks_without_ready_marker(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'store.ready').write_bytes(b'ready')
    with pytest.raises(ValueError):
        copy_store(source, source / 'copy')
    (source / 'linked').symlink_to(tmp_path / 'outside')
    target = tmp_path / 'copy'
    with pytest.raises((ValueError, OSError)):
        copy_store(source, target)
    assert not (target / 'store.ready').exists()


def test_unreadable_orphan_directory_cannot_publish_complete_backup(tmp_path):
    source = tmp_path / 'source'
    hidden = source / 'warehouse' / 'unreadable'
    hidden.mkdir(parents=True)
    (hidden / 'orphan.parquet').write_bytes(b'keep-even-unreferenced-files')
    (source / 'store.ready').write_bytes(b'ready')
    (source / 'catalog.sqlite').write_bytes(b'closed-catalog')
    hidden.chmod(0)
    try:
        with pytest.raises(PermissionError):
            copy_store(source, tmp_path / 'copy')
    finally:
        hidden.chmod(0o700)
    assert not (tmp_path / 'copy' / 'store.ready').exists()


def test_special_fifo_is_rejected_without_waiting_for_a_writer(tmp_path):
    import os
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'store.ready').write_bytes(b'ready')
    os.mkfifo(source / 'orphan.fifo')
    code = '''from tools.acceptance.backup import copy_store
from pathlib import Path
import sys
try:
    copy_store(Path(sys.argv[1]),Path(sys.argv[2]))
except ValueError:
    sys.exit(0)
sys.exit(2)
'''
    try:
        result = subprocess.run([sys.executable, '-c', code, str(source), str(tmp_path / 'copy')],
            capture_output=True, text=True, timeout=2)
    except subprocess.TimeoutExpired:
        pytest.fail('backup blocked on a FIFO instead of rejecting it')
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / 'copy' / 'store.ready').exists()
