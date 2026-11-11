"""Quiesced whole-store copies while the native owning lease remains held."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import select
import stat
import subprocess


@contextmanager
def quiesced(admin, source, store_id):
    process = subprocess.Popen([str(admin), 'quiesce-store', '--root', str(source), '--store-id', store_id],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        if not select.select([process.stdout], [], [], 30)[0]:
            raise RuntimeError('native quiesce handshake timed out')
        line = process.stdout.readline()
        if not line:
            raise RuntimeError('native quiesce failed: ' + process.stderr.read(4096))
        status = json.loads(line)
        if status != {'state': 'quiesced', 'catalog_closed': True, 'store_id': store_id}:
            raise RuntimeError('invalid quiesce handshake')
        yield process
    finally:
        if process.poll() is None:
            try:
                _, error = process.communicate('release\n', timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
                raise RuntimeError('quiesce owner did not release')
        else:
            _, error = process.communicate(timeout=5)
        if process.returncode:
            raise RuntimeError('quiesce owner failed: ' + error[-4096:])


def _sync(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_file(source, destination, relative):
    if not stat.S_ISREG(source.lstat().st_mode):
        raise ValueError('backup rejects symlinks and special files')
    descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        original = os.fstat(descriptor)
        if not stat.S_ISREG(original.st_mode):
            raise ValueError('backup rejects special files')
        digest = hashlib.sha256()
        with os.fdopen(descriptor, 'rb', closefd=False) as reader, destination.open('xb') as writer:
            os.chmod(destination, 0o600)
            while chunk := reader.read(1024 * 1024):
                digest.update(chunk)
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        current = os.fstat(descriptor)
        if ((original.st_dev, original.st_ino, original.st_size, original.st_mtime_ns, original.st_ctime_ns)
                != (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns)):
            raise ValueError('source changed while copying')
        copied = _digest(destination)
        if copied != digest.hexdigest():
            raise ValueError('backup checksum differs')
        return {'path': str(relative), 'bytes': str(original.st_size), 'sha256': copied}
    finally:
        os.close(descriptor)


def copy_store(source, destination):
    """Caller must hold quiesced(); never overwrite, follow links, or omit orphans."""
    source = Path(source).resolve(strict=True)
    destination = Path(destination)
    if not destination.is_absolute() or destination.parent.resolve(strict=True) != destination.parent:
        raise ValueError('destination requires a canonical existing parent')
    if destination == source or destination.is_relative_to(source) or source.is_relative_to(destination):
        raise ValueError('backup roots must be disjoint')
    destination.mkdir(mode=0o700)  # exclusive: existing destinations fail, including symlinks
    records = []
    directories = [destination]
    ready = None
    def traversal_failed(error):
        raise error
    for root, children, files in os.walk(source, followlinks=False, onerror=traversal_failed):
        relative = Path(root).relative_to(source)
        target = destination / relative
        for name in sorted(children):
            entry = Path(root) / name
            if not stat.S_ISDIR(entry.lstat().st_mode):
                raise ValueError('backup rejects symlinks and special files')
            child = target / name
            child.mkdir(mode=0o700)
            directories.append(child)
        for name in sorted(files):
            entry = Path(root) / name
            if relative == Path('.') and name == 'store.ready':
                ready = (entry, target / name, relative / name)
            else:
                records.append(_copy_file(entry, target / name, relative / name))
    if ready is None:
        raise ValueError('source has no completed store marker')
    for directory in reversed(directories):
        _sync(directory)
    # An interrupted partial copy must never look like a completed store.
    records.append(_copy_file(*ready))
    _sync(destination)
    _sync(destination.parent)
    return {'files': records, 'source': str(source), 'destination': str(destination)}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--admin', required=True, type=Path)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--store-id', required=True)
    parser.add_argument('--destination', required=True, type=Path)
    args = parser.parse_args()
    with quiesced(args.admin, args.source, args.store_id) as owner:
        evidence = copy_store(args.source, args.destination)
        if owner.poll() is not None:
            raise RuntimeError('source ownership ended before copy validation')
        subprocess.run([str(args.admin), 'inspect-store', '--root', str(args.destination),
            '--store-id', args.store_id], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(json.dumps({'status': 'PASS', 'copy': evidence}))


if __name__ == '__main__':
    main()
