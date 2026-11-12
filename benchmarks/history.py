#!/usr/bin/env python3
"""Run full eight-table TPC-H SF=10 ordinary appends and fresh-process verification."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import time

EXPECTED_ROWS = [5, 25, 100000, 1500000, 2000000, 8000000, 15000000, 59986052]


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    driver, data = args.driver.resolve(strict=True), args.data.resolve(strict=True)
    profile = json.loads(args.profile.read_text())
    profile['checkpoint_reopen'] = True
    save(output / 'profile.json', profile)
    with driver.open('rb') as binary:
        digest = hashlib.file_digest(binary, 'sha256').hexdigest()
    save(output / 'environment.json', {
        'platform': platform.platform(), 'machine': platform.machine(),
        'driver_sha256': digest,
        'profile': profile, 'oracle': 'exact typed multiset, independent parsing, GNU sort -S 64M --parallel=1',
        'qualification': 'fresh processes; OS page cache retained; shared host; no power-loss qualification',
    })
    invocations = []

    def invoke(label, mode, config):
        path = output / (label + '-profile.json')
        save(path, config)
        command = [str(driver), mode, str(output / 'store'), str(data),
                   str(path), str(output / 'ledger.json')]
        with (output / (label + '.json')).open('w') as stdout, \
                (output / (label + '.log')).open('w') as stderr:
            with subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True) as process:
                try:
                    status = process.wait(timeout=args.timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                    raise
        if status:
            raise RuntimeError(f'{label} failed ({status}); see {label}.log')
        value = json.loads((output / (label + '.json')).read_text())
        invocations.append({'label': label, 'command': command, 'result': value})
        save(output / 'invocations.json', invocations)
        print(f'{label}: {json.dumps(value)}', flush=True)
        return value

    started = time.monotonic()
    loaded = invoke('load', 'load', profile)
    if loaded.get('rows') != EXPECTED_ROWS or loaded.get('committed') != 8661 or loaded.get('operations') != 8669:
        raise RuntimeError('full SF=10 cardinalities or ordinary append counts differ')
    verified = invoke('cold-reopen', 'verify', profile)
    if not verified.get('verified') or not verified.get('all_rows_verified'):
        raise RuntimeError('full source and native row verification did not complete')
    lowered = []
    for section, field, value, error in (
            (None, 'max_operations', 8668, 'CONFIG_TOO_SMALL'),
            (None, 'max_catalog_bytes', verified['usage']['catalog_bytes'] - 4096, 'CONFIG_TOO_SMALL'),
            ('history', 'max_snapshots', 1, 'CORRUPTION'),
            ('history', 'max_manifests', 1, 'CORRUPTION'),
            ('history', 'max_data_files', 1, 'CORRUPTION'),
            ('history', 'max_manifest_bytes', 1, 'CORRUPTION'),
            ('history', 'max_manifest_list_bytes', 1, 'CORRUPTION'),
            ('history', 'max_footer_bytes', 1, 'CORRUPTION'),
            ('history', 'max_root_bytes', 1, 'CORRUPTION'),
            ('history', 'max_root_file_bytes', 1024, 'CORRUPTION'),
            ('history', 'max_commit_bytes', 1, 'CORRUPTION'),
            ('history', 'max_recovery_workspace_bytes', 8192, 'CORRUPTION')):
        config = json.loads(json.dumps(profile))
        (config[section] if section else config)[field] = value
        result = invoke('lower-' + field, 'verify', config)
        if result.get('opened') or result.get('error') != error:
            raise RuntimeError(f'{field} did not fail closed: {result}')
        lowered.append(field)
    restored = invoke('restored-profile', 'verify', dict(profile, verify_rows=False))
    if not restored.get('verified'):
        raise RuntimeError('original profile did not restore receipts, retries and historical/initial reads')
    save(output / 'summary.json', {'loaded': loaded, 'cold_reopen': verified,
        'lowered_limits_rejected': lowered, 'restored_profile': restored,
        'wall_seconds': time.monotonic() - started})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--driver', type=Path, default=Path('build/release/benchmarks/glamdb_history_bench'))
    parser.add_argument('--data', required=True, type=Path, help='all eight complete SF=10 .tbl files')
    parser.add_argument('--output', required=True, type=Path, help='new disposable campaign directory')
    parser.add_argument('--profile', type=Path, default=Path(__file__).parent / 'profiles/tpch-sf10.json')
    parser.add_argument('--timeout', type=int, default=10800, help='seconds per subprocess')
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    run(args)


if __name__ == '__main__':
    main()
