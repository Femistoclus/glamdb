"""Real native history budgets, duplicate multiplicities and cold-process recovery."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

DRIVER = str(Path(sys.argv[1]).resolve())
del sys.argv[1]
REPOSITORY = Path(__file__).resolve().parents[2]
PROFILE = REPOSITORY / 'benchmarks/profiles/tpch-sf10.json'
ROWS = {
    'region': '0|AFRICA|region comment|',
    'nation': '0|ALGERIA|0|nation comment|',
    'supplier': '1|Supplier|address|0|10-000-000|12.50|supplier comment|',
    'customer': '1|Customer|address|0|10-000-000|-12.50|BUILDING|customer comment|',
    'part': '1|part|manufacturer|brand|type|7|BOX|42.50|part comment|',
    'partsupp': '1|1|3|4.50|partsupp comment|',
    'orders': '1|1|O|42.50|1996-01-01|HIGH|clerk|0|order comment|',
    'lineitem': '1|1|1|1|17|42.50|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER|TRUCK|line comment|',
}


class HistoryAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp'))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.data = self.root / 'data'
        self.data.mkdir()
        # Pairs cross ordinary append boundaries. Distinct values also expose
        # a checkpoint that accidentally restarts or skips the source stream.
        for table, row in ROWS.items():
            (self.data / (table + '.tbl')).write_text(''.join(
                row[:-1] + f' group {index // 2}|\n' for index in range(13)))
        self.profile = json.loads(PROFILE.read_text())
        self.profile.update(batch_rows=5, table_row_limit=13)

    def invoke(self, mode, profile=None, success=True):
        path = self.root / 'profile.json'
        path.write_text(json.dumps(profile or self.profile))
        result = subprocess.run([DRIVER, mode, str(self.root / 'store'), str(self.data),
                                 str(path), str(self.root / 'ledger.json')],
                                text=True, capture_output=True, timeout=60, check=True)
        value = json.loads(result.stdout)
        self.assertEqual(value['opened'], success, result.stderr + result.stdout)
        return value

    def test_duplicate_rows_original_outcomes_retries_and_budget_restoration(self):
        self.profile.update(checkpoint_reopen=True, checkpoint_every=7)
        loaded = self.invoke('load')
        self.assertEqual(loaded['committed'], 24)
        self.assertEqual(loaded['operations'], 32)  # eight permanently bound failures
        self.assertEqual(loaded['rows'], [13] * 8)
        self.assertEqual(loaded['checkpoint_reopens'], 3)
        reopened = self.invoke('verify')
        self.assertTrue(reopened['verified'])
        for section, field, limit, error in (
                (None, 'max_operations', 31, 'CONFIG_TOO_SMALL'),
                ('history', 'max_snapshots', 23, 'CORRUPTION'),
                ('history', 'max_manifests', 23, 'CORRUPTION'),
                ('history', 'max_recovery_workspace_bytes', 8192, 'CORRUPTION')):
            lower = json.loads(json.dumps(self.profile))
            (lower[section] if section else lower)[field] = limit
            with self.subTest(field=field):
                self.assertEqual(self.invoke('verify', lower, success=False)['error'], error)
        self.assertTrue(self.invoke('verify')['verified'])

    def test_oracle_detects_changed_values_without_changing_row_count(self):
        self.invoke('load')
        file = self.data / 'lineitem.tbl'
        file.write_text(file.read_text().replace('line comment', 'different comment', 1))
        path = self.root / 'profile.json'
        result = subprocess.run([DRIVER, 'verify', str(self.root / 'store'), str(self.data),
                                 str(path), str(self.root / 'ledger.json')],
                                text=True, capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('exact row multiset differs', result.stderr)

    def test_oracle_detects_changed_duplicate_multiplicity(self):
        self.invoke('load')
        file = self.data / 'lineitem.tbl'
        file.write_text((ROWS['lineitem'] + '\n') * 12)
        path = self.root / 'profile.json'
        result = subprocess.run([DRIVER, 'verify', str(self.root / 'store'), str(self.data),
                                 str(path), str(self.root / 'ledger.json')],
                                text=True, capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('source has fewer rows than ledger', result.stderr)

    def test_full_oracle_rejects_an_unconsumed_source_suffix(self):
        self.invoke('load')
        file = self.data / 'lineitem.tbl'
        file.write_text(file.read_text() + ROWS['lineitem'] + '\n')
        self.profile['table_row_limit'] = 0
        path = self.root / 'profile.json'
        path.write_text(json.dumps(self.profile))
        result = subprocess.run([DRIVER, 'verify', str(self.root / 'store'), str(self.data),
                                 str(path), str(self.root / 'ledger.json')],
                                text=True, capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('source contains unconsumed rows', result.stderr)

    def test_oracle_rejects_a_numeric_suffix(self):
        self.invoke('load')
        file = self.data / 'lineitem.tbl'
        lines = file.read_text().splitlines()
        # Keep the first operation's prefix valid; only the full-table oracle
        # can catch this suffix, rather than the later exact retry parser.
        lines[6] = lines[6].replace('1|1|1|1|', '1junk|1|1|1|', 1)
        file.write_text('\n'.join(lines) + '\n')
        path = self.root / 'profile.json'
        result = subprocess.run([DRIVER, 'verify', str(self.root / 'store'), str(self.data),
                                 str(path), str(self.root / 'ledger.json')],
                                text=True, capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('invalid oracle integer', result.stderr)

    def test_oracle_normalizes_negative_zero(self):
        file = self.data / 'supplier.tbl'
        file.write_text(file.read_text().replace('12.50', '0.00'))
        self.invoke('load')
        file.write_text(file.read_text().replace('0.00', '-0.00'))
        self.assertTrue(self.invoke('verify')['verified'])


if __name__ == '__main__':
    unittest.main()
