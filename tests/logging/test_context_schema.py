"""New native domain fields retain the existing finite collector contract."""
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("glamdb_logging", ROOT / "tools/logging/glamdb_logging.py")


class ContextSchemaTest(unittest.TestCase):
    def setUp(self):
        self.module = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(self.module)

    def test_core_outcome_fields_are_typed_and_preserved(self):
        event = self.module.sanitize_record('operation_completed', operation='scan_next', outcome='error',
            phase='data', error_code='IO_ERROR', byte_count=123)
        self.assertEqual(event['event'], 'operation_completed')
        self.assertEqual(event['operation'], 'scan_next')
        self.assertEqual(event['phase'], 'data')
        self.assertEqual(event['error_code'], 'IO_ERROR')
        self.assertEqual(event['byte_count'], 123)

    def test_untrusted_diagnostics_never_become_log_fields(self):
        private = 'secret-payload-canary'
        event = self.module.sanitize_record('operation_completed', phase=private, error_code=private,
            byte_count=private, message=private, url=private, headers=private, exception=private)
        self.assertEqual(event['phase'], 'none')
        self.assertEqual(event['error_code'], 'OTHER')
        self.assertNotIn('byte_count', event)
        self.assertNotIn(private, str(event))
