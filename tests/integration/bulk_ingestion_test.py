"""Fresh-process bulk acceptance and an opt-in bounded-memory measurement.

CTest runs only the small campaign. A manual campaign retains JSON evidence:

  python3 tests/integration/bulk_ingestion_test.py --driver \
    build/release/benchmarks/glamdb_bulk_ingestion_bench --large \
    --output /tmp/glamdb-bulk-large --address-space-mib 768

The large profile generates 1,048,576 rows with 1,024-byte payloads (over 1 GiB
of canonical input) separately in ordinary and bulk processes. RLIMIT_AS applies
to each ingestion and reopen process, including its libraries and allocator;
768 MiB is an address-space ceiling, not an RSS reservation. Use an optimized,
unsanitized Linux build. No source file or complete in-memory dataset is made.
"""

import argparse
import json
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import time
import unittest


DRIVER = None


def run_driver(driver, command, method, root, ledger, rows, payload_bytes, chunk_rows,
               *, address_space_mib=0, timeout=180, expected=0):
    # Temporary directories may be selected through an alias such as /var/tmp.
    # Canonicalize their existing parent while leaving a store-root symlink for
    # the engine's strict root validation to reject.
    root = root.parent.resolve(strict=True) / root.name
    ledger = ledger.resolve()

    def limit_address_space():
        ceiling = address_space_mib * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))

    arguments = [str(driver), command, method, str(root), str(ledger),
                 str(rows), str(payload_bytes), str(chunk_rows)]
    started = time.monotonic()
    process = subprocess.Popen(
        arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        preexec_fn=limit_address_space if address_space_mib else None)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        process.terminate()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
        raise
    if process.returncode != expected:
        raise AssertionError(
            f"{arguments!r} exited {process.returncode}, expected {expected}\n{stdout}\n{stderr}")
    if expected:
        return None
    result = json.loads(stdout)
    result["process_wall_ms"] = (time.monotonic() - started) * 1000
    result["address_space_limit_bytes"] = address_space_mib * 1024 * 1024 or None
    return result


def campaign(driver, output, *, rows, payload_bytes, chunk_rows,
             address_space_mib=0, timeout=600):
    output.mkdir(parents=True, exist_ok=False)
    evidence = {"rows": rows, "payload_bytes": payload_bytes, "chunk_rows": chunk_rows,
                "address_space_limit_bytes": address_space_mib * 1024 * 1024 or None,
                "reopen_cache_state": "fresh process; operating-system caches are not evicted",
                "power_loss_qualified": False}
    for method in ("ordinary", "bulk"):
        root = output / method
        ledger = output / f"{method}.ledger.json"
        for command in ("ingest", "reopen"):
            result = run_driver(driver, command, method, root, ledger, rows,
                                payload_bytes, chunk_rows, address_space_mib=address_space_mib,
                                timeout=timeout)
            if not result["verified"] or result["exact_rows_verified"] != rows:
                raise AssertionError(f"{method} {command} did not verify the complete dataset")
            (output / f"{method}.{command}.json").write_text(json.dumps(result, indent=2) + "\n")
            evidence[f"{method}_{command}"] = result
    (output / "summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


class BulkIngestionProcessTest(unittest.TestCase):
    def test_temporary_parent_alias_is_resolved_and_store_symlink_remains_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory).resolve()
            physical = parent / "physical"
            physical.mkdir()
            alias = parent / "alias"
            alias.symlink_to(physical, target_is_directory=True)
            root = alias / "store"
            ledger = parent / "ledger.json"
            for command in ("ingest", "reopen"):
                result = run_driver(DRIVER, command, "bulk", root, ledger, 2, 16, 2)
                self.assertTrue(result["verified"])
                self.assertEqual(result["exact_rows_verified"], 2)
            store_alias = parent / "store-alias"
            store_alias.symlink_to(physical / "store", target_is_directory=True)
            run_driver(DRIVER, "reopen", "bulk", store_alias, ledger, 2, 16, 2, expected=1)

    def test_bulk_and_ordinary_preserve_exact_rows_history_and_receipts_after_reopen(self):
        # Dropped/duplicated/corrupted rows, partial bulk publication, changed
        # receipts, or replaying a successful ID as a new write must fail here.
        with tempfile.TemporaryDirectory() as directory:
            evidence = campaign(DRIVER, Path(directory) / "campaign", rows=24578,
                                payload_bytes=1024, chunk_rows=3072)
            for method, operations in (("ordinary", 9), ("bulk", 1)):
                with self.subTest(method=method):
                    written = evidence[f"{method}_ingest"]
                    reopened = evidence[f"{method}_reopen"]
                    self.assertEqual(written["receipt_count"], operations)
                    self.assertEqual(written["snapshot_count"], operations)
                    self.assertEqual(written["source_passes"], 2 if method == "bulk" else 1)
                    self.assertEqual(written["source_rows_read"], 49156 if method == "bulk" else 24578)
                    self.assertGreater(written["input_canonical_bytes"], 16 * 1024 * 1024)
                    self.assertLessEqual(written["source_max_batch_rows"], 3072)
                    self.assertGreater(written["data_files"], 1)
                    self.assertGreaterEqual(written["row_groups"], written["data_files"])
                    self.assertGreater(written["reachable_metadata_bytes"], 0)
                    self.assertGreater(written["io"]["file_syncs"], 0)
                    self.assertGreater(written["io"]["directory_syncs"], 0)
                    self.assertGreater(written["rss_peak_bytes"], 0)
                    self.assertGreaterEqual(written["final_ack_ms"], 0)
                    self.assertGreater(written["ingest_mib_per_second"], 0)
                    if method == "bulk":
                        self.assertGreater(written["validation_mib_per_second"], 0)
                    self.assertEqual(reopened["receipts"], written["receipts"])
                    self.assertEqual(reopened["snapshot_count"], operations)
                    self.assertTrue(reopened["initial_empty_verified"])
                    self.assertEqual(reopened["historical_rows_verified"], 24578 if method == "bulk" else 3072)
                    self.assertTrue(reopened["retry_receipt_verified"])
                    self.assertEqual(reopened["retry_io"]["written_bytes"], 0)
                    self.assertEqual(reopened["retry_io"]["read_opens"], 0)
                    self.assertEqual(reopened["retry_io"]["file_syncs"], 0)
                    self.assertEqual(reopened["retry_io"]["directory_syncs"], 0)
                    self.assertGreater(reopened["open_ms"], 0)
            self.assertEqual(evidence["ordinary_ingest"]["input_canonical_bytes"],
                             evidence["bulk_ingest"]["input_canonical_bytes"])
            self.assertGreater(evidence["ordinary_ingest"]["data_files"],
                               evidence["bulk_ingest"]["data_files"])

    def test_process_death_before_publication_leaves_no_rows_and_a_permanent_failure(self):
        # Publishing a prefix or forgetting a registered failed operation on
        # either recovery would turn an interrupted source into partial success.
        for command, exit_code in (("crash-source", 86), ("crash-data", 87)):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "store"
                ledger = Path(directory) / "ledger.json"
                run_driver(DRIVER, command, "bulk", root, ledger, 24578, 1024, 3072,
                           expected=exit_code)
                if command == "crash-data":
                    self.assertTrue(list((root / "warehouse").rglob("*.parquet")),
                                    "data crash must occur after a real data-file sync")
                for _ in range(2):
                    result = run_driver(DRIVER, "crash-reopen", "bulk", root, ledger,
                                        24578, 1024, 3072)
                    self.assertEqual(result["exact_rows_verified"], 0)
                    self.assertEqual(result["snapshot_count"], 0)
                    self.assertEqual(result["failure_code"], "INTERRUPTED")
                    self.assertTrue(result["failed_retry_verified"])
                    self.assertTrue(result["verified"])


def main():
    global DRIVER
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        DRIVER = Path(sys.argv.pop(1)).resolve()
        unittest.main()
        return
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--large", action="store_true", help="generate more than 1 GiB without a source file")
    parser.add_argument("--rows", type=int)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--chunk-rows", type=int, default=4096)
    parser.add_argument("--address-space-mib", type=int, default=768)
    parser.add_argument("--timeout", type=int, default=600)
    arguments = parser.parse_args()
    rows = arguments.rows if arguments.rows is not None else (1048576 if arguments.large else 24578)
    if (rows <= 0 or rows > 25600000 or arguments.payload_bytes < 16 or arguments.payload_bytes > 1048576
            or not 1 <= arguments.chunk_rows <= 10000 or arguments.address_space_mib < 0
            or arguments.timeout <= 0):
        parser.error("invalid finite workload or process limit")
    # Key: 1+4+20 bytes; payload: 1+4+size; nullable value: at least 1 byte.
    if arguments.large and rows * (31 + arguments.payload_bytes) < 1024 ** 3:
        parser.error("--large requires at least 1 GiB of canonical row input")
    if rows * (39 + arguments.payload_bytes) > 4 * 1024 ** 3:
        parser.error("workload can exceed the fixed 4 GiB bulk canonical bound")
    result = campaign(arguments.driver.resolve(), arguments.output.resolve(), rows=rows,
                      payload_bytes=arguments.payload_bytes, chunk_rows=arguments.chunk_rows,
                      address_space_mib=arguments.address_space_mib, timeout=arguments.timeout)
    print(json.dumps({"output": str(arguments.output.resolve()), "rows": rows,
                      "input_canonical_bytes": result["bulk_ingest"]["input_canonical_bytes"],
                      "verified": True}))


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    main()
