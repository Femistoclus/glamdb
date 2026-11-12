"""Bounded Arrow streaming into DuckDB, with independent legal-answer checks."""

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from itertools import chain
from pathlib import Path
import tempfile
import threading
import time
import math

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .schema import COLUMNS, STRING_COLUMNS, TIME_COLUMNS


DEFAULT_BUDGETS = {"memory_bytes": 512 * 1024**2, "temp_bytes": 2 * 1024**3,
                   "max_candidate_bytes": 8 * 1024**3, "max_result_rows": 100000,
                   "max_result_bytes": 64 * 1024**2, "rtol": 1e-10, "atol": 1e-12}


def validate_options(timeout, **budgets):
    """Validate the complete query resource profile before any network access."""
    if set(budgets) - DEFAULT_BUDGETS.keys():
        raise ValueError("unknown query budget")
    values = dict(DEFAULT_BUDGETS, **budgets)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("query timeout must be finite positive seconds")
    if any(type(values[name]) not in (int, float) or not math.isfinite(values[name]) or values[name] < 0
           for name in ("rtol", "atol")):
        raise ValueError("query comparison tolerances must be finite nonnegative values")
    if any(type(values[name]) is not int or values[name] < 1 for name in DEFAULT_BUDGETS
           if name not in ("rtol", "atol")):
        raise ValueError("query budgets must be finite positive integers")
    if values["memory_bytes"] < 32 * 1024**2:
        raise ValueError("DuckDB memory budget must be at least 32 MiB")
    return values


def logical_batch(batch):
    arrays, names = [], batch.schema.names
    for name, array in zip(names, batch.columns):
        if name == "EventDate" and pa.types.is_timestamp(array.type):
            array = pc.cast(array, pa.date32())
        elif name in TIME_COLUMNS and pa.types.is_timestamp(array.type):
            array = pc.cast(array, pa.timestamp("us"))
        arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays, names=names)


def source_factory(source, rows, *, batch_rows=8192):
    """Independent PyArrow source conversion; no scan planner or ingest codec."""
    def create():
        with pq.ParquetFile(source) as parquet:
            if tuple(parquet.schema_arrow.names) != COLUMNS:
                raise ValueError("oracle source must retain all 105 columns")
            remaining = rows
            for batch in parquet.iter_batches(batch_size=batch_rows):
                if not remaining:
                    break
                batch = batch.slice(0, min(remaining, batch.num_rows))
                arrays = []
                for name, array in zip(COLUMNS, batch.columns):
                    if name == "EventDate":
                        if array.type == pa.uint16():
                            # The official Parquet source stores Date as epoch days.
                            array = pc.cast(pc.cast(array, pa.int32()), pa.date32())
                        elif pa.types.is_integer(array.type):
                            array = pc.strptime(pc.cast(array, pa.string()), format="%Y%m%d", unit="us")
                        array = pc.cast(array, pa.date32())
                    elif name in TIME_COLUMNS:
                        if pa.types.is_integer(array.type):
                            array = pc.cast(array, pa.timestamp("s"))
                        array = pc.cast(array, pa.timestamp("us"))
                    else:
                        array = pc.cast(array, pa.string() if name in STRING_COLUMNS else pa.int64(), safe=True)
                    if array.null_count:
                        raise ValueError("oracle source contains unsupported required NULL values")
                    arrays.append(array)
                remaining -= batch.num_rows
                yield pa.RecordBatch.from_arrays(arrays, names=COLUMNS)
            if remaining:
                raise ValueError("oracle source ended before the manifest prefix")
    def reader():
        iterator = iter(create())
        first = next(iterator, None)
        if first is None:
            schema = pa.schema([(name, pa.string() if name in STRING_COLUMNS else pa.date32() if
                                 name == "EventDate" else pa.timestamp("us") if name in TIME_COLUMNS else pa.int64())
                                for name in COLUMNS])
            return pa.RecordBatchReader.from_batches(schema, [])
        return pa.RecordBatchReader.from_batches(first.schema, chain((first,), iterator))
    return reader


def _named(sql, columns):
    return f"SELECT * FROM ({sql.rstrip(';')}) AS cb(" + ",".join(f"c{i}" for i in range(columns)) + ")"


def _sample(value):
    if isinstance(value, (Decimal, int)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        return value[:128]
    return value


def _matches(db, plan, rtol, atol):
    """Boundary ties compare sort-key sequences and valid row multiplicities."""
    count = db.execute("SELECT COUNT(*) FROM reference_full").fetchall()[0][0]
    expected = max(0, count - plan.offset)
    if plan.limit is not None:
        expected = min(expected, plan.limit)
    actual = db.execute("SELECT COUNT(*) FROM actual").fetchall()[0][0]
    if actual != expected:
        return False, "result cardinality differs from the reference window"
    columns = plan.output_columns + plan.order_columns
    names = ",".join(f"c{i}" for i in range(columns))
    db.execute(f"CREATE TEMP TABLE a AS SELECT {names},COUNT(*) AS n FROM actual GROUP BY ALL")
    db.execute(f"CREATE TEMP TABLE r AS SELECT {names},COUNT(*) AS n FROM reference_full GROUP BY ALL")
    types = db.execute("DESCRIBE actual").fetchall()
    comparisons = []
    for index, column in enumerate(types):
        left, right = f"a.c{index}", f"r.c{index}"
        if column[1] in ("FLOAT", "DOUBLE"):
            comparisons.append(f"({left} IS NOT DISTINCT FROM {right} OR "
                               f"ABS({left}-{right}) <= {atol}+{rtol}*GREATEST(ABS({left}),ABS({right})))")
        else:
            comparisons.append(f"{left} IS NOT DISTINCT FROM {right}")
    db.execute("CREATE TEMP TABLE matches AS SELECT a.rowid ai,r.rowid ri,a.n an,r.n rn FROM a "
               "LEFT JOIN r ON " + " AND ".join(comparisons))
    # Reject ambiguous approximate matches instead of reusing reference rows.
    invalid = db.execute("SELECT COUNT(*) FROM (SELECT ai FROM matches GROUP BY ai "
                         "HAVING COUNT(ri)<>1 OR MAX(an)>MAX(rn))").fetchall()[0][0]
    reused = db.execute("SELECT COUNT(*) FROM (SELECT ri FROM matches WHERE ri IS NOT NULL "
                        "GROUP BY ri HAVING COUNT(*)>1)").fetchall()[0][0]
    if invalid or reused:
        return False, "result values or duplicate multiplicities differ"
    if plan.order_columns:
        keys = ",".join(f"c{i}" for i in range(plan.output_columns, columns))
        limit = f" LIMIT {plan.limit}" if plan.limit is not None else ""
        offset = f" OFFSET {plan.offset}" if plan.offset else ""
        expected_keys = db.execute(f"SELECT {keys} FROM reference_full ORDER BY {plan.order_by}{limit}{offset}").fetchall()
        actual_keys = db.execute(f"SELECT {keys} FROM actual ORDER BY rowid").fetchall()
        import math
        for left, right in zip(actual_keys, expected_keys):
            if any(not (math.isclose(a, b, rel_tol=rtol, abs_tol=atol) if isinstance(a, float)
                        and isinstance(b, float) else a == b) for a, b in zip(left, right)):
                return False, "result order or selected boundary keys differ"
    return True, None


@contextmanager
def _observed_scan(client, collection, plan, timeout, metrics):
    with client.scan_batches(collection, **plan.scan, deadline=timeout) as batches:
        try:
            yield batches
        finally:
            metrics["arrow_wire_bytes"] = getattr(batches, "bytes_read", 0)
            metrics["integrity"] = getattr(batches, "integrity", "unavailable")


class _Deadline:
    def __init__(self, timeout):
        self.until = time.monotonic() + timeout

    def remaining(self):
        seconds = self.until - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("complete query deadline exceeded")
        return seconds

    def call(self, connection, operation, *args):
        # DuckDB clears an idle interrupt when the next statement starts.
        # Arm a fresh timer only while this particular operation is active.
        timer = threading.Timer(self.remaining(), connection.interrupt)
        timer.daemon = True
        timer.start()
        try:
            result = operation(*args)
        except Exception:
            self.remaining()
            raise
        finally:
            timer.cancel()
            timer.join()
        self.remaining()
        return result

    def batches(self, source):
        iterator = iter(source)
        while True:
            self.remaining()
            try:
                batch = next(iterator)
            except StopIteration:
                self.remaining()
                return
            self.remaining()
            yield batch


class _DeadlineConnection:
    def __init__(self, connection, deadline):
        self.connection, self.deadline = connection, deadline

    def execute(self, sql):
        self.deadline.call(self.connection, self.connection.execute, sql)
        return self

    def fetchall(self):
        return self.deadline.call(self.connection, self.connection.fetchall)

    def register(self, name, value):
        self.deadline.call(self.connection, self.connection.register, name, value)

    def unregister(self, name):
        self.deadline.call(self.connection, self.connection.unregister, name)

    def arrow_batches(self, sql, batch_rows):
        reader = self.deadline.call(self.connection, lambda: self.connection.sql(sql).to_arrow_reader(batch_rows))
        try:
            while True:
                try:
                    batch = self.deadline.call(self.connection, next, reader)
                except StopIteration:
                    self.deadline.remaining()
                    return
                yield batch
        finally:
            reader.close()


def _execute_query(client, collection, plan, *, mode, workspace, oracle_factory=None,
                   timeout=120, memory_bytes=512 * 1024**2, temp_bytes=2 * 1024**3,
                   max_candidate_bytes=8 * 1024**3, max_result_rows=100000,
                   max_result_bytes=64 * 1024**2, rtol=1e-10, atol=1e-12, progress):
    if mode not in ("scan", "reduce"):
        raise ValueError("query mode must be scan or reduce")
    validate_options(timeout, memory_bytes=memory_bytes, temp_bytes=temp_bytes,
                     max_candidate_bytes=max_candidate_bytes, max_result_rows=max_result_rows,
                     max_result_bytes=max_result_bytes, rtol=rtol, atol=atol)
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    started, cpu_started = time.perf_counter(), time.process_time()
    deadline = _Deadline(timeout)
    metrics = progress
    metrics.update({"query_id": plan.query_id, "mode": mode, "candidate_rows": 0, "candidate_arrow_bytes": 0,
                    "batches": 0, "sql_validation": "NOT_APPLICABLE" if mode == "scan" else "NOT_VALIDATED",
                    "complete": False, "result_verified": False, "result_validation_ms": 0.0,
                    "engine": "GlamDB scan" if mode == "scan" else "GlamDB + DuckDB",
                    "measurement": "fresh streamed scan; client reduction and complete input drain included"})

    with _observed_scan(client, collection, plan, deadline.remaining(), metrics) as batches:
        def converted():
            for batch in deadline.batches(batches):
                metrics["candidate_rows"] += batch.num_rows
                metrics["candidate_arrow_bytes"] += batch.nbytes
                metrics["batches"] += 1
                if metrics["candidate_arrow_bytes"] > max_candidate_bytes:
                    raise ValueError("candidate Arrow byte budget exhausted")
                value = logical_batch(batch) if mode == "reduce" else batch
                deadline.remaining()
                yield value
        iterator = iter(converted())
        if mode == "scan":
            for _ in iterator:
                pass
            metrics["status"] = "PASS"
        else:
            import duckdb
            first = next(iterator, None)
            if first is None:
                schema = pa.schema([(name, pa.string() if name in STRING_COLUMNS else pa.date32() if
                                     name == "EventDate" else pa.timestamp("us") if name in TIME_COLUMNS else pa.int64())
                                    for name in plan.projection])
            else:
                schema = first.schema
            deadline.remaining()
            with tempfile.TemporaryDirectory(prefix="clickbench-", dir=workspace) as temporary:
                with duckdb.connect(config={"memory_limit": f"{memory_bytes}B", "max_temp_directory_size": f"{temp_bytes}B",
                                             "temp_directory": temporary, "threads": 1,
                                             "preserve_insertion_order": True}) as connection:
                    db = _DeadlineConnection(connection, deadline)
                    db.execute("SET enable_external_access=false")
                    db.execute("SET TimeZone='UTC'")
                    # HTTP paging can yield a Locust greenlet. Keep it outside
                    # native DuckDB callbacks, whose thread state cannot be
                    # reentered by another user's connection on that thread.
                    db.register("__cb_batch", pa.Table.from_batches([] if first is None else [first], schema=schema))
                    db.execute("CREATE TEMP TABLE hits AS SELECT * FROM __cb_batch")
                    for batch in iterator:
                        db.register("__cb_batch", pa.Table.from_batches([batch]))
                        db.execute("INSERT INTO hits SELECT * FROM __cb_batch")
                    db.unregister("__cb_batch")
                    columns = plan.output_columns + plan.order_columns
                    db.execute("CREATE TEMP TABLE actual AS " + _named(plan.validation_sql, columns))
                    count = db.execute("SELECT COUNT(*) FROM actual").fetchall()[0][0]
                    if count > max_result_rows:
                        raise ValueError("result row budget exhausted")
                    result_bytes, sample = 0, []
                    output = db.arrow_batches("SELECT * FROM actual", 8192)
                    try:
                        for batch in output:
                            result_bytes += batch.nbytes
                            if result_bytes > max_result_bytes:
                                raise ValueError("result byte budget exhausted")
                            if len(sample) < 10:
                                sample.extend([[_sample(value) for value in row.values()][:plan.output_columns]
                                               for row in batch.slice(0, 10 - len(sample)).to_pylist()])
                            deadline.remaining()
                    finally:
                        output.close()
                    metrics.update(output_rows=count, result_arrow_bytes=result_bytes, sample=sample,
                                   elapsed_seconds=time.perf_counter() - started,
                                   client_cpu_seconds=time.process_time() - cpu_started)
                    if oracle_factory is not None:
                        validating = time.perf_counter()
                        oracle, checked = None, None
                        try:
                            db.execute("DROP TABLE hits")
                            deadline.remaining()
                            oracle = oracle_factory()
                            deadline.remaining()
                            checked = pa.RecordBatchReader.from_batches(oracle.schema, deadline.batches(oracle))
                            db.register("hits", checked)
                            db.execute("CREATE TEMP TABLE reference_full AS " + _named(plan.reference_sql, columns))
                            db.unregister("hits")
                            good, reason = _matches(db, plan, rtol, atol)
                            deadline.remaining()
                            metrics.update(status="PASS" if good else "FAIL",
                                           sql_validation="PASS" if good else "FAIL",
                                           reason=reason, result_verified=good)
                        finally:
                            if checked is not None:
                                checked.close()
                            if oracle is not None:
                                oracle.close()
                            metrics["result_validation_ms"] = (time.perf_counter() - validating) * 1000
                    else:
                        metrics["status"] = "INCOMPLETE"
    deadline.remaining()
    metrics.setdefault("elapsed_seconds", time.perf_counter() - started)
    metrics.setdefault("client_cpu_seconds", time.process_time() - cpu_started)
    metrics["candidate_rows_per_second"] = metrics["candidate_rows"] / metrics["elapsed_seconds"]
    metrics["complete"] = True
    return metrics


def execute_query(client, collection, plan, *, mode, workspace, oracle_factory=None,
                  timeout=120, memory_bytes=512 * 1024**2, temp_bytes=2 * 1024**3,
                  max_candidate_bytes=8 * 1024**3, max_result_rows=100000,
                  max_result_bytes=64 * 1024**2, rtol=1e-10, atol=1e-12,
                  progress=None, page_counter=None):
    """Retain partial work and exclude independent validation from query time."""
    from ..runtime import error_code

    metrics = {} if progress is None else progress
    started, cpu_started = time.perf_counter(), time.process_time()
    pages = page_counter.pages if page_counter is not None else None
    try:
        return _execute_query(client, collection, plan, mode=mode, workspace=workspace,
                              oracle_factory=oracle_factory, timeout=timeout, memory_bytes=memory_bytes,
                              temp_bytes=temp_bytes, max_candidate_bytes=max_candidate_bytes,
                              max_result_rows=max_result_rows, max_result_bytes=max_result_bytes,
                              rtol=rtol, atol=atol, progress=metrics)
    except Exception as error:
        metrics.update(status="FAIL", reason=error_code(error), complete=False, result_verified=False)
        raise
    except BaseException:
        metrics.update(status="INCOMPLETE", reason="CANCELLED", complete=False, result_verified=False)
        raise
    finally:
        metrics.setdefault("elapsed_seconds", time.perf_counter() - started)
        metrics.setdefault("client_cpu_seconds", time.process_time() - cpu_started)
        metrics["logical_latency_ms"] = metrics["elapsed_seconds"] * 1000
        metrics["pages"] = page_counter.pages - pages if page_counter is not None else None
