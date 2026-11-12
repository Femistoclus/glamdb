"""Bounded Arrow streaming into DuckDB, with independent legal-answer checks."""

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
                        if pa.types.is_integer(array.type):
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


def execute_query(client, collection, plan, *, mode, workspace, oracle_factory=None,
                  timeout=120, memory_bytes=512 * 1024**2, temp_bytes=2 * 1024**3,
                  max_candidate_bytes=8 * 1024**3, max_result_rows=100000,
                  max_result_bytes=64 * 1024**2, rtol=1e-10, atol=1e-12):
    if mode not in ("scan", "reduce"):
        raise ValueError("query mode must be scan or reduce")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("query timeout must be finite positive seconds")
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in (rtol, atol)):
        raise ValueError("query comparison tolerances must be finite nonnegative values")
    if any(type(value) is not int or value < 1 for value in
           (memory_bytes, temp_bytes, max_candidate_bytes, max_result_rows, max_result_bytes)):
        raise ValueError("query budgets must be finite positive integers")
    if memory_bytes < 32 * 1024**2:
        raise ValueError("DuckDB memory budget must be at least 32 MiB")
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    started, cpu_started = time.perf_counter(), time.process_time()
    metrics = {"query_id": plan.query_id, "mode": mode, "candidate_rows": 0, "candidate_arrow_bytes": 0,
               "batches": 0, "sql_validation": "NOT_APPLICABLE" if mode == "scan" else "NOT_VALIDATED",
               "engine": "GlamDB scan" if mode == "scan" else "GlamDB + DuckDB",
               "measurement": "fresh streamed scan; client reduction and complete input drain included"}

    with client.scan_batches(collection, **plan.scan, deadline=timeout) as batches:
        def converted():
            for batch in batches:
                metrics["candidate_rows"] += batch.num_rows
                metrics["candidate_arrow_bytes"] += batch.nbytes
                metrics["batches"] += 1
                if metrics["candidate_arrow_bytes"] > max_candidate_bytes:
                    raise ValueError("candidate Arrow byte budget exhausted")
                yield logical_batch(batch)
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
            reader = pa.RecordBatchReader.from_batches(schema, chain(() if first is None else (first,), iterator))
            with tempfile.TemporaryDirectory(prefix="clickbench-", dir=workspace) as temporary:
                with duckdb.connect(config={"memory_limit": f"{memory_bytes}B", "max_temp_directory_size": f"{temp_bytes}B",
                                             "temp_directory": temporary, "threads": 1,
                                             "preserve_insertion_order": True}) as db:
                    db.execute("SET enable_external_access=false")
                    timer = threading.Timer(timeout, db.interrupt)
                    timer.daemon = True
                    timer.start()
                    try:
                        db.register("hits", reader)
                        columns = plan.output_columns + plan.order_columns
                        db.execute("CREATE TEMP TABLE actual AS " + _named(plan.validation_sql, columns))
                        # LIMIT may stop DuckDB early; finish the original fixed-view scan.
                        for _ in iterator:
                            pass
                        count = db.execute("SELECT COUNT(*) FROM actual").fetchall()[0][0]
                        if count > max_result_rows:
                            raise ValueError("result row budget exhausted")
                        result_bytes, sample = 0, []
                        output = db.sql("SELECT * FROM actual").to_arrow_reader(8192)
                        for batch in output:
                            result_bytes += batch.nbytes
                            if result_bytes > max_result_bytes:
                                raise ValueError("result byte budget exhausted")
                            if len(sample) < 10:
                                sample.extend([[_sample(value) for value in row.values()][:plan.output_columns]
                                               for row in batch.slice(0, 10 - len(sample)).to_pylist()])
                        metrics.update(output_rows=count, result_arrow_bytes=result_bytes, sample=sample,
                                       elapsed_seconds=time.perf_counter() - started,
                                       client_cpu_seconds=time.process_time() - cpu_started)
                        if oracle_factory is not None:
                            db.unregister("hits")
                            oracle = oracle_factory()
                            try:
                                db.register("hits", oracle)
                                db.execute("CREATE TEMP TABLE reference_full AS " + _named(plan.reference_sql, columns))
                                good, reason = _matches(db, plan, rtol, atol)
                            finally:
                                oracle.close()
                            metrics.update(status="PASS" if good else "FAIL",
                                           sql_validation="PASS" if good else "FAIL", reason=reason)
                        else:
                            metrics["status"] = "INCOMPLETE"
                    finally:
                        timer.cancel()
                        timer.join()
                        reader.close()
    metrics.setdefault("elapsed_seconds", time.perf_counter() - started)
    metrics.setdefault("client_cpu_seconds", time.process_time() - cpu_started)
    metrics["candidate_rows_per_second"] = metrics["candidate_rows"] / metrics["elapsed_seconds"]
    return metrics
