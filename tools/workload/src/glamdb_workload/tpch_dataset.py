"""Pinned reference DBGEN integration and bounded eight-table datasets."""

from decimal import Decimal
import hashlib
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import time

from glamdb_sdk.request import json_bytes

from .dataset import Dataset, MAX_PART_BYTES, manifest_json, write_json
from .tpch_schema import PROFILES, SCHEMA_VERSION, TABLES, convert_row, logical_fields, table_schema


# The TPC download is registration-gated. These bytes are pinned from the named
# mirror, not asserted to be an independently authenticated official ZIP.
SPECIFICATION_URL = "https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp"
MIRROR_COMMIT = "392ba0c26dae888fcbc27b68b5626182e4d744b5"
ARCHIVE_URL = ("https://raw.githubusercontent.com/cloudberry-contrib/TPC-H-Toolkit/"
               + MIRROR_COMMIT + "/00_compile_tpch/TPCH-software-code-3.0.1.zip")
ARCHIVE_SHA256 = "23fd9a525cd19ffe81089f0c3cc5ba4deedfcd203adfbc5b0135d2005f4f2d0a"
SOURCE_SHA256 = "6876073d91b143511c280893da863eeb6a28162e66c2adfbcc00ac82cfb3adb7"
DISTRIBUTIONS_SHA256 = "4dc56300de1e5ac5e1900519794538e2b5d1b6caf5cd55703bf53000dd748876"
SOURCE_FILES = (
    "bcd2.c", "bcd2.h", "bm_utils.c", "build.c", "config.h", "driver.c", "dss.h", "dsstypes.h",
    "load_stub.c", "makefile.suite", "permute.c", "permute.h", "print.c", "qgen.c", "release.h",
    "rnd.c", "rnd.h", "rng64.c", "rng64.h", "shared.h", "speed_seed.c", "text.c", "tpcd.h", "varsub.c",
)
_HASH = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_SCALE = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z", re.ASCII)
_STANDARD_SCALES = {1, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000}
_BASE_COUNTS = {"region": 5, "nation": 25, "supplier": 10000, "customer": 150000,
                "part": 200000, "partsupp": 800000, "orders": 1500000}
# A finite initial metadata profile admits SF=1/SF=10 with ordinary default
# parts. Larger/tiny-part campaigns require a streamed metadata implementation.
MAX_DATASET_PARTS = 65536
MAX_TOP_MANIFEST_BYTES = 1024 * 1024
MAX_TABLE_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_TOTAL_TABLE_MANIFEST_BYTES = 64 * 1024 * 1024
METADATA_BUDGET = {
    "max_dataset_parts": MAX_DATASET_PARTS,
    "max_top_manifest_bytes": MAX_TOP_MANIFEST_BYTES,
    "max_table_manifest_bytes": MAX_TABLE_MANIFEST_BYTES,
    "max_total_table_manifest_bytes": MAX_TOTAL_TABLE_MANIFEST_BYTES,
}


def file_sha256(path):
    """Hash files without allocating their whole contents."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _scale(value):
    if type(value) is int:
        value = str(value)
    if not isinstance(value, str) or len(value) > 32 or not _SCALE.fullmatch(value):
        raise ValueError("scale_factor must be exact plain decimal text")
    factor = Decimal(value)
    if factor == Decimal("0.001"):
        raise ValueError("scale_factor 0.001 is unsupported: reference DBGEN can generate duplicate PARTSUPP keys")
    if not (factor in (Decimal("0.01"), Decimal("0.1")) or
            1 <= factor <= 100000 and factor == factor.to_integral_value()):
        raise ValueError("supported scale factors are 0.01, 0.1 and integers 1 through 100000")
    return value, factor


def expected_counts(scale_factor):
    """Exact fixed/table counts; LINEITEM remains the generator's actual count."""
    _, factor = _scale(scale_factor)
    numerator, denominator = factor.as_integer_ratio()
    counts = {t: n if t in ("region", "nation") else n * numerator // denominator
              for t, n in _BASE_COUNTS.items()}
    if factor == 1:
        counts["lineitem"] = 6001215
    elif factor == 10:
        counts["lineitem"] = 59986052
    return counts


def _part_count_lower_bound(scale_factor, part_rows, generator_parts):
    counts = expected_counts(scale_factor)
    counts.setdefault("lineitem", counts["orders"])
    return sum(max((rows + part_rows - 1) // part_rows,
                   1 if table in ("region", "nation") else generator_parts)
               for table, rows in counts.items())


def _bounded_metadata(path, byte_limit):
    with Path(path).open("rb") as source:
        raw = source.read(byte_limit + 1)
    if len(raw) > byte_limit:
        raise ValueError(f"TPC-H metadata exceeds byte limit {byte_limit}: {Path(path).name}")
    return raw


def _write_metadata(path, value, byte_limit):
    # Descriptor/source counts already have a finite global bound before JSON
    # serialization; byte ceilings also bound on-disk metadata and later reads.
    size = len(json_bytes(value)) + 1
    if size > byte_limit:
        raise ValueError(f"TPC-H metadata exceeds byte limit {byte_limit}: {Path(path).name}")
    write_json(path, value)
    return size


class _GenerationBudget:
    def __init__(self):
        self.parts = 0
        self.table_metadata_bytes = 0

    def claim_part(self):
        if self.parts >= MAX_DATASET_PARTS:
            raise ValueError(f"TPC-H metadata part limit {MAX_DATASET_PARTS} exceeded; "
                             "increase part_rows/part_bytes or use a supported metadata profile")
        self.parts += 1


def _generator_provenance(dbgen, distributions, executable_pin):
    """Bind an explicitly supplied executable to unmodified pinned sources."""
    try:
        source_files = {name: file_sha256(dbgen.parent / name) for name in SOURCE_FILES}
    except OSError as exc:
        raise ValueError("DBGEN requires the pinned 3.0.1 source files beside its executable") from exc
    source_digest = hashlib.sha256(json_bytes(source_files)).hexdigest()
    if source_digest != SOURCE_SHA256:
        raise ValueError("DBGEN source does not match the pinned 3.0.1 package")
    if not dbgen.is_file() or not os.access(dbgen, os.X_OK):
        raise ValueError("DBGEN executable is missing or not executable")
    binary_digest = file_sha256(dbgen)
    if executable_pin is not None and (not isinstance(executable_pin, str) or
                                     not _HASH.fullmatch(executable_pin) or binary_digest != executable_pin):
        raise ValueError("DBGEN executable SHA-256 does not match dbgen_sha256")
    try:
        distribution_digest = file_sha256(distributions)
    except OSError as exc:
        raise ValueError("DBGEN distributions are missing") from exc
    if distribution_digest != DISTRIBUTIONS_SHA256:
        raise ValueError("DBGEN distribution SHA-256 does not match the pinned package")
    # The 3.0.1 package retained release.h's internal 3.0.0 version. Preserve
    # that distinction instead of accepting an unrelated binary by its banner.
    check = subprocess.run([str(dbgen), "-h"], capture_output=True, timeout=30, check=False)
    banner = (check.stdout + check.stderr).decode("utf-8", errors="replace")
    if "TPC-H Population Generator (Version 3.0.0 build 0)" not in banner:
        raise ValueError("DBGEN reported version differs from the pinned source package")
    return {
        "name": "TPC-H DBGEN", "package_version": "3.0.1", "reported_version": "3.0.0",
        "reported_build": 0, "specification_url": SPECIFICATION_URL,
        "archive_url": ARCHIVE_URL, "archive_sha256": ARCHIVE_SHA256,
        "archive_origin": "pinned Cloudberry mirror; official archive bytes not independently compared",
        "mirror_commit": MIRROR_COMMIT, "source_sha256": source_digest, "source_files": source_files,
        "distributions_sha256": distribution_digest, "executable_sha256": binary_digest,
        "rng": "reference default seeds; no updates",
    }


def _identity(manifest):
    fields = ("version", "format", "scale_factor", "type_profile", "schema_version", "generator",
              "partition_plan", "part_rows", "part_bytes", "tables", "rows", "bytes")
    return "sha256:" + hashlib.sha256(json_bytes({key: manifest[key] for key in fields})).hexdigest()


class _TableWriter:
    def __init__(self, output, table, profile, part_rows, part_bytes, budget):
        self.path = output / table
        self.path.mkdir()
        self.table, self.profile = table, profile
        self.budget = budget
        self.block = bytearray()
        self.count = 0
        self.partition = None
        self.manifest = {
            "version": 1, "format": "glamdb-jsonl", "complete": False,
            "schema": table_schema(table, profile).to_dict(), "rows": 0, "bytes": 0,
            "part_rows": part_rows, "part_bytes": part_bytes, "parts": [], "sources": [],
            "tpch": {"table": table, "schema_version": SCHEMA_VERSION, "type_profile": profile,
                     "fields": logical_fields(table, profile)},
        }

    def flush(self):
        if not self.count:
            return
        self.budget.claim_part()
        ordinal = len(self.manifest["parts"])
        name = f"part-{ordinal:08d}.jsonl"
        with (self.path / name).open("xb") as target:
            target.write(self.block)
        self.manifest["parts"].append({
            "id": f"{self.table}:{ordinal:08d}", "path": name,
            "rows": self.count, "bytes": len(self.block),
            "sha256": hashlib.sha256(self.block).hexdigest(), "generator_partition": self.partition,
        })
        self.manifest["rows"] += self.count
        self.manifest["bytes"] += len(self.block)
        self.block.clear()
        self.count = 0

    def convert(self, source, partition):
        self.flush()
        self.partition = partition
        digest, rows, size = hashlib.sha256(), 0, 0
        with source.open("rb") as handle:
            # A valid DBGEN row is small; a bounded read also rejects a damaged
            # source lacking newlines without allocating a whole .tbl file.
            for line in iter(lambda: handle.readline(MAX_PART_BYTES + 1), b""):
                if len(line) > MAX_PART_BYTES:
                    raise ValueError("DBGEN source row exceeds the bounded conversion limit")
                digest.update(line)
                rows += 1
                size += len(line)
                encoded = json_bytes(convert_row(self.table, line.decode("utf-8"), self.profile)) + b"\n"
                if len(encoded) > self.manifest["part_bytes"]:
                    raise ValueError("converted DBGEN row exceeds part_bytes")
                if self.count and (self.count == self.manifest["part_rows"] or
                                   len(self.block) + len(encoded) > self.manifest["part_bytes"]):
                    self.flush()
                self.block.extend(encoded)
                self.count += 1
        self.flush()
        self.manifest["sources"].append({"path": source.name, "generator_partition": partition,
                                         "rows": rows, "bytes": size, "sha256": digest.hexdigest()})
        return size

    def finish(self):
        self.flush()
        self.manifest["complete"] = True
        remaining = MAX_TOTAL_TABLE_MANIFEST_BYTES - self.budget.table_metadata_bytes
        if remaining < 1:
            raise ValueError("TPC-H aggregate table metadata byte limit exceeded")
        size = _write_metadata(self.path / "manifest.json", self.manifest,
                               min(MAX_TABLE_MANIFEST_BYTES, remaining))
        self.budget.table_metadata_bytes += size
        return {"path": self.table, "manifest_sha256": file_sha256(self.path / "manifest.json"),
                "rows": self.manifest["rows"], "bytes": self.manifest["bytes"]}


def generate(*, scale_factor, output, dbgen, type_profile, generator_parts=1,
             part_rows=10000, part_bytes=MAX_PART_BYTES, distributions=None, dbgen_sha256=None):
    """Generate reference chunks of one global SF, then publish complete JSONL."""
    scale_factor, factor = _scale(scale_factor)
    if type_profile not in PROFILES:
        raise ValueError("type_profile must be native or legacy-scaled")
    if type(part_rows) is not int or not 1 <= part_rows <= 10000:
        raise ValueError("part_rows must be between one and 10000")
    if type(part_bytes) is not int or not 1024 <= part_bytes <= MAX_PART_BYTES:
        raise ValueError("part_bytes must be between 1024 and 8 MiB")
    if type(generator_parts) is not int or not 1 <= generator_parts <= min(100000, expected_counts(scale_factor)["supplier"]):
        raise ValueError("generator_parts must be positive and no greater than the global supplier count or 100000")
    minimum_parts = _part_count_lower_bound(scale_factor, part_rows, generator_parts)
    if minimum_parts > MAX_DATASET_PARTS:
        raise ValueError(f"TPC-H metadata part limit {MAX_DATASET_PARTS} is insufficient: "
                         f"this scale/partition/row plan needs at least {minimum_parts} parts; "
                         "increase part_rows or use a supported metadata profile")
    dbgen = Path(dbgen).resolve()
    distributions = Path(distributions).resolve() if distributions is not None else dbgen.parent / "dists.dss"
    provenance = _generator_provenance(dbgen, distributions, dbgen_sha256)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "version": 1, "format": "glamdb-tpch", "complete": False,
        "dataset_id": None, "scale_factor": scale_factor,
        "scale_class": "standard" if factor in _STANDARD_SCALES else "nonstandard-development",
        "type_profile": type_profile, "schema_version": SCHEMA_VERSION, "generator": provenance,
        "partition_plan": {"method": "DBGEN native -C/-S skip-ahead", "generator_parts": generator_parts,
                           "global_scale_factor": scale_factor, "fixed_tables": ["region", "nation"],
                           "fixed_table_generations": 1, "steps": list(range(1, generator_parts + 1))},
        "part_rows": part_rows, "part_bytes": part_bytes, "tables": {}, "rows": 0, "bytes": 0,
        "metadata_budget": dict(METADATA_BUDGET),
        "source_bytes": 0, "versions": {"conversion": 1, "glamdb-sdk": version("glamdb-sdk"),
                                         "glamdb-workload": version("glamdb-workload"),
                                         "python": platform.python_version()},
        "timings": {"generator_seconds": 0.0, "conversion_seconds": 0.0},
    }
    _write_metadata(output / "manifest.json", manifest, MAX_TOP_MANIFEST_BYTES)
    budget = _GenerationBudget()
    writers = {t: _TableWriter(output, t, type_profile, part_rows, part_bytes, budget) for t in TABLES}
    scratch = output / "_source"
    scratch.mkdir()
    # DBGEN's first default invocation emits all tables. The fixed tables
    # ignore -C/-S; subsequent chunks select only scalable table groups.
    # This avoids its expensive text-pool initialization for every table
    # in the ordinary single-chunk profile.
    jobs = [(None, TABLES, 1)]
    jobs += [(code, tables, step) for step in range(2, generator_parts + 1)
             for code, tables in (("s", ("supplier",)), ("c", ("customer",)),
                                  ("p", ("part", "partsupp")), ("o", ("orders", "lineitem")))]
    for ordinal, (code, tables, step) in enumerate(jobs):
        folder = scratch / f"chunk-{ordinal:08d}"
        folder.mkdir()
        args = [str(dbgen), "-s", scale_factor, "-b", str(distributions)]
        if code is not None:
            args += ["-T", code]
        if generator_parts > 1:
            args += ["-C", str(generator_parts), "-S", str(step)]
        # DBGEN uses a 256-byte internal output path buffer. Relative DSS_PATH
        # keeps long user dataset paths out of that legacy buffer.
        env = dict(os.environ, DSS_PATH=".", DSS_CONFIG=".")
        started = time.perf_counter()
        with (output / "generator.log").open("ab") as log:
            result = subprocess.run(args, cwd=folder, env=env, stdin=subprocess.DEVNULL,
                                    stdout=log, stderr=subprocess.STDOUT, check=False)
        manifest["timings"]["generator_seconds"] += time.perf_counter() - started
        if result.returncode:
            raise ValueError(f"DBGEN generator failed with exit status {result.returncode}; see generator.log")
        started = time.perf_counter()
        for table in tables:
            fixed = table in ("region", "nation")
            suffix = f".{step}" if not fixed and generator_parts > 1 else ""
            source = folder / f"{table}.tbl{suffix}"
            if not source.is_file() or source.is_symlink():
                raise ValueError(f"DBGEN generator did not produce {source.name}")
            manifest["source_bytes"] += writers[table].convert(source, 0 if fixed else step)
            source.unlink()
        manifest["timings"]["conversion_seconds"] += time.perf_counter() - started
        folder.rmdir()
    counts = expected_counts(scale_factor)
    for table, writer in writers.items():
        rows = writer.manifest["rows"]
        if table in counts and rows != counts[table]:
            raise ValueError(f"DBGEN {table} row count {rows} differs from expected {counts[table]}")
        if table == "lineitem" and not counts["orders"] <= rows <= 7 * counts["orders"]:
            raise ValueError("DBGEN lineitem row count is inconsistent with orders")
        manifest["tables"][table] = writer.finish()
    scratch.rmdir()
    manifest["rows"] = sum(t["rows"] for t in manifest["tables"].values())
    manifest["bytes"] = sum(t["bytes"] for t in manifest["tables"].values())
    manifest["dataset_id"] = _identity(manifest)
    manifest["complete"] = True
    _write_metadata(output / "manifest.json", manifest, MAX_TOP_MANIFEST_BYTES)
    return manifest


class TPCHDataset:
    """Strict top-level binding of eight existing generic Dataset objects."""

    def __init__(self, path):
        self.path = Path(path).resolve()
        try:
            if (self.path / "manifest.json").is_symlink():
                raise ValueError("unsafe TPC-H manifest path")
            raw = _bounded_metadata(self.path / "manifest.json", MAX_TOP_MANIFEST_BYTES)
            self._manifest_sha256 = hashlib.sha256(raw).hexdigest()
            self.manifest = manifest_json(raw)
            self._check_manifest()
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValueError("invalid TPC-H dataset manifest") from exc
        except OSError as exc:
            raise ValueError("TPC-H dataset part or manifest is missing or unreadable") from exc

    def _check_manifest(self):
        m = self.manifest
        if type(m.get('version')) is not int or m.get("version") != 1 or m.get("format") != "glamdb-tpch" or m.get("complete") is not True:
            raise ValueError("TPC-H dataset is incomplete or has unsupported version/format")
        if "metadata_budget" in m and m["metadata_budget"] != METADATA_BUDGET:
            raise ValueError("unsupported TPC-H metadata budget limits")
        self.scale_factor, factor = _scale(m["scale_factor"])
        self.profile = m["type_profile"]
        if self.profile not in PROFILES or type(m['schema_version']) is not int or m["schema_version"] != SCHEMA_VERSION:
            raise ValueError("unsupported TPC-H schema/profile")
        if (type(m['versions']['conversion']) is not int or m["versions"]["conversion"] != 1 or
                any(not isinstance(m["versions"][key], str) or not m["versions"][key]
                    for key in ("glamdb-sdk", "glamdb-workload", "python"))):
            raise ValueError("unsupported or invalid TPC-H conversion version metadata")
        if (m["scale_class"] != ("standard" if factor in _STANDARD_SCALES else "nonstandard-development") or
                any(type(m["timings"][key]) not in (int, float) or not math.isfinite(m["timings"][key]) or
                    m["timings"][key] < 0 for key in ("generator_seconds", "conversion_seconds"))):
            raise ValueError("invalid TPC-H scale or generation timing metadata")
        generator = m["generator"]
        if (generator["package_version"] != "3.0.1" or generator["reported_version"] != "3.0.0" or
                generator["source_sha256"] != SOURCE_SHA256 or generator["archive_sha256"] != ARCHIVE_SHA256 or
                generator["distributions_sha256"] != DISTRIBUTIONS_SHA256 or
                not _HASH.fullmatch(generator["executable_sha256"])):
            raise ValueError("unsupported or damaged TPC-H generator provenance")
        if generator.get('rng') != 'reference default seeds; no updates':
            raise ValueError('unsupported TPC-H generator RNG configuration')
        inventory = generator.get('source_files')
        if inventory is not None:
            if (not isinstance(inventory, dict) or set(inventory) != set(SOURCE_FILES)
                    or any(not isinstance(value, str) or not _HASH.fullmatch(value) for value in inventory.values())
                    or hashlib.sha256(json_bytes({name: inventory[name] for name in SOURCE_FILES})).hexdigest() != SOURCE_SHA256):
                raise ValueError('TPC-H generator source inventory contradicts its pinned digest')
        for key, expected_value in {'archive_url': ARCHIVE_URL, 'mirror_commit': MIRROR_COMMIT,
                                    'specification_url': SPECIFICATION_URL}.items():
            if key in generator and generator[key] != expected_value:
                raise ValueError('TPC-H generator source provenance is inconsistent')
        plan = m["partition_plan"]
        parts = plan["generator_parts"]
        if (type(parts) is not int or parts < 1 or parts > min(100000, expected_counts(self.scale_factor)["supplier"]) or
                plan["global_scale_factor"] != self.scale_factor or plan["steps"] != list(range(1, parts + 1)) or
                plan["fixed_tables"] != ["region", "nation"] or type(plan['fixed_table_generations']) is not int or plan["fixed_table_generations"] != 1 or
                plan["method"] != "DBGEN native -C/-S skip-ahead"):
            raise ValueError("invalid TPC-H partition plan")
        if type(m["part_rows"]) is not int or not 1 <= m["part_rows"] <= 10000 or type(m["part_bytes"]) is not int or not 1024 <= m["part_bytes"] <= MAX_PART_BYTES:
            raise ValueError("invalid bounded TPC-H part limits")
        if set(m["tables"]) != set(TABLES):
            raise ValueError("TPC-H dataset requires all eight tables")
        self.tables = {}
        descriptor_count = 0
        table_metadata_bytes = 0
        expected = expected_counts(self.scale_factor)
        for table in TABLES:
            item = m["tables"][table]
            if item["path"] != table or not _HASH.fullmatch(item["manifest_sha256"]):
                raise ValueError("unsafe TPC-H table path or invalid manifest hash")
            folder = self.path / table
            path = folder / "manifest.json"
            if folder.is_symlink() or path.is_symlink() or folder.resolve().parent != self.path:
                raise ValueError("unsafe TPC-H table path")
            remaining = MAX_TOTAL_TABLE_MANIFEST_BYTES - table_metadata_bytes
            if remaining < 1:
                raise ValueError("TPC-H aggregate table metadata byte limit exceeded")
            dataset = Dataset(folder, max_manifest_bytes=min(MAX_TABLE_MANIFEST_BYTES, remaining))
            table_metadata_bytes += dataset._manifest_bytes
            if dataset._manifest_sha256 != item["manifest_sha256"]:
                raise ValueError("TPC-H table manifest hash mismatch")
            tm = dataset.manifest
            descriptor_count += len(tm["parts"])
            if descriptor_count > MAX_DATASET_PARTS:
                raise ValueError(f"TPC-H metadata part limit {MAX_DATASET_PARTS} exceeded")
            if (tm.get("complete") is not True or type(tm.get('tpch', {}).get('schema_version')) is not int or dataset.schema.to_dict() != table_schema(table, self.profile).to_dict() or
                    tm.get("tpch") != {"table": table, "schema_version": SCHEMA_VERSION,
                                       "type_profile": self.profile, "fields": logical_fields(table, self.profile)}):
                raise ValueError("TPC-H table schema/profile changed or table is incomplete")
            if (type(item["rows"]) is not int or type(item["bytes"]) is not int or
                    item["rows"] != tm["rows"] or item["bytes"] != tm["bytes"] or
                    sum(p["bytes"] for p in tm["parts"]) != tm["bytes"] or
                    tm["part_rows"] != m["part_rows"] or tm["part_bytes"] != m["part_bytes"] or
                    table in expected and tm["rows"] != expected[table]):
                raise ValueError("TPC-H table row/byte count or part limits mismatch")
            previous = -1
            partition_rows = {}
            for ordinal, part in enumerate(tm["parts"]):
                partition = part["generator_partition"]
                if (part["path"] != f"part-{ordinal:08d}.jsonl" or part["id"] != f"{table}:{ordinal:08d}" or
                        not _HASH.fullmatch(part["sha256"]) or (folder / part["path"]).is_symlink() or
                        part["rows"] > m["part_rows"] or part["bytes"] > m["part_bytes"] or
                        type(partition) is not int or partition < previous or
                        partition not in ([0] if table in ("region", "nation") else range(1, parts + 1))):
                    raise ValueError("invalid TPC-H part identity, partition, path or bounds")
                previous = partition
                partition_rows[partition] = partition_rows.get(partition, 0) + part["rows"]
            sources = tm["sources"]
            steps = [0] if table in ("region", "nation") else list(range(1, parts + 1))
            if (not isinstance(sources, list) or [s["generator_partition"] for s in sources] != steps or
                    sum(s["rows"] for s in sources) != tm["rows"] or
                    any(type(s["rows"]) is not int or s["rows"] <= 0 or type(s["bytes"]) is not int or
                        s["bytes"] <= 0 or not _HASH.fullmatch(s["sha256"]) or
                        type(s["generator_partition"]) is not int or
                        s["path"] != f"{table}.tbl" + (f".{s['generator_partition']}" if parts > 1 and s["generator_partition"] else "") or
                        partition_rows.get(s["generator_partition"], 0) != s["rows"]
                        for s in sources)):
                raise ValueError("invalid TPC-H source partition inventory")
            self.tables[table] = dataset
        if (type(m["rows"]) is not int or type(m["bytes"]) is not int or type(m["source_bytes"]) is not int or
                m["rows"] != sum(t.manifest["rows"] for t in self.tables.values()) or
                m["bytes"] != sum(t.manifest["bytes"] for t in self.tables.values()) or
                m["source_bytes"] != sum(s["bytes"] for t in self.tables.values() for s in t.manifest["sources"]) or
                not expected["orders"] <= self.tables["lineitem"].manifest["rows"] <= 7 * expected["orders"]):
            raise ValueError("TPC-H dataset row/byte totals mismatch")
        if m["dataset_id"] != _identity(m):
            raise ValueError("TPC-H dataset identity mismatch")

    def validate(self):
        """Recheck immutable manifests and decode/hash every bounded data part."""
        try:
            raw = _bounded_metadata(self.path / "manifest.json", MAX_TOP_MANIFEST_BYTES)
            if hashlib.sha256(raw).hexdigest() != self._manifest_sha256:
                raise ValueError("TPC-H dataset manifest changed after opening")
            self._check_manifest()
            for dataset in self.tables.values():
                for _ in dataset.iter_parts():
                    pass
        except OSError as exc:
            raise ValueError("TPC-H dataset part or manifest is missing or unreadable") from exc
