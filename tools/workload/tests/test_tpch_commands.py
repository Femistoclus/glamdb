"""TPC-H CLI boundary: arguments, adapter selection, and public exit codes."""

import json
import sys
from types import ModuleType

import pytest

import glamdb_workload
from glamdb_workload.__main__ import main


@pytest.fixture
def adapters(monkeypatch):
    """Replace only command adapters that invoke DBGEN or a remote server."""
    calls = []
    result = {"status": "PASS", "detail": "adapter result"}
    for module_name, functions in (
        ("tpch_dataset", ("generate",)),
        ("tpch_lifecycle", ("initialize", "load", "resolve", "verify", "status")),
        ("tpch_scan", ("run",)),
    ):
        module = ModuleType("glamdb_workload." + module_name)
        for name in functions:
            def call(*args, _name=name, _module=module_name, **kwargs):
                calls.append((_module + "." + _name, args, kwargs))
                return result.copy()

            setattr(module, name, call)
        monkeypatch.setitem(sys.modules, module.__name__, module)
        monkeypatch.setattr(glamdb_workload, module_name, module, raising=False)
    return calls, result


@pytest.mark.parametrize("arguments,adapter,positional,keywords", [
    (
        ["generate", "--scale-factor", "0.01", "--dbgen", "/tools/dbgen",
         "--type-profile", "native", "--output", "dataset with spaces"],
        "tpch_dataset.generate", (),
        {"scale_factor": "0.01", "output": "dataset with spaces", "dbgen": "/tools/dbgen",
         "type_profile": "native", "generator_parts": 1, "part_rows": 10000,
         "part_bytes": 8388608, "distributions": None, "dbgen_sha256": None},
    ),
    (
        ["init", "--url", "http://127.0.0.1:8080", "--dataset", "data",
         "--name", "tpch-run", "--state", "run.json"],
        "tpch_lifecycle.initialize", (),
        {"url": "http://127.0.0.1:8080", "dataset": "data", "name": "tpch-run",
         "state": "run.json", "limits": None, "table_prefix": "", "mode": None, "timeout": 30.0,
         "ca_file": None},
    ),
    (
        ["load", "--state", "run.json"], "tpch_lifecycle.load", ("run.json",),
        {"batch_rows": 10000, "batch_bytes": 8388608, "workers": 1, "resume": False,
         "timeout": 30.0, "ca_file": None},
    ),
    (
        ["resolve", "--state", "run.json"], "tpch_lifecycle.resolve", ("run.json",),
        {"retry": False, "timeout": 30.0, "ca_file": None},
    ),
    (
        ["verify", "--state", "run.json"], "tpch_lifecycle.verify", ("run.json",),
        {"format": "arrow", "level": "full", "timeout": 300.0, "ca_file": None},
    ),
    (
        ["scan", "--state", "run.json", "--report", "report"],
        "tpch_scan.run", ("run.json",),
        {"queries": "all", "mode": "scan", "format": "arrow", "consume": "batches",
         "users": 1, "processes": 1, "warmup": 1, "repeat": 5, "report": "report",
         "timeout": 300.0, "ca_file": None},
    ),
    (
        ["status", "--state", "run.json"], "tpch_lifecycle.status", ("run.json",),
        {"timeout": 30.0, "ca_file": None},
    ),
])
def test_tpch_command_selects_adapter_and_preserves_defaults(
    adapters, capsys, arguments, adapter, positional, keywords
):
    calls, result = adapters
    assert main(["bench", "tpch", *arguments]) == 0
    assert calls == [(adapter, positional, keywords)]
    output = capsys.readouterr()
    assert json.loads(output.out) == result
    assert output.err == ""


def test_generate_preserves_exact_scale_and_provenance_options(adapters, capsys):
    calls, _ = adapters
    assert main([
        "bench", "tpch", "generate", "--scale-factor", "0.0100", "--output", "data",
        "--dbgen", "dbgen with spaces", "--type-profile", "legacy-scaled",
        "--generator-parts", "4", "--part-rows", "9000", "--part-bytes", "7000000",
        "--distributions", "dists with spaces.dss", "--dbgen-sha256", "a" * 64,
    ]) == 0
    assert calls == [("tpch_dataset.generate", (), {
        "scale_factor": "0.0100", "output": "data", "dbgen": "dbgen with spaces",
        "type_profile": "legacy-scaled", "generator_parts": 4, "part_rows": 9000,
        "part_bytes": 7000000, "distributions": "dists with spaces.dss",
        "dbgen_sha256": "a" * 64,
    })]
    assert capsys.readouterr().err == ""


def test_init_reads_limits_file_before_creating_target(adapters, tmp_path, capsys):
    calls, _ = adapters
    limits = tmp_path / "limits with spaces.json"
    limits.write_text('{"max_collections":16,"max_operations":12000}')
    assert main([
        "bench", "tpch", "init", "--url", "https://example.org", "--dataset", "data",
        "--name", "tpch-run", "--state", "run.json", "--limits", str(limits),
        "--table-prefix", "variant_", "--timeout", "12.5", "--ca-file", "ca.pem",
    ]) == 0
    assert calls == [("tpch_lifecycle.initialize", (), {
        "url": "https://example.org", "dataset": "data", "name": "tpch-run",
        "state": "run.json", "limits": {"max_collections": 16, "max_operations": 12000},
        "table_prefix": "variant_", "mode": None, "timeout": 12.5, "ca_file": "ca.pem",
    })]
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("arguments,adapter,keywords", [
    (["load", "--batch-rows", "8000", "--batch-bytes", "6000000", "--workers", "4", "--resume"],
     "tpch_lifecycle.load", {"batch_rows": 8000, "batch_bytes": 6000000, "workers": 4, "resume": True}),
    (["resolve", "--retry"], "tpch_lifecycle.resolve", {"retry": True}),
    (["verify", "--format", "json", "--level", "rows"],
     "tpch_lifecycle.verify", {"format": "json", "level": "rows"}),
    (["scan", "--queries", "1,6", "--mode", "reduce", "--format", "json", "--consume", "rows",
      "--users", "7", "--processes", "3", "--warmup", "0", "--repeat", "2", "--report", "report"],
     "tpch_scan.run", {"queries": "1,6", "mode": "reduce", "format": "json", "consume": "rows",
                       "users": 7, "processes": 3, "warmup": 0, "repeat": 2, "report": "report"}),
    (["status"], "tpch_lifecycle.status", {}),
])
def test_state_commands_apply_requested_controls(adapters, capsys, arguments, adapter, keywords):
    calls, _ = adapters
    assert main(["bench", "tpch", *arguments, "--state", "run.json",
                 "--timeout", "9", "--ca-file", "authority.pem"]) == 0
    assert calls == [(adapter, ("run.json",), keywords | {"timeout": 9.0, "ca_file": "authority.pem"})]
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("status,code", [("PASS", 0), ("FAIL", 1), ("INCOMPLETE", 3)])
@pytest.mark.parametrize("command", ["init", "load", "resolve", "verify", "scan", "status"])
def test_lifecycle_status_controls_public_exit_code(adapters, capsys, command, status, code):
    _, result = adapters
    result["status"] = status
    extra = []
    if command == "init":
        extra = ["--url", "http://127.0.0.1:8080", "--dataset", "data", "--name", "run"]
    elif command == "scan":
        extra = ["--report", "report"]
    assert main(["bench", "tpch", command, "--state", "run.json", *extra]) == code
    assert json.loads(capsys.readouterr().out)["status"] == status


@pytest.mark.parametrize("body", ["[]", "{", '{"max_operations":NaN}',
                                  '{"max_operations":1,"max_operations":2}'])
def test_invalid_limits_cannot_create_a_target(adapters, tmp_path, capsys, body):
    calls, _ = adapters
    limits = tmp_path / "limits.json"
    limits.write_text(body)
    assert main(["bench", "tpch", "init", "--url", "http://127.0.0.1:8080", "--dataset", "data",
                 "--name", "run", "--state", "run.json", "--limits", str(limits)]) == 2
    assert calls == []
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("arguments", [
    ["generate", "--scale-factor", "1", "--dbgen", "dbgen", "--output", "data"],
    ["generate", "--scale-factor", "1", "--dbgen", "dbgen", "--output", "data",
     "--type-profile", "auto"],
    ["init", "--dataset", "data", "--name", "run", "--state", "run.json"],
    ["load", "--state", "run.json", "--workers", "0"],
    ["load", "--state", "run.json", "--batch-rows", "0"],
    ["scan", "--state", "run.json"],
    ["scan", "--state", "run.json", "--report", "report", "--repeat", "0"],
    ["scan", "--state", "run.json", "--report", "report", "--warmup", "-1"],
    ["scan", "--state", "run.json", "--report", "report", "--users", "0"],
    ["scan", "--state", "run.json", "--report", "report", "--processes", "0"],
    ["verify", "--state", "run.json", "--level", "unknown"],
    ["status", "--state", "run.json", "--timeout", "nan"],
    ["status", "--state", "run.json", "--timeout", "inf"],
    ["status", "--state", "run.json", "--timeout", "0"],
    ["load", "--state", "run.json", "--database", "another-target"],
    ["clean", "--state", "run.json", "--url", "http://127.0.0.1:2"],
])
def test_invalid_cli_arguments_stop_before_adapters(adapters, capsys, arguments):
    calls, _ = adapters
    with pytest.raises(SystemExit) as error:
        main(["bench", "tpch", *arguments])
    assert error.value.code == 2
    assert calls == []
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize('mode', ['append', 'kv'])
def test_init_accepts_explicit_collection_mode(adapters, mode):
    calls, _ = adapters
    assert main(['bench', 'tpch', 'init', '--url', 'http://127.0.0.1:8080',
                 '--dataset', 'data', '--name', 'run', '--state', 'run.json', '--mode', mode]) == 0
    assert calls[0][2]['mode'] == mode
