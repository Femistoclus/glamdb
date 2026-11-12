#!/usr/bin/env python3
"""Validate complete raw samples and export evidence without per-file metadata."""
import csv
import hashlib
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    raw = [json.loads(p.read_text()) for p in sorted((root / "results").glob("*.json"))]
    main_cases = [c for c in raw if "finished_utc" in c]
    supplements = [c for c in raw if isinstance(c.get("operation"), dict)]
    assert len(main_cases) == 20
    cases = main_cases + [{"fixture": c["fixture"], "operations": [c["operation"]],
                          "peak_rss_bytes": c["peak_rss_bytes"], "scope": c["scope"]} for c in supplements]
    compact = []
    rows = []
    full_values = set()
    paired = {}
    for case in cases:
        fixture = case["fixture"]
        assert fixture["logical_decoded_bytes"] == fixture["size_gib"] * 1024**3
        files = fixture["files"]
        assert sum(f["rows"] for f in files) == fixture["rows"]
        assert sum(f["decoded_bytes"] for f in files) == fixture["logical_decoded_bytes"]
        assert max(f["decoded_bytes"] for f in files) <= 8 * 1024**2
        assert max(f["physical_bytes"] for f in files) <= 8 * 1024**2
        if fixture["layout"] == "disjoint":
            assert all(a["hi"] < b["lo"] for a, b in zip(files, files[1:]))
        pair_id = (fixture["size_gib"], fixture["style"])
        paired.setdefault(pair_id, set()).add(fixture["logical_row_fingerprint"])
        short = {k: v for k, v in fixture.items() if k != "files"}
        short.update({"file_count": len(files),
                      "max_file_decoded_bytes": max(f["decoded_bytes"] for f in files),
                      "max_file_compressed_bytes": max(f["physical_bytes"] for f in files)})
        compact.append({**case, "fixture": short})
        for operation in case["operations"]:
            for sample in operation["samples"]:
                if sample["status"] in ["measured_component_rewrite", "measured_component_full_value_lookup"]:
                    assert sample["compressed_read_bytes"] + sample["compressed_written_bytes"] <= 8 * 1024**3
                    assert sample["seconds"] <= 60
                if sample["status"] == "measured_component_full_value_lookup":
                    assert sample["matching_rows"] == 1
                    assert sample["returned_full_row_decoded_bytes"] >= 2048
            if fixture["layout"] == "disjoint" and operation["operation"] == "upsert_1000" and operation["workload"] in ["ordered", "clustered"]:
                assert all(s["status"] == "measured_component_rewrite" for s in operation["samples"])
            if operation["operation"] == "point_hit_full_value":
                full_values.add((fixture["size_gib"], fixture["style"], fixture["layout"]))
            sample = operation["samples"][0]
            requested = sample.get("keys", sample.get("changed_keys", 1000))
            rows.append({"gib": fixture["size_gib"], "style": fixture["style"], "layout": fixture["layout"],
                         "operation": operation["operation"], "workload": operation["workload"],
                         "status_first_sample": sample["status"], "samples": len(operation["samples"]),
                         **{k: sample[k] for k in ["candidate_files", "read_files", "matching_files", "rewritten_files",
                             "compressed_read_bytes", "compressed_written_bytes", "decoded_read_bytes", "decoded_written_bytes"]},
                         "decoded_read_amplification_per_2048B_requested_row": sample["decoded_read_bytes"]/(requested*2048),
                         "decoded_write_amplification_per_2048B_requested_row": sample["decoded_written_bytes"]/(requested*2048),
                         "projected_rewritten_files": sample.get("projected_rewritten_files",0),
                         "projected_compressed_total_io_bytes": sample.get("projected_compressed_total_io_bytes",0),
                         **{k: operation["latency"][k] for k in ["p50_ms", "p95_ms", "p99_ms"]},
                         "peak_case_rss_bytes": case["peak_rss_bytes"]})
    assert len(paired) == 10 and all(len(v) == 1 for v in paired.values())
    assert len(full_values) == 20
    actual_hashes = {}
    originals = {(c["fixture"]["size_gib"],c["fixture"]["style"],c["fixture"]["layout"]):c["fixture"] for c in main_cases}
    for case in cases:
        f = case["fixture"]
        key = (f["size_gib"],f["style"],f["layout"])
        assert f["compressed_bytes"] == originals[key]["compressed_bytes"]
        if "physical_sample_row_hashes" in f:
            actual_hashes[key] = f["physical_sample_row_hashes"]
    assert len(actual_hashes) == 20
    for size,style in paired:
        assert actual_hashes[(size,style,"disjoint")] == actual_hashes[(size,style,"overlap")]
    (root / "measurements.json").write_text(json.dumps(compact,indent=2)+"\n")
    with (root / "summary.csv").open("w",newline="") as out:
        writer = csv.DictWriter(out,fieldnames=rows[0].keys())
        writer.writeheader(); writer.writerows(rows)
    verification = {"main_cases":20, "paired_deterministic_fixtures":10, "paired_physical_sample_hashes":10,
                    "full_value_lookup_cases":20,
                    "all_sample_checks":"PASS", "max_peak_rss_bytes":max(c["peak_rss_bytes"] for c in cases),
                    "source_sha256":{p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in [root/"layout_experiment.py", root/"payload_generator.cpp", Path(__file__)]}}
    (root / "verification.json").write_text(json.dumps(verification,indent=2)+"\n")
    print(json.dumps(verification))


if __name__ == "__main__":
    main()
