def test_pure_power_model_catches_all_deliberately_broken_variants(verify):
    result = verify.run_power_model(seed=17, seeded=100)
    assert result["verification_status"] == "PASS"
    assert result["evidence_kind"] == "modeled-power"
    assert result["power_durability"] == "NOT VALIDATED"
    assert result["acceptance_gate"] == "OPEN"
    assert result["cycles_checked"] >= 200
    assert set(result["broken_variants_detected"]) == {"unsynced-data", "unsynced-directory", "early-ack"}
    assert all(result["counterexamples"][variant] for variant in result["broken_variants_detected"])
    assert result["failures"] == []
    assert result == verify.run_power_model(seed=17, seeded=100)


def test_power_model_seed_changes_campaign_but_keeps_boundaries(verify):
    first = verify.run_power_model(seed=17, seeded=1)
    second = verify.run_power_model(seed=18, seeded=1)
    assert first["schedule"] != second["schedule"]
    assert first["cycles_checked"] == second["cycles_checked"] == 105
