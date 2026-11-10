"""Small independent ordering model, not a POSIX/SQLite implementation emulator.

    The cut discards every volatile data and namespace change. Catalog commit is
    modeled as an atomic durability primitive; real SQLite sector/journal faults,
    OS cache loss, firmware and hardware flush behavior are outside this model.
"""

from .evidence import fault_schedule

_CLASSES = ("data", "manifest", "manifest_list", "root")
_BROKEN = ("unsynced-data", "unsynced-directory", "early-ack")


def _simulate(cut_point, points, variant=None):
    volatile_data, durable_data = {}, {}
    volatile_names, durable_names, durable_ancestors = set(), set(), set()
    expected = {kind: f"synthetic-{kind}-body-footer".encode("ascii") for kind in _CLASSES}
    committed = acked = False

    def cut():
        # Only durable data AND durable namespace closure can survive one cut.
        recovered = {name: durable_data[name] for name in durable_names & durable_ancestors if name in durable_data}
        errors = []
        if committed:
            for name, value in expected.items():
                if recovered.get(name) != value:
                    errors.append(f"published {name} is missing or incomplete after cut")
        if acked and not committed:
            errors.append("ACKed operation missing from durable catalog")
        return errors

    for point in points:
        stage, side = point.rsplit(".", 1)
        if side == "after":
            component, _, step = stage.partition(".")
            if component in _CLASSES:
                if step == "create":
                    volatile_names.add(component)
                    volatile_data[component] = b""
                elif step == "write":
                    volatile_data[component] = expected[component][:-7]
                    if variant == "early-ack" and component == "data":
                        acked = True
                elif step == "footer":
                    volatile_data[component] = expected[component]
                elif step == "file_sync" and not (variant == "unsynced-data" and component == "data"):
                    durable_data[component] = volatile_data[component]
                elif step == "directory_sync" and not (variant == "unsynced-directory" and component == "data"):
                    durable_names.update(volatile_names & {component})
                elif step == "ancestor_directory_sync":
                    durable_ancestors.add(component)
            elif stage == "sqlite.outer_commit":
                committed = True
            elif stage == "ack":
                acked = True
        if point == cut_point:
            return cut()
    raise ValueError("model cut point not present in event stream")


def run_power_model(*, seed=17, seeded=100) -> dict:
    """Replay boundaries and seeded cuts; require three known bad designs to fail."""
    schedule = fault_schedule(seed, seeded=seeded)
    points = [entry["cut_point"] for entry in schedule if entry["kind"] == "boundary"]
    failures, counterexamples = [], {}
    for entry in schedule:
        errors = _simulate(entry["cut_point"], points)
        if errors:
            failures.append({"schedule_id": entry["schedule_id"], "cut_point": entry["cut_point"], "errors": errors})
    for variant in _BROKEN:
        for point in points:
            errors = _simulate(point, points, variant)
            if errors:
                counterexamples[variant] = {"cut_point": point, "errors": errors}
                break
        if variant not in counterexamples:
            failures.append({"variant": variant, "errors": ["deliberately broken variant escaped detection"]})
    return {
        "evidence_kind": "modeled-power", "verification_status": "FAIL" if failures else "PASS",
        "power_durability": "NOT VALIDATED", "acceptance_gate": "OPEN",
        "cycles_checked": len(schedule), "schedule": schedule, "failures": failures,
        "broken_variants_detected": list(counterexamples), "counterexamples": counterexamples,
        "model_limitations": ["Catalog commit is an atomic durable primitive; SQLite page/journal recovery is not emulated.", "No process kill, real POSIX fault injection or hardware power cut was executed.", "This helper does not close T-POWER, T-FAULTS or a release gate."],
    }
