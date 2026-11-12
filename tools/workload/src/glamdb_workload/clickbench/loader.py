"""Locust schedules disjoint frozen writes under the engine's publication gate."""

import json
import time

from ..runtime import add_stage, failure_kind, latency_report, stage_report
from ..write_runtime import _locust_runtime, _send_once
from .model import save_json
from .plan import request_for
from .target import collection_mode


def execute_load(client, log, data, saved, path, *, timeout):
    import gevent
    from gevent.lock import Semaphore

    User, task, Environment, StopUser = _locust_runtime()
    workers = saved["append_plan"]["workers"]
    shared = {"next_worker": 0, "active": workers, "stopped": False, "stages": {}, "contention": 0.0}
    gate = Semaphore(1)

    class Loader(User):
        def on_start(self):
            self.index = shared["next_worker"]
            shared["next_worker"] += 1

        @task
        def publish(self):
            waiting = time.perf_counter()
            with gate:
                shared["contention"] += time.perf_counter() - waiting
                if shared["stopped"]:
                    raise StopUser()
                chunk = log.db.execute("SELECT c.* FROM clickbench_chunks c LEFT JOIN operations o USING(operation_id) "
                                       "WHERE o.operation_id IS NULL ORDER BY c.ordinal LIMIT 1").fetchone()
                if chunk is None:
                    raise StopUser()
                # Keep publication in source order even if scheduling is uneven.
                # Every worker owns disjoint ordinals; workers never replay a copy.
                if chunk["worker"] != self.index:
                    gevent.sleep(0)
                    return
                began = time.perf_counter()
                try:
                    current = client.get_collection(saved["collection_id"], deadline=timeout)
                    if current.get("current_snapshot_id") != saved["snapshot_id"]:
                        raise ValueError("foreign writes changed the benchmark target before publication")
                    request = request_for(data, saved, chunk, saved["snapshot_id"])
                    prepared = time.perf_counter()
                    if (request.payload_hash != chunk["payload_hash"] or
                            len(request.body) > saved["append_plan"]["batch_bytes"]):
                        raise ValueError("prepared request differs from its frozen source plan")
                    log.prepare(request)
                    journaled = time.perf_counter()
                    state = _send_once(client, log, request, deadline=timeout)
                    sent = time.perf_counter()
                    record = log.db.execute("SELECT outcome,error_code FROM operations WHERE operation_id=?",
                                            (request.operation_id,)).fetchone()
                    if state == "committed":
                        receipt = json.loads(record["outcome"])["result"]
                        saved["snapshot_id"] = receipt["snapshot_id"]
                        saved["confirmed_rows"] += chunk["rows"]
                        save_json(path, saved)
                    else:
                        shared["stopped"] = True
                    ended = time.perf_counter()
                    for name, seconds in {"sdk_prepare": prepared - began, "journal": journaled - prepared + ended - sent,
                                          "http_send": sent - journaled, "end_to_end": ended - began}.items():
                        add_stage(shared["stages"], name, seconds)
                    self.environment.events.request.fire(
                        request_type="SDK" if state == "committed" else failure_kind(record["error_code"] or state),
                        name="clickbench/" + ("upsert" if collection_mode(saved) == "kv" else "append"),
                        response_time=(sent - journaled) * 1000, response_length=len(request.body),
                        exception=None if state == "committed" else RuntimeError(state))
                except BaseException as error:
                    shared["stopped"] = True
                    shared["failure"] = error
                    raise StopUser() from error
            gevent.sleep(0)

        def on_stop(self):
            shared["active"] -= 1
            if shared["active"] == 0:
                gevent.spawn(self.environment.runner.quit)

    environment = Environment(user_classes=[Loader])
    runner = environment.create_local_runner()
    runner.start(workers, spawn_rate=workers)
    try:
        runner.greenlet.join()
    finally:
        runner.quit()
    if "failure" in shared:
        raise shared["failure"]
    return {"scheduler": "Locust", "workers": workers, "publication_concurrency": 1,
            "assignment": "global disjoint source ordinals modulo worker count",
            "writer_contention_seconds": shared["contention"],
            "timing_stages": stage_report(shared["stages"]), "latency": latency_report(environment.stats)}
