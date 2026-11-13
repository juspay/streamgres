"""HTTP checks against the isolated container showcase; standard library only."""

import argparse
import json
import subprocess
import time
import urllib.request
import urllib.error

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("base", help="URL of a disposable ticket-desk container stack")
parser.add_argument("--runtime", choices=["docker", "podman"])
parser.add_argument("--app", help="container ID/name for lifecycle checks")
parser.add_argument("--postgres", help="container ID/name for lifecycle checks")
args = parser.parse_args()
if any([args.runtime, args.app, args.postgres]) and not all(
    [args.runtime, args.app, args.postgres]
):
    parser.error("--runtime, --app and --postgres must be supplied together")
base = args.base.rstrip("/")
record = {}


def call(path, data=None):
    payload = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(
        base + path,
        data=payload,
        headers={"Content-Type": "application/json"} if payload else {},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.load(response)


def check(ok, label):
    if not ok:
        raise AssertionError(label)
    print("PASS", label, flush=True)


for attempt in range(90):
    try:
        metrics = call("/api/metrics")
        break
    except (OSError, ValueError):
        time.sleep(1)
else:
    raise RuntimeError("container service did not become ready")
check(metrics["cache"]["running"], "container cache ready")
check(metrics["lab"] is False, "fault experiments disabled by default")
call(
    "/api/writer",
    {
        "enabled": False,
        "rate": 8,
        "operations": [
            "status",
            "comment",
            "insert",
            "delete",
            "move",
            "rename",
            "assign",
            "bulk",
            "rollback",
            "toast",
        ],
    },
)
shapes = call("/api/lookups")["shapes"]
check(len(shapes) == 23, "all 23 workload shapes available")
for shape in shapes:
    check(
        not shape.get("error") and shape["served"] == shape["expected"],
        "sync verdict " + shape["name"],
    )
    for adapter in ["sync", "async"]:
        compared = call("/api/compare/" + shape["name"] + "?adapter=" + adapter)
        check(
            compared["equal"], adapter + " results equal PostgreSQL: " + shape["name"]
        )
created = call(
    "/tickets?wait=true",
    {"org_id": 1, "title": "Container persistence probe", "priority": "high"},
)
id = created["id"]
record["ticket_id"] = id
check(
    created["committed"] and created["caught_up"],
    "container write committed and barrier completed",
)
changed = call(f"/tickets/{id}/status?wait=true", {"status": "pending"})
check(changed["observed"]["data"][0]["status"] == "pending", "committed status visible")
comment = call(
    f"/tickets/{id}/comments?wait=true", {"body": "Container comment persisted"}
)
check(
    comment["committed"] and comment["caught_up"], "container comment barrier completed"
)
check(
    any(
        row["body"] == "Container comment persisted"
        for row in call(f"/tickets/{id}/comments")["data"]
    ),
    "comment readback",
)
verify = call("/api/verify?adapter=async", {})
check(
    verify["mismatches"] == 0 and verify["errors"] == 0,
    "async verification has no mismatches or read errors",
)
load = call(
    "/api/load", {"name": "ticket", "adapter": "async", "concurrency": 2, "seconds": 1}
)
check(
    load["cached"]["requests"] > 0
    and load["cached"]["errors"] == 0
    and load["cached"]["memory"] > 0
    and load["postgres"]["requests"] > 0
    and load["postgres"]["errors"] == 0,
    "both container routes complete load test without errors",
)
try:
    call("/api/lab/connection", {})
    raise AssertionError("lab fault unexpectedly allowed")
except urllib.error.HTTPError as error:
    check(error.code == 400, "safe-mode fault route refuses mutation")
call(
    "/api/writer",
    {
        "enabled": True,
        "rate": 8,
        "operations": ["status", "comment", "bulk", "rollback", "assign", "rename"],
    },
)
for _ in range(3):
    verification = call("/api/verify", {})
    check(
        verification["mismatches"] == 0 and verification["errors"] == 0,
        "verification while writer active",
    )
call(
    "/api/writer",
    {
        "enabled": False,
        "rate": 8,
        "operations": ["status", "comment", "bulk", "rollback", "assign", "rename"],
    },
)
print("RECORD " + json.dumps(record), flush=True)

if args.runtime:

    def run(*parts):
        return subprocess.check_output(
            [args.runtime, *parts], text=True, timeout=45
        ).strip()

    def slots():
        return int(
            run(
                "exec",
                args.postgres,
                "psql",
                "-U",
                "postgres",
                "-d",
                "ticketdesk",
                "-Atc",
                "SELECT count(*) FROM pg_replication_slots WHERE database=current_database() AND NOT temporary",
            )
        )

    def ready():
        for _ in range(90):
            try:
                m = call("/api/metrics")
                if m["cache"]["running"] and m["cache"]["lag_ms"] is not None:
                    row = call(f"/tickets/{id}?route=postgres")
                    if row["data"][0]["title"] == "Container persistence probe":
                        for adapter in ["sync", "async"]:
                            check_row = call(f"/tickets/{id}?adapter={adapter}")
                            if check_row["data"][0]["id"] != id:
                                raise AssertionError("wrong persisted row")
                        return row
            except (OSError, ValueError, KeyError, IndexError):
                pass
            time.sleep(1)
        raise AssertionError("service did not recover after restart")

    check(
        run("exec", args.app, "id", "-u") == "10001", "runtime uses non-root UID 10001"
    )
    check(
        run(
            "exec",
            args.app,
            "sh",
            "-c",
            "test -x /usr/local/bin/demo && test ! -d /src && ! command -v cargo && echo lean",
        )
        == "lean",
        "runtime contains executable without builder sources or Cargo",
    )
    check(slots() == 1, "one persistent replication slot while running")
    stream = urllib.request.urlopen(base + "/api/events", timeout=10)
    started = time.monotonic()
    try:
        run("stop", "--time", "20", args.app)
    finally:
        stream.close()
    check(
        run("inspect", "--format", "{{.State.ExitCode}}", args.app) == "0",
        "SIGTERM shutdown exits cleanly with activity stream open",
    )
    check(time.monotonic() - started < 20, "shutdown completes within grace period")
    check(slots() == 0, "shutdown releases replication slot")
    run("start", args.app)
    ready()
    check(
        call(f"/tickets/{id}")["data"][0]["status"] == "pending",
        "app restart preserves created ticket and status",
    )
    check(
        any(
            row["body"] == "Container comment persisted"
            for row in call(f"/tickets/{id}/comments")["data"]
        ),
        "app restart preserves comment",
    )
    check(slots() == 1, "app restart has exactly one slot")
    run("restart", "--time", "20", args.postgres)
    ready()
    compared = call("/api/compare/ticket?adapter=async")
    check(compared["equal"], "async comparisons recover after PostgreSQL restart")
    check(
        call(f"/tickets/{id}?route=postgres")["data"][0]["title"]
        == "Container persistence probe",
        "PostgreSQL volume preserves ticket across restart",
    )
    check(slots() == 1, "PostgreSQL restart does not leak replication slots")
