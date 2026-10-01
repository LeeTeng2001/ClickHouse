"""Job-local S3 endpoint for performance tests: one shared server, one namespace per measured ClickHouse server."""

import os
import signal
import socket
import subprocess
import time
from pathlib import Path

from ci.praktika.utils import Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"

# Reused untouched; in CI its `weed` binary is baked into the performance-comparison image.
SETUP_SCRIPT = "./ci/jobs/scripts/functional_tests/setup_seaweedfs.sh"
SETUP_SCRIPT_TIMEOUT_SEC = 240

# Referencing this named collection is what marks a test as needing the endpoint.
COLLECTION = "perf_s3"

# Fixed conventions of setup_seaweedfs.sh, shared with the stateless suite.
# TODO: parameterize port/bucket/credentials when the script reuse is cleaned up.
S3_PORT = 11111
S3_BUCKET = "test"
S3_ACCESS_KEY = "clickhouse"
S3_SECRET_KEY = "clickhouse"


def endpoint_url(namespace):
    """One namespace (key prefix) of the shared store: `left`/`right` isolate writes, `local` serves single-server runs."""
    # TODO: when S3 read datasets arrive, add a side-independent collection (e.g. perf_s3_data) seeded once per job.
    return f"http://localhost:{S3_PORT}/{S3_BUCKET}/perf/{namespace}/"


def test_requires_s3(test_path):
    """Whether a performance-test file uses the job-local S3 endpoint (content-based, not file naming)."""
    # TODO: parse the XML instead of a substring scan.
    try:
        with open(test_path, "r", encoding="utf-8") as f:
            return COLLECTION in f.read()
    except OSError:
        # Unreadable files still reach perf.py, which reports the real error.
        return False


def _is_healthy():
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    try:
        return subprocess.run(
            [
                "aws",
                "--endpoint-url",
                f"http://localhost:{S3_PORT}",
                "s3",
                "ls",
                f"s3://{S3_BUCKET}",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        ).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _port_occupied():
    try:
        with socket.create_connection(("localhost", S3_PORT), timeout=1):
            return True
    except OSError:
        return False


def _owned_pid():
    try:
        pid = int((Path(temp_dir) / "seaweedfs.pid").read_text().strip())
        command = subprocess.check_output(
            ["ps", "-ww", "-p", str(pid), "-o", "args="], text=True
        )
        if "weed server " in command and f"-dir={temp_dir}/seaweedfs_data" in command:
            return pid
    except (OSError, ValueError, subprocess.CalledProcessError):
        pass
    return None


def ensure(log_path):
    """Bring up the job-local S3 endpoint; idempotent and fail-close."""
    if _port_occupied() and _is_healthy():
        if _owned_pid() is None:
            print(f"s3_service: port {S3_PORT} has a healthy but unowned S3 endpoint")
            return False
        # Reuse keeps stage re-entry (praktika --param) working without re-provisioning.
        # TODO: a reused daemon (and data dir) serves the previous run's objects; add a reset/seed marker and scratch cleanup between test files.
        print(f"s3_service: reusing the healthy S3 endpoint on localhost:{S3_PORT}")
        return True
    if _owned_pid() is not None:
        stop()
    elif _port_occupied():
        print(f"s3_service: port {S3_PORT} is occupied by an unowned endpoint")
        return False
    for _ in range(20):
        if not _port_occupied():
            break
        time.sleep(0.25)
    if _port_occupied():
        print(f"s3_service: port {S3_PORT} is occupied by an unusable endpoint")
        return False

    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    print(f"s3_service: starting the S3 endpoint via {SETUP_SCRIPT}")
    with open(log_path, "w", encoding="utf-8") as log:
        # `stateful` provisions the server and the bucket only: no anonymous identity, no data upload.
        # The nohup'd daemon inherits the log fd, so its output lands in log_path after the script exits.
        proc = subprocess.Popen(
            [SETUP_SCRIPT, "stateful", "./tests"],
            stdout=log,
            stderr=subprocess.STDOUT,
            env={
                **os.environ,
                "TEMP_DIR": temp_dir,
                "SEAWEEDFS_PID_FILE": f"{temp_dir}/seaweedfs.pid",
            },
        )
    try:
        returncode = proc.wait(timeout=SETUP_SCRIPT_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        proc.kill()
        print(
            f"s3_service: {SETUP_SCRIPT} did not finish in {SETUP_SCRIPT_TIMEOUT_SEC}s"
        )
        _print_log_tail(log_path)
        return False
    if returncode != 0:
        print(f"s3_service: {SETUP_SCRIPT} exited with [{returncode}]")
        _print_log_tail(log_path)
        return False
    if _owned_pid() is None or not _is_healthy():
        print("s3_service: endpoint is not healthy after a successful setup")
        _print_log_tail(log_path)
        return False
    return True


def write_side_override(config_dir, side):
    """Point one server's `perf_s3` collection at its own namespace - the only per-server config delta."""
    # A config *file*, so compare.sh::restart (confirm_changes) starts the servers with it too.
    path = f"{config_dir}/config.d/zzz-perf-s3-side-override.xml"
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            f"""<!-- Generated by ci/jobs/scripts/perf/s3_service.py: this server's own namespace of the shared object store. -->
<clickhouse>
    <named_collections>
        <{COLLECTION}>
            <url replace="replace">{endpoint_url(side)}</url>
        </{COLLECTION}>
    </named_collections>
</clickhouse>
"""
        )
    print(f"{path}: {COLLECTION} url set to {endpoint_url(side)}")
    return True


def stop():
    """Stop the job-local S3 daemon (best effort)."""
    pid = _owned_pid()
    if pid is None:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError as error:
        print(f"s3_service: could not stop owned daemon {pid}: {error}")
        return
    for _ in range(20):
        if not _port_occupied():
            (Path(temp_dir) / "seaweedfs.pid").unlink(missing_ok=True)
            return
        time.sleep(0.25)
    print(f"s3_service: port {S3_PORT} remains occupied after stopping daemon {pid}")


def _print_log_tail(log_path):
    try:
        subprocess.run(["tail", "-n", "50", log_path], check=False)
    except OSError as error:
        print(f"s3_service: could not read {log_path}: {error}")
