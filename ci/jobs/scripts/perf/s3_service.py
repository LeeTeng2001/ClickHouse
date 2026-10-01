"""Job-local S3 endpoint: isolated write namespaces and one shared read-only dataset."""

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
READ_DATASET_DATABASE = "tpch_ice10_s3"
READ_DATASET_DIRECTORY = "tpch_ice_sf10"

# Fixed conventions of setup_seaweedfs.sh, shared with the stateless suite.
# TODO: parameterize port/bucket/credentials when the script reuse is cleaned up.
S3_PORT = 11111
S3_BUCKET = "test"
S3_ACCESS_KEY = "clickhouse"
S3_SECRET_KEY = "clickhouse"


def endpoint_url(namespace):
    """One namespace of the shared store: left/right for writes, data for immutable reads."""
    return f"http://localhost:{S3_PORT}/{S3_BUCKET}/perf/{namespace}/"


def test_requires_s3(test_path):
    """Whether a test uses the S3 endpoint, directly or through pre-attached tables."""
    # TODO: parse the XML instead of a substring scan.
    try:
        with open(test_path, "r", encoding="utf-8") as f:
            content = f.read()
            return COLLECTION in content or READ_DATASET_DATABASE in content
    except OSError:
        # Unreadable files still reach perf.py, which reports the real error.
        return False


def test_requires_read_dataset(test_path):
    """The read and TPC-H suites refer to tables attached before the servers start."""
    try:
        with open(test_path, "r", encoding="utf-8") as f:
            return READ_DATASET_DATABASE in f.read()
    except OSError:
        return False


def _client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{S3_PORT}",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def seed_read_dataset(dataset_dir):
    """Upload the downloaded Iceberg dataset once; both servers read this S3 prefix."""
    from botocore.exceptions import ClientError

    source = Path(dataset_dir)
    if not source.is_dir():
        raise RuntimeError(f"S3 read dataset is missing: {source}")
    files = sorted(path for path in source.rglob("*") if path.is_file())
    if not files:
        raise RuntimeError(f"S3 read dataset is empty: {source}")

    s3 = _client()
    uploaded = 0
    for path in files:
        key = f"perf/data/{READ_DATASET_DIRECTORY}/{path.relative_to(source).as_posix()}"
        size = path.stat().st_size
        try:
            existing_size = s3.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"]
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
            existing_size = None
        if existing_size != size:
            s3.upload_file(str(path), S3_BUCKET, key)
            uploaded += 1
            if s3.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"] != size:
                raise RuntimeError(f"S3 read dataset upload has wrong size: {key}")
    print(f"s3_service: shared read dataset verified ({len(files)} objects, {uploaded} uploaded)")
    return True


def clear_dummy_dataset():
    """Discard orphaned objects from an interrupted run of the fixed-path smoke test."""
    s3 = _client()
    for side in ("left", "right"):
        prefix = f"perf/{side}/iceberg_suite_s3_dummy_events/"
        while True:
            objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("Contents", [])
            if not objects:
                break
            result = s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects], "Quiet": True},
            )
            if result.get("Errors"):
                raise RuntimeError(f"Failed to clear S3 smoke dataset {prefix}: {result['Errors']}")
    print("s3_service: cleared prior smoke dataset in both write namespaces")
    return True


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
    # weed server drains its embedded volume server on SIGTERM (the volume
    # pre-stop grace period alone defaults to 10s). Five seconds reports a
    # spurious failure even though the daemon exits normally soon afterwards.
    for _ in range(120):
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
