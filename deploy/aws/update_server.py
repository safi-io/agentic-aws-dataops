"""Install a source release on the cloud server; retain the prior release for rollback."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile

import boto3
from botocore.config import Config
from dotenv import dotenv_values


SERVICES = ["dataops-web", "dataops-daemon", "dataops-dashboard"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()
    os.environ.update(json.loads(Path("/etc/dataops/deployment.json").read_text()))
    os.environ.update({k: v for k, v in dotenv_values("/etc/dataops/runtime.env").items() if v is not None})
    from dagster import DagsterInstance, DagsterRunStatus, RunsFilter
    with DagsterInstance.get() as instance:
        active = instance.get_runs(filters=RunsFilter(statuses=[
            DagsterRunStatus.STARTING, DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING,
        ]), limit=1)
    if active:
        raise RuntimeError("A Dagster run is active. Retry this update when it finishes.")
    release_name = Path(args.key).name.removesuffix(".tar.gz")
    if len(release_name) != 64 or any(c not in "0123456789abcdef" for c in release_name):
        raise ValueError("Expected the SHA-256 source archive name.")
    release = Path("/opt/dataops/releases") / release_name
    archive_path = release.with_suffix(".tar.gz")
    s3 = boto3.client("s3", config=Config(retries={"mode": "standard", "total_max_attempts": 4}))
    s3.download_file(args.bucket, args.key, str(archive_path))
    if hashlib.sha256(archive_path.read_bytes()).hexdigest() != release_name:
        raise ValueError("Source archive checksum mismatch.")
    release.mkdir(exist_ok=True)
    with tarfile.open(archive_path) as archive:
        archive.extractall(release, filter="data")
    current = Path("/opt/dataops/app")
    previous = current.resolve()
    subprocess.run(["systemctl", "stop", *SERVICES], check=True)
    try:
        subprocess.run([
            "/opt/dataops/venv/bin/pip", "install", "--no-cache-dir",
            "-r", str(release / "deploy/aws/requirements.txt"),
        ], check=True)
        next_link = Path("/opt/dataops/app.next")
        next_link.unlink(missing_ok=True)
        next_link.symlink_to(release, target_is_directory=True)
        next_link.replace(current)
        subprocess.run([
            "/opt/dataops/venv/bin/python",
            "/opt/dataops/app/deploy/aws/configure_server.py",
        ], check=True)
        Path("/opt/dataops/previous-release").write_text(str(previous))
    except Exception:
        current.unlink()
        current.symlink_to(previous, target_is_directory=True)
        raise
    finally:
        subprocess.run(["systemctl", "start", *SERVICES], check=True)
    subprocess.run(["systemctl", "reload", "caddy"], check=True)
    print(f"Activated release {release_name}.")


if __name__ == "__main__":
    main()
