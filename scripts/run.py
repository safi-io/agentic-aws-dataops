"""Launch this project's apps with an explicit environment and durable run history."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

from dotenv import dotenv_values
from pydantic import ValidationError


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Settings


def project_path(value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=("dashboard", "dagster", "job"))
    parser.add_argument("--env-file", default=os.environ.get("DATAOPS_ENV_FILE", ".env"))
    parser.add_argument("--port", type=int, help="Override the dashboard or Dagster port")
    parser.add_argument("--job", default="orders_data_quality_job")
    parser.add_argument("--config", help="Dagster job run configuration YAML")
    args = parser.parse_args()

    env_file = project_path(args.env_file)
    if not env_file.is_file():
        parser.error(f"Environment file not found: {env_file}")
    child_env = os.environ.copy()
    child_env.update(
        {key: value for key, value in dotenv_values(env_file).items() if value is not None}
    )
    child_env["DATAOPS_ENV_FILE"] = str(env_file)
    child_env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(PROJECT_ROOT), child_env.get("PYTHONPATH")) if part
    )

    # Validate the same values the child will receive without printing secrets.
    values = {key: child_env[key] for key in Settings.model_fields if key in child_env}
    try:
        Settings(_env_file=env_file, **values)
    except ValidationError as exc:
        for error in exc.errors(include_input=False, include_url=False):
            field = ".".join(str(part) for part in error["loc"]) or "configuration"
            print(f"Invalid {field}: {error['msg']}", file=sys.stderr)
        return 2

    if args.service == "dashboard":
        command = [
            sys.executable, "-m", "streamlit", "run",
            str(PROJECT_ROOT / "dashboard" / "app.py"),
            "--server.address", "127.0.0.1",
            "--server.port", str(args.port or 8501),
            "--server.headless", "true",
        ]
        working_directory = PROJECT_ROOT
    else:
        dagster_home = project_path(child_env.get("DAGSTER_HOME", ".dagster_home"))
        dagster_home.mkdir(parents=True, exist_ok=True)
        instance_config = dagster_home / "dagster.yaml"
        if not instance_config.exists():
            instance_config.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
        child_env["DAGSTER_HOME"] = str(dagster_home)
        child_env["PYTHONLEGACYWINDOWSSTDIO"] = "1"
        # Dagster automatically injects CWD/.env into its subprocesses. Run from
        # its storage directory so the local .env cannot replace the selected file.
        working_directory = dagster_home
        if args.service == "dagster":
            command = [
                sys.executable, "-m", "dagster", "dev",
                "--workspace", str(PROJECT_ROOT / "workspace.yaml"),
                "--host", "127.0.0.1", "--port", str(args.port or 3000),
            ]
        else:
            command = [
                sys.executable, "-m", "dagster", "job", "execute",
                "--module-name", "dagster_project", "--attribute", "defs",
                "--working-directory", str(PROJECT_ROOT), "--job", args.job,
            ]
            if args.config:
                config_path = project_path(args.config)
                if not config_path.is_file():
                    parser.error(f"Run configuration not found: {config_path}")
                command.extend(["--config", str(config_path)])

    print(f"Starting {args.service} using {env_file.name}", flush=True)
    try:
        return subprocess.call(command, cwd=working_directory, env=child_env)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
